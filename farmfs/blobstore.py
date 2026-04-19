from farmfs.fs import (
    Path,
    ensure_link,
    ensure_readonly,
    ensure_immutable_readable,
    ensure_dir,
    ftype_selector,
    FILE,
    is_readonly,
    is_user_readable,
    walk,
    walk_from,
    walk_path,
)
import http.client
from http.client import HTTPResponse
import json
import logging
from contextlib import contextmanager
from collections.abc import Callable
from os.path import sep
import re
from typing import ContextManager, IO, Generator, Iterator, List, Optional, Tuple
from urllib.parse import urlparse
from s3lib import Connection as s3conn, ConnectionLifecycleError, LIST_BUCKET_KEY
from farmfs.util import (
    copyfileobj,
    fmap,
    HandleThunk,
    pipeline,
    Readable,
    withHandles2,
)

logger = logging.getLogger(__name__)

_sep_replace_ = re.compile(sep)


def _remove_sep_(path: str) -> str:
    return _sep_replace_.subn("", path)[0]


ReverserFunction = Callable[[str | Path], str]

def fast_reverser(num_segs=3) -> ReverserFunction:
    total_chars = 32
    chars_per_seg = 3
    r = re.compile(
        ("/([0-9a-f]{%d})" % chars_per_seg) * num_segs
        + "/([0-9a-f]{%d})$" % (total_chars - chars_per_seg * num_segs)
    )

    def checksum_from_link_fast(link: str | Path) -> str:
        m = r.search(str(link))
        if m:
            csum = "".join(m.groups())
            return csum
        else:
            raise ValueError("link %s checksum didn't parse" % (link))

    return checksum_from_link_fast


# TODO we should remove references to vol.bs.reverser, as thats leaking format
# information into the volume.
def old_reverser(num_segs=3) -> ReverserFunction:
    """
    Returns a function which takes Paths into the user data and returns blob ids.
    """
    r = re.compile("((/([0-9]|[a-f])+){%d})$" % (num_segs + 1))

    def checksum_from_link(link: str | Path) -> str:
        """Takes a path into the userdata, returns the matching blob id."""
        m = r.search(str(link))
        if m:
            csum_slash = m.group()[1:]
            csum = _remove_sep_(csum_slash)
            return csum
        else:
            raise ValueError("link %s checksum didn't parse" % (link))

    return checksum_from_link


reverser = fast_reverser


def _checksum_to_path(checksum: str, num_segs=3, seg_len=3) -> str:
    segs = [
        checksum[i: i + seg_len]
        for i in range(0, min(len(checksum), seg_len * num_segs), seg_len)
    ]
    segs.append(checksum[num_segs * seg_len:])
    return sep.join(segs)


class Blobstore:
    def __init__(self):
        raise NotImplementedError()


class LifecycleError(Exception):
    pass
class FileBlobstoreSession:
    """
    A wrapper around file handles providing
    blobstore semantics using file handles and lifecycle managment.

    Enforces lifecycle for the active file handle so we can catch
    resource managemnent errors.
    """

    def __init__(self, root: Path, tmp_dir: Path):
        self._root = root
        self._fd: Optional[IO[bytes]] = None
        self._tmp_dir = tmp_dir

    def __enter__(self) -> 'FileBlobstoreSession':
        if self._fd is not None:
            raise LifecycleError("Entering session with open handle")
        return self

    def __exit__(self, *_) -> None:
        if self._fd is not None:
            raise LifecycleError("Exiting session with open handle")

    def _key(self, blob: str) -> Path:
        return Path(_checksum_to_path(blob), self._root)

    @contextmanager
    def _tracked(self, handle: IO[bytes]) -> Generator[IO[bytes], None, None]:
        if self._fd is not None:
            raise LifecycleError("FileBlobstoreSession: handle already acquired")
        if handle.closed:
            raise LifecycleError("Handle closed when acquired.")
        self._fd = handle
        try:
            with handle:
                yield handle
        finally:
            self._fd = None

    def read_handle(self, blob: str) -> ContextManager[Readable[bytes]]:
        """Returns a read handle to the blob's contents."""
        path = self._key(blob)
        return self._tracked(path.open("rb"))

    def _write_handle(self, dst_path: Path) -> HandleThunk[IO[bytes]]:
        def _write_handle_thunk() -> ContextManager[IO[bytes]]:
            return self._tracked(dst_path.safeopen("wb", lambda _: self._tmp_dir))
        return _write_handle_thunk

    # TODO force should exist all all blobstore types.
    def import_via_fd(self, getSrcHandle: HandleThunk[Readable[bytes]], blob: str, force=False) -> bool:
        """
        Imports a new file to the blobstore via copy.
        getSrcHandle is a function which returns a read handle to copy from.
        blob is the blob's id.
        While file is first copied to local temporary storage, then moved to
        the blobstore idepotently.
        """
        dst_path = self._key(blob)
        duplicate = dst_path.exists()
        if force or not duplicate:
            parent = dst_path.parent()
            assert parent is not None, "blob path cannot be root"
            ensure_dir(parent)
            withHandles2(getSrcHandle, self._write_handle(dst_path), copyfileobj)
            ensure_readonly(dst_path)
        # TODO do we want to return duplicate or "we imported"?
        return duplicate


class FileBlobstore:
    def __init__(self, root: Path, tmp_dir: Path, num_segs=3):
        self.root = root
        self.tmp_dir = tmp_dir
        self.reverser = reverser(num_segs)
        self.tmp_dir = tmp_dir

    def _blob_id_to_name(self, blob: str) -> str:
        """Return string name of link relative to root"""
        # TODO someday when blob checksums are parameterized
        # we inject the has params here.
        return _checksum_to_path(blob)

    def blob_path(self, blob: str) -> Path:
        """Return absolute Path to a blob given a blob id."""
        return Path(self._blob_id_to_name(blob), self.root)

    def get_blob_csum(self, path: Path) -> Optional[str]:
        """Return the blob checksum if path is structurally inside this blobstore, else None."""
        if self.root not in path.parents():
            return None
        try:
            return self.reverser(path)
        except ValueError:
            return None

    def exists(self, blob: str) -> bool:
        blob_path = self.blob_path(blob)
        return blob_path.exists()

    def delete_blob(self, blob: str) -> None:
        """Takes a blob, and removes it from the blobstore"""
        blob_path = self.blob_path(blob)
        blob_path.unlink(clean=self.root)

    def import_via_link(self, tree_path: Path, blob: str) -> bool:
        """Adds a file to a blobstore via a hard link."""
        blob_path = self.blob_path(blob)
        duplicate = blob_path.exists()
        if not duplicate:
            ensure_link(blob_path, tree_path)
            ensure_readonly(blob_path)
        return duplicate

    def session(self) -> FileBlobstoreSession:
        """
        Return a session context manager. FileBlobstore has no connection to
        manage, so the session is the blobstore itself wrapped in a nullcontext.
        """
        return FileBlobstoreSession(self.root, self.tmp_dir)

    def blobs(self, start_after: Optional[str] = None, max_items: Optional[int] = None) -> Iterator[str]:
        """Iterator across all blobs in sorted order.

        start_after -- if given, return only blobs strictly after this checksum
                       (exclusive, matching S3 semantics). Seeks directly into
                       the right directory subtree via walk_from() then skips
                       the start_after blob itself if present.
        max_items   -- if given, return at most this many blobs.
        """
        import itertools
        keep_files = ftype_selector([FILE])

        if start_after is not None:
            walker = walk_from(self.root, self.blob_path(start_after))
        else:
            walker = walk(self.root)

        blobs: Iterator[str] = pipeline(
            keep_files,
            fmap(walk_path),
            fmap(self.reverser),
        )(walker)

        if start_after is not None:
            # walk_from is inclusive; skip the start_after blob itself.
            blobs = itertools.dropwhile(lambda b: b == start_after, blobs)
        if max_items is not None:
            blobs = itertools.islice(blobs, max_items)
        return blobs

    def read_handle(self, blob: str) -> IO[bytes]:
        """
        Returns a file like object which has the blob's contents.
        File object is configured to speak bytes.
        """
        # TODO could return a function which returns a handle to make idempotency easier.
        path = self.blob_path(blob)
        fd = path.open("rb")
        return fd

    def blob_chunks(self, blob: str, size: int) -> Generator[bytes, None, None]:
        """
        Returns a generator which returns the blob's content chunked by size.
        """
        path = self.blob_path(blob)
        return path.read_chunks(size)

    def blob_checksum(self, blob: str) -> str:
        """Returns the blob's checksum."""
        path = self.blob_path(blob)
        csum = path.checksum()
        return csum

    def verify_blob_permissions(self, blob: str) -> bool:
        """
        Returns True when the blob has correct permissions: read-only and readable by the current user.
        Returns False when the blob is writable or unreadable by the current user.
        """
        path = self.blob_path(blob)
        return is_readonly(path) and is_user_readable(path)

    def blob_permission_issue(self, blob: str) -> str:
        """Returns a human-readable description of the permission problem for a blob."""
        path = self.blob_path(blob)
        if not is_user_readable(path):
            return "unreadable blob:"
        return "writable blob:"

    def fix_blob_permissions(self, blob: str) -> None:
        path = self.blob_path(blob)
        ensure_immutable_readable(path)


def _s3_putter(bucket: str, key: str) -> Callable[[Readable[bytes], s3conn], bool]:
    """Returns a function that uploads src_fd to (bucket, key), skipping if the object
    already exists. Returns True if the blob was already present, False if it was uploaded."""
    def s3_put(src_fd: Readable[bytes], s3Conn: s3conn) -> bool:
        # put_object2 with if_none_match=True returns None when the object already exists
        # (upload skipped) and a PutResult when it was freshly uploaded.
        result = s3Conn.put_object2(bucket, key, src_fd, if_none_match=True)
        already_existed = result is None
        return already_existed
    return s3_put


def _s3_parse_url(s3_url: str) -> Tuple[str, str]:
    pattern = r"^s3://(?P<bucket_name>[^/]+)/(?P<prefix>.+)$"
    match = re.match(pattern, s3_url)
    if match:
        return match.group("bucket_name"), match.group("prefix")
    else:
        raise ValueError(f"'{s3_url}' is not a valid S3 URL")


def is_s3_exception(e: Exception) -> bool:
    """Check if an exception is a retryable S3-related error."""
    return isinstance(
        e,
        (
            ValueError,
            BrokenPipeError,
            RuntimeError,
            ConnectionResetError,
            ConnectionAbortedError,
            OSError,
            IOError,
            TimeoutError,
        ),
    )


class S3BlobstoreSession:
    """
    A wrapper around an S3 connection providing
    blobstore semantics and using s3lib.Connection connection management.
    """
    def __init__(self, access_id: str, secret: bytes, bucket: str, prefix: str):
        self._access_id = access_id
        self._secret = secret
        self._bucket = bucket
        self._prefix = prefix
        self._conn: s3conn = s3conn(self._access_id, self._secret, conn_timeout=60)
        self._handle_outstanding = False

    def __enter__(self) -> 'S3BlobstoreSession':
        if self._handle_outstanding:
            raise LifecycleError("Entering session with open handle")
        self._conn.__enter__()
        return self

    def __exit__(self, *_) -> None:
        if self._handle_outstanding:
            raise LifecycleError("Exiting session with open handle")
        self._conn.__exit__(*_)

    def _key(self, blob: str) -> str:
        return self._prefix + "/" + blob

    def read_handle(self, blob: str) -> ContextManager[Readable[bytes]]:
        if self._handle_outstanding:
            raise LifecycleError("S3BlobstoreSession: previous read handle must be closed before calling read_handle again")
        try:
            stream, headers = self._conn.get_object2(self._bucket, self._key(blob))
        except ConnectionLifecycleError as e:
            raise LifecycleError(str(e)) from e
        assert stream is not None, f"get_object2 returned no stream for blob {blob}"
        logger.debug("s3 read_handle blob=%s content_length=%s", blob, headers.get("content-length"))
        self._handle_outstanding = True
        _existing_on_close = stream._on_close

        def _on_close() -> None:
            self._clear_handle()
            if _existing_on_close:
                _existing_on_close()

        stream._on_close = _on_close
        return stream

    def _clear_handle(self) -> None:
        self._handle_outstanding = False

    def import_via_fd(self, getSrcHandle: HandleThunk[Readable[bytes]], blob: str, force: bool = False) -> bool:
        key = self._key(blob)
        ioFn = _s3_putter(self._bucket, key)
        with getSrcHandle() as src:
            return ioFn(src, self._conn)


class S3Blobstore:
    def __init__(self, s3_url: str, access_id: str, secret: bytes):
        self.bucket, self.prefix = _s3_parse_url(s3_url)
        self.access_id = access_id
        self.secret = secret

    def _key(self, csum: str) -> str:
        """
        Calcualtes the S3 key name for csum
        """
        return self.prefix + "/" + csum

    def session(self) -> 'S3BlobstoreSession':
        """
        Return a session context manager over a single S3 connection.
        The connection is established on entry and closed on exit.
        """
        return S3BlobstoreSession(self.access_id, self.secret, self.bucket, self.prefix)

    def blobs(self, start_after: Optional[str] = None, max_items: Optional[int] = None) -> Generator[str, None, None]:
        """Iterator across all blobs in sorted order.

        start_after -- if given, return only blobs strictly after this checksum
                       (exclusive, matching S3 semantics). Passed directly to
                       s3lib as start_after on the S3 key.
        max_items   -- if given, return at most this many blobs.
        """
        s3_start_after: Optional[str] = None
        if start_after is not None:
            s3_start_after = self.prefix + "/" + start_after

        with s3conn(self.access_id, self.secret) as s3:
            key_iter = s3.list_bucket(self.bucket, prefix=self.prefix + "/", start=s3_start_after, batch_size=max_items)
            count = 0
            for key in key_iter:
                blob = key[len(self.prefix) + 1:]
                yield blob
                count += 1
                if max_items is not None and count >= max_items:
                    break

    # TODO dict is rather open ended.
    def blob_stats(self) -> Callable[[], Generator[dict, None, None]]:
        # TODO why do we need this? Not portable.
        """Iterator across all blobs, retaining the listing information"""

        def blob_iterator() -> Generator[dict, None, None]:
            with s3conn(self.access_id, self.secret) as s3:
                key_iter = s3.list_bucket2(self.bucket, prefix=self.prefix + "/")
                for head in key_iter:
                    blob = head[LIST_BUCKET_KEY][len(self.prefix) + 1:]
                    head["blob"] = blob
                    yield head

        return blob_iterator

    def url(self, blob: str) -> str:
        key = self.prefix + "/" + blob
        with s3conn(self.access_id, self.secret) as s3:
            return s3.get_object_url(self.bucket, key)


def _parse_http_url(http_url: str) -> tuple[Optional[str], Optional[int]]:
    parsed_url = urlparse(http_url)
    return parsed_url.hostname, parsed_url.port


class HttpBlobstoreSession:
    """
    A session over a single HTTP connection. Use via HttpBlobstore.session().

    Only one read handle may be outstanding at a time — the underlying
    HTTP/1.1 connection is strictly sequential.
    """
    def __init__(self, host: Optional[str], port: Optional[int], conn_timeout: float):
        self._host = host
        self._port = port
        self._conn_timeout = conn_timeout
        self._conn: Optional[http.client.HTTPConnection] = None
        self._handle_outstanding = False

    def __enter__(self) -> 'HttpBlobstoreSession':
        if self._handle_outstanding:
            raise LifecycleError("Entering session with open handle")
        self._conn = http.client.HTTPConnection(
            self._host, self._port, timeout=self._conn_timeout  # type: ignore[arg-type]
        )
        self._conn.connect()
        return self

    def __exit__(self, *_) -> None:
        if self._handle_outstanding:
            raise LifecycleError("Exiting session with open handle")
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    def _request(self, method: str, path: str, body: Optional[str | Readable[bytes]] = None) -> HTTPResponse:
        assert self._conn is not None
        self._conn.request(method, path, body=body)
        return self._conn.getresponse()

    def _clear_handle(self) -> None:
        self._handle_outstanding = False

    def read_handle(self, blob: str) -> ContextManager[Readable[bytes]]:
        if self._conn is None:
            raise RuntimeError("HttpBlobstoreSession: session is not open")
        if self._handle_outstanding:
            raise LifecycleError(
                "HttpBlobstoreSession: previous read handle must be closed before calling read_handle again"
            )
        resp = self._request("GET", "/bs/" + blob)
        if resp.status != http.client.OK:
            raise RuntimeError(f"blobstore returned status code: {resp.status}")
        self._handle_outstanding = True
        _orig_close = resp.close
        # TODO: replace close-patching with an _HttpHandleWrapper class (like _S3HandleWrapper)
        # for consistency and to avoid monkey-patching HTTPResponse.

        def _close_and_clear() -> None:
            self._clear_handle()
            _orig_close()

        resp.close = _close_and_clear  # type: ignore[method-assign]
        return resp

    def import_via_fd(self, getSrcHandle: HandleThunk[Readable[bytes]], blob: str, force: bool = False) -> bool:
        # TODO handle force.
        if self._conn is None:
            raise RuntimeError("HttpBlobstoreSession: session is not open")
        if self._handle_outstanding:
            raise LifecycleError(
                "HttpBlobstoreSession: previous read handle must be closed before calling import_via_fd"
            )
        with (
            getSrcHandle() as src,
            self._request("POST", f"/bs?blob={blob}", body=src) as resp,
        ):
            if resp.status == http.client.CREATED:
                dup = False
            elif resp.status == http.client.OK:
                dup = True
            else:
                raise RuntimeError(f"blobstore returned status code: {resp.status}")
        return dup


class HttpBlobstore:
    def __init__(self, endpoint, conn_timeout):
        self.host, self.port = _parse_http_url(endpoint)
        self.conn_timeout = conn_timeout

    def _request(self, method: str, path: str, body: Optional[str | Readable[bytes]] = None) -> HTTPResponse:
        conn = http.client.HTTPConnection(
            self.host, self.port, timeout=self.conn_timeout
        )
        conn.request(method, path, body=body)
        resp = conn.getresponse()
        return resp

    def session(self) -> 'HttpBlobstoreSession':
        """
        Return a session context manager over a single HTTP connection.
        The connection is established on entry and closed on exit.
        """
        return HttpBlobstoreSession(self.host, self.port, self.conn_timeout)

    def blobs(self, start_after: Optional[str] = None, max_items: Optional[int] = None) -> Iterator[str]:
        """Iterator across all blobs, fetching pages until exhausted.

        start_after -- if given, return only blobs strictly after this checksum
                       (exclusive, matching S3 semantics).
        max_items   -- if given, return at most this many blobs in a single page.
                       Without max_items, all pages are fetched and concatenated.
        """
        result: List[str] = []
        cursor: Optional[str] = start_after
        page_size = max_items if max_items is not None else 1000
        while True:
            params = f"max_items={page_size}"
            if cursor is not None:
                params += f"&start-after={cursor}"
            with self._request("GET", f"/bs?{params}") as resp:
                if resp.status != http.client.OK:
                    raise RuntimeError(f"blobstore returned status code: {resp.status}")
                payload = json.loads(resp.read())
            result.extend(payload["blobs"])
            if max_items is not None:
                break
            cursor = payload["next"]
            if cursor is None:
                break
        return iter(result)

    def blob_checksum(self, blob: str) -> str:
        with self._request("GET", f"/bs/{blob}/checksum") as resp:
            if resp.status != http.client.OK:
                raise RuntimeError(f"blobstore returned status code: {resp.status}")
            payload = resp.read()
        csum = json.loads(payload)
        return csum["csum"]
