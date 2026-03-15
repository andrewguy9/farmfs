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
    walk_path,
)
import http.client
from http.client import HTTPResponse
import json
import logging
from contextlib import nullcontext
from collections.abc import Callable
from os.path import sep
import re
from typing import ContextManager, IO, Generator, Iterator, Optional, Tuple
from urllib.parse import urlparse
from s3lib import Connection as s3conn, LIST_BUCKET_KEY
from farmfs.util import (
    copyfileobj,
    fmap,
    HandleThunk,
    pipeline,
    Readable,
    retry,
    withHandles2,
    withHandles2Thunk,
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

    # TODO should import_via_fd have force for other blobstore types?
    def import_via_fd(self, getSrcHandle: HandleThunk[Readable[bytes]], blob: str, force=False) -> bool:
        """
        Imports a new file to the blobstore via copy.
        getSrcHandle is a function which returns a read handle to copy from.
        blob is the blob's id.
        While file is first copied to local temporary storage, then moved to
        the blobstore idepotently.
        """
        dst_path = self.blob_path(blob)
        getDstHandle = lambda: dst_path.safeopen("wb", lambda _: self.tmp_dir)
        duplicate = dst_path.exists()
        if force or not duplicate:
            parent = dst_path.parent()
            assert parent is not None, "blob path cannot be root"
            ensure_dir(parent)
            withHandles2(getSrcHandle, getDstHandle, copyfileobj)
            ensure_readonly(dst_path)
        # TODO do we want to return duplicate or "we imported"?
        return duplicate

    def session(self) -> ContextManager['FileBlobstore']:
        """
        Return a session context manager. FileBlobstore has no connection to
        manage, so the session is the blobstore itself wrapped in a nullcontext.
        """
        return nullcontext(self)

    def blobs(self) -> Iterator[str]:
        """Iterator across all blobs"""
        keep_files = ftype_selector([FILE])

        blobs: Iterator[str] = pipeline(
            keep_files,
            fmap(walk_path),
            fmap(self.reverser),
        )(walk(self.root))
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


def _s3_putter(bucket: str, key: str) -> Callable[[Readable[bytes], s3conn], None]:
    def s3_put(src_fd: Readable[bytes], s3Conn: s3conn) -> None:
        # TODO provide pre-calculated md5 rather than recompute.
        # TODO put_object doesn't have a work cancellation feature.
        status, headers = s3Conn.put_object(bucket, key, src_fd)
        if status < 200 or status >= 300:
            raise RuntimeError(f"HTTP Status code error: {status} Headers: f{headers}")

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


class _S3HandleWrapper:
    """
    Wraps an S3 HTTPResponse to clear the session's outstanding-handle flag on close.
    """
    def __init__(self, resp: IO[bytes], on_close: Callable[[], None]):
        self._resp = resp
        self._on_close = on_close

    def read(self, size: int = -1) -> bytes:
        return self._resp.read(size)

    def close(self) -> None:
        self._on_close()
        self._resp.close()

    def __enter__(self) -> '_S3HandleWrapper':
        return self

    def __exit__(self, *_) -> None:
        self.close()


class S3BlobstoreSession:
    """
    A session over a single S3 connection. Use via S3Blobstore.session().

    Only one read handle may be outstanding at a time — the underlying
    HTTP/1.1 connection is strictly sequential.
    """
    def __init__(self, access_id: str, secret: bytes, bucket: str, prefix: str):
        self._access_id = access_id
        self._secret = secret
        self._bucket = bucket
        self._prefix = prefix
        self._conn: Optional[s3conn] = None
        self._handle_outstanding = False

    def __enter__(self) -> 'S3BlobstoreSession':
        self._conn = s3conn(self._access_id, self._secret)
        self._conn._connect()
        return self

    def __exit__(self, *_) -> None:
        if self._conn is not None:
            self._conn._disconnect()
            self._conn = None

    def _key(self, blob: str) -> str:
        return self._prefix + "/" + blob

    def read_handle(self, blob: str) -> '_S3HandleWrapper':
        if self._conn is None:
            raise RuntimeError("S3BlobstoreSession: session is not open")
        if self._handle_outstanding:
            raise RuntimeError(
                "S3BlobstoreSession: previous read handle must be closed before calling read_handle again"
            )
        self._handle_outstanding = True
        resp = self._conn.get_object(self._bucket, self._key(blob))
        logger.debug("s3 read_handle blob=%s content_length=%s", blob, resp.length)
        return _S3HandleWrapper(resp, self._clear_handle)

    def _clear_handle(self) -> None:
        self._handle_outstanding = False

    def import_via_fd(self, getSrcHandle: HandleThunk[Readable[bytes]], blob: str) -> bool:
        if self._conn is None:
            raise RuntimeError("S3BlobstoreSession: session is not open")
        if self._handle_outstanding:
            raise RuntimeError(
                "S3BlobstoreSession: previous read handle must be closed before calling import_via_fd"
            )
        key = self._key(blob)
        ioFn = _s3_putter(self._bucket, key)
        with getSrcHandle() as src:
            ioFn(src, self._conn)
        return False


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

    def blobs(self) -> Generator[str, None, None]:
        """Iterator across all blobs"""
        with s3conn(self.access_id, self.secret) as s3:
            key_iter = s3.list_bucket(self.bucket, prefix=self.prefix + "/")
            for key in key_iter:
                blob = key[len(self.prefix) + 1:]
                yield blob

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

    def read_handle(self, blob: str) -> IO[bytes]:
        """Returns a file like object which has the blob's contents"""
        # TODO Could return a function which returns a read handle. Would make idepontency easier.
        s3 = s3conn(self.access_id, self.secret)
        s3._connect()
        resp = s3.get_object(self.bucket, self.prefix + "/" + blob)
        return resp

    def _s3_conn(self) -> s3conn:
        return s3conn(self.access_id, self.secret)

    def import_via_fd(self, getSrcHandle: HandleThunk[Readable[bytes]], blob: str) -> bool:
        """
        Imports a new file to the blobstore via copy.
        getSrcHandle is a function which returns a read handle to copy from.
        blob is the blob's id.
        S3 won't create the blob unless the full upload is a success.
        """
        key = self._key(blob)
        ioFn = _s3_putter(self.bucket, key)
        retry(withHandles2Thunk(getSrcHandle, self._s3_conn, ioFn), is_s3_exception)
        # TODO this note is now false with new S3 API (I think)
        return False  # S3 doesn't give us a good way to know if the blob was already present.

    def url(self, blob: str) -> str:
        key = self.prefix + "/" + blob
        s3 = s3conn(self.access_id, self.secret)
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
        self._conn = http.client.HTTPConnection(
            self._host, self._port, timeout=self._conn_timeout  # type: ignore[arg-type]
        )
        self._conn.connect()
        return self

    def __exit__(self, *_) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    def _request(self, method: str, path: str, body: Optional[str | Readable[bytes]] = None) -> HTTPResponse:
        assert self._conn is not None
        self._conn.request(method, path, body=body)
        return self._conn.getresponse()

    def _clear_handle(self) -> None:
        self._handle_outstanding = False

    def read_handle(self, blob: str) -> HTTPResponse:
        if self._conn is None:
            raise RuntimeError("HttpBlobstoreSession: session is not open")
        if self._handle_outstanding:
            raise RuntimeError(
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

    def import_via_fd(self, getSrcHandle: HandleThunk[Readable[bytes]], blob: str) -> bool:
        if self._conn is None:
            raise RuntimeError("HttpBlobstoreSession: session is not open")
        if self._handle_outstanding:
            raise RuntimeError(
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

    def blobs(self) -> Iterator[str]:
        """Iterator across all blobs."""
        with self._request("GET", "/bs") as resp:
            # TODO raise on error?
            if resp.status != http.client.OK:
                # TODO RuntimeError is the python runtime error type, we need a blobstore specific error type.
                raise RuntimeError(f"blobstore returned status code: {resp.status}")
            list_str = resp.read()
        blobs = json.loads(list_str)
        return iter(blobs)

    def read_handle(self, blob: str) -> HTTPResponse:
        """
        Get a read handle to a blob. Caller is required to release the handle.
        """
        resp = self._request("GET", "/bs/" + blob)
        if resp.status != http.client.OK:
            raise RuntimeError(f"blobstore returned status code: {resp.status}")
        return resp

    def import_via_fd(self, getSrcHandle: HandleThunk[Readable[bytes]], blob: str) -> bool:
        """
        Imports a new file to the blobstore via copy.
        getSrcHandle is a function which returns a read handle to copy from.
        blob is the blob's id.
        farmfs api won't create the blob unless the full upload is a success.
        """
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

    def blob_checksum(self, blob: str) -> str:
        with self._request("GET", f"/bs/{blob}/checksum") as resp:
            if resp.status != http.client.OK:
                raise RuntimeError(f"blobstore returned status code: {resp.status}")
            payload = resp.read()
        csum = json.loads(payload)
        return csum["csum"]
