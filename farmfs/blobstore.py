from farmfs.fs import Path, ensure_link, ensure_readonly, ensure_dir, ftype_selector, FILE, is_readonly, walk
from farmfs.util import safetype, pipeline, fmap, first, copyfileobj, retryFdIo2
import http.client
from os.path import sep
from s3lib import Connection as s3conn, LIST_BUCKET_KEY
import sys
import re
import sqlite3
import json
from urllib.parse import urlparse
from contextlib import contextmanager, closing

_sep_replace_ = re.compile(sep)
def _remove_sep_(path):
    return _sep_replace_.subn("", path)[0]

def fast_reverser(num_segs=3):
    total_chars = 32
    chars_per_seg = 3
    r = re.compile(("/([0-9a-f]{%d})" % chars_per_seg) * num_segs + "/([0-9a-f]{%d})$" % (total_chars - chars_per_seg * num_segs))
    def checksum_from_link_fast(link):
        m = r.search(safetype(link))
        if (m):
            csum = "".join(m.groups())
            return csum
        else:
            raise ValueError("link %s checksum didn't parse" % (link))
    return checksum_from_link_fast


# TODO we should remove references to vol.bs.reverser, as thats leaking format
# information into the volume.
def old_reverser(num_segs=3):
    """
    Returns a function which takes Paths into the user data and returns blob ids.
    """
    r = re.compile("((/([0-9]|[a-f])+){%d})$" % (num_segs + 1))
    def checksum_from_link(link):
        """Takes a path into the userdata, returns the matching blob id."""
        m = r.search(safetype(link))
        if (m):
            csum_slash = m.group()[1:]
            csum = _remove_sep_(csum_slash)
            return csum
        else:
            raise ValueError("link %s checksum didn't parse" % (link))
    return checksum_from_link


reverser = fast_reverser

def _checksum_to_path(checksum, num_segs=3, seg_len=3):
    segs = [checksum[i:i + seg_len] for i in range(0, min(len(checksum), seg_len * num_segs), seg_len)]
    segs.append(checksum[num_segs * seg_len:])
    return sep.join(segs)

class Blobstore:
    def __init__(self):
        raise NotImplementedError()

class FileBlobstore:
    def __init__(self, root, tmp_dir, num_segs=3):
        self.root = root
        self.tmp_dir = tmp_dir
        self.reverser = reverser(num_segs)
        self.tmp_dir = tmp_dir

    def _blob_id_to_name(self, blob):
        """Return string name of link relative to root"""
        # TODO someday when blob checksums are parameterized
        # we inject the has params here.
        return _checksum_to_path(blob)

    def blob_path(self, blob):
        """Return absolute Path to a blob given a blob id."""
        return Path(self._blob_id_to_name(blob), self.root)

    def exists(self, blob):
        blob = self.blob_path(blob)
        return blob.exists()

    def delete_blob(self, blob):
        """Takes a blob, and removes it from the blobstore"""
        blob_path = self.blob_path(blob)
        blob_path.unlink(clean=self.root)

    def import_via_link(self, tree_path, blob):
        """Adds a file to a blobstore via a hard link."""
        blob_path = self.blob_path(blob)
        duplicate = blob_path.exists()
        if not duplicate:
            ensure_link(blob_path, tree_path)
            ensure_readonly(blob_path)
        return duplicate

# TODO should import_via_fd have force for other blobstore types?
    def import_via_fd(self, getSrcHandle, blob, force=False, tries=1):
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
            ensure_dir(dst_path.parent())
            # TODO because we always raise, we actually get no retries. We should figure out what exceptions we should catch.
            always_raise = lambda e: False
            retryFdIo2(getSrcHandle, getDstHandle, copyfileobj, always_raise, tries=tries)
            ensure_readonly(dst_path)
        # TODO do we want to return duplicate or "we imported"?
        return duplicate

    def blobs(self):
        """Iterator across all blobs"""
        blobs = pipeline(
            ftype_selector([FILE]),
            fmap(first),
            fmap(self.reverser),)(walk(self.root))
        return blobs

    def read_handle(self, blob):
        """
        Returns a file like object which has the blob's contents.
        File object is configured to speak bytes.
        """
        # TODO could return a function which returns a handle to make idempotency easier.
        path = self.blob_path(blob)
        fd = path.open('rb')
        return fd

    def blob_chunks(self, blob, size):
        """
        Returns a generator which returns the blob's content chunked by size.
        """
        path = self.blob_path(blob)
        return path.read_chunks(size)

    def blob_checksum(self, blob):
        """Returns the blob's checksum."""
        path = self.blob_path(blob)
        csum = path.checksum()
        return csum

    def verify_blob_permissions(self, blob):
        """
        Returns True when the blob's permissions is read only (immutable).
        Returns False when the blob is mutable.
        """
        path = self.blob_path(blob)
        return is_readonly(path)

    def fix_blob_permissions(self, blob):
        path = self.blob_path(blob)
        ensure_readonly(path)

def _s3_putter(bucket, key):
    def s3_put(src_fd, s3Conn):
        # TODO provide pre-calculated md5 rather than recompute.
        # TODO put_object doesn't have a work cancellation feature.
        status, headers = s3Conn.put_object(bucket, key, src_fd)
        if status < 200 or status >= 300:
            raise RuntimeError(f"HTTP Status code error: {status} Headers: f{headers}")
    return s3_put

def _s3_parse_url(s3_url):
    pattern = r'^s3://(?P<bucket_name>[^/]+)/(?P<prefix>.+)$'
    match = re.match(pattern, s3_url)
    if match:
        return match.group('bucket_name'), match.group('prefix')
    else:
        raise ValueError(f"'{s3_url}' is not a valid S3 URL")

class CacheBlobstore:
    def __init__(self, store, conn):
        self.store = store
        self.reverser = store.reverser
        self.conn = conn
        self._initialize_database()

    def transaction(self, transactor):
        """Execute a transactor function within a transaction.
        transactor receives the cursor, performs operations, and optionally returns a value.
        - If transactor raises: rollback and re-raise
        - If transactor returns: commit and return the value
        - If commit fails: rollback and raise"""
        cursor = self.conn.cursor()
        try:
            result = transactor(cursor)
            self.conn.commit()
            return result
        except Exception:
            self.conn.rollback()
            raise
        finally:
            cursor.close()

    def _initialize_database(self):
        with closing(self.conn.cursor()) as cursor:
            cursor.execute("CREATE TABLE IF NOT EXISTS blobs (blob TEXT PRIMARY KEY)")
            self.conn.commit()
            self._validate_schema()

    def _validate_schema(self):
        """Validate that the blobs table has the expected schema."""
        with closing(self.conn.cursor()) as cursor:
            # Check if table exists
            cursor.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='blobs'"
            )
            if not cursor.fetchone():
                raise ValueError("Cache database missing 'blobs' table")

            # Check table structure
            cursor.execute("PRAGMA table_info(blobs)")
            columns = cursor.fetchall()
            if not columns:
                raise ValueError("Cache 'blobs' table is empty or corrupted")

            # Verify we have a 'blob' column
            blob_col = next((col for col in columns if col[1] == "blob"), None)
            if not blob_col:
                raise ValueError("Cache 'blobs' table missing 'blob' column")

            # Verify it's TEXT type
            if blob_col[2] != "TEXT":
                raise ValueError(
                    f"Cache 'blob' column has wrong type {blob_col[2]}, expected TEXT"
                )

    def blob_path(self, csum):
        """Return absolute Path to a blob given a csum"""
        return self.store.blob_path(csum)

    def exists(self, csum, check_below=False):
        """Check if blob exists.
        If check_below=False: check this cache layer only (fast)
        If check_below=True: check underlying store layer"""
        if check_below:
            return self.store.exists(csum)
        else:
            with closing(self.conn.cursor()) as cursor:
                cursor.execute("SELECT 1 FROM blobs WHERE blob = ?", (csum,))
                return cursor.fetchone() is not None

    def delete_blob(self, csum):
        """Delete a blob from cache and underlying store.
        Both deletions are atomic - either both succeed or both are rolled back."""
        def transactor(cursor):
            cursor.execute("DELETE FROM blobs WHERE blob = ?", (csum,))
            blobsRemoved = cursor.rowcount
            if blobsRemoved > 0:
                # We'll delete from store after transaction commits
                return blobsRemoved
            return 0

        blobsRemoved = self.transaction(transactor)
        if blobsRemoved > 0:
            self.store.delete_blob(csum)

    def import_via_link(self, path, csum, force=False):
        """Adds a file to a blobstore via a hard link.
        Note: duplicate return value may be stale due to concurrent operations.
        Use blobstore-level locking for strict consistency."""
        duplicate = self.exists(csum)
        if not force and duplicate:
            return duplicate

        self.store.import_via_link(path, csum)
        with closing(self.conn.cursor()) as cursor:
            cursor.execute("INSERT OR REPLACE INTO blobs (blob) VALUES (?)", (csum,))
            self.conn.commit()
        return duplicate

    def import_via_fd(self, getSrcHandle, csum, force=False, tries=1):
        """Imports a new file to the blobstore via copy.
        Note: duplicate return value may be stale due to concurrent operations.
        Use blobstore-level locking for strict consistency."""
        duplicate = self.exists(csum)
        if not force and duplicate:
            return duplicate

        self.store.import_via_fd(getSrcHandle, csum, force=force, tries=tries)
        with closing(self.conn.cursor()) as cursor:
            cursor.execute("INSERT OR REPLACE INTO blobs (blob) VALUES (?)", (csum,))
            self.conn.commit()
        return duplicate

    def blobs(self,):
        """Iterator across all blobs in sorted order."""
        with closing(self.conn.cursor()) as cursor:
            cursor.execute("SELECT blob FROM blobs ORDER BY blob")
            for row in cursor:
                yield row[0]

    def read_handle(self, blob):
        return self.store.read_handle(blob)

    def blob_chunks(self, blob, size):
        return self.store.blob_chunks(blob, size)

    def blob_checksum(self, blob):
        return self.store.blob_checksum(blob)

    def verify_blob_permissions(self, blob):
        return self.store.verify_blob_permissions(blob)

    def fix_blob_permissions(self, blob):
        self.store.fix_blob_permissions(blob)

class S3Blobstore:
    def __init__(self, s3_url, access_id, secret):
        self.bucket, self.prefix = _s3_parse_url(s3_url)
        self.access_id = access_id
        self.secret = secret

    def _key(self, csum):
        """
        Calcualtes the S3 key name for csum
        """
        return self.prefix + "/" + csum

    def blobs(self):
        """Iterator across all blobs"""
        def blob_iterator():
            with s3conn(self.access_id, self.secret) as s3:
                key_iter = s3.list_bucket(self.bucket, prefix=self.prefix + "/")
                for key in key_iter:
                    blob = key[len(self.prefix) + 1:]
                    yield blob
        return blob_iterator

    def blob_stats(self):
        # TODO why do we need this? Not portable.
        """Iterator across all blobs, retaining the listing information"""
        def blob_iterator():
            with s3conn(self.access_id, self.secret) as s3:
                key_iter = s3.list_bucket2(self.bucket, prefix=self.prefix + "/")
                for head in key_iter:
                    blob = head[LIST_BUCKET_KEY][len(self.prefix) + 1:]
                    head['blob'] = blob
                    yield head
        return blob_iterator

    def read_handle(self, blob):
        """Returns a file like object which has the blob's contents"""
        # TODO Could return a function which returns a read handle. Would make idepontency easier.
        s3 = s3conn(self.access_id, self.secret)
        s3._connect()
        resp = s3.get_object(self.bucket, self.prefix + "/" + blob)
        return resp

    def _s3_conn(self):
        return s3conn(self.access_id, self.secret)

    def import_via_fd(self, getSrcHandle, blob):
        """
        Imports a new file to the blobstore via copy.
        getSrcHandle is a function which returns a read handle to copy from.
        blob is the blob's id.
        S3 won't create the blob unless the full upload is a success.
        """
        key = self._key(blob)
        s3_exceptions = lambda e: isinstance(e, (ValueError, BrokenPipeError, RuntimeError))
        retryFdIo2(getSrcHandle, self._s3_conn, _s3_putter(self.bucket, key), s3_exceptions)
        return False  # S3 doesn't give us a good way to know if the blob was already present.

    def url(self, blob):
        key = self.prefix + "/" + blob
        s3 = s3conn(self.access_id, self.secret)
        return s3.get_object_url(self.bucket, key)


def _parse_http_url(http_url):
    parsed_url = urlparse(http_url)
    return parsed_url.hostname, parsed_url.port

class HttpBlobstore:
    def __init__(self, endpoint, conn_timeout):
        self.host, self.port = _parse_http_url(endpoint)
        self.conn_timeout = conn_timeout

    def _request(self, method, path, body=None):
        conn = http.client.HTTPConnection(self.host, self.port, timeout=self.conn_timeout)
        conn.request(method, path, body=body)
        resp = conn.getresponse()
        return resp

    def blobs(self):
        """Iterator across all blobs."""
        def blob_fetcher():
            with self._request('GET', '/bs') as resp:
                if resp.status != http.client.OK:
                    raise RuntimeError(f"blobstore returned status code: {resp.status}")
                list_str = resp.read()
            blobs = json.loads(list_str)
            return iter(blobs)
        return blob_fetcher

    def read_handle(self, blob):
        """
        Get a read handle to a blob. Caller is required to release the handle.
        """
        resp = self._request('GET', '/bs/' + blob)
        if resp.status != http.client.OK:
            raise RuntimeError(f"blobstore returned status code: {resp.status}")
        return resp

    def import_via_fd(self, getSrcHandle, blob):
        """
        Imports a new file to the blobstore via copy.
        getSrcHandle is a function which returns a read handle to copy from.
        blob is the blob's id.
        farmfs api won't create the blob unless the full upload is a success.
        """
        with getSrcHandle() as src, self._request('POST', f"/bs?blob={blob}", body=src) as resp:
            if resp.status == http.client.CREATED:
                dup = False
            elif resp.status == http.client.OK:
                dup = True
            else:
                raise RuntimeError(f"blobstore returned status code: {resp.status}")
        return dup

    def blob_checksum(self, blob):
        with self._request('GET', f"/bs/{blob}/checksum") as resp:
            if resp.status != http.client.OK:
                raise RuntimeError(f"blobstore returned status code: {resp.status}")
            payload = resp.read()
        csum = json.loads(payload)
        return csum['csum']
