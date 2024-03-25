from farmfs.fs import Path, ensure_link, ensure_readonly, ensure_dir, ftype_selector, FILE, is_readonly, walk
from farmfs.util import safetype, pipeline, fmap, first, copyfileobj, retryFdIo2
import http.client
from os.path import sep
from s3lib import Connection as s3conn, LIST_BUCKET_KEY
import sys
import re
import json
from urllib.parse import urlparse

if sys.version_info >= (3, 0):
    def make_with_compatible(resp):
        """
        In python 3xx urllib response objects are compatible with
        python with syntax enter and exit semantics.
        So this function is a noop.
        """
        pass
else:
    def make_with_compatible(resp):
        """
        In python 2.7 urllib response objects are not compatible with
        python with syntax enter and exit semantics.
        This function adds __enter__ and __exit__ functions so that we can use
        with syntax on py27 and 3xx.
        """
        assert not hasattr(resp, "__enter__")
        resp.__enter__ = lambda: resp
        resp.__exit__ = lambda a, b, c: resp.close()


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

    def import_via_fd(self, getSrcHandle, blob, tries=1):
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
        if not duplicate:
            ensure_dir(dst_path.parent())
            # TODO because we always raise, we actually get no retries.
            always_raise = lambda e: False
            retryFdIo2(getSrcHandle, getDstHandle, copyfileobj, always_raise, tries=tries)
            ensure_readonly(dst_path)
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
        Returns True when the blob's permissions is read only.
        Returns False when the blob is mutable.
        """
        path = self.blob_path(blob)
        return is_readonly(path)

def _ensure_bs_tables_exist(conn):
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS volumes (
            volumeId INTEGER PRIMARY KEY AUTOINCREMENT,
            uuid TEXT UNIQUE NOT NULL
        )
        """)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS blobs (
            blob TEXT,
            volumeId INTEGER,
            PRIMARY KEY (blob, volumeId),
            FOREIGN KEY (volumeId) REFERENCES volumes(volumeId)
        )
        """)

def _ensure_bs_uuid_exists(conn, uuid):
    conn.execute(
        """
        INSERT OR IGNORE INTO volumes (uuid) VALUES (?);
        """,
        [uuid])

class Sqlite3BlobstoreCache:
    def __init__(self, conn, bs):
        self.conn = conn
        self.bs = bs
        # TODO you need to have input bs come with a uuid.
        uuid = "D770164F-DE35-4BFF-BE0C-2BA29D0272EE"
        self.uuid = uuid
        _ensure_bs_tables_exist(conn)
        _ensure_bs_uuid_exists(conn, uuid)


    def _blob_id_to_name(self, blob):
        """Return string name of link relative to root"""
        # TODO someday when blob checksums are parameterized
        # we inject the has params here.
        return self.bs._checksum_to_path(blob)

    def blob_path(self, blob):
        """Return absolute Path to a blob given a blob id."""
        return self.bs.blob_path(blob)

    def exists(self, blob):
        cur = self.conn.cursor()
        cur.execute(
            """
            SELECT b.blob
            FROM blobs b
            JOIN volumes v ON b.volumeId = v.volumeId
            WHERE b.blob = ? and v.uuid = ?;
            """,
            ([blob, self.uuid])
        )
        result = cur.fetchone()
        return result is not None

    def delete_blob(self, blob):
        """Takes a blob, and removes it from the blobstore"""
        # TODO remove from SQLITE
        cur = self.conn.cursor()
        cur.execute(
            """
            DELETE FROM blobs
            WHERE blob = ? and volumeId = (
                SELECT volumeId FROM volumes WHERE uuid = ?
            );
            """,
            [blob, self.uuid]
        )
        self.bs.delete_blob(blob)

    def import_via_link(self, tree_path, blob):
        """Adds a file to a blobstore via a hard link."""
        duplicate = self.bs.import_via_link(tree_path, blob)
        cur = self.conn.cursor()
        cur.execute(
            """
            INSERT OR IGNORE INTO blobs (blob, volumeId)
            VALUES (?, ?);
            """,
            [blob, self.uuid]
        )
        return duplicate

    def import_via_fd(self, getSrcHandle, blob, tries=1):
        """
        Imports a new file to the blobstore via copy.
        getSrcHandle is a function which returns a read handle to copy from.
        blob is the blob's id.
        While file is first copied to local temporary storage, then moved to
        the blobstore idepotently.
        """
        if self.exists(blob):
            return True
        duplicate = self.bs.import_via_fd(getSrcHandle, blob, tries) # should be False, unless we are stale.
        cur = self.conn.cursor()
        cur.execute(
            """
            INSERT OR IGNORE INTO blobs (blob, volumeId)
            VALUES (?, ?);
            """,
            [blob, self.uuid]
        )
        return duplicate

    def blobs(self):
        """Iterator across all blobs"""
        def blob_iter():
            cursor = self.conn.cursor()
            cursor.execute("""
                SELECT b.blob
                FROM blobs b
                JOIN volumes v ON b.volumeId = v.volumeId
                WHERE v.uuid = ?
                ORDER BY b.blob ASC
                """,
                [self.uuid])
            for (blob) in cursor.fetchall():
                yield blob
        return blob_iter()

    def read_handle(self, blob):
        """
        Returns a file like object which has the blob's contents.
        File object is configured to speak bytes.
        """
        return self.bs.read_handle(blob)

    def blob_chunks(self, blob, size):
        """
        Returns a generator which returns the blob's content chunked by size.
        """
        return self.bs.blob_chunks(blob, size)

    def blob_checksum(self, blob):
        """Returns the blob's checksum."""
        return self.bs.blob_checksum(blob)

    def verify_blob_permissions(self, blob):
        """
        Returns True when the blob's permissions is read only.
        Returns False when the blob is mutable.
        """
        return self.bs.verify_blob_permissions(blob)

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
        make_with_compatible(resp)
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
        make_with_compatible(resp)
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
