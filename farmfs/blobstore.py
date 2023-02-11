from farmfs.fs import Path, ensure_link, ensure_readonly, ensure_symlink, ensure_copy, ftype_selector, FILE, is_readonly, walk
from farmfs.util import safetype, pipeline, fmap, first, repeater
from os.path import sep
from s3lib import Connection as s3conn, LIST_BUCKET_KEY
import sys
import re

if sys.version_info >= (3, 0):
    def make_with_compatible(data):
        """
        In python 3xx urllib response payloads are compatible with
        python with syntax enter and exit semantics.
        So this function is a noop.
        """
        pass
else:
    def make_with_compatible(data):
        """
        In python 2.7 urllib response payloads are not compatible with
        python with syntax enter and exit semantics.
        This function adds __enter__ and __exit__ functions so that we can use
        with syntax on py27 and 3xx.
        """
        assert not hasattr(data, "__enter__")
        data.__enter__ = lambda: data
        data.__exit__ = lambda a, b, c: data.close()


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
    Returns a function which takes Paths into the user data and returns csums.
    """
    r = re.compile("((/([0-9]|[a-f])+){%d})$" % (num_segs + 1))
    def checksum_from_link(link):
        """Takes a path into the userdata, returns the matching csum."""
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
    def __init__(self, root, num_segs=3):
        self.root = root
        self.reverser = reverser(num_segs)

    def _csum_to_name(self, csum):
        """Return string name of link relative to root"""
        # TODO someday when csums are parameterized
        # we inject the has params here.
        return _checksum_to_path(csum)

    def csum_to_path(self, csum):
        """Return absolute Path to a blob given a csum"""
        # TODO remove callers so we can make internal.
        return Path(self._csum_to_name(csum), self.root)

    def exists(self, csum):
        blob = self.csum_to_path(csum)
        return blob.exists()

    def delete_blob(self, csum):
        """Takes a csum, and removes it from the blobstore"""
        blob_path = self.csum_to_path(csum)
        blob_path.unlink(clean=self.root)

    def import_via_link(self, path, csum):
        """Adds a file to a blobstore via a hard link."""
        blob = self.csum_to_path(csum)
        duplicate = blob.exists()
        if not duplicate:
            ensure_link(blob, path)
            ensure_readonly(blob)
        return duplicate

    def fetch_blob(self, remote, csum):
        src_blob = remote.csum_to_path(csum)
        dst_blob = self.csum_to_path(csum)
        duplicate = dst_blob.exists()
        if not duplicate:
            ensure_copy(dst_blob, src_blob)

    def link_to_blob(self, path, csum):
        """Forces path into a symlink to csum"""
        new_link = self.csum_to_path(csum)
        ensure_symlink(path, new_link)
        ensure_readonly(path)

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
        path = self.csum_to_path(blob)
        fd = path.open('rb')
        return fd

    def blob_checksum(self, blob):
        """Returns the blob's checksum."""
        path = self.csum_to_path(blob)
        csum = path.checksum()
        return csum

    def verify_blob_permissions(self, blob):
        """
        Returns True when the blob's permissions is read only.
        Returns False when the blob is mutable.
        """
        path = self.csum_to_path(blob)
        return is_readonly(path)


class S3Blobstore:
    def __init__(self, bucket, prefix, access_id, secret):
        self.bucket = bucket
        self.prefix = prefix
        self.access_id = access_id
        self.secret = secret

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
        s3 = s3conn(self.access_id, self.secret)
        s3._connect()
        data = s3.get_object(self.bucket, self.prefix + "/" + blob)
        make_with_compatible(data)
        return data

    def upload(self, csum, path):
        key = self.prefix + "/" + csum
        def uploader():
            with path.open('rb') as f:
                with s3conn(self.access_id, self.secret) as s3:
                    # TODO provide pre-calculated md5 rather than recompute.
                    # TODO put_object doesn't have a work cancellation feature.
                    result = s3.put_object(self.bucket, key, f)
            return result
        http_success = lambda status_headers: status_headers[0] >= 200 and status_headers[0] < 300
        s3_exception = lambda e: isinstance(e, ValueError)
        upload_repeater = repeater(uploader, max_tries=3, predicate=http_success, catch_predicate=s3_exception)
        return upload_repeater

    def url(self, csum):
        key = self.prefix + "/" + csum
        s3 = s3conn(self.access_id, self.secret)
        return s3.get_object_url(self.bucket, key)

