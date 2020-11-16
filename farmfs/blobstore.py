from farmfs.fs import Path, ensure_link, ensure_readonly, ensure_symlink, ensure_copy
from func_prototypes import typed, returned
from farmfs.util import safetype
from os.path import sep

@returned(safetype)
@typed(safetype, int, int)
def _checksum_to_path(checksum, num_segs=3, seg_len=3):
  segs = [ checksum[i:i+seg_len] for i in range(0, min(len(checksum), seg_len * num_segs), seg_len)]
  segs.append(checksum[num_segs*seg_len:])
  return sep.join(segs)

class Blobstore:
    def __init__(self):
        pass

class FileBlobstore:
    def __init__(self, root):
        self.root = root

    def _csum_to_name(self, csum):
        """Return string name of link relative to root"""
        #TODO someday when csums are parameterized, we inject the has params here.
        return _checksum_to_path(csum)

    def csum_to_path(self, csum):
        """Return absolute Path to a blob given a csum"""
        #TODO remove callers so we can make internal.
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
        ensure_symlink(path, self.csum_to_path(csum))
        ensure_readonly(path)

class S3Blobstore:
    def init(self):
        pass

