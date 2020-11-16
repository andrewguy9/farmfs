from farmfs.fs import Path
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
        return Path(self._csum_to_name(csum), self.root)

    def exists(self, csum):
        blob = self.csum_to_path(csum)
        return blob.exists()

    def delete_blob(self, csum):
        """Takes a csum, and removes it from the blobstore"""
        blob_path = self.csum_to_path(csum)
        blob_path.unlink(clean=self.root)

class S3Blobstore:
    def init(self):
        pass

