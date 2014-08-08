from os.path import join
from os.path import isfile, isdir, islink
from keydb import KeyDB
from fs import ensure_dir
from fs import normalize
from fs import find_seq
from fs import parents
from fs import dir_gen
from fs import checksum
from fs import import_file


def _metadata_path(root):
  return join(root, ".farmfs")

def _userdata_path(mdd):
  return join(mdd, "userdata")

def _keys_path(mdd):
  return join(mdd, "keys")

def mkfs(root):
  abs_path = normalize(root)
  ensure_dir(abs_path)
  mdd = _metadata_path(abs_path)
  ensure_dir(mdd)
  ensure_dir(_userdata_path(mdd))
  ensure_dir(_keys_path(mdd))
  vol = FarmFSVolume(mdd)
  kdb = vol.keydb
  kdb.write("roots", [root])

def find_metadata_path(cwd):
  mdd = find_seq(".farmfs", parents(cwd))
  if mdd is None:
    raise ValueError("Volume not found: %s" % cwd)
  return mdd

class FarmFSVolume:
  def __init__(self, mdd):
    self.mdd = mdd
    self.udd = _userdata_path(mdd)
    self.keydb = KeyDB(_keys_path(mdd))

  def roots(self):
    return self.keydb.read("roots")

  def freeze(self, parents):
    for parent in parents:
      print "Reading %s" % parent
      for path in dir_gen(parent):
        if path in map(_metadata_path, self.roots()):
          print "excluded %s" % path
        elif isfile(path):
          csum = checksum(path)
          print "file %s has checksum %s" % (path, csum)
          import_file(path, csum, self.udd)
        elif isdir(path):
          print "dir %s" % path
          self.freeze([path])
        elif islink(path):
          print "link %s" % path
        else:
          raise ValueError("%s is not a file/dir/link" % path)

