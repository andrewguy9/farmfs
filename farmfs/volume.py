from os.path import join
from os.path import isfile, isdir, islink
from keydb import KeyDB
from fs import ensure_dir
from fs import normalize
from fs import find_seq
from fs import parents
from fs import dir_gen
from fs import checksum
from fs import import_file, export_file

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

  def walk(self, paths):
    roots = self.roots()
    for path in paths:
      if path in map(_metadata_path, roots):
        print "excluded %s" % path
      elif islink(path):
        yield (path, "link")
      elif isfile(path):
        yield (path, "file")
      elif isdir(path):
        for x in self.walk([path]):
          yield x
      else:
        raise ValueError("%s is not a file/dir/link" % path)

  def freeze(self, parents):
    for (path, type_) in self.walk(parents):
      if type_ == "link":
        print "skipping", path
        pass
      elif type_ == "file":
        print "Importing %s" % path
        import_file(path, self.udd)
      else:
        raise ValueError("%s is not a file/link" % path)

  def thaw(self, parents):
    for (path, type_) in self.walk(parents):
      if type_ == "link":
        export_file(path)
      elif type_ == "file":
        print "file %s" % path
      else:
        raise ValueError("%s is not a file/link" % path)
