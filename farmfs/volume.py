from os.path import join
from keydb import KeyDB
from fs import ensure_dir
from fs import normalize
from fs import find_seq
from fs import parents
from fs import dir_gen
from fs import validate_checksum
from fs import import_file, export_file
from fs import entries

def _metadata_path(root):
  return join(root, ".farmfs")

def _userdata_path(mdd):
  return join(mdd, "userdata")

def _keys_path(mdd):
  return join(mdd, "keys")

def mkfs(root):
  print "mkfs at", root
  abs_path = normalize(root)
  print "abs_path is", abs_path
  ensure_dir(abs_path)
  mdd = _metadata_path(abs_path)
  ensure_dir(mdd)
  ensure_dir(_userdata_path(mdd))
  ensure_dir(_keys_path(mdd))
  vol = FarmFSVolume(mdd)
  kdb = vol.keydb
  kdb.write("roots", [abs_path])

def find_metadata_path(cwd):
  mdd = find_seq(".farmfs", parents(cwd))
  if mdd is None:
    raise ValueError("Volume not found: %s" % cwd)
  return mdd

class FarmFSVolume:
  def __init__(self, mdd):
    self.mdd = mdd
    self.udd = _userdata_path(mdd)
    self.keydbd = _keys_path(mdd)
    self.keydb = KeyDB(self.keydbd)

  def roots(self):
    return self.keydb.read("roots")

  def freeze(self, parents):
    exclude = map(_metadata_path, self.roots())
    for (path, type_) in entries(parents, exclude):
      if type_ == "link":
        print "skipping", path
        pass
      elif type_ == "file":
        print "Importing %s" % path
        import_file(path, self.udd)
      else:
        raise ValueError("%s is not a file/link" % path)

  def thaw(self, parents):
    exclude = map(_metadata_path, self.roots())
    for (path, type_) in entries(parents, exclude):
      if type_ == "link":
        export_file(path)
      elif type_ == "file":
        print "file %s" % path
      else:
        raise ValueError("%s is not a file/link" % path)

  def check_userdata(self):
    print "Checking Userdata under:", self.udd
    for (path, type_) in entries(self.udd):
      if type_ == "file":
        if not validate_checksum(path):
          print "CORRUPTION:", path

