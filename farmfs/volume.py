from os.path import join
from os import readlink
from keydb import KeyDB
from fs import ensure_dir
from fs import normalize
from fs import find_seq
from fs import parents
from fs import dir_gen
from fs import validate_checksum, validate_link
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

  """Return set of roots backed by FarmFS"""
  def roots(self):
    return self.keydb.read("roots")

  """Yield set of files not backed by FarmFS under paths"""
  def thawed(self, paths):
    exclude = map(_metadata_path, self.roots())
    for (path, type_) in entries(paths, exclude):
      if type_ == "file":
        yield path

  """Yield set of files backed by FarmFS under paths"""
  def frozen(self, paths):
    exclude = map(_metadata_path, self.roots())
    for (path, type_) in entries(paths, exclude):
      if type_ == "link":
        yield path

  """Back all files under paths with FarmFS"""
  def freeze(self, paths):
    for path in self.thawed(paths):
      import_file(path, self.udd)

  """Thaw all files under paths, to allow editing"""
  def thaw(self, paths):
    for path in self.frozen(paths):
      export_file(path)

  def remove(self, paths):
    for path in paths:
      unlink(path)

  """Make sure all backed file hashes match thier file contents"""
  def check_userdata_hashes(self):
    for (path, type_) in entries(self.udd):
      if type_ == "file":
        if not validate_checksum(path):
          print "CORRUPTION: checksum mismatch in ", path

  """Make sure all FarmFS links are backed"""
  def check_inbound_links(self):
    exclude = map(_metadata_path, self.roots())
    for (path, type_) in entries(self.roots(), exclude):
      if type_ == "link":
        if not validate_link(path):
          print "CORRUPTION: broken link in ", path

  """Return a checksum_path -> count map for each unique file backed by FarmFS"""
  def count(self):
    counts = {}
    #populate counts with placeholders for root scan.
    for (path, type_) in entries(self.udd):
      if type_ == "file":
        counts[path]=0
    exclude = map(_metadata_path, self.roots())
    for (path, type_) in entries(self.roots(), exclude):
      if type_ == "link":
        ud_path = readlink(path)
        try:
          counts[ud_path]+=1
        except KeyError:
          raise ValueError("Encounted unexpected link: %s from file %s" % (ud_path, path))
    return counts

  """Yields a set of paths which reference a given checksum_path name."""
  def reverse(self, udd_name):
    exclude = map(_metadata_path, self.roots())
    for (path, type_) in entries(self.roots(), exclude):
      if type_ == "link":
        ud_path = readlink(path)
        if ud_path == udd_name:
          yield path

