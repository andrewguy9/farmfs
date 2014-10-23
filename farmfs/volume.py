from keydb import KeyDB
from fs import Path
from fs import find_in_seq
from fs import validate_checksum
from fs import import_file, export_file
from snapshot import SnapshotDatabase
from snapshot import TreeSnapshot
from snapshot import snap_reduce

def _metadata_path(root):
  assert isinstance(root, Path)
  return root.join(".farmfs")

def _userdata_path(mdd):
  assert isinstance(mdd, Path)
  return mdd.join("userdata")

def _keys_path(mdd):
  assert isinstance(mdd, Path)
  return mdd.join("keys")

def _snaps_path(mdd):
  assert isinstance(mdd, Path)
  return mdd.join("snaps")

def mkfs(root):
  assert isinstance(root, Path)
  print "mkfs at", root
  root.mkdir()
  mdd = _metadata_path(root)
  mdd.mkdir()
  _userdata_path(mdd).mkdir()
  _keys_path(mdd).mkdir()
  _snaps_path(mdd).mkdir()
  vol = FarmFSVolume(mdd)
  kdb = KeyDB(_keys_path(mdd))
  kdb.write("root", str(root))

def _find_metadata_path(path):
  assert isinstance(path, Path)
  mdd = find_in_seq(".farmfs", path.parents())
  if mdd is None:
    raise ValueError("Volume not found: %s" % path)
  return mdd

def getvol(path):
  assert isinstance(path, Path)
  mdd = _find_metadata_path(path)
  vol = FarmFSVolume(mdd)
  return vol

class FarmFSVolume:
  def __init__(self, mdd):
    assert isinstance(mdd, Path)
    self.mdd = mdd
    self.udd = _userdata_path(mdd)
    self.keydbd = _keys_path(mdd)
    self.keydb = KeyDB(self.keydbd)
    self.snapdb = SnapshotDatabase(self.keydb)

  """Return set of root of FarmFS volume."""
  def root(self):
    return Path(self.keydb.read("root"))


  """Yield set of files not backed by FarmFS under paths"""
  def thawed(self, paths):
    exclude = _metadata_path(self.root())
    for path in paths:
      for (entry, type_) in path.entries(exclude):
        if type_ == "file":
          yield entry

  """Yield set of files backed by FarmFS under paths"""
  def frozen(self, paths):
    exclude = _metadata_path(self.root())
    for path in paths:
      for (entry, type_) in path.entries(exclude):
        if type_ == "link":
          yield entry

  """Back all files under paths with FarmFS"""
  def freeze(self, paths):
    for path in self.thawed(paths):
      import_file(path, self.udd)

  """Thaw all files under paths, to allow editing"""
  def thaw(self, paths):
    for path in self.frozen(paths):
      export_file(path)

  """Make sure all backed file hashes match thier file contents"""
  def check_userdata_hashes(self):
    for (path, type_) in self.udd.entries():
      if type_ == "file":
        if not validate_checksum(path):
          yield path

  """Make sure that all links in the tree and in all snaps are backed."""
  def check_links(self):
    for (name, count) in self.count().items():
      path = self.udd.join(name)
      if not path.exists():
        yield path

  """Get a snap object which represents the tree of the volume."""
  def tree(self):
    root = self.root()
    udd = self.udd
    exclude = _metadata_path(root)
    tree_snap = TreeSnapshot(root, udd, exclude)
    return tree_snap

  """Create a snapshot of the volume's current stats"""
  def snap(self, name):
    tree = self.tree()
    self.snapdb.save(name, tree)

  """Return a checksum_path -> count map for each unique file backed by FarmFS"""
  def count(self):
    tree_snap = self.tree()
    key_snaps = self.snapdb.list()
    for snap_name in self.keydb.list("snaps/"):
      snap = self.keydb.get(snap_name)
      key_snaps.append(snap)
    snaps = [tree_snap] + key_snaps
    counts = snap_reduce(snaps)
    return counts

  """Yields a set of paths which reference a given checksum_path name."""
  def reverse(self, udd_name):
    #TODO SCAN THE SNAPS FOR THIS SILLY PANTS.
    root = self.root()
    exclude = _metadata_path(root)
    for (path, type_) in root.entries(exclude):
      if type_ == "link":
        ud_path = path.readlink()
        if ud_path == udd_name:
          yield path

  """ Yield all the relative paths (basestring) for all the files in the userdata store."""
  def userdata(self):
   # We populate counts with all hash paths from the userdata directory.
   for (path, type_) in self.udd.entries():
     assert isinstance(path, Path)
     if type_ == "file":
       yield path.relative_to(self.udd)
     elif type_ == "dir":
       pass
     else:
       raise ValueError("%s is f invalid type %s" % (path, type_))

  """Yields the names of files which are being garbage collected"""
  def gc(self):
    referenced_hashes = set(self.count().keys())
    udd_hashes = set(self.userdata())
    missing_data = referenced_hashes - udd_hashes
    assert len(missing_data) == 0, "Missing %s\nReferenced %s\nExisting %s\n" % (missing_data, referenced_hashes, udd_hashes)
    orphaned_data = udd_hashes - referenced_hashes
    for blob in orphaned_data:
      yield blob
      blob_path = self.udd.join(blob)
      blob_path.unlink(clean=self.udd)

