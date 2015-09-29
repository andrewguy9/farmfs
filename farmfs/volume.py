from keydb import KeyDB
from keydb import KeyDBWindow
from keydb import KeyDBFactory
from fs import Path
from fs import ensure_link, ensure_symlink, ensure_readonly
from snapshot import TreeSnapshot
from snapshot import snap_reduce
from snapshot import encode_snapshot
from snapshot import decode_snapshot
from os.path import sep
from itertools import combinations
from func_prototypes import typed, returned

def _metadata_path(root):
  assert isinstance(root, Path)
  return root.join(".farmfs")

@returned(Path)
@typed(Path)
def _userdata_path(root):
  return _metadata_path(root).join("userdata")

@returned(Path)
@typed(Path)
def _keys_path(root):
  return _metadata_path(root).join("keys")

@returned(Path)
@typed(Path)
def _snaps_path(root):
  return _metadata_path(root).join("snaps")

def mkfs(root):
  assert isinstance(root, Path)
  root.mkdir()
  _metadata_path(root).mkdir()
  _userdata_path(root).mkdir()
  _keys_path(root).mkdir()
  _snaps_path(root).mkdir()
  vol = FarmFSVolume(root)
  kdb = KeyDB(_keys_path(root))
  # Make sure root key is removed.
  kdb.delete("root")

@returned(basestring)
@typed(basestring, int, int)
def _checksum_to_path(checksum, num_segs=3, seg_len=3):
  segs = [ checksum[i:i+seg_len] for i in range(0, min(len(checksum), seg_len * num_segs), seg_len)]
  segs.append(checksum[num_segs*seg_len:])
  return sep.join(segs)

def _validate_checksum(path):
  csum = path.checksum()
  return path._path.endswith(_checksum_to_path(csum)) #TODO DONT REFERENCE _PATH #TODO WOULD BE BETTER WITH FRAMES.

def directory_signatures(snap):
  dirs = {}
  for entry in snap:
    if entry.is_link():
      (path, _, ref) = entry.get_tuple()
      parent = Path(path).parent()
      try:
        dirs[parent].update([ref])
      except KeyError:
        dirs[parent] = set([ref])
  return dirs

def encode_volume(vol):
  return str(vol.root)

def decode_volume(vol):
  return FarmFSVolume(Path(vol))

class FarmFSVolume:
  def __init__(self, root):
    assert isinstance(root, Path)
    self.root = root
    self.mdd = _metadata_path(root)
    self.udd = _userdata_path(root)
    self.keydb = KeyDB(_keys_path(root))
    self.snapdb = KeyDBFactory(KeyDBWindow("snaps", self.keydb), encode_snapshot, decode_snapshot)
    self.remotedb = KeyDBFactory(KeyDBWindow("remotes", self.keydb), encode_volume, decode_volume)

  """Yield set of files not backed by FarmFS under path"""
  def thawed(self, path):
    exclude = self.mdd
    for (entry, type_) in path.entries(exclude):
      if type_ == "file":
        yield entry

  """Yield set of files backed by FarmFS under path"""
  def frozen(self, path):
    exclude = self.mdd
    for (entry, type_) in path.entries(exclude):
      if type_ == "link":
        yield entry

  """Back all files under path with FarmFS"""
  def freeze(self, path):
    for p in self.thawed(path):
      self._import_file(p)

  #NOTE: This assumes a posix storage engine.
  def _import_file(self, path):
    assert isinstance(path, Path)
    assert isinstance(self.udd, Path)
    blob = self.udd.join(_checksum_to_path(path.checksum()))
    print "Processing %s with csum %s" % (path, self.udd)
    if blob.exists():
      print "Found a copy of file already in userdata, skipping copy"
    else:
      print "Putting link at %s" % blob
      ensure_link(blob, path)
      ensure_readonly(blob)
    ensure_symlink(path, blob)
    ensure_readonly(path)

  """Thaw all files under path, to allow editing"""
  def thaw(self, path):
    for p in self.frozen(path):
      self._export_file(p)

  #Note: This assumes a posix storage engine.
  def _export_file(self, user_path):
    assert isinstance(user_path, Path)
    csum_path = user_path.readlink()
    user_path.unlink()
    csum_path.copy(user_path)

  """Make sure all backed file hashes match thier file contents"""
  def check_userdata_hashes(self):
    for (path, type_) in self.udd.entries():
      if type_ == "file":
        if not _validate_checksum(path):
          yield path

  """Make sure that all links in the tree and in all snaps are backed."""
  def check_links(self):
    for (name, count) in self.count().items():
      path = self.udd.join(name)
      if not path.exists():
        yield path

  def fsck(self):
    for bad_link in self.check_links():
      yield "CORRUPTION: broken link in ", bad_link
    for bad_hash in self.check_userdata_hashes():
      yield "CORRUPTION: checksum mismatch in ", bad_hash

  """Get a snap object which represents the tree of the volume."""
  def tree(self):
    root = self.root
    udd = self.udd
    exclude = self.mdd
    tree_snap = TreeSnapshot(root, udd, exclude)
    return tree_snap

  """Return a checksum_path -> count map for each unique file backed by FarmFS"""
  def count(self):
    tree_snap = self.tree()
    key_snaps = []
    for snap_name in self.snapdb.list():
      snap = self.snapdb.read(snap_name)
      key_snaps.append(snap)
    snaps = [tree_snap] + key_snaps
    counts = snap_reduce(snaps)
    return counts

  """Yields a set of paths which reference a given checksum_path name."""
  def reverse(self, udd_name):
    #TODO SCAN THE SNAPS FOR THIS SILLY PANTS.
    exclude = self.mdd
    for (path, type_) in self.root.entries(exclude):
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

  """Yields similarity data for directories"""
  def similarity(self):
    tree = self.tree()
    dir_sigs = directory_signatures(tree)
    combos = combinations(dir_sigs.items(), 2)
    for ((dir_a, sigs_a), (dir_b, sigs_b)) in combos:
      jac_sim = float(len(sigs_a.intersection(sigs_b)))/len(sigs_a.union(sigs_b))
      yield (dir_a, dir_b, jac_sim)
