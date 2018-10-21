from errno import ENOENT as NoSuchFile
from keydb import KeyDB
from keydb import KeyDBWindow
from keydb import KeyDBFactory
from fs import Path
from fs import ensure_link, ensure_symlink, ensure_readonly
from snapshot import TreeSnapshot
from snapshot import KeySnapshot
from snapshot import snap_reduce
from os.path import sep
from itertools import combinations
from func_prototypes import typed, returned
from functools import partial
import re

def _metadata_path(root):
  assert isinstance(root, Path)
  return root.join(".farmfs")

@returned(Path)
@typed(Path)
def _keys_path(root):
  return _metadata_path(root).join("keys")

@returned(Path)
@typed(Path)
def _snaps_path(root):
  return _metadata_path(root).join("snaps")

def mkfs(root, udd):
  assert isinstance(root, Path)
  assert isinstance(udd, Path)
  root.mkdir()
  _metadata_path(root).mkdir()
  _keys_path(root).mkdir()
  _snaps_path(root).mkdir()
  kdb = KeyDB(_keys_path(root))
  # Make sure root key is removed.
  kdb.delete("root")
  kdb.write('udd', str(udd))
  udd.mkdir()
  kdb.write('status', {})
  vol = FarmFSVolume(root)

@returned(basestring)
@typed(basestring, int, int)
def _checksum_to_path(checksum, num_segs=3, seg_len=3):
  segs = [ checksum[i:i+seg_len] for i in range(0, min(len(checksum), seg_len * num_segs), seg_len)]
  segs.append(checksum[num_segs*seg_len:])
  return sep.join(segs)

_sep_replace_ = re.compile(sep)
@returned(basestring)
@typed(basestring)
def _remove_sep_(path):
    return _sep_replace_.subn("",path)[0]

def reverser(num_segs=3):
  r = re.compile("((\/([0-9]|[a-f])+){%d})$" % (num_segs+1))
  def checksum_from_link(link):
    m = r.search(str(link))
    if (m):
      csum_slash = m.group()[1:]
      csum = _remove_sep_(csum_slash)
      return csum
    else:
      raise ValueError("link %s checksum didn't parse" %(link))
  return checksum_from_link

def _validate_checksum(link2csum, path):
  csum = path.checksum()
  link_csum = link2csum(path)
  return csum == link_csum

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

def encode_snapshot(snap):
  return map(lambda x: x.get_dict(), snap)

def decode_snapshot(splitter, reverser, data):
  return KeySnapshot(data, splitter, reverser)

class FarmFSVolume:
  def __init__(self, root):
    assert isinstance(root, Path)
    self.root = root
    self.mdd = _metadata_path(root)
    self.keydb = KeyDB(_keys_path(root))
    self.udd = Path(self.keydb.read('udd'))
    self.reverser = reverser()
    self.snapdb = KeyDBFactory(KeyDBWindow("snaps", self.keydb), encode_snapshot, partial(decode_snapshot, _checksum_to_path, self.reverser))
    self.remotedb = KeyDBFactory(KeyDBWindow("remotes", self.keydb), encode_volume, decode_volume)


    exclude_file = Path('.farmignore', self.root)
    self.exclude = [str(self.mdd)]
    try:
        with exclude_file.open('r') as exclude_fd:
          for pattern in exclude_fd.readlines():
            pattern = str(Path(pattern.strip(), root))
            self.exclude.append(pattern)
    except IOError as e:
      if e.errno == NoSuchFile:
          pass
      else: raise e


  """Yield set of files not backed by FarmFS under path"""
  def thawed(self, path):
    for (entry, type_) in path.entries(self.exclude):
      if type_ == "file":
        yield entry

  """Yield set of files backed by FarmFS under path"""
  def frozen(self, path):
    for (entry, type_) in path.entries(self.exclude):
      if type_ == "link":
        yield entry

  #NOTE: This assumes a posix storage engine.
  def freeze(self, path):
    assert isinstance(path, Path)
    assert isinstance(self.udd, Path)
    csum = path.checksum()
    blob = self.udd.join(_checksum_to_path(csum))
    duplicate = blob.exists()
    if not duplicate:
      ensure_link(blob, path)
      ensure_readonly(blob)
    ensure_symlink(path, blob)
    ensure_readonly(path)
    return {"path":path, "csum":csum, "was_dup":duplicate}

  #Note: This assumes a posix storage engine.
  def thaw(self, user_path):
    assert isinstance(user_path, Path)
    csum_path = user_path.readlink()
    user_path.unlink()
    csum_path.copy(user_path)
    return user_path

  """Find all broken links and point them back at UDD"""
  def repair_link(self, path):
    assert(path.islink())
    oldlink = path.readlink()
    if oldlink.isfile():
        print "Link %s is ok" % path
        return
    csum = self.reverser(oldlink)
    newlink = Path(_checksum_to_path(csum), self.udd) # Should be part of volume.
    if not newlink.isfile():
      raise ValueError("%d is missing, cannot relink" % newlink)
    else:
      print "Relinking %s from %s to %s" % (path, oldlink, newlink)
      path.unlink()
      path.symlink(newlink)

  def check_userdata_hashes(self):#TODO MAKE A FUNCTOR
    """Make sure all backed file hashes match thier file contents"""
    link2csum = reverser()
    for (path, type_) in self.udd.entries():
      if type_ == "file":
        if not _validate_checksum(link2csum, path):
          yield path

  def check_link(self, udd_name):
    """Returns true if link is valid, false if invalid"""
    full_path = self.udd.join(udd_name)
    return full_path.exists();
  """
  TODO: There is a problem in checklinks where snaps and trees seem different.
  ('CORRUPTION: broken link in ', u'd41/d8c/d98/f00b204e9800998ecf8427e')
  ('CORRUPTION: broken link in ', u'/d41/d8c/d98/f00b204e9800998ecf8427e')
  """
  def check_links(self): #TODO MAKE A FUNCTOR.
    """Make sure that all links in the tree and in all snaps are backed in userdata"""
    for name in self.count().keys():
      if not self.check_link(name):
        yield name

  def fsck(self):
    for bad_link in self.check_links():
      yield "CORRUPTION: broken link in ", bad_link
    for bad_hash in self.check_userdata_hashes():
      yield "CORRUPTION: checksum mismatch in ", bad_hash

  """Get a snap object which represents the tree of the volume."""
  def tree(self):
    root = self.root
    udd = self.udd
    tree_snap = TreeSnapshot(root, udd, self.exclude, reverser=self.reverser)
    return tree_snap

  """Return a checksum_path -> count map for each unique file backed by FarmFS"""
  #TODO would be good to move this out of volume.
  #TODO would be good to turn this into a more composable design.
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
    for (path, type_) in self.root.entries(self.exclude):
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
