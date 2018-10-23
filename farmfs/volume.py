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
from farmfs.util import *
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
      parent = str(Path(path).parent()) #TODO this is illicit creation of Path, putting keys relative to abs root!
      try:
        dirs[parent].update([ref])
      except KeyError:
        dirs[parent] = set([ref])
  return dirs

def encode_volume(vol):
  return str(vol.root)

def decode_volume(vol, key):
  return FarmFSVolume(Path(vol))

def encode_snapshot(snap):
  return map(lambda x: x.get_dict(), snap)

def decode_snapshot(splitter, reverser, data, key):
  return KeySnapshot(data, key, splitter, reverser)

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

  def csum_to_name(self, csum):
    """Return string name of link relative to udd"""
    #TODO someday when csums are parameterized, we inject the has params here.
    return _checksum_to_path(csum)

  def csum_to_path(self, csum):
    """Return absolute Path to a blob given a csum"""
    return Path(self.csum_to_name(csum), self.udd)

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
    blob = self.csum_to_path(csum)
    assert blob == self.udd.join(_checksum_to_path(csum))
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
    newlink = self.csum_to_path(csum)
    assert newlink == Path(_checksum_to_path(csum), self.udd)
    if not newlink.isfile():
      raise ValueError("%d is missing, cannot relink" % newlink)
    else:
      print "Relinking %s from %s to %s" % (path, oldlink, newlink)
      path.unlink()
      path.symlink(newlink)

  def check_userdata_hashes(self):
    select_files = partial(filter, lambda x: x[1] == "file")
    get_path = fmap(lambda x: x[0])
    link2csum = reverser() #Get from volume?
    checker = compose(invert, partial(_validate_checksum, link2csum))
    select_broken = partial(filter, checker)
    return transduce(
        select_files,
        get_path,
        select_broken,
        fmap(link2csum)
        )(self.udd.entries())

  def check_link(self, udd_path):
    """Returns true if link is valid, false if invalid"""
    assert isinstance(udd_path, Path)
    return udd_path.exists();

  def check_links(self):
    tree = self.tree()
    snaps = map(lambda x: self.snapdb.read(x), self.snapdb.list())
    select_links = partial(filter, lambda x: x.is_link())
    get_checksum = lambda x:x.csum()
    groupby_checksum = partial(groupby, get_checksum)
    select_broken = partial(filter,
            lambda (csum, items): not self.csum_to_path(csum).exists())
    return transduce(
        concat,
        select_links,
        groupby_checksum,
        select_broken,
        ) ([tree]+snaps)

  """Get a snap object which represents the tree of the volume."""
  def tree(self):
    tree_snap = TreeSnapshot(self.root, self.udd, self.exclude, reverser=self.reverser)
    return tree_snap

  #TODO DEPRICATE THIS?
  #TODO would be good to move this out of volume.
  #TODO would be good to turn this into a more composable design.
  def count(self):
    """Return a {checksum : count} for each unique file backed by FarmFS"""
    snaps = [self.tree()] + map(lambda x: self.snapdb.read(x), self.snapdb.list())
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
  def userdata_csums(self):
   # We populate counts with all hash paths from the userdata directory.
   for (path, type_) in self.udd.entries():
     assert isinstance(path, Path)
     if type_ == "file":
       yield self.reverser(path)
     elif type_ == "dir":
       pass
     else:
       raise ValueError("%s is f invalid type %s" % (path, type_))

  """Yields the names of files which are being garbage collected"""
  def gc(self):
    referenced_hashes = set(self.count().keys()) #TODO usage of count()
    udd_hashes = set(self.userdata_csums())
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
      intersection = len(sigs_a.intersection(sigs_b))
      count_a = len(sigs_a)
      count_b = len(sigs_b)
      yield (dir_a, count_a, dir_b, count_b, intersection)
