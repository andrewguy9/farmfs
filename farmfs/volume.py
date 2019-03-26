from errno import ENOENT as NoSuchFile
from keydb import KeyDB
from keydb import KeyDBWindow
from keydb import KeyDBFactory
from util import *
from fs import Path
from fs import ensure_absent, ensure_link, ensure_symlink, ensure_readonly, ensure_copy, ensure_dir
from snapshot import Snapshot, TreeSnapshot, KeySnapshot, SnapDelta
from os.path import sep
from itertools import combinations
from func_prototypes import typed, returned
from functools import partial
from itertools import ifilter
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

def directory_signatures(snap, root):
  dirs = {}
  for entry in snap:
    if entry.is_link():
      (path_str, _, csum) = entry.get_tuple()
      parent = root.join(path_str).parent()
      try:
        dirs[parent].update([csum])
      except KeyError:
        dirs[parent] = set([csum])
  return dirs

def encode_volume(vol):
  return str(vol.root)

def decode_volume(vol, key):
  return FarmFSVolume(Path(vol))

def encode_snapshot(snap):
  return map(lambda x: x.get_dict(), snap)

def decode_snapshot(reverser, data, key):
  return KeySnapshot(data, key, reverser)

class FarmFSVolume:
  def __init__(self, root):
    assert isinstance(root, Path)
    self.root = root
    self.mdd = _metadata_path(root)
    self.keydb = KeyDB(_keys_path(root))
    self.udd = Path(self.keydb.read('udd'))
    self.reverser = reverser()
    self.snapdb = KeyDBFactory(KeyDBWindow("snaps", self.keydb), encode_snapshot, partial(decode_snapshot, self.reverser))
    self.remotedb = KeyDBFactory(KeyDBWindow("remotes", self.keydb), encode_volume, decode_volume)
    self.check_userdata_blob = compose(invert, partial(_validate_checksum, self.reverser))

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
        print "Link %s is ok" % path #TODO printing
        return
    csum = self.reverser(oldlink)
    newlink = self.csum_to_path(csum)
    assert newlink == Path(_checksum_to_path(csum), self.udd)
    if not newlink.isfile():
      raise ValueError("%d is missing, cannot relink" % newlink)
    else:
      print "Relinking %s from %s to %s" % (path, oldlink, newlink) #TODO printing
      path.unlink()
      path.symlink(newlink)

  def userdata_files(self):
    select_files = partial(ifilter, lambda x: x[1] == "file")
    get_path = fmap(lambda x: x[0])
    select_userdata_files = transduce(
        select_files,
        get_path)
    return select_userdata_files(self.udd.entries())

  def check_link(self, udd_path):
    """Returns true if link is valid, false if invalid"""
    assert isinstance(udd_path, Path)
    return udd_path.exists();

  def link_checker(self):
    """Return a transducer which given a list of SnapshotItems, checks the links against the blobstore"""
    select_links = partial(ifilter, lambda x: x.is_link())
    get_checksum = lambda x:x.csum()
    groupby_checksum = partial(groupby, get_checksum)
    select_broken = partial(ifilter,
            lambda (csum, items): not self.csum_to_path(csum).exists())
    return transduce(
            select_links,
            groupby_checksum,
            select_broken)

  def trees(self):
    """Returns an iterator which lists all SnapshotItems from all local snaps + the working tree"""
    tree = self.tree()
    snaps = map(lambda x: self.snapdb.read(x), self.snapdb.list())
    return transduce(
      concat
      )([tree]+snaps)

  """Get a snap object which represents the tree of the volume."""
  def tree(self):
    tree_snap = TreeSnapshot(self.root, self.udd, self.exclude, reverser=self.reverser)
    return tree_snap

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
    items = self.trees()
    select_links = partial(ifilter, lambda x: x.is_link())
    get_csums = fmap(lambda item: item.csum())
    referenced_hashes = transduce(
            select_links,
            get_csums,
            uniq,
            set
            )(items)
    udd_hashes = set(self.userdata_csums())
    missing_data = referenced_hashes - udd_hashes
    assert len(missing_data) == 0, "Missing %s\nReferenced %s\nExisting %s\n" % (missing_data, referenced_hashes, udd_hashes)
    orphaned_csums = udd_hashes - referenced_hashes
    for csum in orphaned_csums:
      yield csum
      blob_path = self.csum_to_path(csum)
      blob_path.unlink(clean=self.udd)

  """Yields similarity data for directories"""
  def similarity(self):
    tree = self.tree()
    dir_sigs = directory_signatures(tree, self.root)
    combos = combinations(dir_sigs.items(), 2)
    for ((dir_a, sigs_a), (dir_b, sigs_b)) in combos:
      intersection = len(sigs_a.intersection(sigs_b))
      count_a = len(sigs_a)
      count_b = len(sigs_b)
      yield (dir_a, count_a, dir_b, count_b, intersection)

def tree_patcher(local_vol, remote_vol):
    return concatMap(partial(tree_patch, local_vol, remote_vol))

def noop():
    pass

@typed(FarmFSVolume, FarmFSVolume, SnapDelta)
def tree_patch(local_vol, remote_vol, delta):
  path = local_vol.root.join(delta._path)
  assert local_vol.root in path.parents(), "Tried to apply op to %s when root is %s" % (path, local_vol.root)
  if delta._csum is not None:
    dst_blob = local_vol.csum_to_path(delta._csum)
    src_blob = remote_vol.csum_to_path(delta._csum)
  else:
    dst_blob = None
    src_blob = None
  if delta._mode == delta.REMOVED:
    return [(partial(ensure_absent, path), "Apply Removing %s" % delta._path)]
  elif delta._mode == delta.DIR:
    return [(partial(ensure_dir, path), "Apply mkdir %s" % delta._path)]
  elif delta._mode == delta.LINK:
    #TODO i've complected tree diff and blob-store diff.
    #TODO If order gets mixed up we might not copy data, or copy multiple times.
    ops = []
    if dst_blob.exists():
      ops.append((noop, "Apply No need to copy blob, already exists"))
    else:
      ops.append((partial(ensure_copy, dst_blob, src_blob), "Apply Blob missing from local, copying"))
    ops.append((partial(ensure_symlink, path, dst_blob), "Apply mklink %s -> %s" % (delta._path, delta._csum)))
    return ops
  else:
    raise ValueError("Unknown mode in SnapDelta: %s" % delta._mode)

@typed(Snapshot, Snapshot)
def tree_diff(tree, snap):
  tree_parts = tree.__iter__()
  snap_parts = snap.__iter__()
  t = None
  s = None
  while True:
    if t == None:
      try:
        t = tree_parts.next()
      except StopIteration:
        pass
    if s == None:
      try:
        s = snap_parts.next()
      except StopIteration:
        pass
    if t is None and s is None:
      return # We are done!
    elif t is not None and s is not None:
      # We have components from both sides!
      if t._path < s._path:
        # The tree component is not present in the snap. Delete it.
        yield SnapDelta(t._path, SnapDelta.REMOVED, None)
        t = None
      elif s._path < t._path:
        # The snap component is not part of the tree. Create it
        yield SnapDelta(s._path, s._type, s._csum)
        s = None
      elif t._path == s._path:
        if t._type == "dir" and s._type == "dir":
          pass
        elif t._type == "link" and s._type == "link":
          if t.csum() == s.csum():
            pass
          else:
            yield SnapDelta(t._path, t._type, s._csum)
        elif t._type == "link" and s._type == "dir":
          yield SnapDelta(t._path, SnapDelta.REMOVED, None)
          yield SnapDelta(s._path, SnapDelta.DIR, None)
        elif t._type == "dir" and s._type == "link":
          yield SnapDelta(t._path, SnapDelta.REMOVED, None)
          yield SnapDelta(s._path, SnapDelta.LINK, s._csum)
        else:
          raise ValueError("Unable to process tree/snap: unexpected types:", s._type, t._type)
        s = None
        t = None
      else:
        raise ValueError("Found pair that doesn't respond to > < == cases")
    elif t is not None:
      yield SnapDelta(t._path, SnapDelta.REMOVED, None)
      t = None
    elif s is not None:
      yield SnapDelta(s._path, s._type, s._csum)
      s = None
    else:
      raise ValueError("Encountered case where s t were both not none, but neither of them were none.")

