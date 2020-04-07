from errno import ENOENT as NoSuchFile
from farmfs.keydb import KeyDB
from farmfs.keydb import KeyDBWindow
from farmfs.keydb import KeyDBFactory
from farmfs.util import *
from farmfs.fs import Path
from farmfs.fs import ensure_absent, ensure_link, ensure_symlink, ensure_readonly, ensure_copy, ensure_dir
from farmfs.snapshot import Snapshot, TreeSnapshot, KeySnapshot, SnapDelta, encode_snapshot, decode_snapshot
from os.path import sep
from itertools import combinations, chain
try:
    from itertools import imap
except ImportError:
    # On python3 map is lazy.
    imap = map
from func_prototypes import typed, returned
from functools import partial
try:
    from itertools import ifilter
except ImportError:
    ifilter = filter
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
  kdb.write('udd', safetype(udd))
  udd.mkdir()
  kdb.write('status', {})
  vol = FarmFSVolume(root)

@returned(safetype)
@typed(safetype, int, int)
def _checksum_to_path(checksum, num_segs=3, seg_len=3):
  segs = [ checksum[i:i+seg_len] for i in range(0, min(len(checksum), seg_len * num_segs), seg_len)]
  segs.append(checksum[num_segs*seg_len:])
  return sep.join(segs)

_sep_replace_ = re.compile(sep)
@returned(safetype)
@typed(safetype)
def _remove_sep_(path):
    return _sep_replace_.subn("",path)[0]

def reverser(num_segs=3):
  r = re.compile("((\/([0-9]|[a-f])+){%d})$" % (num_segs+1))
  def checksum_from_link(link):
    m = r.search(safetype(link))
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
  return safetype(vol.root)

def decode_volume(vol, key):
  return FarmFSVolume(Path(vol))

def encode_snapshot(snap):
  return list(imap(lambda x: x.get_dict(), snap))

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
    self.exclude = [safetype(self.mdd)]
    try:
        with exclude_file.open('rb') as exclude_fd:
          for raw_pattern in exclude_fd.readlines():
            pattern = ingest(raw_pattern.strip())
            excluded = safetype(Path(pattern, root))
            self.exclude.append(excluded)
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

  def repair_link(self, path):
    """Find all broken links and point them back at UDD"""
    assert(path.islink())
    oldlink = path.readlink()
    if oldlink.isfile():
        return
    csum = self.reverser(oldlink)
    newlink = self.csum_to_path(csum)
    assert newlink == Path(_checksum_to_path(csum), self.udd)
    if not newlink.isfile():
      raise ValueError("%s is missing, cannot relink" % newlink)
    else:
      path.unlink()
      path.symlink(newlink)
      return newlink

  def userdata_files(self):
    select_files = partial(ifilter, lambda x: x[1] == "file")
    get_path = fmap(lambda x: x[0])
    select_userdata_files = pipeline(
        select_files,
        get_path)
    return select_userdata_files(self.udd.entries())

  def link_checker(self):
    """Return a pipeline which given a list of SnapshotItems, returns the SnapshotItems with broken links to the blobstore"""
    select_links = partial(ifilter, lambda x: x.is_link())
    is_broken = lambda x: not self.blob_checker(x.csum())
    select_broken = partial(ifilter, is_broken)
    return pipeline(
            select_links,
            select_broken)

  def blob_checker(self, csum):
    """Returns true if the csum is in the store, false otherwise"""
    return self.csum_to_path(csum).exists()

  def trees(self):
    """Returns an iterator which contains all trees for the volume.
    The Local tree and all the snapshots"""
    tree = self.tree()
    snaps = imap(lambda x: self.snapdb.read(x), self.snapdb.list())
    return chain([tree], snaps)

  def items(self):
    """Returns an iterator which lists all SnapshotItems from all local snaps + the working tree"""
    return pipeline(
      concat)(self.trees())

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

  """ Yield all the relative paths (safetype) for all the files in the userdata store."""
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
    items = self.items()
    select_links = partial(ifilter, lambda x: x.is_link())
    get_csums = fmap(lambda item: item.csum())
    referenced_hashes = pipeline(
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

@typed(FarmFSVolume, FarmFSVolume)
def tree_patcher(local_vol, remote_vol):
    return fmap(partial(tree_patch, local_vol, remote_vol))

def noop():
    pass

def blob_import(src_blob, dst_blob):
  if dst_blob.exists():
    return "Apply No need to copy blob, already exists"
  else:
    ensure_copy(dst_blob, src_blob)
    return "Apply Blob missing from local, copying"

@typed(FarmFSVolume, FarmFSVolume, SnapDelta)
def tree_patch(local_vol, remote_vol, delta):
  path = delta.path(local_vol.root)
  assert local_vol.root in path.parents(), "Tried to apply op to %s when root is %s" % (path, local_vol.root)
  if delta.csum is not None:
    dst_blob = local_vol.csum_to_path(delta.csum)
    src_blob = remote_vol.csum_to_path(delta.csum)
  else:
    dst_blob = None
    src_blob = None
  if delta.mode == delta.REMOVED:
    return (noop, partial(ensure_absent, path), ("Apply Removing %s", path))
  elif delta.mode == delta.DIR:
    return (noop, partial(ensure_dir, path), ("Apply mkdir %s", path))
  elif delta.mode == delta.LINK:
    blob_op = partial(blob_import, src_blob, dst_blob)
    tree_op = partial(ensure_symlink, path, dst_blob)
    tree_desc = ("Apply mklink %s -> " + delta.csum, path)
    return (blob_op, tree_op, tree_desc)
  else:
    raise ValueError("Unknown mode in SnapDelta: %s" % delta.mode)

#TODO yields lots of SnapDelta. Maybe in wrong file?
@typed(Snapshot, Snapshot)
def tree_diff(tree, snap):
  tree_parts = iter(tree)
  snap_parts = iter(snap)
  t = None
  s = None
  while True:
    if t == None:
      try:
        t = next(tree_parts)
      except StopIteration:
        pass
    if s == None:
      try:
        s = next(snap_parts)
      except StopIteration:
        pass
    if t is None and s is None:
      return # We are done!
    elif t is not None and s is not None:
      # We have components from both sides!
      if t < s:
        # The tree component is not present in the snap. Delete it.
        yield SnapDelta(t.pathStr(), SnapDelta.REMOVED)
        t = None
      elif s < t:
        # The snap component is not part of the tree. Create it
        yield SnapDelta(*s.get_tuple())
        s = None
      elif t == s:
        if t.is_dir() and s.is_dir():
          pass
        elif t.is_link() and s.is_link():
          if t.csum() == s.csum():
            pass
          else:
            change = t.get_dict()
            change['csum'] = s.csum()
            yield SnapDelta(t._path, t._type, s._csum)
        elif t.is_link() and s.is_dir():
          yield SnapDelta(t.pathStr(), SnapDelta.REMOVED)
          yield SnapDelta(s.pathStr(), SnapDelta.DIR)
        elif t.is_dir() and s.is_link():
          yield SnapDelta(t.pathStr(), SnapDelta.REMOVED)
          yield SnapDelta(s.pathStr(), SnapDelta.LINK, s.csum())
        else:
          raise ValueError("Unable to process tree/snap: unexpected types:", s.get_dict()['type'], t.get_dict()['type'])
        s = None
        t = None
      else:
        raise ValueError("Found pair that doesn't respond to > < == cases")
    elif t is not None:
      yield SnapDelta(t.pathStr(), SnapDelta.REMOVED)
      t = None
    elif s is not None:
      yield SnapDelta(*s.get_tuple())
      s = None
    else:
      raise ValueError("Encountered case where s t were both not none, but neither of them were none.")

