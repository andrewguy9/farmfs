from fs import Path, ensure_absent, ensure_dir, ensure_symlink, ensure_copy, target_exists
from func_prototypes import typed
from delnone import delnone
# from farmfs.volume import FarmFSVolume

class SnapshotItem:
  def __init__(self, path, type, ref=None, csum=None, splitter=None, reverser=None, snap=None):
    assert type in ["link", "dir"], type
    assert isinstance(path, basestring)
    assert (ref is None) or isinstance(ref, basestring)
    assert (snap is None) or isinstance(snap, basestring)
    if type == "link":
      if ref is not None and csum is not None:
        raise ValueError("Either ref or csum should be specified for links")
      elif ref:
        csum = reverser(ref)
      elif csum:
        ref = splitter(csum)
      else:
        raise ValueError("Either ref or csum are required for links")
    self._path = path
    self._type = type
    self._ref = ref
    self._csum = csum
    self._snap = snap

  def get_tuple(self):
    if self._ref:
      ref = self._ref
    else:
      ref = None
    return (self._path, self._type, ref)

  def get_dict(self):
    return delnone(dict(path=self._path,
            type=self._type,
            csum=self._csum,
            snap=self._snap))

  def is_dir(self):
    return self._type == "dir"

  def is_link(self):
    return self._type == "link"

  def csum(self):
    assert self._type == "link", "Encountered unexpected type %s in SnapshotItem for path %s" % (self._type, self._path)
    return self._csum

  def __unicode__(self):
    return u'<%s %s %s>' % (self._type, self._path, self._ref)

  def __str__(self):
    return unicode(self).encode('utf-8')

class Snapshot:
  pass

class TreeSnapshot(Snapshot):
  def __init__(self, root, udd, exclude, reverser):
    assert isinstance(root, Path)
    self.root = root
    self.udd = udd
    self.exclude = exclude
    self.reverser = reverser

  def __iter__(self):
    root = self.root
    udd = self.udd
    exclude = self.exclude
    def tree_snap_iterator():
      for entry, type_ in root.entries(exclude):
        tree_path = entry.relative_to(root)
        if type_ == "link":
          ud_path = entry.readlink().relative_to(udd)
        elif type_ == "dir":
          ud_path = None
        elif type_ == "file":
          continue
        else:
          raise ValueError("Encounted unexpected type %s for path %s" % (type_, entry))
        yield SnapshotItem(tree_path, type_, ud_path, reverser=self.reverser)
    return tree_snap_iterator()

class KeySnapshot(Snapshot):
  def __init__(self, data, name, splitter=None, reverser=None):
    self.data = data
    self._splitter = splitter
    self._reverser = reverser
    self._name = name

  def __iter__(self):
    def key_snap_iterator():
      for item in self.data:
        if isinstance(item, list):
          assert len(item) == 3
          yield SnapshotItem(*item, reverser=self._reverser)
        elif isinstance(item, dict):
          params = dict(item, splitter=self._splitter, reverser=self._reverser, snap=self._name)
          yield SnapshotItem(**params)
    return key_snap_iterator()

class SnapDelta:
  REMOVED='removed'
  DIR='dir'
  LINK='link'
  _modes = [REMOVED, DIR, LINK]
  def __init__(self, path, mode, csum):
    assert isinstance(path, basestring)
    assert isinstance(mode, basestring) and mode in self._modes
    if mode == self.LINK:
      assert csum is not None and csum.count("/") == 0
    else:
      assert csum is None
    self._path = path
    self._mode = mode
    self._csum = csum

  def __str__(self):
    return "%s %s %s" % (self._mode, self._path, self._csum)

  def __repr__(self):
    return str(self)

#TODO Returns  link /children/inside /c76/472/ba1/90d1b56c59c51b6295e0677
#TODO should be dicts...
@typed(Snapshot, Snapshot)
def snap_diff(tree, snap):
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
    print "comp", t, "vs", s
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

def pull_apply(delta, local_vol, remote_vol):
  isinstance(delta, SnapDelta)
  # isinstance(local_vol, FarmFSVolume)
  # isinstance(remote_vol, FarmFSVolume)
  path = local_vol.root.join(delta._path)
  assert local_vol.root in path.parents(), "Tried to apply op to %s when root is %s" % (path, local_vol.root)
  if delta._csum is not None:
    dst_blob = local_vol.csum_to_path(delta._csum)
    src_blob = remote_vol.csum_to_path(delta._csum)
  else:
    dst_blob = None
    src_blob = None
  if delta._mode == delta.REMOVED:
    print "Apply", "Removing %s" % delta._path
    ensure_absent(path)
    # print "Apply", "Removing %s complete" % delta._path
  elif delta._mode == delta.DIR:
    print "Apply", "mkdir %s" % delta._path
    ensure_dir(path)
  elif delta._mode == delta.LINK:
    print "Apply", "mklink %s -> %s" % (delta._path, delta._csum)
    if dst_blob.exists():
      print "Apply", "No need to copy blob, already exists"
    else:
      print "Apply", "Blob missing from local, copying"
      ensure_copy(dst_blob, src_blob)
    ensure_symlink(path, dst_blob)
  else:
    raise ValueError("Unknown mode in SnapDelta: %s" % delta._mode)

def snap_pull(local_vol, local_tree, remote_vol, remote_tree):
  # assert isinstance(local_vol, FarmFSVolume)
  assert isinstance(local_tree, TreeSnapshot)
  # assert isinstance(remote_vol, FarmFSVolume)
  assert isinstance(remote_tree, Snapshot)
  deltas = snap_diff(local_tree, remote_tree)
  for delta in list(deltas):
    print "diff", delta
    pull_apply(delta, local_vol, remote_vol)

