from keydb import KeyDB #TODO TRY AND FORGET ABOUT KEYDB
from fs import Path, ensure_absent, ensure_dir, ensure_symlink, ensure_copy, target_exists
from func_prototypes import typed

class SnapshotItem:
  def __init__(self, path, type_, ref):
    assert type_ in ["link", "dir"], type_
    assert isinstance(path, basestring)
    assert (ref is None) or isinstance(ref, basestring)
    if type_ == "link":
      assert ref is not None
    self._path = path
    self._type = type_
    self._ref = ref

  def get_tuple(self):
    if self._ref:
      ref = self._ref
    else:
      ref = None
    return (self._path, self._type, ref)

  def is_dir(self):
    return self._type == "dir"

  def is_link(self):
    return self._type == "link"

  def ref(self):
    assert self._type == "link", "Encountered unexpected type %s in SnapshotItem for path" % \
      (self._type, self._path)
    return self._ref

  def __unicode__(self):
    return u'<%s %s %s>' % (self._type, self._path, self._ref)

  def __str__(self):
    return unicode(self).encode('utf-8')

def encode_snapshot(snap):
  return map(lambda x: x.get_tuple(), snap)

def decode_snapshot(data):
  return KeySnapshot(data)

class Snapshot:
  pass

class TreeSnapshot(Snapshot):
  def __init__(self, root, udd, exclude):
    assert isinstance(root, Path)
    self.root = root
    self.udd = udd
    self.exclude = exclude

  def __iter__(self):
    root = self.root
    udd = self.udd
    exclude = self.exclude
    def tree_snap_iterator():
      for entry, type_ in root.entries(exclude):
        tree_path = entry.relative_to(root)
        if type_ == "link":
          ud_path = entry.readlink(entry.parent()).relative_to(udd) #TODO THIS MIGHT BE WRONG.
        elif type_ == "dir":
          ud_path = None
        else:
          raise ValueError("Encounted unexpected type %s for path %s" % (type_, entry))
        yield SnapshotItem(tree_path, type_, ud_path)
    return tree_snap_iterator()

class KeySnapshot(Snapshot):
  def __init__(self, data):
    self.data = data

  def __iter__(self):
    def key_snap_iterator():
      for path, type_, ud_path in self.data:
        yield SnapshotItem(path, type_, ud_path)
    return key_snap_iterator()

def snap_reduce(snaps):
  counts = {}
  # Now we walk the paths reducing the unique userdata paths we encounter.
  for snap in snaps:
    assert isinstance(snap, Snapshot), type(snap)
    for i in snap:
      assert isinstance(i, SnapshotItem)
      if i.is_link():
        try:
          counts[i.ref()] += 1
        except KeyError:
          counts[i.ref()] = 1
      elif i.is_dir():
        pass
      else:
        raise ValueError("Encounted unexpected type: %s from file %s" % (i._type, i._path))
  return counts

class SnapDelta:
  REMOVED='removed'
  DIR='dir'
  LINK='link'
  _modes = [REMOVED, DIR, LINK]
  def __init__(self, path, mode, blob):
    assert isinstance(path, basestring)
    assert mode in self._modes
    if mode == self.LINK:
      assert blob is not None
    else:
      assert blob is None
    self._path = path
    self._mode = mode
    self._blob = blob

  def __str__(self):
    return "%s %s %s" % (self._mode, self._path, self._blob)

  def __repr__(self):
    return str(self)

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
        yield SnapDelta(s._path, s._type, s._ref)
        s = None
      elif t._path == s._path:
        if t._type == "dir" and s._type == "dir":
          pass
        elif t._type == "link" and s._type == "link":
          if t.ref() == s.ref():
            pass
          else:
            yield SnapDelta(t._path, t._type, s._ref)
        elif t._type == "link" and s._type == "dir":
          yield SnapDelta(t._path, SnapDelta.REMOVED, None)
          yield SnapDelta(s._path, SnapDelta.DIR, None)
        elif t._type == "dir" and s._type == "link":
          yield SnapDelta(t._path, SnapDelta.REMOVED, None)
          yield SnapDelta(s._path, SnapDelta.LINK, s._ref)
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
      yield SnapDelta(s._path, s._type, s._ref)
      s = None
    else:
      raise ValueError("Encountered case where s t were both not none, but neither of them were none.")

def pull_apply(delta, local_root, local_udd, remote_udd):
  assert isinstance(local_root, Path)
  assert isinstance(local_udd, Path)
  assert isinstance(remote_udd, Path)
  path = local_root.join(delta._path)
  if delta._blob is not None:
    dst_blob = local_udd.join(delta._blob)
    src_blob = remote_udd.join(delta._blob)
  else:
    dst_blob = None
    src_blob = None
  if delta._mode == delta.REMOVED:
    print "Removing %s" % delta._path
    ensure_absent(path)
  elif delta._mode == delta.DIR:
    print "mkdir %s" % delta._path
    ensure_dir(path)
  elif delta._mode == delta.LINK:
    print "mklink %s -> %s" % (delta._path, delta._blob)
    if dst_blob.exists():
      print "No need to copy blob, already exists"
    else:
      print "Blob missing from local, copying"
      ensure_copy(dst_blob, src_blob)
    ensure_symlink(path, dst_blob)
  else:
    raise ValueError("Unknown mode in SnapDelta: %s" % delta._mode)

@typed(Path, TreeSnapshot, Path, Snapshot, Path)
def snap_pull(local_root, local_tree, local_udd, remote_snap, remote_udd):
  deltas = list(snap_diff(local_tree, remote_snap))
  for delta in deltas:
    pull_apply(delta, local_root, local_udd, remote_udd)

