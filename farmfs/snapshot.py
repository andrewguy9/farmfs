from keydb import KeyDB, KeyDBWindow
from fs import Path, ensure_absent, ensure_dir, ensure_symlink, target_exists

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
          ud_path = entry.readlink().relative_to(udd)
        elif type_ == "dir":
          ud_path = None
        else:
          raise ValueError("Encounted unexpected type %s for path %s" % (type_, entry))
        yield SnapshotItem(tree_path, type_, ud_path)
    return tree_snap_iterator()

class KeySnapshot(Snapshot):
  def __init__(self, keydb, name):
    assert isinstance(name, basestring)
    self.db = keydb
    self.name = name

  def __iter__(self):
    data = self.db.read(self.name)
    assert data is not None, "Failed to read snap data from db"
    def key_snap_iterator():
      for path, type_, ud_path in data:
        yield SnapshotItem(path, type_, ud_path)
    return key_snap_iterator()

SNAP_PATH="snaps"
class SnapshotDatabase:
  def __init__(self, keydb):
    assert isinstance(keydb, KeyDB)
    self.window = KeyDBWindow(SNAP_PATH, keydb)

  def list(self):
    return self.window.list()

  def delete(self, name):
    self.window.delete(name)

  #TODO I DONT THINK THIS SHOULD BE A PROPERTY OF THE DB UNLESS WE HAVE SOME ITERATOR BASED
  #     RECORD TYPE.
  def save(self, name, snap):
    l = []
    for i in snap:
      l.append( i.get_tuple() )
    self.window.write(name, l)

  def get(self, name):
    return KeySnapshot(self.window, name)

def snap_reduce(snaps):
  counts = {}
  # Now we walk the paths reducing the unique userdata paths we encounter.
  for snap in snaps:
    assert isinstance(snap, Snapshot)
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

  def apply(self, root, udd):
    assert isinstance(root, Path)
    assert isinstance(udd, Path)
    path = root.join(self._path)
    if self._blob is not None:
      blob = udd.join(self._blob)
    else:
      blob = None
    if self._mode == self.REMOVED:
      print "Removing %s" % self._path
      ensure_absent(path)
    elif self._mode == self.DIR:
      print "mkdir %s" % self._path
      ensure_dir(path)
      pass
    elif self._mode == self.LINK:
      print "mklink %s -> %s" % (self._path, self._blob)
      ensure_symlink(path, blob)
    else:
      raise ValueError("Unknown mode in SnapDelta: %s" % self._mode)

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
            yield SnapDelta(t._path, t._type, t._ref)
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

def snap_restore(root, tree, udd, snap):
  assert isinstance(root, Path)
  assert isinstance(tree, TreeSnapshot)
  assert isinstance(udd, Path)
  assert isinstance(snap, KeySnapshot)
  deltas = list(snap_diff(tree, snap))
  for delta in deltas:
    print delta
  for delta in deltas:
    delta.apply(root, udd)
