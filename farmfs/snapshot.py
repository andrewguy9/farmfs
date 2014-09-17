from keydb import KeyDB
from fs import Path, ensure_absent, ensure_dir, ensure_symlink

class SnapshotItem:
  def __init__(self, path, type_, ref):
    assert type_ in ["link", "dir"], type_
    assert isinstance(path, Path)
    assert (ref is None) or isinstance(ref, Path)
    if type_ == "link":
      assert ref is not None
    self._path = path
    self._type = type_
    self._ref = ref

  def get_tuple(self):
    if self._ref:
      ref = str(self._ref)
    else:
      ref = None
    return (str(self._path), str(self._type), ref)

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
  def __init__(self, paths, exclude):
    self.paths = paths
    self.exclude = exclude

  def __iter__(self):
    paths = self.paths
    exclude = self.exclude
    def tree_snap_iterator():
      for path in paths:
        for entry, type_ in path.entries(exclude):
          if type_ == "link":
            ud_path = entry.readlink()
          elif type_ == "dir":
            ud_path = None
          else:
            raise ValueError("Encounted unexpected type %s for path %s" % (type_, entry))
          yield SnapshotItem(entry, type_, ud_path)
    return tree_snap_iterator()

class KeySnap(Snapshot):
  def __init__(self, keydb, name):
    assert isinstance(name, basestring)
    self.db = keydb
    self.name = name

  def __iter__(self):
    data = self.db.read(self.name)
    assert data is not None, "Failed to read snap data from db"
    def key_snap_iterator():
      for path, type_, ud_path in data:
        path = Path(path)
        if ud_path is not None:
          ud_path = Path(ud_path)
        i = SnapshotItem(path, type_, ud_path)
        yield i
    return key_snap_iterator()

class SnapshotDatabase:
  def __init__(self, root):
    self.keydb = KeyDB(root)

  def list(self):
    return self.keydb.list()

  def delete(self, name):
    self.keydb.delete(name)

  #TODO I DONT THINK THIS SHOULD BE A PROPERTY OF THE DB UNLESS WE HAVE SOME ITERATOR BASED
  #     RECORD TYPE.
  def save(self, name, snap):
    l = []
    for i in snap:
      l.append( i.get_tuple() )
    self.keydb.write(name, l)

  def get(self, name):
    return KeySnap(self.keydb, name)

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
    assert isinstance(path, Path)
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

  def apply(self):
    if self._mode == self.REMOVED:
      print "Removing %s" % self._path
      ensure_absent(self._path)
    elif self._mode == self.DIR:
      print "mkdir %s" % self._path
      ensure_dir(self._path)
      pass
    elif self._mode == self.LINK:
      print "mklink %s -> %s" % (self._path, self._blob)
      ensure_symlink(self._path, self._blob)
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

def snap_restore(tree, snap):
  deltas = list(snap_diff(tree, snap))
  for delta in deltas:
    print delta
  for delta in deltas:
    delta.apply()
