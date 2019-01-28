from fs import Path
from func_prototypes import typed
from delnone import delnone

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

  def __cmp__(self, other):
    assert other is None or isinstance(other, SnapshotItem)
    if other is None:
      return -1
    self_path = Path(self._path)
    other_path = Path(other._path)
    return cmp(self_path, other_path)

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
      last_path = None
      for path, type_ in root.entries(exclude):
        tree_str = path.relative_to(root)
        if last_path:
          assert last_path < path, "Order error: %s < %s" % (last_path, Path)
        last_path = path
        if type_ == "link":
          ud_str = path.readlink().relative_to(udd)
        elif type_ == "dir":
          ud_str = None
        elif type_ == "file":
          continue
        else:
          raise ValueError("Encounted unexpected type %s for path %s" % (type_, entry))
        yield SnapshotItem(tree_str, type_, ud_str, reverser=self.reverser)
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
          parsed = SnapshotItem(*item, reverser=self._reverser)
        elif isinstance(item, dict):
          params = dict(item, splitter=self._splitter, reverser=self._reverser, snap=self._name)
          parsed = SnapshotItem(**params)
        yield parsed
    return iter(sorted(key_snap_iterator()))

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

