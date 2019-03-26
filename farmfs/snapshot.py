from fs import Path
from func_prototypes import typed
from delnone import delnone

class SnapshotItem:
  def __init__(self, path, type, csum=None):
    assert type in ["link", "dir"], type
    assert isinstance(path, basestring)
    if type == "link":
      if csum is None:
        raise ValueError("checksum should be specified for links")
    self._path = path
    self._type = type
    self._csum = csum

  def __cmp__(self, other):
    assert other is None or isinstance(other, SnapshotItem)
    if other is None:
      return -1
    self_path = Path(self._path)
    other_path = Path(other._path)
    return cmp(self_path, other_path)

  def get_tuple(self):
    return (self._path, self._type, self._csum)

  def get_dict(self):
    return delnone(dict(path=self._path,
            type=self._type,
            csum=self._csum))

  def is_dir(self):
    return self._type == "dir"

  def is_link(self):
    return self._type == "link"

  def csum(self):
    assert self._type == "link", "Encountered unexpected type %s in SnapshotItem for path %s" % (self._type, self._path)
    return self._csum

  def __unicode__(self):
    return u'<%s %s %s>' % (self._type, self._path, self._csum)

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
      last_path = None # Note: last_path is just used to debug snapshot order issues. Remove once we have confidence.
      for path, type_ in root.entries(exclude):
        if last_path:
          assert last_path < path, "Order error: %s < %s" % (last_path, Path)
        last_path = path
        if type_ == "link":
          ud_str = self.reverser(path.readlink().relative_to(udd))
        elif type_ == "dir":
          ud_str = None
        elif type_ == "file":
          continue
        else:
          raise ValueError("Encounted unexpected type %s for path %s" % (type_, entry))
        yield SnapshotItem(path.relative_to(root), type_, ud_str)
    return tree_snap_iterator()

class KeySnapshot(Snapshot):
  def __init__(self, data, name, reverser):
    self.data = data
    self._reverser = reverser
    self._name = name

  def __iter__(self):
    def key_snap_iterator():
      for item in self.data:
        if isinstance(item, list):
          assert len(item) == 3
          (path_str, type_, ref) = item
          if ref is not None:
            csum = self._reverser(ref)
          else:
            csum = None
          parsed = SnapshotItem(path_str, type_, csum)
        elif isinstance(item, dict):
          parsed = SnapshotItem(**item)
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

