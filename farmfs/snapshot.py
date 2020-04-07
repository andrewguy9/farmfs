from farmfs.fs import Path, LINK, DIR, FILE, ingest
from func_prototypes import typed
from delnone import delnone
from os.path import sep
from functools import total_ordering
from farmfs.util import safetype
from future.utils import python_2_unicode_compatible

try:
    from itertools import imap
except ImportError:
    # On python3 map is lazy.
    imap = map

@total_ordering
@python_2_unicode_compatible
class SnapshotItem:
  def __init__(self, path, type, csum=None):
    assert isinstance(type, safetype)
    assert type in [LINK, DIR], type
    if (isinstance(path, Path)):
        path = path._path #TODO reaching into path.
    assert isinstance(path, safetype)
    if type == LINK:
      if csum is None:
        raise ValueError("checksum should be specified for links")
    self._path = ingest(path)
    self._type = ingest(type)
    self._csum = csum and ingest(csum) # csum can be None.

  #TODO create a path comparator. cmp has different semantics.
  def __cmp__(self, other):
    assert other is None or isinstance(other, SnapshotItem)
    if other is None:
      return -1
    self_path = Path(self._path)
    other_path = Path(other._path)
    return self_path.__cmp__(other_path)

  def __eq__(self, other):
    return self.__cmp__(other) == 0

  def __ne__(self, other):
    return self.__cmp__(other) != 0

  def __lt__(self, other):
    return self.__cmp__(other) < 0

  def get_tuple(self):
    return (self._path, self._type, self._csum)

  def get_dict(self):
    return delnone(dict(path=self._path,
            type=self._type,
            csum=self._csum))

  def pathStr(self):
    assert isinstance(self._path, safetype)
    return self._path;

  def is_dir(self):
    return self._type == DIR

  def is_link(self):
    return self._type == LINK

  def csum(self):
    assert self._type == LINK, "Encountered unexpected type %s in SnapshotItem for path %s" % (self._type, self._path)
    return self._csum

  def __str__(self):
    return "<%s %s %s>" % (self._type, self._path, self._csum)

class Snapshot:
  pass

class TreeSnapshot(Snapshot):
  def __init__(self, root, udd, exclude, reverser):
    assert isinstance(root, Path)
    self.root = root
    self.udd = udd
    self.exclude = exclude
    self.reverser = reverser
    self.name = '<tree>'

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
        if type_ == LINK:
          ud_str = self.reverser(path.readlink().relative_to(udd))
        elif type_ == DIR:
          ud_str = None
        elif type_ == FILE:
          continue
        else:
          raise ValueError("Encounted unexpected type %s for path %s" % (type_, entry))
        yield SnapshotItem(path.relative_to(root), type_, ud_str)
    return tree_snap_iterator()

class KeySnapshot(Snapshot):
  def __init__(self, data, name, reverser):
    assert(data)
    self.data = data
    self._reverser = reverser
    self.name = name

  def __iter__(self):
    def key_snap_iterator():
      assert(self.data)
      for item in self.data:
        if isinstance(item, list):
          assert len(item) == 3
          (path_str, type_, ref) = item
          assert isinstance(path_str, safetype)
          assert isinstance(type_, safetype)
          if ref is not None:
            csum = self._reverser(ref)
            assert isinstance(csum, safetype)
          else:
            csum = None
          parsed = SnapshotItem(path_str, type_, csum)
        elif isinstance(item, dict):
          parsed = SnapshotItem(**item)
        yield parsed
    return iter(sorted(key_snap_iterator()))

@python_2_unicode_compatible
class SnapDelta:
  REMOVED=u'removed'
  DIR=DIR
  LINK=LINK
  _modes = [REMOVED, DIR, LINK]
  def __init__(self, pathStr, mode, csum=None):
    assert isinstance(pathStr, safetype), "didn't expect type %s" % type(pathStr)
    assert isinstance(mode, safetype) and mode in self._modes
    if mode == self.LINK:
      # Make sure that we are looking at a csum, not a path.
      assert csum is not None and csum.count(sep) == 0
    else:
      assert csum is None
    self._pathStr = pathStr
    self.mode = mode
    self.csum = csum

  def path(self, root):
    return root.join(self._pathStr)

  def __str__(self):
      return "{"+self.path("")+","+self.mode+","+self.csum+"}" # Not a great encoding.

def encode_snapshot(snap):
  return list(imap(lambda x: x.get_dict(), snap))

def decode_snapshot(splitter, reverser, data, key):
  return KeySnapshot(data, key, splitter, reverser)

