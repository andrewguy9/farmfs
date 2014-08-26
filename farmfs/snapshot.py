from fs import entries
from os import readlink
from keydb import KeyDB

class SnapshotItem:
  def __init__(self, path, type_, ref):
    assert type_ in ["link", "file", "dir"], type_ #TODO ITS ARGUABLE THAT FILE SHOULD'NT BE ALLOWED.
    self._path = path
    self._type = type_
    self._ref = ref

  def get_tuple(self):
    return (self._path, self._type, self._ref)

  def is_file(self):
    return self._type == "file"

  def is_dir(self):
    return self._type == "dir"

  def is_link(self):
    return self._type == "link"

  def ref(self):
    assert self._type == "link", "Encountered unexpected type %s in SnapshotItem for path" % \
      (self._type, self._path)
    return self._ref

class Snapshot:
  pass

class TreeSnapshot(Snapshot):
  def __init__(self, paths, exclude):
    self.paths = paths
    self.exclude = exclude

  def __iter__(self):
    walk = entries(self.paths, self.exclude)
    def tree_snap_iterator():
      for path, type_ in walk:
        if type_ == "file":
          raise ValueError("Untracked file found: %s" % path)
        if type_ == "link":
          ud_path = readlink(path)
        if type_ == "dir":
          ud_path = None
        yield SnapshotItem(path, type_, ud_path)
    return tree_snap_iterator()

class KeySnap(Snapshot):
  def __init__(self, keydb, name):
    self.db = keydb
    self.name = name

  def __iter__(self):
    def key_snap_iterator():
      data = self.db.read(self.name)
      for path, type_, ud_path in data:
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

def snap_reduce(hash_paths, snaps):
  counts = {}
  # We populate counts with all hash paths from the userdata directory.
  for (path, type_) in entries(hash_paths):
    if type_ == "file":
      counts[path]=0
    elif type_ == "dir":
      pass
    else:
      raise ValueError("%s is f invalid type %s" % (path, type_))
  # Now we walk the paths reducing the unique userdata paths we encounter.
  for snap in snaps:
    assert isinstance(snap, Snapshot)
    for i in snap.__iter__():
      assert isinstance(i, SnapshotItem)
      if i.is_link():
        try:
          counts[i.ref()]+=1
        except KeyError:
          raise ValueError("Encounted unexpected link: %s from file %s" % (i._type, i._path))
      elif i.is_dir():
        pass
      else:
        raise ValueError("Encounted unexpected type: %s from file %s" % (i._type, i._path))
  return counts
