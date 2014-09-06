from keydb import KeyDB
from fs import Path

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

  def make_missing(self):
    assert self._path.isfile()
    self._path.unlink() #TODO THIS PROBABLY NEEDS TO BE RECURSIVE...

  def make_present(self):
    assert self._path.parent().isdir()
    if self.is_dir():
      self._path.mkdir()
    elif self.is_link():
      assert self._ref is not None and self._ref.isfile()
      self._path.symlink(self.ref())
    else:
      raise ValueError("unknown type for snap_item: %s" % self._type)

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

def snap_restore(tree, snap):
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
      print "*** START ***"
      print "tree", t
      print "snap", s
      if t._path < s._path:
        # The tree component is not present in the snap. Delete it.
        print "tree component missing in snap"
        print "Deleting tree component"
        t.make_missing()
        t = None
      elif s._path < t._path:
        # The snap component is not part of the tree. Create it
        print "snap component missing in tree"
        print "Creating snap componemnt"
        s.make_present()
        s = None
      elif t._path == s._path:
        print "Paths match"
        if t._type == "dir" and s._type == "dir":
          print "both tree and snap components are dirs."
          print "no work"
        elif t._type == "link" and s._type == "link":
          print "snap and tree are both links"
          if t.ref() == s.ref():
            print "refs match"
            print "no work"
          else:
            print "Ref mismatch"
            print "replace tree with snap's ref"
            s.make_present()
        elif t._type == "link" and s._type == "dir":
          print "Found link, expected dir"
          print "deleting tree's link"
          t.make_missing()
          print "making dir"
          s.make_present()
        elif t._type == "dir" and s._type == "link":
          print "Found dir, expected link"
          print "recursively deleting directory"
          t.make_missing()
          print "Adding new link"
          s.make_present()
        else:
          raise ValueError("Unable to process tree/snap: unexpected types:", s._type, t._type)
        s = None
        t = None
      else:
        raise ValueError("Found pair that doesn't respond to > < == cases")
    elif t is not None:
      print "Tree object already exists, no work"
      t = None
    elif s is not None:
      print "creating snap component"
      s.make_present()
      s = None
    else:
      raise ValueError("Encountered case where s t were both not none, but neither of them were none.")

