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
            raise ValueError("Encounted unexpected type %s for path %s" % (type_, path))
          yield SnapshotItem(path, type_, ud_path)
    return tree_snap_iterator()

class KeySnap(Snapshot):
  def __init__(self, keydb, name):
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

def snap_reduce(hash_paths, snaps):
  counts = {}
  # We populate counts with all hash paths from the userdata directory.
  for hash_path in hash_paths:
    for (path, type_) in hash_path.entries():
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
      if t < s:
        # The tree component is not present in the snap. Delete it.
        print "tree component missing in snap"
        print "Deleting tree component"
        pass # Delete t
        t = None
      elif s < t:
        # The snap component is not part of the tree. Create it
        print "snap component missing in tree"
        print "Creating snap componemnt"
        pass # Create s
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
            pass #TODO REPLACE t's REF WITH s's REF
        elif t._type == "link" and s._type == "dir":
          print "Found link, expected dir"
          print "deleting tree's link"
          pass #TODO UNLINK t
          print "making dir"
          pass #MAKE DIR
        elif t_type == "dir" and s._type == "link":
          print "Found dir, expected link"
          print "recursively deleting directory"
          pass #TODO WALK DIRECTORY t
          print "Removing directory"
          pass #delete directory
          print "Adding new link"
          pass #ADD LINK to s's REF
        else:
          raise ValueError("Unable to process tree/snap: unexpected types:", s._type, t._type)
        s = None
        t = None
      else:
        raise ValueError("Found pair that doesn't respond to > < == cases")
    elif t is not None:
      print "creating tree component"
      pass #TODO MAKE T BE WHAT IT WANTS TO BE
      t = None
    elif s is not None:
      print "creating snap component"
      pass #TODO MAKE S BE WHAT IT WANTS TO BE
      s = None
    else:
      raise ValueError("Encountered case where s t were both not none, but neither of them were none.")

