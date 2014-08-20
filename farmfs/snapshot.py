from fs import entries
from os import readlink
from keydb import KeyDB

class snap:
  pass

class path_snap(snap):
  def __init__(self, paths, exclude):
    self.paths = paths
    self.exclude = exclude

  def __iter__(self):
    walk = entries(self.paths, self.exclude)
    def path_snap_iterator():
      for path, type_ in walk:
        if type_ == "file":
          raise ValueError("Untracked file found: %s" % path)
        if type_ == "link":
          ud_path = readlink(path)
        if type_ == "dir":
          ud_path = None
        yield (type_, path, ud_path)
    return path_snap_iterator()

class key_snap(snap):
  def __init__(self, keydb, name):
    self.db = keydb
    self.name = name

  def __iter__(self):
    data = self.db.read(self.name)
    return data.__iter__()

class snapdb:
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
    for (t, p, h) in snap:
      l.append( (t,p,h) )
    self.keydb.write(name, l)

  def get(self, name):
    return key_snap(self.keydb, name)

def snap_reduce(hash_paths, snaps):
  counts = {}
  # We populate counts with all hash paths from the userdata directory.
  for (path, type_) in entries(hash_paths):
    if type_ == "file":
      counts[path]=0
  # Now we walk the paths reducing the unique userdata paths we encounter.
  for snap in snaps:
    for (type_, path, ud_path) in snap:
      if type_ == "link":
        try:
          counts[ud_path]+=1
        except KeyError:
          raise ValueError("Encounted unexpected link: %s from file %s" % (ud_path, path))
  return counts
