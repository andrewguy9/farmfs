from fs import Path
from fs import ensure_dir
from fs import ensure_file
from hashlib import md5
from json import loads, JSONEncoder
from errno import ENOENT as NoSuchFile
from errno import EISDIR as IsDirectory
from os.path import sep

def checksum(value_str):
  return md5(str(value_str)).hexdigest()

class KeyDB:
  def __init__(self, db_path):
    assert isinstance(db_path, Path)
    self.root = db_path

  #TODO I DONT THINK THIS SHOULD BE A PROPERTY OF THE DB UNLESS WE HAVE SOME ITERATOR BASED RECORD TYPE.
  def write(self, key, value):
    assert isinstance(key, basestring)
    value_str = JSONEncoder().encode(value)
    value_hash = checksum(value_str)
    key_path = self.root.join(key)
    with ensure_file(key_path, 'w') as f:
      f.write(value_str)
      f.write("\n")
      f.write(value_hash)
      f.write("\n")

  def read(self, key):
    assert isinstance(key, basestring)
    try:
      with self.root.join(key).open('r') as f:
        obj_str = f.readline().strip()
        checksum_str = f.readline().strip()
      assert(checksum(obj_str) == checksum_str)
      obj = loads(obj_str)
      return obj
    except IOError as e:
      if e.errno == NoSuchFile or e.errno == IsDirectory:
        return None
      else:
        raise e

  def list(self, query=None):
    if query is None:
      query = ""
    assert isinstance(query, basestring)
    query_path = self.root.join(query)
    assert self.root in query_path.parents(), "%s is not a parent of %s" % (self.root, query_path)
    if query_path.exists and query_path.isdir():
      return [ p.relative_to(self.root, leading_sep=False)
          for (p,t) in query_path.entries()
          if t == 'file' ]
    else:
      return []

  def delete(self, key):
    assert isinstance(key, basestring)
    path = self.root.join(key)
    path.unlink(clean=self.root)

class KeyDBWindow(KeyDB):
  def __init__(self, window, keydb):
    assert isinstance(window, basestring)
    assert isinstance(keydb, KeyDB)
    self.prefix = window + sep
    self.keydb = keydb

  def write(self, key, value):
    self.keydb.write(self.prefix+key, value)

  def read(self, key):
    return self.keydb.read(self.prefix+key)

  def list(self,):
    return [ x[len(self.prefix):] for x in self.keydb.list(self.prefix) ]

  def delete(self, key):
    self.keydb.delete(self.prefix+key)

class KeyDBFactory():
  def __init__(self, keydb, type_):
    self.keydb = keydb
    self.type_ = type_

  def write(self, key, value):
    self.keydb.write(key, value)

  def read(self, key):
    return self.type_(self.keydb.read(key))

  def list(self,):
    return self.keydb.list()

  def delete(self, key):
    self.keydb.delete(key)

