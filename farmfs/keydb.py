from fs import Path
from fs import ensure_dir
from fs import ensure_file
from hashlib import md5
from json import loads, JSONEncoder, JSONDecoder
from errno import ENOENT as NoSuchFile
from errno import EISDIR as IsDirectory

def checksum(value_str):
  return md5(str(value_str)).hexdigest()

class KeyDB:
  def __init__(self, db_path):
    assert isinstance(db_path, Path)
    self.root = db_path
    self.enc = JSONEncoder()
    self.dec = JSONDecoder()

  def write(self, key, value):
    assert isinstance(key, basestring)
    value_str = self.enc.encode(value)
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
