from fs import Path
from fs import ensure_dir
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
    ensure_dir(key_path.parent())
    with key_path.open('w') as f:
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

  def list(self):
    return [ p.relative_to(self.root, leading_sep=False) for (p,t) in self.root.entries() if t == 'file' ]

  def delete(self, key):
    assert isinstance(key, basestring)
    path = self.root.join(key)
    path.unlink(clean=self.root)
