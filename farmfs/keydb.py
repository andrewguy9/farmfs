from fs import Path
from hashlib import md5
from json import loads, JSONEncoder, JSONDecoder
from errno import ENOENT as NoSuchFile

def checksum(value_str):
  return md5(str(value_str)).hexdigest()

def _key_path(db_path, key):
  return db_path.join(key)

class KeyDB:
  def __init__(self, db_path):
    assert isinstance(db_path, Path)
    self.root = db_path
    self.enc = JSONEncoder()
    self.dec = JSONDecoder()

  def write(self, key, value):
    value_str = self.enc.encode(value)
    value_hash = checksum(value_str)
    with _key_path(self.root, key).open('w') as f:
      f.write(value_str)
      f.write("\n")
      f.write(value_hash)
      f.write("\n")

  def read(self, key):
    try:
      with _key_path(self.root, key).open('r') as f:
        obj_str = f.readline().strip()
        checksum_str = f.readline().strip()
      assert(checksum(obj_str) == checksum_str)
      obj = loads(obj_str)
      return obj
    except IOError as e:
      if e.errno == NoSuchFile:
        return None
      else:
        raise e

  def list(self):
    return self.root.dir_gen()

  def delete(self, name):
    _key_path(self.root, name).unlink()
