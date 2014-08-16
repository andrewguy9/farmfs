from os import sep
from os import listdir
from hashlib import md5
from json import dump, load, JSONEncoder, JSONDecoder
from errno import ENOENT as NoSuchFile

def checksum(value_str):
  return md5(str(value_str)).hexdigest()

def key_path(db_path, key):
  return db_path + sep + key

class KeyDB:
  def __init__(self, db_path):
    self.root = db_path
    self.enc = JSONEncoder()
    self.dec = JSONDecoder()

  def write(self, key, value):
    value_str = self.enc.encode(value)
    value_hash = checksum(value_str)
    obj = {}
    obj['value'] = value_str
    obj['checksum'] = value_hash
    with open(key_path(self.root, key), 'w') as f:
      dump(obj, f)

  def read(self, key):
    try:
      with open(key_path(self.root, key), 'r') as f:
        obj = load(f)
      value_str = obj['value']
      value_hash = obj['checksum']
      assert(checksum(value_str) == value_hash)
      value = self.dec.decode(value_str)
      return value
    except IOError as e:
      if e.errno == NoSuchFile:
        return None
      else:
        raise e

  def list(self):
    keys = listdir(self.root)
    return keys
