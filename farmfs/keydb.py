from os import sep
from hashlib import md5
from json import dump, load
from errno import ENOENT as NoSuchFile

def checksum(value):
  return md5(value).hexdigest()

def key_path(db_path, key):
  return db_path + sep + key

class keydb:
  def __init__(self, db_path):
    self.root = db_path

  def write(self, key, value):
    value_hash = checksum(value)
    obj = {}
    obj['value'] = value
    obj['checksum'] = value_hash
    with open(key_path(self.root, key), 'w') as f:
      dump(obj, f)

  def read(self, key):
    try:
      with open(key_path(self.root, key), 'r') as f:
        obj = load(f)
      value = obj['value']
      value_hash = obj['checksum']
      assert(checksum(value) == value_hash)
      return value
    except IOError as e:
      if e.errno == NoSuchFile:
        return None
      else:
        raise e

