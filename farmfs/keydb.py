from os import sep
from os import listdir
from os import unlink
from hashlib import md5
from json import loads, JSONEncoder, JSONDecoder
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
    with open(key_path(self.root, key), 'w') as f:
      f.write(value_str)
      f.write("\n")
      f.write(value_hash)
      f.write("\n")

  def read(self, key):
    try:
      with open(key_path(self.root, key), 'r') as f:
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
    keys = listdir(self.root)
    return keys

  def delete(self, name):
    unlink(key_path(self.root, name))
