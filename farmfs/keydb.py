from farmfs.fs import Path
from farmfs.fs import ensure_dir
from farmfs.fs import ensure_file
from farmfs.fs import Path
from hashlib import md5
from json import loads, JSONEncoder
from errno import ENOENT as NoSuchFile
from errno import EISDIR as IsDirectory
from os.path import sep
from func_prototypes import typed, returned
from farmfs.util import ingest, egest, safetype

@returned(str)
@typed(bytes)
def checksum(value_bytes):
  """Input string should already be coersed into an encoding before being provided"""
  return md5(value_bytes).hexdigest()

class KeyDB:
  def __init__(self, db_path):
    assert isinstance(db_path, Path)
    self.root = db_path

  #TODO I DONT THINK THIS SHOULD BE A PROPERTY OF THE DB UNLESS WE HAVE SOME ITERATOR BASED RECORD TYPE.
  def write(self, key, value):
    key = safetype(key)
    value_json = JSONEncoder(ensure_ascii=False).encode(value)
    value_bytes = egest(value_json)
    value_hash = egest(checksum(value_bytes))
    key_path = self.root.join(key)
    with ensure_file(key_path, 'wb') as f:
      f.write(value_bytes)
      f.write(b"\n")
      f.write(value_hash)
      f.write(b"\n")

  def readraw(self, key):
    key = safetype(key)
    try:
      with self.root.join(key).open('rb') as f:
        obj_bytes = f.readline().strip()
        obj_bytes_checksum = checksum(obj_bytes).encode('utf-8')
        key_checksum = f.readline().strip()
      if obj_bytes_checksum != key_checksum:
        raise ValueError("Checksum mismatch for key %s. Expected %s, calculated %s" % (key, key_checksum, obj_bytes_checksum))
      obj_str = egest(obj_bytes)
      return obj_str
    except IOError as e:
      if e.errno == NoSuchFile or e.errno == IsDirectory:
        return None
      else:
        raise e

  def read(self, key):
    obj_str = self.readraw(key)
    if obj_str is None:
      return None
    else:
      obj = loads(obj_str)
      return obj

  def list(self, query=None):
    if query is None:
      query = ""
    query = safetype(query)
    query_path = self.root.join(query)
    assert self.root in query_path.parents(), "%s is not a parent of %s" % (self.root, query_path)
    if query_path.exists and query_path.isdir():
      return [ p.relative_to(self.root, leading_sep=False)
          for (p,t) in query_path.entries()
          if t == 'file' ]
    else:
      return []

  def delete(self, key):
    key = safetype(key)
    path = self.root.join(key)
    path.unlink(clean=self.root)

class KeyDBWindow(KeyDB):
  def __init__(self, window, keydb):
    window = safetype(window)
    assert isinstance(keydb, KeyDB)
    self.prefix = window + sep
    self.keydb = keydb

  def write(self, key, value):
    assert(key)
    assert(value)
    self.keydb.write(self.prefix+key, value)

  def read(self, key):
    return self.keydb.read(self.prefix+key)

  def list(self,):
    return [ x[len(self.prefix):] for x in self.keydb.list(self.prefix) ]

  def delete(self, key):
    self.keydb.delete(self.prefix+key)

class KeyDBFactory():
  def __init__(self, keydb, encoder, decoder):
    self.keydb = keydb
    self.encoder = encoder
    self.decoder = decoder

  def write(self, key, value):
    self.keydb.write(key, self.encoder(value))

  def read(self, key):
    return self.decoder(self.keydb.read(key), key)

  def list(self,):
    return self.keydb.list()

  def delete(self, key):
    self.keydb.delete(key)

