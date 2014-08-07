from os.path import join
from keydb import KeyDB
from fs import ensure_dir
from fs import normalize
from fs import find_seq
from fs import parents

def __metadata_path(root):
  return join(root, ".farmfs")

def __userdata_path(mdd):
  return join(mdd, "userdata")

def __keys_path(mdd):
  return join(mdd, "keys")

def mkfs(root):
  abs_path = normalize(root)
  ensure_dir(abs_path)
  mdd = __metadata_path(abs_path)
  ensure_dir(mdd)
  ensure_dir(__userdata_path(mdd))
  ensure_dir(__keys_path(mdd))

def find_metadata_path(cwd):
  mdd = find_seq(".farmfs", parents(cwd))
  if mdd is None:
    raise ValueError("Volume not found: %s" % cwd)
  return mdd

class FarmFSVolume:
  def __init__(self, mdd):
    self.mdd = mdd
    self.keydb = KeyDB(mdd)

