from os.path import join
from fs import ensure_dir
from fs import normalize
from fs import find_seq
from fs import parents

def __metadata_path(root):
  return join(root, ".farmfs")

def __userdata_path(root):
  return join(__metadata_path(root), "userdata")

def __keys_path(root):
  return join(__metadata_path(root), "keys")

def mkfs(root):
  abs_path = normalize(root)
  ensure_dir(abs_path)
  ensure_dir(__metadata_path(abs_path))
  ensure_dir(__userdata_path(abs_path))
  ensure_dir(__keys_path(abs_path))

def findroot(cwd):
  root = find_seq(".farmfs", parents(cwd))
  if root is None:
    raise ValueError("Volume not found: %s" % cwd)
  return root

class volume:
  def __init__(self, root):
    self.root = root

