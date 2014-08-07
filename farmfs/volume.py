from os.path import join
from fs import ensure_dir
from fs import normalize

def __metadata_path(root):
  return join(root, ".metadata")

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

class volume:
  def __init__(self, root):
    self.root = root

