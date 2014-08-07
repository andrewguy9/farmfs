from fs import ensure_dir
from fs import normalize
from keydb import keydb
from os.path import join

def metadata_path(root):
  return join(root, ".metadata")

def userdata_path(root):
  return join(metadata_path(root), "userdata")

def keys_path(root):
  return join(metadata_path(root), "keys")

def mkfs(args):
  root = normalize(args.root)
  ensure_dir(root)
  ensure_dir(metadata_path(root))
  ensure_dir(userdata_path(root))
  ensure_dir(keys_path(root))
  print "FileSystem Created!"

def writekey(args):
  db = keydb(keys_path(args.root))
  value = db.write(args.key, args.value)

def readkey(args):
  db = keydb(keys_path(args.root))
  value = db.read(args.key)
  if value is not None:
    print value
    exit(0)
  else:
    exit(0)
