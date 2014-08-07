from fs import ensure_dir
from os import sep
from keydb import keydb

def userdata_path(root):
  return root + sep + ".userdata"

def metadata_path(root):
  return root + sep + ".metadata"

def keys_path(root):
  return metadata_path(root) + sep + "keys"

def mkfs(args):
  ensure_dir(args.root)
  ensure_dir(userdata_path(args.root))
  ensure_dir(metadata_path(args.root))
  ensure_dir(keys_path(args.root))
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
