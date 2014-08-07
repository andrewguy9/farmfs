from keydb import keydb
from volume import mkfs as make_volume

def mkfs(args):
  make_volume(args.root)
  print "FileSystem Created %s" % args.root
  exit(0)

def writekey(args):
  db = keydb(keys_path(args.root))
  value = db.write(args.key, args.value)
  exit(0)

def readkey(args):
  db = keydb(keys_path(args.root))
  value = db.read(args.key)
  if value is not None:
    print value
    exit(0)
  else:
    exit(0)
