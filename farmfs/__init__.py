from volume import mkfs as make_volume
from volume import find_metadata_path
from volume import FarmFSVolume
from fs import normalize

def mkfs(args):
  make_volume(args.root)
  print "FileSystem Created %s" % args.root
  exit(0)

def writekey(args):
  vol = FarmFSVolume(find_metadata_path(normalize('.')))
  db = vol.keydb
  value = db.write(args.key, args.value)
  exit(0)

def readkey(args):
  vol = FarmFSVolume(find_metadata_path(normalize('.')))
  db = vol.keydb
  value = db.read(args.key)
  if value is not None:
    print value
  exit(0)

def findvol(args):
  root = find_metadata_path(normalize('.'))
  print "Volume found at: %s" % root
  exit(0)

def ingest(args):
  vol = FarmFSVolume(find_metadata_path(normalize('.')))
  vol.ingest(vol.roots())

