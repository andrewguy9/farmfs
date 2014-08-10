from volume import mkfs as make_volume
from volume import find_metadata_path
from volume import FarmFSVolume
from fs import normalize
from fs import entries

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

def freeze(args):
  vol = FarmFSVolume(find_metadata_path(normalize('.')))
  vol.freeze(map(normalize, args.files))

def thaw(args):
  vol = FarmFSVolume(find_metadata_path(normalize('.')))
  vol.thaw(map(normalize, args.files))

def fsck(args):
  vol = FarmFSVolume(find_metadata_path(normalize('.')))
  vol.check_userdata()

def walk(args):
  vol = FarmFSVolume(find_metadata_path(normalize('.')))
  if args.walk == "root":
    parents = map(normalize, vol.roots())
    exclude = vol.mdd
    match = ["file", "dir", "link"]
  elif args.walk == "userdata":
    parents = map(normalize, [vol.udd])
    exclude = vol.mdd
    match = ["file"]
  elif args.walk == "keys":
    parents = map(normalize, [vol.keydbd])
    exclude = vol.mdd
    match = ["file"]
  else:
    raise ValueException("Unknown walk: %s" % args.walk)
  walk = entries(parents, exclude)
  for path, type_ in walk:
    if type_ in match:
      print type_, path
