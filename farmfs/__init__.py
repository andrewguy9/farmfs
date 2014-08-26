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

def list_keys(args):
  vol = FarmFSVolume(find_metadata_path(normalize('.')))
  db = vol.keydb
  for key in db.list():
    print key

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
  retcode = 0
  vol = FarmFSVolume(find_metadata_path(normalize('.')))
  for bad_hash in vol.check_userdata_hashes():
    print "CORRUPTION: checksum mismatch in ", bad_hash
    retcode = 1
  for bad_link in vol.check_inbound_links():
    print "CORRUPTION: broken link in ", bad_link
    retcode = 1
  if retcode == 0:
    print "fsck found no issues"
  exit(retcode)

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

def count(args):
  vol = FarmFSVolume(find_metadata_path(normalize('.')))
  counts = vol.count()
  for f, c in counts.items():
    print c, f

def reverse(args):
  vol = FarmFSVolume(find_metadata_path(normalize('.')))
  for x in vol.reverse(args.udd_name):
    print x

def status(args):
  vol = FarmFSVolume(find_metadata_path(normalize('.')))
  paths = map(normalize, args.paths)
  for thawed in vol.thawed(paths):
    print thawed

def gc(args):
  vol = FarmFSVolume(find_metadata_path(normalize('.')))
  for f in vol.gc():
    print "Removing", f

def snap(args):
  vol = FarmFSVolume(find_metadata_path(normalize('.')))
  snapdb = vol.snapdb
  if args.action == 'make':
    vol.snap(args.name)
  elif args.action == 'list':
    for snap in snapdb.list():
      print snap
  elif args.action == 'read':
    snap = snapdb.get(args.name)
    for i in snap:
      print i._type, i._ref, i._path #TODO IMPL DETAILS ARE HERE!
  elif args.action == 'delete':
    snapdb.delete(args.name)
  elif args.action == 'restore':
    raise NotImplemented("restore is not a snap verb yet")
  else:
    raise ValueError("Unknown action %s in snap command" % args.action)

