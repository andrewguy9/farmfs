from volume import mkfs as make_volume
from volume import getvol
from fs import Path
from snapshot import snap_reduce, snap_pull
from keydb import KeyDBWindow

def mkfs(root):
  make_volume(Path(root))
  print "FileSystem Created %s" % root
  exit(0)

def key(args):
  vol = getvol(Path('.'))
  db = vol.keydb

  name_verbs = ['read', 'write', 'delete']
  if args.action in name_verbs:
    try:
      name = args.name
      assert name is not None
    except Exception:
      print "Name parameter is required for key %s" % args.action
      exit(1)

  value_verbs = ['write',]
  if args.action in value_verbs:
    try:
      value = args.value
      assert value is not None
    except Exception:
      print "value parameter is required for key %s" % args.action
      exit(1)

  if args.action == 'read':
    key_value = db.read(name)
    if key_value is not None:
      print key_value
    exit(0)
  elif args.action == 'write':
    db.write(name, value)
    exit(0)
  elif args.action == 'list':
    for key in db.list(args.name):
      print key
  elif args.action == 'delete':
    db.delete(name)
  else:
    raise ValueError("Action %s not recognized" % action)

def findvol(path):
  vol = getvol(Path(path))
  print "Volume found at: %s" % vol.root()

def freeze(paths):
  paths = map(Path, paths)
  vol = getvol(Path('.'))
  vol.freeze(paths)

def thaw(paths):
  paths = map(Path, paths)
  vol = getvol(Path('.'))
  vol.thaw(paths)

def fsck():
  retcode = 0
  vol = getvol(Path('.'))
  print "Looking for broken links"
  for bad_link in vol.check_links():
    print "CORRUPTION: broken link in ", bad_link
    retcode = 2
  print "Looking for corrupt files"
  for bad_hash in vol.check_userdata_hashes():
    print "CORRUPTION: checksum mismatch in ", bad_hash
    retcode = 1
  if retcode == 0:
    print "fsck found no issues"
  exit(retcode)

def walk(args):
  vol = getvol(Path('.'))
  if args.walk == "root":
    parents = [vol.root()]
    exclude = vol.mdd
    match = ["file", "dir", "link"]
  elif args.walk == "userdata":
    parents = map(Path, [vol.udd])
    exclude = vol.mdd
    match = ["file"]
  elif args.walk == "keys":
    parents = map(Path, [vol.keydbd])
    exclude = vol.mdd
    match = ["file"]
  else:
    raise ValueException("Unknown walk: %s" % args.walk)
  for parent in parents:
    for (path, type_) in parent.entries(exclude):
      if type_ in match:
        print type_, path

def similarity():
  vol = getvol(Path('.'))
  for (dir_a, dir_b, sim) in vol.similarity():
    print sim, dir_a, dir_b

def count():
  vol = getvol(Path('.'))
  counts = vol.count()
  for f, c in counts.items():
    print c, f

def reverse(link):
  vol = getvol(Path('.'))
  for x in vol.reverse(Path(link)):
    print x

def status(paths):
  vol = getvol(Path('.'))
  paths = map(Path, paths)
  for thawed in vol.thawed(paths):
    print thawed

def gc():
  vol = getvol(Path('.'))
  for f in vol.gc():
    print "Removing", f

#TODO THIS SHOULD BE A BUNCH OF FUNCTIONS.
def snap(action, name):
  vol = getvol(Path('.'))
  snapdb = vol.snapdb
  name_verbs = ['make', 'read', 'delete', 'restore']
  if action in name_verbs:
    try:
      name = name
      assert name is not None
    except Exception:
      print "Name parameter is required for snap %s" % action
      exit(1)

  if action == 'make':
    vol.snap(name)
  elif action == 'list':
    for snap in snapdb.list():
      print snap
  elif action == 'read':
    snap = snapdb.get(name)
    for i in snap:
      print i
  elif action == 'delete':
    snapdb.delete(name)
  elif action == 'restore':
    snap = snapdb.get(name)
    tree = vol.tree()
    snap_pull(vol.root(), tree, vol.udd, snap, vol.udd)
  else:
    raise ValueError("Unknown action %s in snap command" % action)

def checksum(paths):
  for n in paths:
    p = Path(n)
    print p.checksum(), p

def remote_add(name, location):
  vol = getvol(Path('.'))
  keydb = vol.keydb
  window = KeyDBWindow("remotes", keydb)
  window.write(name, location)

def remote_remove(name):
  vol = getvol(Path('.'))
  keydb = vol.keydb
  window = KeyDBWindow("remotes", keydb)
  window.delete(name)

def remote_list():
  vol = getvol(Path('.'))
  keydb = vol.keydb
  window = KeyDBWindow("remotes", keydb)
  for remote in window.list():
    print remote

def pull(remote_name, snap_name):
  vol = getvol(Path('.'))
  keydb = vol.keydb
  window = KeyDBWindow("remotes", keydb)
  remote_location = window.read(remote_name)
  print "remote location", remote_location
  remote_vol = getvol(Path(remote_location))
  if snap_name is not None:
    snap = remote_vol.snapdb.get(snap_name)
  else:
    snap = remote_vol.tree()
  snap_pull(vol.root(), vol.tree(), vol.udd, snap, remote_vol.udd)
