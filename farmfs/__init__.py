from volume import mkfs as make_volume
from volume import getvol
from fs import Path
from snapshot import snap_reduce, snap_pull
from keydb import KeyDBWindow

def mkfs(args):
  make_volume(args.root)
  print "FileSystem Created %s" % args.root
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

def findvol(args):
  vol = getvol(Path('.'))
  print "Volume found at: %s" % vol.root()

def freeze(args):
  assert isinstance(args.files, list)
  vol = getvol(Path('.'))
  vol.freeze(map(Path, args.files))

def thaw(args):
  vol = getvol(Path('.'))
  vol.thaw(map(Path, args.files))

def fsck(args):
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

def score_dups(tree, counts, root):
  scores = {}
  for si in tree:
    path = si._path
    if si.is_link():
      udd_path = si.ref()
      try:
        path_score = counts[udd_path]
      except KeyError:
        raise ValueError("Expected %s to be in userdata" %udd_path)
      abs_path = root.join(path)
      parent = abs_path.parent()
      assert parent is not None
      assert parent.isdir()
      try:
        (s,t) = scores[parent]
        s+=path_score
        t+=1
        scores[parent] = (s,t)
      except KeyError:
        scores[parent] = (1,1)
    elif si.is_dir():
      pass
    else:
      raise ValueError("Unknown type of file")
  return scores

def dup(args):
  vol = getvol(Path('.'))
  tree = vol.tree()
  counts = snap_reduce([tree])
  scores = score_dups(tree, counts, vol.root())
  for (d, s) in scores.items():
    print s[0], s[1], d

def count(args):
  vol = getvol(Path('.'))
  counts = vol.count()
  for f, c in counts.items():
    print c, f

def reverse(args):
  vol = getvol(Path('.'))
  for x in vol.reverse(args.udd_name):
    print x

def status(args):
  vol = getvol(Path('.'))
  paths = map(Path, args.paths)
  for thawed in vol.thawed(paths):
    print thawed

def gc(args):
  vol = getvol(Path('.'))
  for f in vol.gc():
    print "Removing", f

def snap(args):
  vol = getvol(Path('.'))
  snapdb = vol.snapdb
  name_verbs = ['make', 'read', 'delete', 'restore']
  if args.action in name_verbs:
    try:
      name = args.name
      assert name is not None
    except Exception:
      print "Name parameter is required for snap %s" % args.action
      exit(1)

  if args.action == 'make':
    vol.snap(name)
  elif args.action == 'list':
    for snap in snapdb.list():
      print snap
  elif args.action == 'read':
    snap = snapdb.get(name)
    for i in snap:
      print i
  elif args.action == 'delete':
    snapdb.delete(name)
  elif args.action == 'restore':
    snap = snapdb.get(name)
    tree = vol.tree()
    snap_pull(vol.root(), tree, vol.udd, snap, vol.udd)
  else:
    raise ValueError("Unknown action %s in snap command" % args.action)

def csum(args):
  for n in args.name:
    p = Path(n)
    print p.checksum(), p

def remote(args):
  vol = getvol(Path('.'))
  keydb = vol.keydb
  window = KeyDBWindow("remotes", keydb)
  name_verbs = ['add', 'remove']
  if args.action in name_verbs:
    try:
      name = args.name
      assert name is not None
    except Exception:
      print "name parameter is required for remote %s" % args.action
      exit(1)

  location_verbs = ['add']
  if args.action in location_verbs:
    try:
      location = args.location
      assert location is not None
    except Exception:
      print "location parameter is required for remote %s" % args.action
      exit(1)

  if args.action == 'add':
    window.write(name, location)
  elif args.action == 'remove':
    window.delete(name)
  elif args.action == 'list':
    for remote in window.list():
      print remote
  else:
    raise ValueError("Unknown action %s in snap command" % args.action)

def pull(args):
  remote_name = args.remote
  vol = getvol(Path('.'))
  keydb = vol.keydb
  window = KeyDBWindow("remotes", keydb)
  remote_location = window.read(remote_name)
  remote_vol = getvol(Path(remote_location))
  if args.snap:
    snap = remote_vol.snapdb.get(args.snap)
  else:
    snap = remote_vol.tree()
  snap_pull(vol.root(), vol.tree(), vol.udd, snap, remote_vol.udd)
