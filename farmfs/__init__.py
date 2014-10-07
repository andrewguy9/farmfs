from volume import mkfs as make_volume
from volume import getvol
from fs import Path
from snapshot import snap_restore, snap_reduce

def mkfs(args):
  make_volume(args.root)
  print "FileSystem Created %s" % args.root
  exit(0)

def writekey(args):
  vol = getvol(Path('.'))
  db = vol.keydb
  value = db.write(args.key, args.value)
  exit(0)

def readkey(args):
  vol = getvol(Path('.'))
  db = vol.keydb
  value = db.read(args.key)
  if value is not None:
    print value
  exit(0)

def list_keys(args):
  vol = getvol(Path('.'))
  db = vol.keydb
  for key in db.list():
    print key

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
    snap_restore(vol.root(), tree, vol.udd, snap)
  else:
    raise ValueError("Unknown action %s in snap command" % args.action)

def csum(args):
  for n in args.name:
    p = Path(n)
    print p.checksum(), p

