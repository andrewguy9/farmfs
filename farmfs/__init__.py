from volume import mkfs as make_volume
from volume import FarmFSVolume
from fs import Path
from fs import find_in_seq
from snapshot import snap_reduce, snap_pull
from keydb import KeyDBWindow

def makePath(path):
  #TODO SOMEDAY THIS WILL WORK WITH FRAMES OF REFERENCE.
  return Path(path)

#TODO REDUNDANT.
def mkfs(root):
  make_volume(root)
  print "FileSystem Created %s" % root
  exit(0)

def _find_metadata_path(path):
  assert isinstance(path, Path)
  mdd = find_in_seq(".farmfs", path.parents())
  if mdd is None:
    raise ValueError("Volume not found: %s" % path)
  return mdd

def getvol(path):
  assert isinstance(path, Path)
  mdd = _find_metadata_path(path)
  vol = FarmFSVolume(mdd)
  return vol

#TODO THIS WOULD BE BETTER AS A BUNCH OF FUNCTIONS.
def walk(verb):
  vol = getvol(Path('.'))
  if verb == "root":
    parents = [vol.root()]
    exclude = vol.mdd
    match = ["file", "dir", "link"]
  elif verb == "userdata":
    parents = map(Path, [vol.udd])
    exclude = vol.mdd
    match = ["file"]
  elif verb == "keys":
    parents = map(Path, [vol.keydbd])
    exclude = vol.mdd
    match = ["file"]
  else:
    raise ValueException("Unknown walk: %s" % args.walk)
  for parent in parents:
    for (path, type_) in parent.entries(exclude):
      if type_ in match:
        print type_, path

def reverse(vol, link):
  for x in vol.reverse(Path(link)):
    yield x

def gc(vol):
  for f in vol.gc():
    yield f

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

#TODO WHY NOT PART OF VOLUME?
def remote_add(vol, name, location):
  keydb = vol.keydb
  window = KeyDBWindow("remotes", keydb)
  window.write(name, location)

#TODO WHY NOT PART OF VOLUME?
def remote_remove(vol, name):
  keydb = vol.keydb
  window = KeyDBWindow("remotes", keydb)
  window.delete(name)

#TODO WHY NOT PART OF VOLUME?
def remote_list(vol):
  keydb = vol.keydb
  window = KeyDBWindow("remotes", keydb)
  for remote in window.list():
    print remote

def pull(vol, remote_name, snap_name):
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
