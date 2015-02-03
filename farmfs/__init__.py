from volume import mkfs as make_volume
from volume import FarmFSVolume
from fs import Path
from fs import find_in_seq
from snapshot import snap_reduce, snap_pull
from keydb import KeyDBWindow
from prototype import typed, returned

@returned(Path)
@typed(basestring)
def makePath(path):
  #TODO SOMEDAY THIS WILL WORK WITH FRAMES OF REFERENCE.
  return Path(path)

@returned(Path)
@typed(Path)
def _find_metadata_path(path):
  mdd = find_in_seq(".farmfs", path.parents())
  if mdd is None:
    raise ValueError("Volume not found: %s" % path)
  return mdd

@returned(FarmFSVolume)
@typed(Path)
def getvol(path):
  mdd = _find_metadata_path(path)
  vol = FarmFSVolume(mdd)
  return vol

@typed(FarmFSVolume, Path)
def reverse(vol, link):
  for x in vol.reverse(link):
    yield x

@typed(FarmFSVolume)
def gc(vol):
  for f in vol.gc():
    yield f

#TODO WHY NOT PART OF VOLUME?
@typed(FarmFSVolume, basestring, FarmFSVolume)
def remote_add(vol, name, remote):
  keydb = vol.keydb
  window = KeyDBWindow("remotes", keydb)
  window.write(name, str(remote.root())) #TODO THIS MUST BE ABSOLUTE PATH...

#TODO WHY NOT PART OF VOLUME?
@typed(FarmFSVolume, basestring)
def remote_remove(vol, name):
  keydb = vol.keydb
  window = KeyDBWindow("remotes", keydb)
  window.delete(name)

#TODO WHY NOT PART OF VOLUME?
@typed(FarmFSVolume)
def remote_list(vol):
  keydb = vol.keydb
  window = KeyDBWindow("remotes", keydb)
  for remote in window.list():
    print remote

@typed(FarmFSVolume, basestring, basestring)
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

