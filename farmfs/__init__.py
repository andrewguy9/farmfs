from volume import mkfs as make_volume
from volume import FarmFSVolume
from fs import Path
from fs import find_in_seq
from keydb import KeyDBWindow
from func_prototypes import typed, returned

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

