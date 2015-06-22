from volume import mkfs as make_volume
from volume import FarmFSVolume
from fs import Path
from fs import find_in_seq
from keydb import KeyDBWindow
from func_prototypes import typed, returned
from os import getcwdu

@returned(Path)
@typed(basestring)
def makePath(path):
  return Path(path, Path(getcwdu()))

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

