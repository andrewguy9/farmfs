from volume import mkfs as make_volume
from volume import FarmFSVolume
from fs import Path
from keydb import KeyDBWindow
from func_prototypes import typed, returned
from os import getcwdu

@returned(Path)
@typed(Path)
def _find_root_path(path):
  candidates = map(lambda x: x.join(".farmfs"), path.parents())
  matches = filter(lambda x: x.isdir(), candidates)
  if len(matches) > 1:
    raise ValueError("Farmfs volumes cannot be nested")
  if len(matches) == 0:
   raise ValueError("Volume not found: %s" % path)
  return matches[0].parent()

@returned(FarmFSVolume)
@typed(Path)
def getvol(path):
  root = _find_root_path(path)
  vol = FarmFSVolume(root)
  return vol

@typed(FarmFSVolume, Path)
def reverse(vol, link):
  for x in vol.reverse(link):
    yield x

@typed(FarmFSVolume)
def gc(vol):
  for f in vol.gc():
    yield f

