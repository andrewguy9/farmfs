from farmfs.volume import FarmFSVolume
from farmfs.fs import Path
from farmfs.util import take, ingest
try:
    from os import getcwdu
    getcwd_utf = lambda: ingest(getcwdu())
except ImportError:
    from os import getcwdb
    getcwd_utf = lambda: ingest(getcwdb())
try:
    from itertools import imap
except ImportError:
    # On python3 map is lazy.
    imap = map
try:
    from itertools import ifilter
except ImportError:
    ifilter = filter

cwd = Path(getcwd_utf())

def _find_root_path(path):
    candidates = imap(lambda x: x.join(".farmfs"), path.parents())
    matches = ifilter(lambda x: x.isdir(), candidates)
    root = next(take(1)(matches), None)
    if root:
        nested_root = next(take(1)(matches), None)
        if nested_root:
            raise ValueError("Farmfs volumes cannot be nested")
        return root.parent()
    else:
        raise ValueError("Volume not found: %s" % path)

def getvol(path):
    root = _find_root_path(path)
    vol = FarmFSVolume(root)
    return vol
