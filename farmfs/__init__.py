from farmfs.volume import FarmFSVolume
from farmfs.fs import Path
from farmfs.util import take, ingest
from os import getcwdb

getcwd_utf = lambda: ingest(getcwdb())

cwd = Path(getcwd_utf())


def _find_root_path(path):
    candidates = map(lambda x: x.join(".farmfs"), path.parents())
    matches = filter(lambda x: x.isdir(), candidates)
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
