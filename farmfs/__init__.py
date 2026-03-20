from farmfs.volume import FarmFSVolume
from farmfs.fs import Path
from farmfs.util import ingest
from os import getcwdb

getcwd_utf = lambda: ingest(getcwdb())
cwd = Path(getcwd_utf())

def _find_root_path(path: Path) -> Path:
    candidates = map(lambda x: x.join(".farmfs"), path.parents())
    matches = list(filter(lambda x: x.isdir(), candidates))
    if len(matches) > 1:
        raise ValueError("Farmfs volumes cannot be nested")
    elif len(matches) == 0:
        raise ValueError("Volume not found: %s" % path)
    else:
        assert len(matches) == 1
        farmfs_dir = matches[0]

    root_dir = farmfs_dir.parent()
    assert root_dir is not None, ".farmfs root cannot be /"
    return root_dir

def getvol(path: Path) -> FarmFSVolume:
    root = _find_root_path(path)
    vol = FarmFSVolume(root)
    return vol
