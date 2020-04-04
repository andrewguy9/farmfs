import pytest
from farmfs.fs import Path
from farmfs.ui import farmfs_ui

def test_farmfs_mkfs(tmp_path):
    tmp = Path(str(tmp_path))
    farmfs_ui(['mkfs'], tmp)
    meta = Path(".farmfs", tmp)
    assert meta.isdir()
    userdata = Path("userdata", meta)
    assert userdata.isdir()
    snaps = Path("snaps", meta)
    assert snaps.isdir()
    keys = Path("keys", meta)
    assert keys.isdir()

def test_farmfs_freeze_thaw(tmp_path):
    root = Path(str(tmp_path))
    farmfs_ui(['mkfs'], root)
    a = Path('a', root)
    ab = Path('a/b', root)
    a.mkdir()
    with ab.open('w') as bfd:
        bfd.write("hi")
    assert a.isdir()
    assert ab.isfile()
    farmfs_ui(['freeze'], root)
    assert a.isdir()
    assert ab.islink()
    hi_blob = ab.readlink()
    userdata = Path('.farmfs/userdata', root)
    assert userdata in list(hi_blob.parents())
    with hi_blob.open('r') as hifd:
        content = hifd.read()
    assert content == "hi"
