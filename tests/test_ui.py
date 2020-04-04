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
    r1 = farmfs_ui(['mkfs'], root)
    assert r1 == 0
    a = Path('a', root)
    ab = Path('a/b', root)
    a.mkdir()
    with ab.open('w') as bfd:
        bfd.write("hi")
    assert a.isdir()
    assert ab.isfile()
    r2 = farmfs_ui(['freeze'], root)
    assert r2 == 0
    assert a.isdir()
    assert ab.islink()
    hi_blob = ab.readlink()
    assert hi_blob.isfile()
    userdata = Path('.farmfs/userdata', root)
    assert userdata in list(hi_blob.parents())
    with hi_blob.open('r') as hifd:
        content = hifd.read()
    assert content == "hi"
    r3 = farmfs_ui(['snap', 'make', 'mysnap'], root)
    assert r3 == 0
    snap = root.join(".farmfs/snap/mysnap")
    snap.exists()
    ab.unlink()
    assert not ab.exists()
    assert hi_blob.isfile()
    r4 = farmfs_ui(['snap', 'restore', 'mysnap'], root)
    assert r4 == 0
    assert ab.islink()
    assert hi_blob.isfile()
    assert ab.readlink() == hi_blob
