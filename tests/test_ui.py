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

@pytest.mark.parametrize(
    "parent,child",
    [
        ("a", "b"),
        ],)
def test_farmfs_freeze_thaw(tmp_path, parent, child):
    root = Path(str(tmp_path))
    r1 = farmfs_ui(['mkfs'], root)
    assert r1 == 0
    parent_path = Path(parent, root)
    child_path = Path(child, parent_path)
    parent_path.mkdir()
    with child_path.open('w') as child_fd:
        child_fd.write("hi")
    assert parent_path.isdir()
    assert child_path.isfile()
    r2 = farmfs_ui(['freeze'], root)
    assert r2 == 0
    assert parent_path.isdir()
    assert child_path.islink()
    blob = child_path.readlink()
    assert blob.isfile()
    userdata = Path('.farmfs/userdata', root)
    assert userdata in list(blob.parents())
    with blob.open('r') as check_fd:
        content = check_fd.read()
    assert content == "hi"
    r3 = farmfs_ui(['snap', 'make', 'mysnap'], root)
    assert r3 == 0
    snap = root.join(".farmfs/snap/mysnap")
    snap.exists()
    child_path.unlink()
    assert not child_path.exists()
    assert blob.isfile()
    r4 = farmfs_ui(['snap', 'restore', 'mysnap'], root)
    assert r4 == 0
    assert child_path.islink()
    assert blob.isfile()
    assert child_path.readlink() == blob
