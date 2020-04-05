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
    "parent,child,snap,content",
    [
        ('a', 'b', 'mysnap', 'hi'),
        (u'a', u'b', u'mysnap', u'hi'),
        ],)
def test_farmfs_freeze_snap_thaw(tmp_path, parent, child, snap, content):
    root = Path(str(tmp_path))
    r1 = farmfs_ui(['mkfs'], root)
    assert r1 == 0
    parent_path = Path(parent, root)
    child_path = Path(child, parent_path)
    parent_path.mkdir()
    with child_path.open('w') as child_fd:
        child_fd.write(content)
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
        check_content = check_fd.read()
    assert check_content == content
    r3 = farmfs_ui(['snap', 'make', snap], root)
    assert r3 == 0
    snap_path = root.join(".farmfs/snap").join(snap)
    snap_path.exists()
    child_path.unlink()
    assert not child_path.exists()
    assert blob.isfile()
    r4 = farmfs_ui(['snap', 'restore', snap], root)
    assert r4 == 0
    assert child_path.islink()
    assert blob.isfile()
    assert child_path.readlink() == blob
    r5 = farmfs_ui(['thaw', parent], root)
    assert r5 == 0
    assert child_path.isfile()
    r6 = farmfs_ui(['freeze', child], parent_path)
    assert r6 == 0
    child_path.islink()
