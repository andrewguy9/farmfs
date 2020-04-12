import pytest
from farmfs.fs import Path, ensure_copy
from farmfs.ui import farmfs_ui, dbg_ui
from farmfs.util import egest

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

def test_farmfs_status(tmp_path, capsys):
    tmp = Path(str(tmp_path))
    r1 = farmfs_ui(['mkfs'], tmp)
    captured = capsys.readouterr()
    assert r1 == 0
    a = Path('a', tmp)
    with a.open('w') as a_fd:
        a_fd.write('a')
    r2 = farmfs_ui(['status'], tmp)
    captured = capsys.readouterr()
    assert captured.out == "a\n"
    assert captured.err == ""
    assert r2 == 0
    r3 = farmfs_ui(['freeze'], tmp)
    captured = capsys.readouterr()
    assert r3 == 0
    # assert captured.out == ""
    assert captured.err == ""
    r4 = farmfs_ui(['status'], tmp)
    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err == ""
    assert r4 == 0

def test_farmfs_ignore(tmp_path, capsys):
    root = Path(str(tmp_path))
    r1 = farmfs_ui(['mkfs'], root)
    captured = capsys.readouterr()
    assert r1 == 0
    farm_ignore = Path('.farmignore', root)
    with farm_ignore.open("wb") as fifd:
        fifd.write(egest(u"a\n\u03B1\n"))
    for name in [u'a', u'b', u'\u03B1', u'\u03B2']:
        p = Path(name, root)
        with p.open("w") as fd:
            fd.write("hi")
    r2 = farmfs_ui(['status'], root)
    captured = capsys.readouterr()
    assert r2 == 0
    assert captured.out == u".farmignore\nb\n\u03B2\n"
    assert captured.err == ""

@pytest.mark.parametrize(
    "parent,child,snap,content,read,write",
    [
        ('a', 'b', 'mysnap', 'hi', 'r','w'),
        (u'a', u'b', u'mysnap', u'hi', 'r','w'),
        (u'a', u'b', u'mysnap', b'hi', 'rb','wb'),
        #(u'par ent', u'ch ild', u'my snap', 'hi', 'r','w'), #TODO relative path bug.
        (u"\u03B1", u"\u03B2", 'mysnap', 'hi', 'r','w'),
        (u"\u03B1", u"\u03B2", u"\u0394", 'hi', 'r','w'),
        ],)
def test_farmfs_freeze_snap_thaw(
        tmp_path,
        parent, child,
        snap,
        content,
        read,
        write):
    root = Path(str(tmp_path))
    r1 = farmfs_ui(['mkfs'], root)
    assert r1 == 0
    parent_path = Path(parent, root)
    child_path = Path(child, parent_path)
    parent_path.mkdir()
    with child_path.open(write) as child_fd:
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
    with blob.open(read) as check_fd:
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

def test_farmfs_blob_corruption(tmp_path, capsys):
    root = Path(str(tmp_path))
    r1 = farmfs_ui(['mkfs'], root)
    captured = capsys.readouterr()
    assert r1 == 0
    a = Path('a', root)
    with a.open('w') as a_fd:
        a_fd.write('a')
    r2 = farmfs_ui(['freeze'], root)
    captured = capsys.readouterr()
    assert r2 == 0
    a_blob = a.readlink()
    a_blob.unlink()
    with a_blob.open('w') as a_fd:
        a_fd.write('b')
    r3 = farmfs_ui(['fsck'], root)
    captured = capsys.readouterr()
    assert captured.out == 'CORRUPTION checksum mismatch in blob 0cc175b9c0f1b6a831c399e269772661\n'
    assert captured.err == ""
    assert r3 == 2

def test_farmdbg_reverse(tmp_path, capsys):
    root = Path(str(tmp_path))
    r1 = farmfs_ui(['mkfs'], root)
    captured = capsys.readouterr()
    assert r1 == 0
    a = Path('a', root)
    with a.open('w') as a_fd:
        a_fd.write('a')
    bc = Path('b/c', root)
    ensure_copy(bc, a)
    r2 = farmfs_ui(['freeze'], root)
    captured = capsys.readouterr()
    assert r2 == 0
    r3 = dbg_ui(['walk', 'root'], root)
    captured = capsys.readouterr()
    assert r3 == 0
    assert captured.out == '[{"path": "/", "type": "dir"}, {"csum": "0cc175b9c0f1b6a831c399e269772661", "path": "/a", "type": "link"}, {"path": "/b", "type": "dir"}, {"csum": "0cc175b9c0f1b6a831c399e269772661", "path": "/b/c", "type": "link"}]\n'
    assert captured.err == ''
    r4 = dbg_ui(['walk', 'userdata'], root)
    captured = capsys.readouterr()
    assert r4 == 0
    assert captured.out == '["0cc175b9c0f1b6a831c399e269772661"]\n'
    assert captured.err == ''
