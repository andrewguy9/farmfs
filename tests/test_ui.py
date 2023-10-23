import pytest
from farmfs import getvol
from farmfs.fs import Path, ensure_copy, ensure_readonly
from farmfs.ui import farmfs_ui, dbg_ui
from farmfs.util import egest
from farmfs.volume import mkfs
import uuid
from delnone import delnone
import io
from hashlib import md5

@pytest.fixture
def vol1(tmp):
    root = tmp.join("vol1")
    udd = root.join('.farmfs').join('userdata')
    mkfs(root, udd)
    return root

@pytest.fixture
def vol2(tmp):
    root = tmp.join("vol2")
    udd = root.join('.farmfs').join('userdata')
    mkfs(root, udd)
    return root

def build_file(root, sub_path, content, mode="w"):
    """
    Helper function to build a file under a root.
    Returns the full path of the created file.
    """
    p = Path(sub_path, root)
    with p.open(mode) as fd:
        fd.write(content)
    return p

def build_dir(root, sub_path):
    """
    Helper function to build a dir under a root.
    Returns the full path to the created dir.
    """
    p = Path(sub_path, root)
    p.mkdir()
    return p

def build_checksum(bytes):
    hash = md5()
    hash.update(bytes)
    return str(hash.hexdigest())

def build_blob(vol_path, bytes):
    def get_fake_fd():
        return io.BytesIO(bytes)
    vol = getvol(vol_path)
    csum = build_checksum(bytes)
    vol.bs.import_via_fd(get_fake_fd, csum)
    return csum

def build_link(vol_path, sub_path, blob):
    vol = getvol(vol_path)
    path = vol_path.join(sub_path)
    vol.link(path, blob)
    return path


def test_builders(vol):
    a = build_file(vol, 'a', 'a')
    assert a.content("r") == 'a'
    assert a.checksum() == build_checksum(b'a')
    ablob = build_blob(vol, b'a')
    assert ablob == a.checksum()
    bblob = build_blob(vol, b'b')
    assert bblob == build_checksum(b'b')
    a2 = build_link(vol, 'a2', a.checksum())
    assert a2.checksum() == ablob

def test_farmfs_mkfs(tmp):
    farmfs_ui(['mkfs'], tmp)
    meta = Path(".farmfs", tmp)
    assert meta.isdir()
    userdata = Path("userdata", meta)
    assert userdata.isdir()
    snaps = Path("snaps", meta)
    assert snaps.isdir()
    keys = Path("keys", meta)
    assert keys.isdir()

def test_farmfs_status(vol, capsys):
    build_file(vol, 'a', 'a')
    r = farmfs_ui(['status'], vol)
    captured = capsys.readouterr()
    assert captured.out == "a\n"
    assert captured.err == ""
    assert r == 0
    # Test relative status report.
    d = build_dir(vol, 'd')
    r = farmfs_ui(['status'], d)
    captured = capsys.readouterr()
    assert captured.out == "../a\n"
    assert captured.err == ""
    assert r == 0
    # Freeze a
    r = farmfs_ui(['freeze'], vol)
    captured = capsys.readouterr()
    assert r == 0
    # assert captured.out == ""
    assert captured.err == ""
    r = farmfs_ui(['status'], vol)
    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err == ""
    assert r == 0

def test_farmfs_ignore(vol, capsys):
    build_file(vol, '.farmignore', egest(u"a\n\u03B1\n"), mode="wb")
    for name in [u'a', u'b', u'\u03B1', u'\u03B2']:
        build_file(vol, name, 'hi')
    r = farmfs_ui(['status'], vol)
    captured = capsys.readouterr()
    assert r == 0
    assert captured.out == u".farmignore\nb\n\u03B2\n"
    assert captured.err == ""

@pytest.mark.parametrize(
    "parent,child,snap,content,read,write",
    [
        ('a', 'b', 'mysnap', 'hi', 'r', 'w'),
        (u'a', u'b', u'mysnap', u'hi', 'r', 'w'),
        (u'a', u'b', u'mysnap', b'hi', 'rb', 'wb'),
        # (u'par ent', u'ch ild', u'my snap', 'hi', 'r','w'), #TODO relative path bug.
        (u"\u03B1", u"\u03B2", 'mysnap', 'hi', 'r', 'w'),
        (u"\u03B1", u"\u03B2", u"\u0394", 'hi', 'r', 'w'),
    ],)
def test_farmfs_freeze_snap_thaw(
        vol,
        parent, child,
        snap,
        content,
        read,
        write):
    parent_path = build_dir(vol, parent)
    child_path = build_file(parent_path, child, content, mode=write)
    assert parent_path.isdir()
    assert child_path.isfile()
    r = farmfs_ui(['freeze'], vol)
    assert r == 0
    assert parent_path.isdir()
    assert child_path.islink()
    blob = child_path.readlink()
    assert blob.isfile()
    userdata = Path('.farmfs/userdata', vol)
    assert userdata in list(blob.parents())
    assert blob.content(read) == content
    r = farmfs_ui(['snap', 'make', snap], vol)
    assert r == 0
    snap_path = vol.join(".farmfs/snap").join(snap)
    snap_path.exists()
    child_path.unlink()
    assert not child_path.exists()
    assert blob.isfile()
    r = farmfs_ui(['snap', 'restore', snap], vol)
    assert r == 0
    assert child_path.islink()
    assert blob.isfile()
    assert child_path.readlink() == blob
    r = farmfs_ui(['thaw', parent], vol)
    assert r == 0
    assert child_path.isfile()
    r = farmfs_ui(['freeze', child], parent_path)
    assert r == 0
    child_path.islink()

def test_farmfs_blob_broken(vol, capsys):
    a = build_file(vol, 'a', 'a')
    a_csum = str(a.checksum())
    r = farmfs_ui(['freeze'], vol)
    captured = capsys.readouterr()
    assert r == 0
    a_blob = a.readlink()
    a_blob.unlink()
    r = farmfs_ui(['fsck', '--broken'], vol)
    captured = capsys.readouterr()
    assert captured.out == a_csum + "\n\t<tree>\ta\n"
    assert captured.err == ''
    assert r == 1
    # Test relative pathing.
    d = Path('d', vol)
    d.mkdir()
    r = farmfs_ui(['fsck', '--broken'], d)
    captured = capsys.readouterr()
    assert captured.out == a_csum + "\n\t<tree>\t../a\n"
    assert captured.err == ''
    assert r == 1

def test_farmfs_blob_corruption(vol, capsys):
    a = Path('a', vol)
    with a.open('w') as a_fd:
        a_fd.write('a')
    a_csum = str(a.checksum())
    r = farmfs_ui(['freeze'], vol)
    captured = capsys.readouterr()
    assert r == 0
    a_blob = a.readlink()
    a_blob.unlink()
    with a_blob.open('w') as a_fd:
        a_fd.write('b')
    b_csum = str(a.checksum())
    ensure_readonly(a_blob)
    r = farmfs_ui(['fsck', '--checksums'], vol)
    captured = capsys.readouterr()
    assert captured.out == "CORRUPTION checksum mismatch in blob %s got %s\n" % (a_csum, b_csum)
    assert captured.err == ""
    assert r == 2

def test_farmfs_blob_permission(vol, capsys):
    a = Path('a', vol)
    with a.open('w') as a_fd:
        a_fd.write('a')
    a_csum = str(a.checksum())
    r = farmfs_ui(['freeze'], vol)
    captured = capsys.readouterr()
    assert r == 0
    a_blob = a.readlink()
    a_blob.chmod(0o777)
    r = farmfs_ui(['fsck', '--blob-permissions'], vol)
    captured = capsys.readouterr()
    assert captured.out == 'writable blob:  ' + a_csum + '\n'
    assert captured.err == ""
    assert r == 8

def test_farmfs_ignore_corruption(vol, capsys):
    a = Path('a', vol)
    with a.open('w') as a_fd:
        a_fd.write('a')
    r = farmfs_ui(['freeze'], vol)
    captured = capsys.readouterr()
    assert r == 0
    with vol.join(".farmignore").open("w") as ignore:
        ignore.write("a")
    r = farmfs_ui(['fsck', '--frozen-ignored'], vol)
    captured = capsys.readouterr()
    assert captured.out == 'Ignored file frozen a\n'
    assert captured.err == ""
    assert r == 4

@pytest.mark.parametrize(
    "a,b,c",
    [
        ('a', 'b', 'c'),
        (u'a', u'b', u'c'),
        (u"\u03B1", u"\u03B2", u"\u0394")
    ],)
def test_farmdbg_reverse(vol, capsys, a, b, c):
    a_path = Path(a, vol)
    with a_path.open('w') as a_fd:
        a_fd.write('a')
    a_csum = str(a_path.checksum())
    bc_path = Path(b, vol).join(c)
    ensure_copy(bc_path, a_path)
    r = farmfs_ui(['freeze'], vol)
    captured = capsys.readouterr()
    r = farmfs_ui(['snap', 'make', 'mysnap'], vol)
    assert r == 0
    r = dbg_ui(['walk', 'root'], vol)
    captured = capsys.readouterr()
    assert r == 0
    assert captured.out == ".\tdir\t\n%s\tlink\t%s\n%s\tdir\t\n%s/%s\tlink\t%s\n" % (a, a_csum, b, b, c, a_csum)
    assert captured.err == ''
    r = dbg_ui(['walk', 'userdata'], vol)
    captured = capsys.readouterr()
    assert r == 0
    assert captured.out == a_csum + '\n'
    assert captured.err == ''
    r = dbg_ui(['reverse', a_csum], vol)
    captured = capsys.readouterr()
    assert r == 0
    assert captured.out == a_csum + " <tree> " + a + "\n" \
        + a_csum + " <tree> " + b + "/" + c + "\n"
    assert captured.err == ''
    r = dbg_ui(['reverse', '--all', a_csum], vol)
    captured = capsys.readouterr()
    assert r == 0
    assert captured.out == a_csum + " <tree> " + a + "\n" \
        + a_csum + " <tree> " + b + "/" + c + "\n"        \
        + a_csum + " mysnap " + a + "\n"                  \
        + a_csum + " mysnap " + b + "/" + c + "\n"
    assert captured.err == ''
    r = dbg_ui(['reverse', '--snap', 'mysnap', a_csum], vol)
    captured = capsys.readouterr()
    assert r == 0
    assert captured.out == a_csum + " mysnap " + a + "\n" + a_csum + " mysnap " + b + "/" + c + "\n"
    assert captured.err == ''

def test_gc(vol, capsys):
    sk = Path('sk', vol)
    sd = Path('sd', vol)
    tk = Path('tk', vol)
    td = Path('td', vol)
    # Make sk, freeze, snap, delete
    with sk.open('w') as fd:
        fd.write('sk')
    r = farmfs_ui(['freeze'], vol)
    captured = capsys.readouterr()
    assert r == 0
    sk_blob = sk.readlink()
    r = farmfs_ui(['snap', 'make', 'snk'], vol)
    captured = capsys.readouterr()
    assert r == 0
    sk.unlink()
    # Make sd, freeze, snap, delete, delete snap
    with sd.open('w') as fd:
        fd.write('sd')
    sd_csum = str(sd.checksum())
    r = farmfs_ui(['freeze'], vol)
    captured = capsys.readouterr()
    assert r == 0
    sd_blob = sd.readlink()
    r = farmfs_ui(['snap', 'make', 'snd'], vol)
    captured = capsys.readouterr()
    assert r == 0
    sd.unlink()
    r = farmfs_ui(['snap', 'delete', 'snd'], vol)
    captured = capsys.readouterr()
    assert r == 0
    # Make tk and td, freeze, delete td
    with tk.open('w') as fd:
        fd.write('tk')
    with td.open('w') as fd:
        fd.write('td')
    r = farmfs_ui(['freeze'], vol)
    captured = capsys.readouterr()
    assert r == 0
    tk_blob = tk.readlink()
    td_blob = td.readlink()
    td_csum = str(td.checksum())
    td.unlink()
    # GC --noop
    assert sk_blob.exists()
    assert sd_blob.exists()
    assert tk_blob.exists()
    assert td_blob.exists()
    r = farmfs_ui(['gc', '--noop'], vol)
    captured = capsys.readouterr()
    assert captured.out == 'Removing ' + sd_csum + '\nRemoving ' + td_csum + '\n'
    assert captured.err == ''
    assert r == 0
    assert sk_blob.exists()
    assert sd_blob.exists()
    assert tk_blob.exists()
    assert td_blob.exists()
    # GC
    r = farmfs_ui(['gc'], vol)
    captured = capsys.readouterr()
    assert captured.out == 'Removing ' + sd_csum + '\nRemoving ' + td_csum + '\n'
    assert captured.err == ''
    assert r == 0
    assert sk_blob.exists()
    assert not sd_blob.exists()
    assert tk_blob.exists()
    assert not td_blob.exists()

def test_missing(vol, capsys):
    a = Path('a', vol)
    b = Path('b', vol)
    b2 = Path('b2', vol)
    c = Path('c.txt', vol)
    d = Path('d', vol)
    ignore = Path('.farmignore', vol)
    # Make a,b,b2; freeze, snap, delete
    with a.open('w') as fd:
        # Checksum for a_mask should not appear missing, as a exists.
        fd.write('a_masked')
    with b.open('w') as fd:
        fd.write('b')
    b_csum = str(b.checksum())
    with b2.open('w') as fd:
        fd.write('b')
    with c.open('w') as fd:
        fd.write('c')
    r = farmfs_ui(['freeze'], vol)
    captured = capsys.readouterr()
    assert r == 0
    r = farmfs_ui(['snap', 'make', 'snk1'], vol)
    captured = capsys.readouterr()
    # Remove b's
    a.unlink()
    with a.open('w') as fd:
        fd.write('a')
    b.unlink()
    b2.unlink()
    c.unlink()
    # Setup ignore
    with ignore.open('w') as fd:
        fd.write('*.txt\n*/*.txt\n')
    # Look for missing checksum:
    r = dbg_ui(['missing', 'snk1'], vol)
    captured = capsys.readouterr()
    assert r == 4
    assert captured.err == ""
    assert captured.out == b_csum + "\tb\n" + b_csum + "\tb2\n"
    # Make d; freeze snap, delete
    with d.open('w') as fd:
        fd.write('d')
    d_csum = str(d.checksum())
    r = farmfs_ui(['freeze'], vol)
    captured = capsys.readouterr()
    assert r == 0
    r = farmfs_ui(['snap', 'make', 'snk2'], vol)
    captured = capsys.readouterr()
    d.unlink()
    # Look for missing checksum:
    r = dbg_ui(['missing', 'snk1', 'snk2'], vol)
    captured = capsys.readouterr()
    assert r == 4
    assert captured.err == ""
    removed_lines = set(['', b_csum + "\tb", b_csum + "\tb2", d_csum + "\td"])
    assert set(captured.out.split("\n")) == removed_lines

def test_blobtype(vol, capsys):
    a = Path('a', vol)
    b = Path('b', vol)
    # Make a,b; freeze, snap, delete
    with a.open('w') as fd:
        fd.write('a')
    with b.open('w') as fd:
        fd.write('XSym\n1234\n')
    r = farmfs_ui(['freeze'], vol)
    captured = capsys.readouterr()
    assert r == 0
    # Check file type for a
    a_csum = str(a.checksum())
    b_csum = str(b.checksum())
    r = dbg_ui(['blobtype', a_csum, b_csum], vol)
    captured = capsys.readouterr()
    assert r == 0
    assert captured.err == ""
    assert captured.out == a_csum + " unknown\n" + b_csum + " inode/symlink\n"

def test_fix_link(vol1, vol2, capsys):
    # Setup vol1
    a = build_file(vol1, "a", "a")
    b = build_file(vol1, "b", "b")
    c = Path('c', vol1)
    cd = Path('c/d', vol1)
    # Make a,b; freeze, snap, delete
    a_csum = str(a.checksum())
    r = farmfs_ui(['freeze'], vol1)
    captured = capsys.readouterr()
    assert r == 0
    # Setup vol2
    e = build_file(vol2, 'e', 'e')
    e_csum = str(e.checksum())
    r = farmfs_ui(['freeze'], vol2)
    captured = capsys.readouterr()
    assert r == 0
    # Setup remote
    r = farmfs_ui(['remote', 'add', 'vol2', '../vol2'], vol1)
    captured = capsys.readouterr()
    assert r == 0
    # Check file type for a
    r = dbg_ui(['fix', 'link', a_csum, 'b'], vol1)
    captured = capsys.readouterr()
    assert r == 0
    assert captured.err == ""
    assert captured.out == ""
    assert a.readlink() == b.readlink()
    # Try to fix link to a missing blob, e
    with pytest.raises(ValueError):
        r = dbg_ui(['fix', 'link', e_csum, 'e'], vol1)
    captured = capsys.readouterr()
    assert r == 0
    assert captured.err == ""
    assert captured.out == "blob " + e_csum + " doesn't exist\n"
    # Pull e from remote.
    r = dbg_ui(['fix', 'link', '--remote', 'vol2', e_csum, 'e'], vol1)
    captured = capsys.readouterr()
    assert r == 0
    assert captured.out == "blob " + e_csum + " doesn't exist\n"
    assert captured.err == ""
    # Try to fix a link to a missing target.
    r = dbg_ui(['fix', 'link', a_csum, 'c'], vol1)
    captured = capsys.readouterr()
    assert r == 0
    assert captured.err == ""
    assert captured.out == ""
    assert a.readlink() == c.readlink()
    # Try to fix a link to a missing target, in a dir which is blobked by a link
    r = dbg_ui(['fix', 'link', a_csum, 'c/d'], vol1)
    captured = capsys.readouterr()
    assert r == 0
    assert captured.err == ""
    assert captured.out == ""
    assert a.readlink() == cd.readlink()

def test_blob(vol, capsys):
    # Make a,b,b2; freeze, snap, delete
    a = build_file(vol, 'a', 'a')
    a_csum = str(a.checksum())
    b = build_file(vol, 'b', 'b')
    b_csum = str(b.checksum())
    r = farmfs_ui(['freeze'], vol)
    captured = capsys.readouterr()
    assert r == 0
    # get blob paths
    r = dbg_ui(['blob', 'path', a_csum, b_csum], vol)
    captured = capsys.readouterr()
    assert r == 0
    a_rel = a.readlink().relative_to(vol)
    b_rel = b.readlink().relative_to(vol)
    assert captured.out == a_csum + " " + a_rel + "\n" + b_csum + " " + b_rel + "\n"
    assert captured.err == ""
    # get blob value
    r = dbg_ui(['blob', 'read', a_csum], vol)
    captured = capsys.readouterr()
    assert captured.out == "a"  # TODO this is str, not bytes.
    assert r == 0
    r = dbg_ui(['blob', 'read', b_csum], vol)
    captured = capsys.readouterr()
    assert captured.out == "b"
    assert r == 0
    # test read multiple blobs.
    r = dbg_ui(['blob', 'read', a_csum, b_csum], vol)
    captured = capsys.readouterr()
    assert captured.out == "ab"
    assert r == 0

def test_rewrite_links(tmp, vol1, capsys):
    # Make a
    a = build_file(vol1, 'a', 'a')
    a_csum = str(a.checksum())
    r = farmfs_ui(['freeze'], vol1)
    captured = capsys.readouterr()
    assert r == 0
    # Move from vol1 to vol2
    vol2 = tmp.join("vol2")
    vol1.rename(vol2)
    # Reinit the fs. This will fix the udd directory pointer.
    r = farmfs_ui(['mkfs'], vol2)
    captured = capsys.readouterr()
    assert r == 0
    # Rewrite the links
    r = dbg_ui(['rewrite-links'], vol2)
    captured = capsys.readouterr()
    vol2a = vol2.join('a')
    vol2a_blob = vol2a.readlink()
    assert r == 0
    assert captured.out == "Relinked a to " + str(vol2a_blob) + "\n"
    assert captured.err == ""
    assert a_csum == vol2a.checksum() == vol2a_blob.checksum()

@pytest.mark.parametrize(
    "mode,name,uploaded,downloaded",
    [
        ('local', None, ['a'], []),
        ('snap', 'testsnap', ['a', 'b'], ['a', 'b'])
    ],)
def test_s3_upload_download(vol1, vol2, capsys, mode, name, uploaded, downloaded):
    uploads = len(uploaded)
    checksums = set()
    # Make Blobs a, b, c
    blob_a = build_blob(vol1, b'a')
    blob_b = build_blob(vol1, b'b')
    blob_c = build_blob(vol1, b'c')
    if 'a' in downloaded:
        checksums.add(blob_a)
    if 'b' in downloaded:
        checksums.add(blob_b)
    if 'c' in downloaded:
        checksums.add(blob_c)
    # Build a and b in the tree
    a = build_link(vol1, 'a', blob_a)
    b = build_link(vol1, 'b', blob_b)
    # Build out snapshot: a and b will be in snap.
    r = farmfs_ui(['snap', 'make', 'testsnap'], vol1)
    assert r == 0
    b.unlink()  # remove b from tree. tree has just a.
    # XXX VERIFY START
    print("vol1", vol1, vol1.ftype())
    print("a   ", a, a.ftype())
    print("b   ", b)
    print("***USERDATA***")
    r = dbg_ui(['walk', 'userdata'], vol1)
    print("***ROOT***")
    r = dbg_ui(['walk', 'root'], vol1)
    print("***SNAP***")
    r = dbg_ui(['walk', 'snap', 'testsnap'], vol1)
    print("***KEYS***")
    r = dbg_ui(['walk', 'keys'], vol1)
    captured = capsys.readouterr()
    print(captured.out)
    print(captured.err)
    captured = capsys.readouterr()
    # XXX VERIFY END
    # upload to s3
    bucket = 's3libtestbucket'
    prefix = str(uuid.uuid1())
    # Assert s3 bucket/prefix is empty
    r = dbg_ui(['s3', 'list', bucket, prefix], vol1)
    captured = capsys.readouterr()
    assert r == 0
    assert captured.out == ""
    assert captured.err == ""
    # Upload the contents.
    r = dbg_ui(delnone(['s3', 'upload', mode, name, '--quiet', bucket, prefix]), vol1)
    captured = capsys.readouterr()
    assert r == 0
    assert captured.out ==                       \
        'Calculating remote blobs\n' +           \
        'Remote Blobs: 0\n' +                    \
        'Calculating desired blobs\n' +          \
        'Desired Blobs: %s\n' % uploads +        \
        'Uploading %s blobs to s3\n' % uploads + \
        'Successfully uploaded\n'
    assert captured.err == ""
    # Upload again
    r = dbg_ui(delnone(['s3', 'upload', mode, name, '--quiet', bucket, prefix]), vol1)
    captured = capsys.readouterr()
    assert r == 0
    assert captured.out ==                \
        'Calculating remote blobs\n' +    \
        'Remote Blobs: %s\n' % uploads +  \
        'Calculating desired blobs\n' +   \
        'Desired Blobs: %s\n' % uploads + \
        'Uploading 0 blobs to s3\n' +     \
        'Successfully uploaded\n'
    assert captured.err == ""
    # verify checksums
    r = dbg_ui(['s3', 'check', bucket, prefix], vol1)
    captured = capsys.readouterr()
    assert r == 0
    assert captured.out == "All S3 blobs etags match\n"
    assert captured.err == ""
    # verify corrupt checksum
    a_blob = a.readlink()
    a_blob.unlink()
    with a_blob.open('w') as fd:
        fd.write('b')
    b_csum = str(a.checksum())
    ensure_readonly(a_blob)
    prefix2 = str(uuid.uuid1())
    r = dbg_ui(delnone(['s3', 'upload', mode, name, '--quiet', bucket, prefix2]), vol1)
    captured = capsys.readouterr()
    assert r == 0
    r = dbg_ui(['s3', 'check', bucket, prefix2], vol1)
    captured = capsys.readouterr()
    assert r == 2
    assert captured.out == blob_a + " " + b_csum + "\n"
    assert captured.err == ""
    # Read the files from s3:
    r = dbg_ui(['s3', 'read', bucket, prefix, blob_a, blob_a], vol1)
    captured = capsys.readouterr()
    assert r == 0
    assert captured.out == "aa"
    assert captured.err == ""
    # Copy snapshot over
    # TODO need an API for moving snapshots
    if name is not None:
        # .farmfs/keys/snaps/testsnap
        # .farmfs/tmp/
        src_snap = vol1.join(".farmfs/keys/snaps").join(name)
        assert src_snap.exists()
        dst_dir = vol2.join(".farmfs/keys/snaps")
        dst_dir.mkdir()  # Hack, keydb doesn't create spaces early.
        assert dst_dir.exists()
        dst_snap = dst_dir.join(name)
        tmp_dir = vol2.join(".farmfs/tmp")
        assert tmp_dir.exists()
        src_snap.copy_file(dst_snap, tmpdir=tmp_dir)
        assert dst_snap.exists()
        expected_downloads = uploads
    else:
        expected_downloads = 0
    # setup attempt to download blobs.
    r = dbg_ui(delnone(['s3', 'download', mode, name, '--quiet', bucket, prefix]), vol2)
    captured = capsys.readouterr()
    assert r == 0
    assert captured.out ==                                      \
        'Calculating remote blobs\n' +                          \
        'Remote Blobs: %s\n' % uploads +                        \
        'Calculating desired blobs\n' +                         \
        'Desired Blobs: %s\n' % expected_downloads +            \
        'Calculating local blobs\n' +                           \
        'Local Blobs: 0\n'                                      \
        'downloading %s blobs from s3\n' % expected_downloads + \
        'Successfully downloaded\n'
    assert captured.err == ""
    # download again, no blobs missing:
    r = dbg_ui(delnone(['s3', 'download', mode, name, '--quiet', bucket, prefix]), vol2)
    captured = capsys.readouterr()
    assert r == 0
    assert captured.out ==                                      \
        'Calculating remote blobs\n' +                          \
        'Remote Blobs: %s\n' % uploads +                        \
        'Calculating desired blobs\n' +                         \
        'Desired Blobs: %s\n' % expected_downloads +            \
        'Calculating local blobs\n' +                           \
        'Local Blobs: %s\n' % expected_downloads +              \
        'downloading 0 blobs from s3\n' +                       \
        'Successfully downloaded\n'
    assert captured.err == ""
    # check blobs were added
    r = dbg_ui(delnone(['walk', 'userdata']), vol2)
    captured = capsys.readouterr()
    assert r == 0
    assert captured.out == "".join([c + "\n" for c in sorted(checksums)])

def test_farmfs_similarity(vol, capsys):
    a_path = build_dir(vol, 'a')
    b_path = build_dir(vol, 'b')
    for i in [1, 2, 3]:
        build_file(a_path, str(i), str(i))
    for i in [1, 2, 4, 5]:
        build_file(b_path, str(i), str(i))
    # Freeze
    r = farmfs_ui(['freeze'], vol)
    captured = capsys.readouterr()
    assert r == 0
    r = farmfs_ui(['similarity', "a", "b"], vol)
    captured = capsys.readouterr()
    assert r == 0
    assert captured.out == "left\tboth\tright\tjaccard_similarity\n1\t2\t2\t0.4\n"

def test_redact(vol, capsys):
    # Create files with different patterns.
    a = build_file(vol, 'a.txt', 'a')
    b = Path('b.jpg', vol)
    b = build_file(vol, 'b.jpg', 'b')
    r = farmfs_ui(['freeze'], vol)
    captured = capsys.readouterr()
    assert r == 0

    # Create a snap with these files in it.
    r = farmfs_ui(['snap', 'make', 'testsnap'], vol)
    captured = capsys.readouterr()
    assert r == 0

    # Test what files would be redacted.
    r = dbg_ui(['redact', 'pattern', '--noop', '*.txt', 'testsnap'], vol)
    captured = capsys.readouterr()
    assert r == 0
    assert captured.out == "redacted a.txt\n"

    # Verify that the filesystem wasn't changed.
    r = farmfs_ui(['snap', 'restore', 'testsnap'], vol)
    captured = capsys.readouterr()
    assert r == 0
    assert a.exists()
    assert b.exists()

    # Actually redact these files.
    r = dbg_ui(['redact', 'pattern', '*.txt', 'testsnap'], vol)
    captured = capsys.readouterr()
    assert r == 0
    assert captured.out == "redacted a.txt\n"

    # Verify that the snap has the pattern redacted.
    r = farmfs_ui(['snap', 'restore', 'testsnap'], vol)
    captured = capsys.readouterr()
    assert r == 0
    assert not a.exists()
    assert b.exists()
