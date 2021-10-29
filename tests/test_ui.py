import pytest
from farmfs.fs import Path, ensure_copy, ensure_readonly
from farmfs.ui import farmfs_ui, dbg_ui
from farmfs.util import egest, ingest
import uuid

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
    with a.open('w') as a_fd: a_fd.write('a')
    r2 = farmfs_ui(['status'], tmp)
    captured = capsys.readouterr()
    assert captured.out == "a\n"
    assert captured.err == ""
    assert r2 == 0
    # Test relative status report.
    d = Path('d', tmp)
    d.mkdir()
    r3 = farmfs_ui(['status'], d)
    captured = capsys.readouterr()
    assert captured.out == "../a\n"
    assert captured.err == ""
    assert r3 == 0
    # Freeze a
    r4 = farmfs_ui(['freeze'], tmp)
    captured = capsys.readouterr()
    assert r4 == 0
    # assert captured.out == ""
    assert captured.err == ""
    r5 = farmfs_ui(['status'], tmp)
    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err == ""
    assert r5 == 0

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

def test_farmfs_blob_broken(tmp_path, capsys):
    root = Path(str(tmp_path))
    r1 = farmfs_ui(['mkfs'], root)
    captured = capsys.readouterr()
    assert r1 == 0
    a = Path('a', root)
    with a.open('w') as a_fd: a_fd.write('a')
    a_csum = str(a.checksum())
    r2 = farmfs_ui(['freeze'], root)
    captured = capsys.readouterr()
    assert r2 == 0
    a_blob = a.readlink()
    a_blob.unlink()
    r3 = farmfs_ui(['fsck', '--broken'], root)
    captured = capsys.readouterr()
    assert captured.out == a_csum + "\n\t<tree>\ta\n"
    assert captured.err == ''
    assert r3 == 1
    # Test relative pathing.
    d = Path('d', root)
    d.mkdir()
    r4 = farmfs_ui(['fsck', '--broken'], d)
    captured = capsys.readouterr()
    assert captured.out == a_csum + "\n\t<tree>\t../a\n"
    assert captured.err == ''
    assert r3 == 1

def test_farmfs_blob_corruption(tmp_path, capsys):
    root = Path(str(tmp_path))
    r1 = farmfs_ui(['mkfs'], root)
    captured = capsys.readouterr()
    assert r1 == 0
    a = Path('a', root)
    with a.open('w') as a_fd: a_fd.write('a')
    a_csum = str(a.checksum())
    r2 = farmfs_ui(['freeze'], root)
    captured = capsys.readouterr()
    assert r2 == 0
    a_blob = a.readlink()
    a_blob.unlink()
    with a_blob.open('w') as a_fd:
        a_fd.write('b')
    ensure_readonly(a_blob)
    r3 = farmfs_ui(['fsck', '--checksums'], root)
    captured = capsys.readouterr()
    assert captured.out == 'CORRUPTION checksum mismatch in blob ' + a_csum + '\n'
    assert captured.err == ""
    assert r3 == 2

def test_farmfs_blob_permission(tmp_path, capsys):
    root = Path(str(tmp_path))
    r1 = farmfs_ui(['mkfs'], root)
    captured = capsys.readouterr()
    assert r1 == 0
    a = Path('a', root)
    with a.open('w') as a_fd: a_fd.write('a')
    a_csum = str(a.checksum())
    r2 = farmfs_ui(['freeze'], root)
    captured = capsys.readouterr()
    assert r2 == 0
    a_blob = a.readlink()
    a_blob.chmod(0o777)
    r3 = farmfs_ui(['fsck', '--blob-permissions'], root)
    captured = capsys.readouterr()
    assert captured.out == 'writable blob:  ' + a_csum + '\n'
    assert captured.err == ""
    assert r3 == 8

def test_farmfs_ignore_corruption(tmp_path, capsys):
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
    with root.join(".farmignore").open("w") as ignore:
        ignore.write("a")
    r3 = farmfs_ui(['fsck', '--frozen-ignored'], root)
    captured = capsys.readouterr()
    assert captured.out == 'Ignored file frozen a\n'
    assert captured.err == ""
    assert r3 == 4

@pytest.mark.parametrize(
    "a,b,c",
    [
        ('a', 'b', 'c'),
        (u'a', u'b', u'c'),
        (u"\u03B1", u"\u03B2", u"\u0394")
        ],)
def test_farmdbg_reverse(tmp_path, capsys, a, b, c):
    root = Path(str(tmp_path))
    r1 = farmfs_ui(['mkfs'], root)
    captured = capsys.readouterr()
    assert r1 == 0
    a_path = Path(a, root)
    with a_path.open('w') as a_fd: a_fd.write('a')
    a_csum = str(a_path.checksum())
    bc_path = Path(b, root).join(c)
    ensure_copy(bc_path, a_path)
    r2 = farmfs_ui(['freeze'], root)
    captured = capsys.readouterr()
    r3 = farmfs_ui(['snap', 'make', 'mysnap'], root)
    assert r3 == 0
    r4 = dbg_ui(['walk', 'root'], root)
    captured = capsys.readouterr()
    assert r4 == 0
    assert captured.out == ".\tdir\t\n%s\tlink\t%s\n%s\tdir\t\n%s/%s\tlink\t%s\n" % (a, a_csum, b, b, c, a_csum)
    assert captured.err == ''
    r5 = dbg_ui(['walk', 'userdata'], root)
    captured = capsys.readouterr()
    assert r5 == 0
    assert captured.out == a_csum + '\n'
    assert captured.err == ''
    r6 = dbg_ui(['reverse', a_csum], root)
    captured = capsys.readouterr()
    assert r6 == 0
    assert captured.out =="<tree> "+a+"\n<tree> "+b+"/"+c+"\n"
    assert captured.err == ''
    r7 = dbg_ui(['reverse', '--all', a_csum], root)
    captured = capsys.readouterr()
    assert r7 == 0
    assert captured.out == "<tree> "+a+"\n<tree> "+b+"/"+c+"\nmysnap "+a+"\nmysnap "+b+"/"+c+"\n"
    assert captured.err == ''
    r8 = dbg_ui(['reverse', '--snap', 'mysnap', a_csum], root)
    captured = capsys.readouterr()
    assert r8 == 0
    assert captured.out =="mysnap "+a+"\nmysnap "+b+"/"+c+"\n"
    assert captured.err == ''

def test_gc(tmp_path, capsys):
    root = Path(str(tmp_path))
    sk = Path('sk', root)
    sd = Path('sd', root)
    tk = Path('tk', root)
    td = Path('td', root)
    # Make the Farm
    r = farmfs_ui(['mkfs'], root)
    captured = capsys.readouterr()
    assert r == 0
    # Make sk, freeze, snap, delete
    with sk.open('w') as fd: fd.write('sk')
    r = farmfs_ui(['freeze'], root)
    captured = capsys.readouterr()
    assert r == 0
    sk_blob = sk.readlink()
    r = farmfs_ui(['snap', 'make', 'snk'], root)
    captured = capsys.readouterr()
    assert r == 0
    sk.unlink()
    # Make sd, freeze, snap, delete, delete snap
    with sd.open('w') as fd: fd.write('sd')
    sd_csum = str(sd.checksum())
    r = farmfs_ui(['freeze'], root)
    captured = capsys.readouterr()
    assert r == 0
    sd_blob = sd.readlink()
    r = farmfs_ui(['snap', 'make', 'snd'], root)
    captured = capsys.readouterr()
    assert r == 0
    sd.unlink()
    r = farmfs_ui(['snap', 'delete', 'snd'], root)
    captured = capsys.readouterr()
    assert r == 0
    # Make tk and td, freeze, delete td
    with tk.open('w') as fd: fd.write('tk')
    with td.open('w') as fd: fd.write('td')
    r = farmfs_ui(['freeze'], root)
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
    r = farmfs_ui(['gc', '--noop'], root)
    captured = capsys.readouterr()
    assert captured.out == 'Removing '+sd_csum+'\nRemoving '+td_csum+'\n'
    assert captured.err == ''
    assert r == 0
    assert sk_blob.exists()
    assert sd_blob.exists()
    assert tk_blob.exists()
    assert td_blob.exists()
    # GC
    r = farmfs_ui(['gc'], root)
    captured = capsys.readouterr()
    assert captured.out == 'Removing '+sd_csum+'\nRemoving '+td_csum+'\n'
    assert captured.err == ''
    assert r == 0
    assert sk_blob.exists()
    assert not sd_blob.exists()
    assert tk_blob.exists()
    assert not td_blob.exists()

def test_missing(tmp_path, capsys):
    root = Path(str(tmp_path))
    a = Path('a', root)
    b = Path('b', root)
    b2 = Path('b2', root)
    c = Path('c.txt', root)
    d = Path('d', root)
    ignore = Path('.farmignore', root)
    # Make the Farm
    r = farmfs_ui(['mkfs'], root)
    captured = capsys.readouterr()
    assert r == 0
    # Make a,b,b2; freeze, snap, delete
    with a.open('w') as fd: fd.write('a_masked') # Checksum for a_mask should not appear missing, as a exists.
    with b.open('w') as fd: fd.write('b')
    b_csum = str(b.checksum())
    with b2.open('w') as fd: fd.write('b')
    with c.open('w') as fd: fd.write('c')
    r = farmfs_ui(['freeze'], root)
    captured = capsys.readouterr()
    assert r == 0
    r = farmfs_ui(['snap', 'make', 'snk1'], root)
    captured = capsys.readouterr()
    # Remove b's
    a.unlink()
    with a.open('w') as fd: fd.write('a')
    b.unlink()
    b2.unlink()
    c.unlink()
    #Setup ignore
    with ignore.open('w') as fd: fd.write('*.txt\n*/*.txt\n')
    # Look for missing checksum:
    r = dbg_ui(['missing', 'snk1'], root)
    captured = capsys.readouterr()
    assert r == 0
    assert captured.err == ""
    assert captured.out == b_csum + "\tb\n" + b_csum + "\tb2\n"
    # Make d; freeze snap, delete
    with d.open('w') as fd: fd.write('d')
    d_csum = str(d.checksum())
    r = farmfs_ui(['freeze'], root)
    captured = capsys.readouterr()
    assert r == 0
    r = farmfs_ui(['snap', 'make', 'snk2'], root)
    captured = capsys.readouterr()
    d.unlink()
    # Look for missing checksum:
    r = dbg_ui(['missing', 'snk1', 'snk2'], root)
    captured = capsys.readouterr()
    assert r == 0
    assert captured.err == ""
    removed_lines = set(['', b_csum + "\tb", b_csum + "\tb2", d_csum + "\td"])
    assert set(captured.out.split("\n")) == removed_lines

def test_blobtype(tmp_path, capsys):
    root = Path(str(tmp_path))
    a = Path('a', root)
    b = Path('b', root)
    # Make the Farm
    r = farmfs_ui(['mkfs'], root)
    captured = capsys.readouterr()
    assert r == 0
    # Make a,b; freeze, snap, delete
    with a.open('w') as fd: fd.write('a')
    with b.open('w') as fd: fd.write('XSym\n1234\n')
    r = farmfs_ui(['freeze'], root)
    captured = capsys.readouterr()
    assert r == 0
    # Check file type for a
    a_csum = str(a.checksum())
    b_csum = str(b.checksum())
    r = dbg_ui(['blobtype', a_csum, b_csum], root)
    captured = capsys.readouterr()
    assert r == 0
    assert captured.err == ""
    assert captured.out == a_csum +" unknown\n" + b_csum + " inode/symlink\n"

def test_fix_link(tmp_path, capsys):
    test_dir = Path(str(tmp_path))
    # Make roots
    vol1 = Path("vol1", test_dir)
    vol1.mkdir()
    vol2 = Path("vol2", test_dir)
    vol2.mkdir()
    # Setup vol1
    r = farmfs_ui(['mkfs'], vol1)
    captured = capsys.readouterr()
    assert r == 0
    a = Path('a', vol1)
    b = Path('b', vol1)
    c = Path('c', vol1)
    cd = Path('c/d', vol1)
    # Make a,b; freeze, snap, delete
    with a.open('w') as fd: fd.write('a')
    a_csum = str(a.checksum())
    with b.open('w') as fd: fd.write('b')
    r = farmfs_ui(['freeze'], vol1)
    captured = capsys.readouterr()
    assert r == 0
    # Setup vol2
    r = farmfs_ui(['mkfs'], vol2)
    captured = capsys.readouterr()
    assert r == 0
    e = Path('e', vol2)
    with e.open('w') as fd: fd.write('e')
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

def test_blob(tmp_path, capsys):
    root = Path(str(tmp_path))
    a = Path('a', root)
    b = Path('b', root)
    # Make the Farm
    r = farmfs_ui(['mkfs'], root)
    captured = capsys.readouterr()
    assert r == 0
    # Make a,b,b2; freeze, snap, delete
    with a.open('w') as fd: fd.write('a')
    a_csum = str(a.checksum())
    with b.open('w') as fd: fd.write('b')
    b_csum = str(b.checksum())
    r = farmfs_ui(['freeze'], root)
    captured = capsys.readouterr()
    assert r == 0
    # get blob paths
    r = dbg_ui(['blob', a_csum, b_csum], root)
    captured = capsys.readouterr()
    assert r == 0
    a_rel = a.readlink().relative_to(root)
    b_rel = b.readlink().relative_to(root)
    assert captured.out == a_csum + " " + a_rel + "\n" + b_csum + " "+ b_rel +"\n"
    assert captured.err == ""

def test_rewrite_links(tmp_path, capsys):
    tmp = Path(str(tmp_path))
    vol1 = tmp.join("vol1")
    vol2 = tmp.join("vol2")
    a = Path('a', vol1)
    # Make the Farm
    r = farmfs_ui(['mkfs'], vol1)
    captured = capsys.readouterr()
    assert r == 0
    # Make a
    with a.open('w') as fd: fd.write('a')
    a_csum = str(a.checksum())
    r = farmfs_ui(['freeze'], vol1)
    captured = capsys.readouterr()
    assert r == 0
    # Move from vol1 to vol2
    vol1.rename(vol2)
    # Reinit the fs. This will fix the udd directory pointer.
    r = farmfs_ui(['mkfs'], vol2)
    captured = capsys.readouterr()
    assert r == 0
    # Rewrite the links
    r = dbg_ui(['rewrite-links', '.'], vol2)
    captured = capsys.readouterr()
    vol2a = vol2.join('a')
    vol2a_blob = str(vol2a.readlink())
    assert r == 0
    assert captured.out == "Relinked a to " + vol2a_blob + "\n"
    assert captured.err == ""

def test_s3_upload(tmp_path, capsys):
    tmp = Path(str(tmp_path))
    vol = tmp.join("vol")
    a = Path('a', vol)
    # Make the Farm
    r = farmfs_ui(['mkfs'], vol)
    captured = capsys.readouterr()
    assert r == 0
    # Make a
    with a.open('w') as fd: fd.write('a')
    a_csum = str(a.checksum())
    r = farmfs_ui(['freeze'], vol)
    captured = capsys.readouterr()
    assert r == 0
    # upload to s3
    bucket = 's3libtestbucket'
    prefix = str(uuid.uuid1())
    # Assert s3 bucket/prefix is empty
    r = dbg_ui(['s3', 'list', bucket, prefix], vol)
    captured = capsys.readouterr()
    assert r == 0
    assert captured.out == ""
    assert captured.err == ""
    # Upload the contents.
    r = dbg_ui(['s3', 'upload', '--quiet', bucket, prefix], vol)
    captured = capsys.readouterr()
    assert r == 0
    assert captured.out == \
            'Fetching remote blobs\n' + \
            'Remote Blobs: 0\n' + \
            'Fetching local blobs\n' + \
            'Local Blobs: 1\n' + \
            'Uploading 1 blobs to s3\n' + \
            'Successfully uploaded\n'
    assert captured.err == ""
    # Upload again
    r = dbg_ui(['s3', 'upload', '--quiet', bucket, prefix], vol)
    captured = capsys.readouterr()
    assert r == 0
    assert captured.out == \
            'Fetching remote blobs\n' + \
            'Remote Blobs: 1\n' + \
            'Fetching local blobs\n' + \
            'Local Blobs: 1\n' + \
            'Uploading 0 blobs to s3\n' + \
            'Successfully uploaded\n'
    assert captured.err == ""
    # verify checksums
    r = dbg_ui(['s3', 'check', bucket, prefix], vol)
    captured = capsys.readouterr()
    assert r == 0
    assert captured.out == "All S3 blobs etags match\n"
    assert captured.err == ""
    # verify corrupt checksum
    a_blob = a.readlink()
    a_blob.unlink()
    with a_blob.open('w') as fd: fd.write('b')
    b_csum = str(a.checksum())
    ensure_readonly(a_blob)
    prefix2 = str(uuid.uuid1())
    r = dbg_ui(['s3', 'upload', '--quiet', bucket, prefix2], vol)
    captured = capsys.readouterr()
    assert r == 0
    r = dbg_ui(['s3', 'check', bucket, prefix2], vol)
    captured = capsys.readouterr()
    assert r == 2
    assert captured.out == a_csum + " " + b_csum + "\n"
    assert captured.err == ""

def test_farmfs_similarity(tmp_path, capsys):
    root = Path(str(tmp_path))
    r1 = farmfs_ui(['mkfs'], root)
    captured = capsys.readouterr()
    assert r1 == 0
    a_path = Path("a", root)
    a_path.mkdir()
    b_path = Path("b", root)
    b_path.mkdir()
    for i in [1,2,3]:
        with Path(str(i), a_path).open('w') as fd: fd.write(str(i))
    for i in [1,2,4,5]:
        with Path(str(i), b_path).open('w') as fd: fd.write(str(i))
    # Freeze
    r2 = farmfs_ui(['freeze'], root)
    captured = capsys.readouterr()
    assert r2 == 0
    r3 = farmfs_ui(['similarity', "a", "b"], root)
    captured = capsys.readouterr()
    assert r3 == 0
    assert captured.out == "left\tboth\tright\tjaccard_similarity\n1\t2\t2\t0.4\n"

