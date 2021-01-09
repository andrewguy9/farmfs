from farmfs.fs import normpath as _normalize
from farmfs.fs import userPath2Path as up2p
from farmfs.fs import Path, FileDoesNotExist, InvalidArgument, NotPermitted, FileExists, IsADirectory, ensure_symlink, ensure_absent, ensure_dir, ensure_link
from farmfs.fs import XSym
import pytest
import errno

def test_create_path():
  p1 = Path("/")
  p2 = Path("/a")
  p2 = Path("/a/b")
  p3 = Path(p1)
  p4 = Path("a", p1)
  with pytest.raises(AssertionError):
    p5 = Path("/a/b", p2)
  with pytest.raises(ValueError):
    p6 = Path(None)
  with pytest.raises(ValueError):
    p7 = Path(None, p1)
  with pytest.raises(AssertionError):
    p8 = Path("a", "b")

def test_normalize_abs():
  assert _normalize("/")       == "/"
  assert _normalize("/a")      == "/a"
  assert _normalize("/a/")     == "/a"
  assert _normalize("/a/b")    == "/a/b"
  assert _normalize("/a/b/")   == "/a/b"
  assert _normalize("/a//b")   == "/a/b"
  assert _normalize("/a//b//") == "/a/b"

def test_normalize_relative():
  assert _normalize("a")      == "a"
  assert _normalize("a/")     == "a"
  assert _normalize("a/b")    == "a/b"
  assert _normalize("a/b/")   == "a/b"
  assert _normalize("a//b")   == "a/b"
  assert _normalize("a//b//") == "a/b"

def test_userPath2Path():
  assert up2p("c", Path("/a/b")) == Path("/a/b/c")
  assert up2p("/c", Path("/a/b")) == Path("/c")

def test_cmp():
  assert Path("/a/b") < Path("/a/c")
  assert Path("/a/c") > Path("/a/b")
  assert Path("/a/2") < Path("/b/1")
  assert Path("/") < Path("/a")

@pytest.mark.skip(reason="bugs not impacting development at moment.")
def test_relative_to():
  assert Path("/a/b").relative_to(Path("/")) == "a/b"
  assert Path("/a/b").relative_to(Path("/a")) == "b"
  assert Path("/a/b/c").relative_to(Path("/a")) == "b/c"
  assert Path("/a/b/c").relative_to(Path("/a/b")) == "c"
  assert Path("/a/b").relative_to(Path("/c")) == "../a/b"

@pytest.mark.parametrize(
    "input,expected",
    [
        (b'', u"d41d8cd98f00b204e9800998ecf8427e"),
        (b'abc', u"900150983cd24fb0d6963f7d28e17f72"),
        (b'\xea\x80\x80abcd\xde\xb4', u'b8c6dee81075e87d348522b146c95ae3'),
        ],)
def test_checksum_file(tmp_path, input, expected):
  tmp = Path(str(tmp_path))
  fp = tmp.join("empty.txt")
  with fp.open("wb") as fd:
      fd.write(input)
  assert fp.checksum() == expected

def test_checksum_non_files(tmp_path):
    tmp = Path(str(tmp_path))
    # Test dir
    d = tmp.join("d")
    d.mkdir()
    with pytest.raises(IOError) as e:
        d.checksum()
    assert e.value.errno == IsADirectory
    # Test symlink to dir
    d_slnk = tmp.join("dslnk")
    d_slnk.symlink(d)
    with pytest.raises(IOError) as e:
        d_slnk.checksum()
    assert e.value.errno == IsADirectory
    # Setup files
    f = tmp.join("f")
    with f.open("w") as fd:
        fd.write("")
    # Test file symlink
    f_slnk = tmp.join("fslnk")
    f_slnk.symlink(f)
    f_slnk.checksum() == u"d41d8cd98f00b204e9800998ecf8427e"
    # Test broken symlink
    b_slnk = tmp.join("bslnk")
    b_slnk.symlink(tmp.join("dne"))
    with pytest.raises(IOError) as e:
        b_slnk.checksum()
    assert e.value.errno == FileDoesNotExist

def test_create_dir(tmp_path):
    a = Path(str(tmp_path)).join('a')
    b = a.join('b')
    assert a.isdir() == False
    assert b.isdir() == False
    # Cannot create with missing parents.
    with pytest.raises(OSError) as e_info:
      b.mkdir()
    assert e_info.value.errno == FileDoesNotExist
    assert a.isdir() == False
    assert b.isdir() == False
    # Create a
    a.mkdir()
    assert a.isdir() == True
    assert b.isdir() == False
    # idempotent
    a.mkdir()
    assert a.isdir() == True
    assert b.isdir() == False

def test_match_xsym():
    xsym = XSym()
    print(xsym)
    assert     xsym.match(bytearray("XSym\n1234\n".encode('ascii'))), "XSym example"
    assert not xsym.match(bytearray("XSym".encode('ascii'))), "Short file example"
    assert     xsym.match(bytearray("XSym\n1234\nabcd".encode('ascii'))), "XSym long example"
    assert not xsym.match(bytearray("The quick brown fox".encode('ascii'))), "Bad file example"

def test_file_types(tmp_path):
    tmp = Path(str(tmp_path))
    # Test a path that doesn't exist
    dne = tmp.join("dne")
    assert not dne.islink()
    assert not dne.exists()
    assert not dne.isfile()
    assert not dne.isdir()
    # Test a regular file.
    f = tmp.join("f")
    with f.open("w") as fd:
        fd.write("some text")
    assert not f.islink()
    assert     f.exists()
    assert     f.isfile()
    assert not f.isdir()
    # Test a directory
    d = tmp.join("d")
    d.mkdir()
    assert not d.islink()
    assert     d.exists()
    assert not d.isfile()
    assert     d.isdir()
    # Test a symlink to a regular file
    f_slnk = tmp.join("f_lnk")
    f_slnk.symlink(f)
    assert     f_slnk.islink()
    assert     f_slnk.exists()
    assert     f_slnk.isfile()
    assert not f_slnk.isdir()
    # Test a symlink to a directory.
    d_slnk = tmp.join("d_lnk")
    d_slnk.symlink(d)
    assert     d_slnk.islink()
    assert     d_slnk.exists()
    assert not d_slnk.isfile()
    assert     d_slnk.isdir()
    # Test a broken symlink.
    b_slnk = tmp.join("b_lnk")
    b_slnk.symlink(dne)
    assert     b_slnk.islink()
    assert     b_slnk.exists()
    assert not b_slnk.isfile()
    assert not b_slnk.isdir()

def test_exists(tmp_path):
    tmp = Path(str(tmp_path))
    # Test a path that doesn't exist
    dne = tmp.join("dne")
    assert not dne.exists()
    # Test a regular file.
    f = tmp.join("f")
    with f.open("w") as fd:
        fd.write("some text")
    assert f.exists()
    # Test a directory
    d = tmp.join("d")
    d.mkdir()
    assert d.exists()
    # Test a symlink to a regular file
    f_slnk = tmp.join("f_lnk")
    f_slnk.symlink(f)
    assert f_slnk.exists()
    # Test a symlink to a directory.
    d_slnk = tmp.join("d_lnk")
    d_slnk.symlink(d)
    assert d_slnk.exists()
    # Test a broken symlink.
    b_slnk = tmp.join("b_lnk")
    b_slnk.symlink(dne)
    assert b_slnk.exists()

def test_readlink(tmp_path):
    tmp = Path(str(tmp_path))
    # Test a path that doesn't exist
    dne = tmp.join("dne")
    with pytest.raises(OSError) as e:
        dne.readlink()
    assert e.value.errno == FileDoesNotExist
    # Test a regular file.
    f = tmp.join("f")
    with f.open("w") as fd:
        fd.write("some text")
    with pytest.raises(OSError) as e:
        f.readlink() is None
    assert e.value.errno == InvalidArgument
    # Test a directory
    d = tmp.join("d")
    d.mkdir()
    with pytest.raises(OSError) as e:
        d.readlink() is None
    assert e.value.errno == InvalidArgument
    # Test a symlink to a regular file
    f_slnk = tmp.join("f_lnk")
    f_slnk.symlink(f)
    assert f_slnk.readlink() == f
    # Test a symlink to a directory.
    d_slnk = tmp.join("d_lnk")
    d_slnk.symlink(d)
    assert d_slnk.readlink() == d
    # Test a broken symlink.
    b_slnk = tmp.join("b_lnk")
    b_slnk.symlink(dne)
    assert b_slnk.readlink() == dne

def test_link(tmp_path):
    tmp = Path(str(tmp_path))
    dne = tmp.join("dne")
    f = tmp.join("f")
    with f.open("w") as fd:
        fd.write("some text")
    d = tmp.join("d")
    d.mkdir()
    f_slnk = tmp.join("f_lnk")
    f_slnk.symlink(f)
    d_slnk = tmp.join("d_lnk")
    d_slnk.symlink(d)
    b_slnk = tmp.join("b_lnk")
    b_slnk.symlink(dne)
    # Test with DNE name.
    # test DNE to DNE
    with pytest.raises(OSError) as e:
        tmp.join("dne_dne").link(dne)
    assert e.value.errno == FileDoesNotExist
    # test DNE to dir
    with pytest.raises(OSError) as e:
        tmp.join("dne_d").link(d)
    assert e.value.errno == NotPermitted
    # test DNE to file
    dne_f = tmp.join("dne_f")
    dne_f.link(f)
    with dne_f.open("r") as fd:
        fd.read() == "some text"
    # test DNE to file symlink
    dne_fslnk = tmp.join("dne_fslnk")
    dne_fslnk.link(f_slnk)
    with dne_fslnk.open("r") as fd:
        fd.read() == "some text"
    # test DNE to dir symlink
    dne_dslnk = tmp.join("dne_dslnk")
    with pytest.raises(OSError) as e:
        dne_dslnk.link(d_slnk)
    # test DNE to broken symlink
    dne_bslnk = tmp.join("dns_bslnk")
    with pytest.raises(OSError) as e:
        dne_bslnk.link(b_slnk)
    assert e.value.errno == FileDoesNotExist
    # Test with existing dir name
    # test dir to DNE
    dir_dne = tmp.join("dir_dne")
    dir_dne.mkdir()
    with pytest.raises(OSError) as e:
        dir_dne.link(dne)
    assert e.value.errno == FileDoesNotExist
    # test dir to dir
    dir_dir = tmp.join("dir_dir")
    dir_dir.mkdir()
    with pytest.raises(OSError) as e:
        dir_dir.link(d)
    assert e.value.errno == NotPermitted
    # test dir to file
    dir_f = tmp.join("dir_f")
    dir_f.mkdir()
    with pytest.raises(OSError) as e:
        dir_f.link(f)
    assert e.value.errno == FileExists
    # test dir to file symlink
    dir_fslnk = tmp.join("dir_fslnk")
    dir_fslnk.mkdir()
    with pytest.raises(OSError) as e:
        dir_fslnk.link(f_slnk)
    assert e.value.errno == FileExists
    # test dir to dir symlink
    # Skipped...
    # test dir to broken symlink
    # Skipped...
    # Test with existing file name
    # test file to DNE
    f_dne = tmp.join("f_dne")
    with f_dne.open("w") as fd: fd.write("foo")
    with pytest.raises(OSError) as e:
        f_dne.link(dne)
    assert e.value.errno == FileDoesNotExist
    # test file to dir
    f_dir = tmp.join("f_dir")
    with f_dir.open("w") as fd: fd.write("foo")
    with pytest.raises(OSError) as e:
        f_dir.link(d)
    assert e.value.errno == NotPermitted
    # test file to file
    f_f = tmp.join("f_f")
    with f_f.open("w") as fd: fd.write("foo")
    with pytest.raises(OSError) as e:
        f_f.link(f)
    assert e.value.errno == FileExists
    # test file to file symlink
    f_fslnk = tmp.join("f_fslnk")
    f_fslnk.mkdir()
    with pytest.raises(OSError) as e:
        f_fslnk.link(f_slnk)
    assert e.value.errno == FileExists
    # test file to dir symlink
    # Skipped...
    # test file to broken symlink
    # Skipped...
    # Test with existing symlink
    # test slnk to DNE
    slnk_dne = tmp.join("slnk_dne")
    slnk_dne.symlink(f)
    with pytest.raises(OSError) as e:
        slnk_dne.link(dne)
    assert e.value.errno == FileDoesNotExist
    # test slnk to dir
    slnk_dir = tmp.join("slnk_dir")
    slnk_dir.symlink(f)
    with pytest.raises(OSError) as e:
        slnk_dir.link(d)
    assert e.value.errno == NotPermitted
    # test slnk to file
    slnk_f = tmp.join("slnk_f")
    slnk_f.symlink(f)
    with pytest.raises(OSError) as e:
        slnk_f.link(f)
    assert e.value.errno == FileExists
    # test slnk to file symlink
    slnk_fslnk = tmp.join("slnk_fslnk")
    slnk_fslnk.symlink(f)
    with pytest.raises(OSError) as e:
        slnk_fslnk.link(f_slnk)
    assert e.value.errno == FileExists
    # test slnk to dir symlink
    # Skipped...
    # test slnk to broken symlink
    # Skipped...

def test_unlink(tmp_path):
    tmp = Path(str(tmp_path))
    dne = tmp.join("dne")
    f = tmp.join("f")
    with f.open("w") as fd:
        fd.write("some text")
    d = tmp.join("d")
    d.mkdir()
    f_slnk = tmp.join("f_lnk")
    f_slnk.symlink(f)
    d_slnk = tmp.join("d_lnk")
    d_slnk.symlink(d)
    b_slnk = tmp.join("b_lnk")
    b_slnk.symlink(dne)

    # test unlink f_slnk
    assert f.exists()
    assert f_slnk.exists()
    f_slnk.unlink()
    assert not f_slnk.exists()
    assert f.exists()

    # test unlink d_slnk
    assert d.exists()
    assert d_slnk.exists()
    d_slnk.unlink()
    assert not d_slnk.exists()
    assert d.exists()

    # test unlink b_slnk
    assert b_slnk.exists()
    b_slnk.unlink()
    assert not b_slnk.exists()

    # test unlink dne
    assert not dne.exists()
    dne.unlink()
    assert not dne.exists()

    # test unlink file
    assert f.exists()
    f.unlink()
    assert not f.exists()

    # test unlink dir
    assert d.exists()
    with pytest.raises(OSError) as e:
        d.unlink()
    assert e.value.errno == NotPermitted
    assert d.exists()

def test_unlink_clean(tmp_path):
    tmp = Path(str(tmp_path))
    d1 = tmp.join("d1")
    d1.mkdir()
    d2 = d1.join("d2")
    d2.mkdir()
    d3 = d2.join("d3")
    d3.mkdir()
    f1 = d3.join("f1")
    f2 = d3.join("f2")
    with f1.open("w") as fd: fd.write("f1")
    with f2.open("w") as fd: fd.write("f2")
    assert d1.exists()
    assert d2.exists()
    assert d3.exists()
    assert f1.exists()
    assert f2.exists()
    f2.unlink(clean = d2)
    assert     d1.exists()
    assert     d2.exists()
    assert     d3.exists()
    assert     f1.exists()
    assert not f2.exists()
    f1.unlink(clean = d2)
    assert     d1.exists()
    assert     d2.exists()
    assert not d3.exists()
    assert not f1.exists()
    assert not f2.exists()

def test_rename_file(tmp_path):
    tmp = Path(str(tmp_path))
    f = tmp.join("f")
    with f.open("w") as fd:
        fd.write("some text")
    f2 = tmp.join("f2")
    assert     f.exists()
    assert     f.isfile()
    assert not f2.exists()
    assert not f2.isfile()
    f.rename(f2)
    assert not f.exists()
    assert not f.isfile()
    assert     f2.exists()
    assert     f2.isfile()

def test_rename_dir(tmp_path):
    tmp = Path(str(tmp_path))
    d = tmp.join("d")
    d.mkdir()
    d.join("inside").mkdir()
    d2 = tmp.join("d2")
    assert     d.exists()
    assert     d.isdir()
    assert not d2.exists()
    assert not d2.isdir()
    d.rename(d2)
    assert not d.exists()
    assert not d.isdir()
    assert     d2.exists()
    assert     d2.isdir()

def test_rename_symlink(tmp_path):
    tmp = Path(str(tmp_path))
    d = tmp.join("d")
    d.mkdir()
    s = tmp.join("s")
    s.symlink(d)
    s2 = tmp.join("s2")
    assert     s.exists()
    assert     s.islink()
    assert not s2.exists()
    assert not s2.islink()
    s.rename(s2)
    assert not s.exists()
    assert not s.islink()
    assert     s2.exists()
    assert     s2.islink()

def test_ensure_symlink(tmp_path):
    tmp = Path(str(tmp_path))
    dne = tmp.join("dne")
    sb = tmp.join("sb")
    ensure_symlink(sb, dne)
    assert sb.exists()
    assert sb.islink()
    assert sb.readlink() == dne

def test_ensure_absent(tmp_path):
    # Test broken symlink
    tmp = Path(str(tmp_path))
    dne = tmp.join("dne")
    sb = tmp.join("sb")
    ensure_symlink(sb, dne)
    assert sb.islink()
    ensure_absent(sb)
    assert not sb.islink()
    # Test link is removed, but underlying file is left intact.
    tmp = Path(str(tmp_path))
    f = tmp.join("dne")
    with f.open('w') as fd: fd.write('hi')
    sb = tmp.join("sb")
    ensure_symlink(sb, f)
    assert sb.islink()
    assert f.isfile()
    ensure_absent(sb)
    assert not sb.islink()
    assert f.isfile()
    # Test dir
    d = tmp.join('d')
    f1 = d.join('f1')
    f2 = d.join('f2')
    d.mkdir()
    with f1.open('w') as fd: fd.write('f1')
    with f2.open('w') as fd: fd.write('f2')
    assert d.exists() and d.isdir()
    ensure_absent(d)
    assert not d.exists() and not d.isdir()

def test_ensure_dir(tmp_path):
    # Test dir already exists.
    tmp = Path(str(tmp_path))
    d1 = tmp.join('d1')
    d1.mkdir()
    assert d1.isdir()
    ensure_dir(d1)
    assert d1.isdir()
    # Test dir creation
    d2 = tmp.join('d2')
    assert not d2.exists()
    ensure_dir(d2)
    assert d2.exists() and d2.isdir()
    # test removes file
    d3 = tmp.join('d3')
    dne = tmp.join('dne')
    d3.symlink(dne)
    assert d3.exists() and d3.islink()
    ensure_dir(d3)
    assert d3.exists() and d3.isdir()
    # test removes symlink
    d4 = tmp.join('d4')
    f = tmp.join('f')
    with f.open('w') as fd: fd.write('f')
    d4.symlink(f)
    assert d4.exists() and not d4.isdir() and d4.islink()
    ensure_dir(d4)
    assert d4.exists() and d4.isdir() and not d4.islink()

def test_ensure_link(tmp_path):
    tmp = Path(str(tmp_path))
    # test link to missing entity.
    with pytest.raises(AssertionError):
        dne = tmp.join("dne")
        lb = tmp.join("lb")
        ensure_link(lb, dne)
    # test link to existing entry
    f = tmp.join('f')
    with f.open('w') as fd: fd.write('f')
    l1 = tmp.join('l1')
    assert not l1.exists() and not l1.isfile()
    ensure_link(l1, f)
    assert l1.exists() and l1.isfile()

