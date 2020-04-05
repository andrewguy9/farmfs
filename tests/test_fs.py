from farmfs.fs import normpath as _normalize
from farmfs.fs import userPath2Path as up2p
from farmfs.fs import Path, FileDoesNotExist
import pytest

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
def test_checksum_empty(tmp_path, input, expected):
  tmp = Path(str(tmp_path))
  fp = tmp.join("empty.txt")
  with fp.open("wb") as fd:
      fd.write(input)
  assert fp.checksum() == expected

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

