from farmfs.fs import normpath as _normalize
from farmfs.fs import userPath2Path as up2p
from farmfs.fs import Path
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
