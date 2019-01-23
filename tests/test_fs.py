from farmfs.fs import normpath as _normalize
from farmfs.fs import userPath2Path as up2p
from farmfs.fs import Path

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
