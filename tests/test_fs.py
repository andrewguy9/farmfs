from farmfs.fs import normpath as _normalize

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


