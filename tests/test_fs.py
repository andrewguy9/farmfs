from farmfs.fs import _normalize

def test_normalize_abs():
  assert _normalize("/")       == "/"
  assert _normalize("/a")      == "/a"
  assert _normalize("/a/")     == "/a"
  assert _normalize("/a/b")    == "/a/b"
  assert _normalize("/a/b/")   == "/a/b"
  assert _normalize("/a//b")   == "/a/b"
  assert _normalize("/a//b//") == "/a/b"

# TODO NORMALIZE CURRENTLY MAKES THINGS ABSOLUTE. THESE TESTS WILL FAIL.
# def test_normalize_relative():
#   assert _normalize("a")      == "a"
#   assert _normalize("a/")     == "a"
#   assert _normalize("a/b")    == "a/b"
#   assert _normalize("a/b/")   == "a/b"
#   assert _normalize("a//b")   == "a/b"
#   assert _normalize("a//b//") == "a/b"


