from farmfs.volume import *
from itertools import permutations
import re
from farmfs.fs import sep, ROOT

def produce_mismatches():
  """ Helper function to produce pairs of paths which have lexographical/path order mismatches"""
  letters = list("abc/+")
  paths = filter(lambda p: re.search("//", p) is None, map(lambda p: sep+p, map(lambda s: reduce(lambda x,y:x+y, s), permutations(letters, 5))))
  combos = list(combinations(paths,2))
  mismatches = filter(lambda (x,y): bool(x<y) != bool(Path(x) < Path(y)), combos)
  return mismatches

def test_mismatches_possible():
  assert len(produce_mismatches()) > 0

def makeLink(path):
    return {"path": path, "csum": "00000000000000000000000000000000", "type": "link"}

def test_tree_diff_order():
  name_a =  "/a/+b"
  name_b = "/a+/b"

  path_a = Path(name_a)
  path_b = Path(name_b)

  link_a = makeLink(name_a)
  link_b = makeLink(name_b)

  left  = KeySnapshot([link_a], "left",  None)
  right = KeySnapshot([link_b], "right", None)

  diff = tree_diff(left, right)
  paths = map(lambda change: change.path(ROOT), diff)
  assert paths == [path_a, path_b]
