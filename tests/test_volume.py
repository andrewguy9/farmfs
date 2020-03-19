from __future__ import print_function
from farmfs.volume import *
from itertools import permutations, combinations, chain, product
from  re import search
from farmfs.fs import sep, ROOT, Path
from tests.trees import makeLink
import pytest

def produce_mismatches(segments):
  """ Helper function to produce pairs of paths which have lexographical/path order mismatches"""
  paths = filter(lambda p: search("//", p) is None, map(lambda p: sep+p, map(lambda s: reduce(lambda x,y:x+y, s), permutations(segments, len(segments)))))
  combos = list(combinations(paths,2))
  mismatches = filter(lambda x_y: bool(x_y[0]<x_y[1]) != bool(Path(x_y[0]) < Path(x_y[1])), combos)
  return mismatches

def test_mismatches_possible():
  """Demonstates that lexographical order and path order are not the same for some kinds of segments."""
  letters = list("abc/+")
  assert len(produce_mismatches(letters)) > 0

def test_tree_diff_order():
  name_a =  "/a/+b"
  name_b = "/a+/b"

  path_a = Path(name_a)
  path_b = Path(name_b)

  link_a = makeLink(name_a, "00000000000000000000000000000000")
  link_b = makeLink(name_b, "00000000000000000000000000000000")

  left  = KeySnapshot([link_a], "left",  None)
  right = KeySnapshot([link_b], "right", None)

  diff = tree_diff(left, right)
  paths = map(lambda change: change.path(ROOT), diff)
  assert paths == [path_a, path_b]

def test_tree(tree):
    try:
        assert len(tree)>=1
        assert tree[0]['path'] == ROOT
        assert tree[0]['type'] == 'dir'
    except AssertionError as e:
        print("Bad tree:", tree)
        raise

def tree_csums(tree):
    links = filter(lambda t: t['type'] == 'link', tree)
    csums = set(map(lambda l: l['csum'], links))
    return csums

def tree_paths(tree):
    paths = set(map(lambda p: p['path'], tree))
    return paths

def find(pred, col):
    for val in col:
        if pred(val):
            return val
    return None

def test_tree_diff(trees):
    before, after = trees
    before_paths = tree_paths(before)
    after_paths = tree_paths(after)
    intersection_paths = before_paths.intersection(after_paths)
    before_csums = tree_csums(before)
    after_csums = tree_csums(after)

    expected_removed_paths = before_paths - after_paths
    expected_added_paths = after_paths - before_paths
    expected_removed_csums = before_csums - after_csums
    expected_added_csums = after_csums - before_csums

    beforeSnap = KeySnapshot(before, "before",  None)
    afterSnap = KeySnapshot(after, "after", None)
    deltas = list(tree_diff(beforeSnap, afterSnap))

    removed = filter(lambda d: d.mode == 'removed', deltas)
    removed_paths = set(map(lambda d: d.path(ROOT), removed))
    added = filter(lambda d: d.mode != 'removed', deltas)
    added_paths = set(map(lambda d: d.path(ROOT), added))
    extra_removed_paths = removed_paths - expected_removed_paths
    try:
        # extra_removed_paths should have moved from dir->link or link->dir.
        assert(expected_removed_paths <= removed_paths)
        extra_added_paths = added_paths - expected_added_paths
        # extra_added_paths should have moved
        assert(expected_added_paths <= added_paths)
        extras = extra_removed_paths.union(extra_added_paths)
        # Extras should appear on both sides.
        assert(all(map(lambda extra: extra in before_paths and extra in after_paths, extras)))

        removed_csums = set(map(lambda d: d.csum, removed))
        added_csums = set(map(lambda d: d.csum, added))
        # When a link is replaced, the CSUM for that link removed but not present in the diff.
        # assert(expected_removed_csums <= removed_csums)
        assert(expected_added_csums <= added_csums)
    except AssertionError as ae:
        print("Conditions:", before, "->", after, "with changes", map(str, deltas))
        raise
