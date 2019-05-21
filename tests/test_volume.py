from farmfs.volume import *
from itertools import permutations, combinations, chain, product
from  re import search
from farmfs.fs import sep, ROOT, Path
from collections import defaultdict
import pytest

def produce_mismatches(segments):
  """ Helper function to produce pairs of paths which have lexographical/path order mismatches"""
  paths = filter(lambda p: search("//", p) is None, map(lambda p: sep+p, map(lambda s: reduce(lambda x,y:x+y, s), permutations(segments, len(segments)))))
  combos = list(combinations(paths,2))
  mismatches = filter(lambda (x,y): bool(x<y) != bool(Path(x) < Path(y)), combos)
  return mismatches

def test_mismatches_possible():
  """Demonstates that lexographical order and path order are not the same for some kinds of segments."""
  letters = list("abc/+")
  assert len(produce_mismatches(letters)) > 0

def makeLink(path, csum):
    return {"path": path, "csum": csum, "type": "link"}

def makeDir(path):
    return {"path": path, "type": "dir"}

def parents(paths):
    ppaths = set([ROOT]).union(map(lambda p: p.parent(), paths))
    return ppaths

def leaves(paths):
    ppaths = parents(paths)
    lpaths = set(paths).difference(ppaths)
    return lpaths

def makeLinkPermutations(paths, csum_options):
    path_csum = product(paths, csum_options)
    links = {path:
            map(lambda csum: makeLink(path, csum), csum_options)
            for path in paths}
    return defaultdict(list, links)

def makeDirectoryPermutations(paths):
    dirs = {path: [makeDir(path)] for path in paths}
    return defaultdict(list, dirs)

def makeTreeOptionDict(paths, csums):
    ppaths = parents(paths)
    assert ROOT in ppaths
    lpaths = leaves(paths)
    dirPaths = ppaths.union(lpaths)
    linkPaths = lpaths
    dirCombos = makeDirectoryPermutations(dirPaths)
    linkCombos = makeLinkPermutations(linkPaths, csums)
    combined = {path: dirCombos[path] + linkCombos[path] for path in paths}
    return combined

def permuteOptions(seq, options):
    optionSeq = [options[item] for item in seq]
    return product(*optionSeq)

def makeTreeOptions(tree, csums):
    return permuteOptions(tree, makeTreeOptionDict(tree, csums))

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

def permute_deep(options):
    options = [permutations(options, pick) for pick in range(1,1+len(options))]
    return list(chain.from_iterable(options))

def combine_deep(options):
    options = [combinations(options, pick) for pick in range(1,1+len(options))]
    return list(chain.from_iterable(options))

#TODO we are generating Path here, but keySnap needs to be tolerant of that. It wants BaseString
def generate_paths(names):
    return map(Path, ["/"]+list(map(lambda segs: "/"+"/".join(segs), permute_deep(names))))

def orphans(paths):
    accum = set()
    for path in paths:
        accum.add(path)
        parent = path.parent()
        if path != ROOT and parent not in accum:
            yield path

def has_orphans(paths):
    return len(list(orphans(paths))) > 0

def no_orphans(paths):
    return not has_orphans(paths)

def tree_shapes(names):
    paths = generate_paths(names)
    shapes = combine_deep(paths)
    return filter(no_orphans, shapes)

def generate_trees():
    shapes = tree_shapes(["a", "b", "+"])
    trees = list(chain(*map(lambda tree: makeTreeOptions(tree, ["1","2"]), shapes)))
    return trees

@pytest.mark.parametrize("index,tree", enumerate(generate_trees()))
def test_tree(index, tree):
    try:
        assert len(tree)>=1
        assert tree[0]['path'] == ROOT
        assert tree[0]['type'] == 'dir'
    except AssertionError as e:
        print "Bad tree:", index, tree
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

#TODO split case generation and application.
def test_tree_diff():
    shapes = tree_shapes(["a","b"])
    trees = chain(*map(lambda tree: makeTreeOptions(tree, ["1","2"]), shapes))
    for before, after in combinations(trees, 2):
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
            print "Conditions:", before, "->", after, "with changes", map(str, deltas)
            raise
