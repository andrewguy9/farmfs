from farmfs.volume import KeySnapshot, tree_diff
from itertools import permutations, combinations
from re import search
from farmfs.fs import sep, ROOT, Path
from tests.trees import makeLink
from functools import reduce
from farmfs.util import uncurry


def produce_mismatches(segments):
    """Helper function to produce pairs of paths which have lexographical/path order mismatches"""
    paths = list(
        filter(
            lambda p: search("//", p) is None,
            map(
                lambda p: sep + p,
                map(
                    lambda s: reduce(lambda x, y: x + y, s),
                    permutations(segments, len(segments)),
                ),
            ),
        )
    )
    combos = list(combinations(paths, 2))
    is_mismatch = uncurry(lambda x, y: bool(x < y) != bool(Path(x) < Path(y)))
    mismatches = list(filter(is_mismatch, combos))
    return mismatches


def test_mismatches_possible():
    """Demonstates that lexographical order and path order are not the same for some kinds of segments."""
    letters = list("abc/+")
    assert len(produce_mismatches(letters)) > 0


def test_tree_diff_order():
    name_a = "/a/+b"
    name_b = "/a+/b"

    path_a = Path(name_a)
    path_b = Path(name_b)

    link_a = makeLink(path_a, "00000000000000000000000000000000")
    link_b = makeLink(path_b, "00000000000000000000000000000000")

    left = KeySnapshot([link_a], "left", None)
    right = KeySnapshot([link_b], "right", None)

    diff = tree_diff(left, right)
    paths = list(map(lambda change: change.path(ROOT), diff))
    assert paths == [path_a, path_b]
