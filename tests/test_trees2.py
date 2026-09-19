"""
Tests for the hyp_trees.py generator itself.

Verifies structural well-formedness of generated trees (root dir present,
every link has a csum, every dir has none) and that Hypothesis's shrinking
converges to a small counterexample on a deliberately-broken invariant.
These replace trees2.py's exhaustive-enumeration self-tests (no duplicate
trees, no csum-relabelling duplicates, exact shape/tree counts) which have
no meaning for a random generator -- there's no fixed universe to check
duplicates against, and "how many trees" isn't a property of a sampler.
"""

from hypothesis import given, settings

from farmfs.fs import DIR, LINK
from tests.hyp_trees import trees


@given(tree=trees())
def test_every_tree_has_root_dir(tree):
    """Every tree must start with the root dir item."""
    assert len(tree) >= 1
    root_items = [i for i in tree if str(i["path"]) in ("/", ".") and i["type"] == DIR]
    assert len(root_items) == 1, f"Expected exactly one root dir, got {root_items} in {tree}"


@given(tree=trees())
def test_every_link_has_csum(tree):
    """Every link item must have a non-None csum."""
    for item in tree:
        if item["type"] == LINK:
            assert item.get("csum") is not None, f"Link missing csum: {item}"


@given(tree=trees())
def test_every_dir_has_no_csum(tree):
    """Every dir item must have csum=None."""
    for item in tree:
        if item["type"] == DIR:
            assert item.get("csum") is None, f"Dir has unexpected csum: {item}"


@given(tree=trees())
def test_no_duplicate_paths_within_a_tree(tree):
    """A single generated tree must never repeat a path."""
    paths = [str(item["path"]) for item in tree]
    assert len(paths) == len(set(paths)), f"Duplicate paths in tree: {tree}"


@given(tree=trees(max_nodes=8))
def test_node_budget_is_respected(tree):
    """max_nodes bounds the number of non-root nodes per draw."""
    non_root_count = len(tree) - 1
    assert non_root_count <= 8, f"Exceeded budget: {non_root_count} nodes in {tree}"


def test_shrinking_converges_on_a_broken_invariant():
    """
    Sanity check the harness itself: given a deliberately-wrong invariant
    (no tree may contain more than 2 nodes total), Hypothesis must find a
    failing example and shrink it to something minimal and legible -- not
    leave a large, hard-to-read counterexample.
    """
    import pytest

    captured = {}

    @given(tree=trees())
    @settings(max_examples=200, database=None)
    def _check(tree):
        captured["tree"] = tree  # overwritten on every call; last one is the shrunk minimal failure
        assert len(tree) <= 2

    with pytest.raises(AssertionError):
        _check()

    # The minimal way to violate "at most 2 nodes" is a 3-item tree (root +
    # 2 non-root nodes) -- shrinking must land on exactly that, not some
    # larger unshrunk example.
    assert len(captured["tree"]) == 3, f"Shrinking did not converge to a minimal example: {captured['tree']}"
