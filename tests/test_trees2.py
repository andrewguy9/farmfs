"""
Tests for the trees2.py generator itself.

Verifies:
  1. No duplicate trees (no two entries are the same set of items)
  2. No two trees are csum-relabellings of each other (canonical partition guarantee)
  3. Exact counts for small n match known values
  4. Shape counts match known sequence: 1, 2, 6, 22, 90, 394 for n=0..5
  5. Every tree is well-formed (root dir present, paths consistent)
"""

from farmfs.fs import DIR, LINK
from tests.trees2 import generate_trees2, shapes


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _tree_key(tree):
    """Hashable, order-independent key for a tree."""
    return frozenset(
        (str(item["path"]), item["type"], item.get("csum"))
        for item in tree
    )


def _normalize_csums(tree):
    """
    Relabel csums in left-to-right first-occurrence order.
    Two trees with the same normalised form are csum-relabellings of each other.
    """
    mapping = {}
    next_id = [0]
    result = []
    for item in sorted(tree, key=lambda i: str(i["path"])):
        c = item.get("csum")
        if c is not None and c not in mapping:
            mapping[c] = str(next_id[0])
            next_id[0] += 1
        result.append((str(item["path"]), item["type"], mapping.get(c) if c is not None else None))
    return frozenset(result)


def _shape_counts(max_n):
    counts = []
    for n in range(max_n + 1):
        counts.append(sum(1 for _ in shapes(n)))
    return counts


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_no_duplicate_trees():
    """No two entries in generate_trees2 should be identical."""
    trees = generate_trees2(max_n=4)
    keys = [_tree_key(t) for t in trees]
    assert len(keys) == len(set(keys)), (
        f"Found {len(keys) - len(set(keys))} duplicate trees"
    )


def test_no_csum_relabellings():
    """No two trees should be related by a pure csum relabelling."""
    trees = generate_trees2(max_n=4)
    normalised = [_normalize_csums(t) for t in trees]
    assert len(normalised) == len(set(normalised)), (
        f"Found {len(normalised) - len(set(normalised))} csum-relabelling duplicates"
    )


def test_tree_counts():
    """
    Tree counts per n=0..4 verified by inspection:
      n=0: 1  (empty root)
      n=1: 2  (link(c0) | empty dir)
      n=2: 7
      n=3: 32
      n=4: (cumulative up to n=4)
    Check cumulative totals for max_n=0..3.
    """
    assert len(generate_trees2(max_n=0)) == 1
    assert len(generate_trees2(max_n=1)) == 3    # 1 + 2
    assert len(generate_trees2(max_n=2)) == 10   # 1 + 2 + 7
    assert len(generate_trees2(max_n=3)) == 42   # 1 + 2 + 7 + 32


def test_shape_counts():
    """
    Shape counts for n=0..5 should be: 1, 2, 6, 22, 90, 394.
    These are the number of structurally distinct ordered rooted trees
    where each node is either a link leaf or a dir (possibly empty).
    """
    expected = [1, 2, 6, 22, 90, 394]
    actual = _shape_counts(max_n=5)
    assert actual == expected, f"Shape counts: expected {expected}, got {actual}"


def test_every_tree_has_root_dir():
    """Every tree must start with the root dir item."""
    for tree in generate_trees2(max_n=3):
        assert len(tree) >= 1
        root_items = [i for i in tree if str(i["path"]) in ("/", ".") and i["type"] == DIR]
        assert len(root_items) == 1, f"Expected exactly one root dir, got {root_items} in {tree}"


def test_every_link_has_csum():
    """Every link item must have a non-None csum."""
    for tree in generate_trees2(max_n=3):
        for item in tree:
            if item["type"] == LINK:
                assert item.get("csum") is not None, f"Link missing csum: {item}"


def test_every_dir_has_no_csum():
    """Every dir item must have csum=None."""
    for tree in generate_trees2(max_n=3):
        for item in tree:
            if item["type"] == DIR:
                assert item.get("csum") is None, f"Dir has unexpected csum: {item}"


def test_csum_classes_are_canonical():
    """
    For each tree, csum classes must form a restricted growth string
    in left-to-right path order: first new class is always one more than
    the current maximum.
    """
    for tree in generate_trees2(max_n=3):
        links = [i for i in tree if i["type"] == LINK]
        seen = {}
        next_class = 0
        for item in links:
            c = item["csum"]
            if c not in seen:
                assert int(c) == next_class, (
                    f"Non-canonical csum class {c} (expected {next_class}) in {tree}"
                )
                seen[c] = next_class
                next_class += 1
