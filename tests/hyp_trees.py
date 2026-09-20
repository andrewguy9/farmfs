"""
Hypothesis-based tree generator for snapshot/diff/patch tests.

Replaces the exhaustive canonicalization-based generator in trees2.py
(shapes + restricted-growth-string csum partitions) with random generation
plus automatic shrinking. Step 1 of the migration reproduced exactly the
old coverage -- a rooted tree of dirs and blob-links only. Step 3
incrementally adds new link kinds; see
/Users/andrewthomson/.claude/plans/lexical-sniffing-horizon.md for the plan.

Increment 1 (2026-09-19): ABS_LINK -- a symlink whose target is another
node already in the tree, written as an absolute in-depot path (NOT the
canonical .farmfs/userdata/<csum> blob path -- an ordinary absolute path
to another tracked file/dir, e.g. /vol/a/b). This is exactly the "archive
a drive, it already had an absolute symlink to another file on it" case.
No cycles: a link can only target a node already placed earlier in the
same generation pass (see _pool in _items()), so the target graph is a
DAG by construction, same trick discussed in the (unbuilt) exhaustive
design this replaced.

Public API
----------
trees(include_abs_links=False) -> SearchStrategy[List[Dict]]
    Each draw produces a tree encoded as a flat list of SnapshotItem-like
    dicts:
        {"path": Path, "type": LINK|DIR, "csum": str|None, "target": Path|None}

    include_abs_links=False (the default) produces exactly today's coverage
    -- dirs and blob-links only, "target" always None -- so every existing
    caller (test_snap.py, test_pull.py, test_diff.py, whose _build_tree
    helpers don't know about ABS_LINK and can't materialize one) is
    unaffected. Pass include_abs_links=True to opt into ABS_LINK items,
    identified by csum is None AND type is LINK, target set to an absolute
    Path to another item already in the same tree. This is deliberately
    opt-in per the plan's "add one link kind at a time" sequencing -- it
    is not wired into the shared fixtures until a materializer and the
    production SnapshotItem/SnapDelta side can actually handle it.

    The csum for blob-links is a decimal string of a small class index
    ("0", "1", ...), matching trees2.py's convention -- callers needing a
    real MD5 digest still call csum_bytes(int(item["csum"])) themselves.

Callers use this directly with @given(tree=trees()) rather than pytest
parametrize -- Hypothesis's own execution model (many calls per test
function, shrinking on failure) doesn't fit the old fixture-based wiring.
"""

from __future__ import annotations

from typing import Dict, List

from hypothesis import strategies as st

from farmfs.fs import DIR, LINK, ROOT, Path

ALPHABET = list("abcdefghijklmnopqrstuvwxyz")

# Small, fixed pool of csum classes -- mirrors trees2.py's restricted-growth
# style coverage (repeated classes exercise dedup) without needing canonical
# partition enumeration, which has no meaning for a random generator.
MAX_CSUM_CLASSES = 4


@st.composite
def _items(draw, budget: int, rel_path: str, pool: List[Path], include_abs_links: bool) -> List[Dict]:
    """
    Draw a dir's children as a flat list of item dicts, consuming at most
    `budget` nodes total. `pool` is the list of absolute Paths already
    placed earlier in this generation pass (ancestors, earlier siblings,
    and everything already placed inside them) -- an ABS_LINK may only
    target something in `pool`, which guarantees the target graph is a DAG
    (a link can never target itself or anything placed after it).

    pool is mutated in place as items are placed, so later siblings (and
    this dir's own later children) see everything placed so far.
    """
    if budget <= 0:
        return []
    count = draw(st.integers(min_value=0, max_value=min(budget, len(ALPHABET))))
    items: List[Dict] = []
    left = budget
    for i in range(count):
        if left <= 0:
            break
        name = ALPHABET[i]
        child_path_str = rel_path.rstrip("/") + "/" + name
        child_path = Path(child_path_str)

        # Decide this child's kind. ABS_LINK only offered when enabled and
        # the pool is non-empty (nothing to target before root exists).
        kinds = ["dir", "blob"]
        if left > 1 and include_abs_links and pool:
            kinds.append("abs_link")
        kind = draw(st.sampled_from(kinds))

        if kind == "dir":
            sub_items = draw(_items(left - 1, child_path_str, pool, include_abs_links))
            items.append({"path": child_path, "type": DIR, "csum": None, "target": None})
            pool.append(child_path)
            items.extend(sub_items)
            left -= 1 + len(sub_items)
        elif kind == "blob":
            csum_class = draw(st.integers(min_value=0, max_value=MAX_CSUM_CLASSES - 1))
            items.append({"path": child_path, "type": LINK, "csum": str(csum_class), "target": None})
            pool.append(child_path)
            left -= 1
        else:  # abs_link
            target = draw(st.sampled_from(pool))
            items.append({"path": child_path, "type": LINK, "csum": None, "target": target})
            pool.append(child_path)
            left -= 1
    return items


@st.composite
def _tree(draw, max_nodes: int, include_abs_links: bool) -> List[Dict]:
    pool: List[Path] = [ROOT]
    items = [{"path": ROOT, "type": DIR, "csum": None, "target": None}]
    items.extend(draw(_items(max_nodes, "/", pool, include_abs_links)))
    return items


def trees(max_nodes: int = 8, include_abs_links: bool = False) -> "st.SearchStrategy[List[Dict]]":
    """
    Strategy producing trees of dirs and blob-links (today's default
    coverage), plus abs-links when include_abs_links=True -- see module
    docstring. max_nodes bounds the number of non-root nodes per draw.
    """
    return _tree(max_nodes, include_abs_links)


def csum_bytes(class_idx: int) -> bytes:
    """Map a class index to a deterministic bytes value for MD5 hashing."""
    return str(class_idx).encode()
