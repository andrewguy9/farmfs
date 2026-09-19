"""
Hypothesis-based tree generator for snapshot/diff/patch tests.

Replaces the exhaustive canonicalization-based generator in trees2.py
(shapes + restricted-growth-string csum partitions) with random generation
plus automatic shrinking. Step 1 of the migration: reproduces exactly
today's coverage -- a rooted tree of dirs and blob-links only, nothing new.
Non-blob link kinds (relative in-depot links, absolute-in-depot links,
broken links, chains) are added incrementally on top of this foundation in
later work; see /Users/andrewthomson/.claude/plans/lexical-sniffing-horizon.md.

Public API
----------
trees() -> SearchStrategy[List[Dict]]
    Each draw produces a tree encoded as a flat list of SnapshotItem-like
    dicts, identical in shape to trees2.generate_trees2()'s output:
        {"path": Path, "type": LINK|DIR, "csum": str|None}

    The csum for links is a decimal string of a small class index ("0",
    "1", ...), matching trees2.py's convention -- callers needing a real
    MD5 digest still call csum_bytes(int(item["csum"])) themselves.

Callers use this directly with @given(tree=trees()) rather than pytest
parametrize -- Hypothesis's own execution model (many calls per test
function, shrinking on failure) doesn't fit the old fixture-based wiring.
"""

from __future__ import annotations

from typing import Dict, List, Optional, Tuple

from hypothesis import strategies as st

from farmfs.fs import DIR, LINK, ROOT, Path

ALPHABET = list("abcdefghijklmnopqrstuvwxyz")

# Small, fixed pool of csum classes -- mirrors trees2.py's restricted-growth
# style coverage (repeated classes exercise dedup) without needing canonical
# partition enumeration, which has no meaning for a random generator.
MAX_CSUM_CLASSES = 4


class _Node:
    """kind: DIR or LINK. children: list of (name, _Node), DIR only."""

    __slots__ = ("kind", "children", "csum_class")

    def __init__(self, kind: str, children: Optional[List[Tuple[str, "_Node"]]] = None, csum_class: Optional[int] = None):
        self.kind = kind
        self.children = children if children is not None else []
        self.csum_class = csum_class


@st.composite
def _node(draw, remaining_budget: int) -> _Node:
    """
    Draw a single node (dir or link leaf). Dirs recurse into children,
    consuming from the same shared budget via _children (see below) --
    remaining_budget bounds how many more nodes this subtree may contain,
    keeping generated trees small enough to materialize and shrink quickly.
    """
    if remaining_budget <= 0 or draw(st.booleans()):
        csum_class = draw(st.integers(min_value=0, max_value=MAX_CSUM_CLASSES - 1))
        return _Node(LINK, csum_class=csum_class)
    children = draw(_children(remaining_budget - 1))
    return _Node(DIR, children=children)


@st.composite
def _children(draw, budget: int) -> List[Tuple[str, _Node]]:
    """
    Draw a list of (name, _Node) pairs for a dir's children, consuming at
    most `budget` nodes total across all children combined, using distinct
    names from ALPHABET in order (mirrors trees2.py's deterministic naming).
    """
    if budget <= 0:
        return []
    count = draw(st.integers(min_value=0, max_value=min(budget, len(ALPHABET))))
    result: List[Tuple[str, _Node]] = []
    left = budget
    for i in range(count):
        if left <= 0:
            break
        child = draw(_node(left))
        result.append((ALPHABET[i], child))
        left -= 1 + _count_nodes(child) - 1  # child itself + its own descendants already drawn
    return result


def _count_nodes(node: _Node) -> int:
    if node.kind == LINK:
        return 1
    return 1 + sum(_count_nodes(c) for _, c in node.children)


def _node_to_items(node: _Node, rel_path: str) -> List[Dict]:
    """Flatten a _Node tree to snapshot-item dicts, same shape as trees2.py."""
    items: List[Dict] = []
    if node.kind == DIR:
        path = ROOT if rel_path == "/" else Path(rel_path)
        items.append({"path": path, "type": DIR, "csum": None})
        for name, child in node.children:
            child_path = rel_path.rstrip("/") + "/" + name
            items.extend(_node_to_items(child, child_path))
    else:
        items.append({"path": Path(rel_path), "type": LINK, "csum": str(node.csum_class)})
    return items


@st.composite
def _tree(draw, max_nodes: int) -> List[Dict]:
    root_children = draw(_children(max_nodes))
    root = _Node(DIR, children=root_children)
    return _node_to_items(root, "/")


def trees(max_nodes: int = 8) -> "st.SearchStrategy[List[Dict]]":
    """
    Strategy producing trees structurally equivalent to trees2.generate_trees2()'s
    output: a rooted tree of dirs and blob-links, flattened to a list of
    {"path", "type", "csum"} dicts.

    max_nodes bounds the number of non-root nodes per draw (kept small so
    materialization and shrinking stay fast).
    """
    return _tree(max_nodes)


def csum_bytes(class_idx: int) -> bytes:
    """Map a class index to a deterministic bytes value for MD5 hashing."""
    return str(class_idx).encode()
