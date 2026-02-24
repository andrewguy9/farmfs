"""
Non-redundant tree generator for snapshot/diff/patch tests.

A tree is an implicit root dir plus N "variables", each either a link(csum_class)
or a dir(children).  Names within a dir are assigned from a fixed alphabet
["a", "b", "c", ...] in sorted order — name assignment is deterministic.

Two trees are the *same test* if one is a relabelling of checksum-class indices,
so we enumerate only canonical checksum partitions (restricted growth strings).

Public API
----------
generate_trees2(max_n=3) -> List[dict]
    Each dict is a tree encoded as a flat list of SnapshotItem-like dicts
    suitable for constructing a KeySnapshot:
        {"path": Path, "type": LINK|DIR, "csum": str|None}

The csum for links is a decimal string of the class index ("0", "1", ...).
Callers that need real MD5 digests should call csum_bytes(class_idx) and
then build_checksum(csum_bytes(class_idx)) from conftest.
"""

from __future__ import annotations

from typing import Dict, Iterator, List, Optional, Tuple

from farmfs.fs import DIR, LINK, ROOT, Path

ALPHABET = list("abcdefghijklmnopqrstuvwxyz")


# ---------------------------------------------------------------------------
# Internal tree representation
# ---------------------------------------------------------------------------

class _Node:
    """
    Internal node used during enumeration.
    kind  : "link" | "dir"
    csum_class: int | None  -- set later during csum assignment; None for dirs
    children: list of (name, _Node)
    """

    def __init__(self, kind: str, children: Optional[List[Tuple[str, "_Node"]]] = None):
        assert kind in ("link", "dir")
        self.kind = kind
        self.children: List[Tuple[str, "_Node"]] = children if children is not None else []
        self.csum_class: Optional[int] = None

    def __repr__(self) -> str:
        if self.kind == "link":
            return f"link({self.csum_class})"
        child_str = ", ".join(f"{n}:{c!r}" for n, c in self.children)
        return f"dir({child_str})"


# ---------------------------------------------------------------------------
# Step 1: enumerate shapes
# ---------------------------------------------------------------------------

def _shapes_children(n: int) -> Iterator[List[Tuple[str, _Node]]]:
    """
    Yield all ways to distribute n nodes among children of a dir node.
    Each child is either a link leaf (costs 1) or a dir with k>=0 children
    (costs 1+k). Children are assigned names a, b, c, ... in order.
    n==0 => one result: empty child list.
    """
    if n == 0:
        yield []
        return

    def recurse(remaining: int, so_far: List[Tuple[str, _Node]]) -> Iterator[List[Tuple[str, _Node]]]:
        if remaining == 0:
            yield list(so_far)
            return
        # Next child name
        idx = len(so_far)
        if idx >= len(ALPHABET):
            return  # ran out of names
        name = ALPHABET[idx]
        # Option A: this child is a link leaf (costs 1)
        link_node = _Node("link")
        so_far.append((name, link_node))
        yield from recurse(remaining - 1, so_far)
        so_far.pop()
        # Option B: this child is a dir with k>=0 sub-children (costs 1+k)
        for k in range(0, remaining):  # dir itself costs 1, sub-children cost k
            sub_children_list = list(_shapes_children_list(k))
            for sub_children in sub_children_list:
                dir_node = _Node("dir", sub_children)
                so_far.append((name, dir_node))
                yield from recurse(remaining - 1 - k, so_far)
                so_far.pop()

    yield from recurse(n, [])


def _shapes_children_list(n: int) -> Iterator[List[Tuple[str, _Node]]]:
    """Same as _shapes_children but returns a list (used internally)."""
    yield from _shapes_children(n)


def shapes(n: int) -> Iterator[_Node]:
    """
    Yield all shapes (rooted trees) with exactly n variables.
    """
    for children in _shapes_children(n):
        yield _Node("dir", children)


# ---------------------------------------------------------------------------
# Step 2: canonical checksum assignment (restricted growth strings)
# ---------------------------------------------------------------------------

def _link_leaves(node: _Node) -> List[_Node]:
    """Collect all link leaf nodes in left-to-right order."""
    result: List[_Node] = []

    def collect(n: _Node) -> None:
        if n.kind == "link":
            result.append(n)
        else:
            for _, child in n.children:
                collect(child)

    collect(node)
    return result


def _assign_csums(leaves: List[_Node], assignments: List[int]) -> None:
    """Assign csum_class values to link leaves from the given assignment list."""
    for leaf, cls in zip(leaves, assignments):
        leaf.csum_class = cls


def _canonical_partitions(k: int) -> Iterator[List[int]]:
    """
    Yield all canonical partitions of k items using restricted growth strings.
    These are sequences s[0..k-1] where s[0]=0 and s[i] <= max(s[0..i-1])+1.
    Equivalent to Bell(k) partitions.
    """
    if k == 0:
        yield []
        return

    def recurse(pos: int, so_far: List[int], next_class: int) -> Iterator[List[int]]:
        if pos == k:
            yield list(so_far)
            return
        # Reuse existing classes 0..next_class-1
        for c in range(next_class):
            so_far.append(c)
            yield from recurse(pos + 1, so_far, next_class)
            so_far.pop()
        # Start a new class
        so_far.append(next_class)
        yield from recurse(pos + 1, so_far, next_class + 1)
        so_far.pop()

    yield from recurse(0, [], 0)


# ---------------------------------------------------------------------------
# Step 3: flatten tree to snapshot-item-like dicts
# ---------------------------------------------------------------------------

def _node_to_items(node: _Node, rel_path: str) -> Iterator[Dict]:
    """
    Recursively flatten a _Node tree to snapshot-item dicts.
    rel_path is the path string relative to root (e.g. "/a/b").
    """
    if node.kind == "dir":
        if rel_path == "/":
            path = ROOT
        else:
            path = Path(rel_path)
        yield {"path": path, "type": DIR, "csum": None}
        for name, child in node.children:
            child_path = (rel_path.rstrip("/") + "/" + name)
            yield from _node_to_items(child, child_path)
    else:  # link
        path = Path(rel_path)
        assert node.csum_class is not None
        yield {"path": path, "type": LINK, "csum": str(node.csum_class)}


def _tree_to_items(root_node: _Node) -> List[Dict]:
    return list(_node_to_items(root_node, "/"))


# ---------------------------------------------------------------------------
# Step 4: combine — public API
# ---------------------------------------------------------------------------

def generate_trees2(max_n: int = 3) -> List[List[Dict]]:
    """
    Generate all non-redundant trees up to max_n variables.

    Returns a list of trees, where each tree is a list of dicts:
        {"path": Path, "type": LINK|DIR, "csum": str|None}

    The csum for links is the string representation of the canonical class
    index ("0", "1", ...).  To get a real MD5 digest, call:
        build_checksum(csum_bytes(int(item["csum"])))
    """
    import copy
    result = []
    for n in range(0, max_n + 1):
        for shape in shapes(n):
            leaves = _link_leaves(shape)
            k = len(leaves)
            for partition in _canonical_partitions(k):
                # Work on a fresh copy so leaves list references stay valid
                shape_copy = copy.deepcopy(shape)
                leaves_copy = _link_leaves(shape_copy)
                _assign_csums(leaves_copy, partition)
                result.append(_tree_to_items(shape_copy))
    return result


# ---------------------------------------------------------------------------
# Helpers for tests
# ---------------------------------------------------------------------------

def csum_bytes(class_idx: int) -> bytes:
    """Map a class index to a deterministic bytes value for MD5 hashing."""
    return str(class_idx).encode()
