"""
Tests for optional size field in SnapshotItem and related snapshot types.

Covers:
  S1  - SnapshotItem without size: size() returns None, not in dict/tuple
  S2  - SnapshotItem with size: size() returns value, present in dict/tuple
  S3  - SnapshotItem equality ignores size (two items same path/type/csum, different size, are equal)
  S4  - KeySnapshot list-format: 3-element (legacy) → size is None
  S5  - KeySnapshot list-format: 4-element (new) → size is preserved
  S6  - KeySnapshot dict-format without 'size' key → size is None
  S7  - KeySnapshot dict-format with 'size' key → size is preserved
  S8  - KeySnapshot passthrough: SnapshotItem with size passes through unchanged
  S9  - TreeSnapshot without sizer: size is None on all items
  S10 - TreeSnapshot with sizer: size is populated for LINK items, None for DIR
  S11 - Round-trip: write snap with size, read back, size is preserved
  S12 - Round-trip: write snap without size (legacy), read back, size is None
"""

from typing import cast

import pytest
from farmfs import getvol
from farmfs.volume import mkfs, tree_diff
from farmfs.snapshot import KeySnapshot, SnapshotItem
from farmfs.fs import Path, DIR, LINK
from tests.conftest import build_blob, build_link, build_dir
from tests.trees2 import csum_bytes


# ---------------------------------------------------------------------------
# Helpers shared with test_snap.py
# ---------------------------------------------------------------------------

def _make_vol(tmp_path_factory, suffix: str) -> Path:
    d = tmp_path_factory.mktemp(suffix)
    root = Path(str(d))
    udd = root.join(".farmfs").join("userdata")
    mkfs(root, udd)
    return root


def _identity_reverser(ref: str) -> str:
    """Reverser that returns the ref unchanged (for tests using plain csum strings)."""
    return ref


# ---------------------------------------------------------------------------
# S1 - SnapshotItem without size
# ---------------------------------------------------------------------------

def test_item_no_size_returns_none():
    item = SnapshotItem("a", LINK, "abc123")
    assert item.size() is None


def test_item_no_size_not_in_dict():
    item = SnapshotItem("a", LINK, "abc123")
    assert "size" not in item.get_dict()


def test_item_no_size_tuple_has_none():
    item = SnapshotItem("a", LINK, "abc123")
    assert item.get_tuple() == ("a", LINK, "abc123", None)


# ---------------------------------------------------------------------------
# S2 - SnapshotItem with size
# ---------------------------------------------------------------------------

def test_item_with_size_returns_value():
    item = SnapshotItem("a", LINK, "abc123", size=42)
    assert item.size() == 42


def test_item_with_size_in_dict():
    item = SnapshotItem("a", LINK, "abc123", size=42)
    assert item.get_dict()["size"] == 42


def test_item_with_size_tuple():
    item = SnapshotItem("a", LINK, "abc123", size=42)
    assert item.get_tuple() == ("a", LINK, "abc123", 42)


def test_dir_item_with_size_in_dict():
    item = SnapshotItem("d", DIR, size=0)
    assert item.get_dict()["size"] == 0


# ---------------------------------------------------------------------------
# S3 - Equality ignores size
# ---------------------------------------------------------------------------

def test_item_equality_ignores_size():
    a = SnapshotItem("a", LINK, "abc123", size=10)
    b = SnapshotItem("a", LINK, "abc123", size=99)
    assert a == b


def test_item_equality_no_size_vs_size():
    a = SnapshotItem("a", LINK, "abc123")
    b = SnapshotItem("a", LINK, "abc123", size=10)
    assert a == b


# ---------------------------------------------------------------------------
# S4 - KeySnapshot: 3-element legacy list → size is None
# ---------------------------------------------------------------------------

def test_key_snap_legacy_list_no_size():
    data = [["a", LINK, "abc123"]]
    snap = KeySnapshot(data, "test", _identity_reverser)
    items = list(snap)
    assert len(items) == 1
    assert items[0].size() is None
    assert items[0].csum() == "abc123"


def test_key_snap_legacy_list_dir_no_size():
    data = [["d", DIR, None]]
    snap = KeySnapshot(data, "test", _identity_reverser)
    items = list(snap)
    assert items[0].size() is None
    assert items[0].is_dir()


# ---------------------------------------------------------------------------
# S5 - KeySnapshot: 4-element new list → size is preserved
# ---------------------------------------------------------------------------

def test_key_snap_new_list_with_size():
    data = [["a", LINK, "abc123", 512]]
    snap = KeySnapshot(data, "test", _identity_reverser)
    items = list(snap)
    assert items[0].size() == 512


def test_key_snap_new_list_size_none_explicit():
    # 4-element list with None size (written by new code on dir)
    data = [["d", DIR, None, None]]
    snap = KeySnapshot(data, "test", _identity_reverser)
    items = list(snap)
    assert items[0].size() is None


# ---------------------------------------------------------------------------
# S6 - KeySnapshot: dict without 'size' key → size is None
# ---------------------------------------------------------------------------

def test_key_snap_dict_no_size():
    data = [{"path": "a", "type": LINK, "csum": "abc123"}]
    snap = KeySnapshot(data, "test", _identity_reverser)
    items = list(snap)
    assert items[0].size() is None


# ---------------------------------------------------------------------------
# S7 - KeySnapshot: dict with 'size' key → size is preserved
# ---------------------------------------------------------------------------

def test_key_snap_dict_with_size():
    data = [{"path": "a", "type": LINK, "csum": "abc123", "size": 1024}]
    snap = KeySnapshot(data, "test", _identity_reverser)
    items = list(snap)
    assert items[0].size() == 1024


# ---------------------------------------------------------------------------
# S8 - KeySnapshot: SnapshotItem passthrough preserves size
# ---------------------------------------------------------------------------

def test_key_snap_item_passthrough_with_size():
    item = SnapshotItem("a", LINK, "abc123", size=77)
    snap = KeySnapshot([item], "test", _identity_reverser)
    items = list(snap)
    assert items[0].size() == 77


def test_key_snap_item_passthrough_no_size():
    item = SnapshotItem("a", LINK, "abc123")
    snap = KeySnapshot([item], "test", _identity_reverser)
    items = list(snap)
    assert items[0].size() is None


# ---------------------------------------------------------------------------
# S9 - TreeSnapshot with blobstore: size populated for LINKs
# ---------------------------------------------------------------------------

def test_tree_snap_with_bs(tmp_path_factory):
    vol_path = _make_vol(tmp_path_factory, "vol")
    content = csum_bytes(0)
    csum = build_blob(vol_path, content)
    build_link(vol_path, "a", csum)

    from farmfs.snapshot import TreeSnapshot
    vol = getvol(vol_path)
    snap = TreeSnapshot(vol_path, vol.is_ignored, reverser=vol.bs.reverser, bs=vol.bs)
    items = [i for i in snap if i.is_link()]
    assert len(items) >= 1
    for item in items:
        assert item.size() == len(content)


# ---------------------------------------------------------------------------
# S10 - TreeSnapshot with sizer: size populated for LINKs, None for DIRs
# ---------------------------------------------------------------------------

def test_tree_snap_with_sizer(tmp_path_factory):
    vol_path = _make_vol(tmp_path_factory, "vol")
    content = csum_bytes(0)
    csum = build_blob(vol_path, content)
    build_link(vol_path, "a", csum)
    build_dir(vol_path, "subdir")
    content2 = csum_bytes(1)
    csum2 = build_blob(vol_path, content2)
    build_link(vol_path, "subdir/b", csum2)

    vol = getvol(vol_path)
    snap = vol.tree()  # uses sizer wired in Volume.tree()
    items = list(snap)

    links = [i for i in items if i.is_link()]
    dirs = [i for i in items if i.is_dir()]

    assert len(links) >= 2
    for item in links:
        assert item.size() is not None
        assert item.size() > 0

    for item in dirs:
        assert item.size() is None


def test_tree_snap_size_matches_blob_content(tmp_path_factory):
    vol_path = _make_vol(tmp_path_factory, "vol")
    content = csum_bytes(0)
    expected_size = len(content)
    csum = build_blob(vol_path, content)
    build_link(vol_path, "a", csum)

    vol = getvol(vol_path)
    snap = vol.tree()
    links = [i for i in snap if i.is_link()]
    assert len(links) == 1
    assert links[0].size() == expected_size


# ---------------------------------------------------------------------------
# S11 - Round-trip: write snap with size, read back, size preserved
# ---------------------------------------------------------------------------

def test_snap_round_trip_with_size(tmp_path_factory):
    vol_path = _make_vol(tmp_path_factory, "vol")
    content = csum_bytes(0)
    csum = build_blob(vol_path, content)
    build_link(vol_path, "a", csum)

    vol = getvol(vol_path)
    # vol.tree() now populates size
    vol.snapdb.write("v1", cast(KeySnapshot, vol.tree()), overwrite=True)

    snap_items = list(vol.snapdb.read("v1"))
    links = [i for i in snap_items if i.is_link()]
    assert len(links) == 1
    assert links[0].size() == len(content)


# ---------------------------------------------------------------------------
# S12 - Round-trip: legacy snap without size reads back with size=None
# ---------------------------------------------------------------------------

def test_snap_round_trip_legacy_no_size(tmp_path_factory):
    """Write a snapshot using KeySnapshot built from legacy 3-element lists (no size),
    then read it back and confirm size is None."""
    vol_path = _make_vol(tmp_path_factory, "vol")
    content = csum_bytes(0)
    csum = build_blob(vol_path, content)
    build_link(vol_path, "a", csum)

    vol = getvol(vol_path)

    # Build a legacy-style KeySnapshot (3-element lists, no size)
    legacy_data = [{"path": "a", "type": LINK, "csum": csum}]
    legacy_snap = KeySnapshot(legacy_data, "legacy", vol.bs.reverser)
    vol.snapdb.write("legacy", cast(KeySnapshot, legacy_snap), overwrite=True)

    snap_items = list(vol.snapdb.read("legacy"))
    links = [i for i in snap_items if i.is_link()]
    assert len(links) == 1
    assert links[0].size() is None


# ---------------------------------------------------------------------------
# S13 - tree_diff: size mismatch with both sides present raises ValueError
# S14 - tree_diff: one side None, other has size → noop (no error)
# ---------------------------------------------------------------------------

def _snap_with_size(csum: str, size, reverser) -> KeySnapshot:
    """Build a single-link KeySnapshot with the given size (may be None)."""
    item = SnapshotItem("a", LINK, csum, size=size)
    return KeySnapshot([item], "test", reverser)


def test_diff_size_mismatch_raises(tmp_path_factory):
    vol_path = _make_vol(tmp_path_factory, "vol")
    content = csum_bytes(0)
    csum = build_blob(vol_path, content)
    build_link(vol_path, "a", csum)

    vol = getvol(vol_path)
    tree = _snap_with_size(csum, 10, vol.bs.reverser)
    snap = _snap_with_size(csum, 99, vol.bs.reverser)
    with pytest.raises(ValueError, match="Size mismatch"):
        tree_diff(tree, snap)


def test_diff_size_tree_none_snap_int_is_noop(tmp_path_factory):
    vol_path = _make_vol(tmp_path_factory, "vol")
    content = csum_bytes(0)
    csum = build_blob(vol_path, content)

    vol = getvol(vol_path)
    tree = _snap_with_size(csum, None, vol.bs.reverser)
    snap = _snap_with_size(csum, 42, vol.bs.reverser)
    # same csum, tree has no size — should produce no deltas
    assert tree_diff(tree, snap) == []


def test_diff_size_tree_int_snap_none_is_noop(tmp_path_factory):
    vol_path = _make_vol(tmp_path_factory, "vol")
    content = csum_bytes(0)
    csum = build_blob(vol_path, content)

    vol = getvol(vol_path)
    tree = _snap_with_size(csum, 42, vol.bs.reverser)
    snap = _snap_with_size(csum, None, vol.bs.reverser)
    # same csum, snap has no size — should produce no deltas
    assert tree_diff(tree, snap) == []


def test_diff_size_both_none_is_noop(tmp_path_factory):
    vol_path = _make_vol(tmp_path_factory, "vol")
    content = csum_bytes(0)
    csum = build_blob(vol_path, content)

    vol = getvol(vol_path)
    tree = _snap_with_size(csum, None, vol.bs.reverser)
    snap = _snap_with_size(csum, None, vol.bs.reverser)
    assert tree_diff(tree, snap) == []


def test_diff_size_both_equal_is_noop(tmp_path_factory):
    vol_path = _make_vol(tmp_path_factory, "vol")
    content = csum_bytes(0)
    csum = build_blob(vol_path, content)

    vol = getvol(vol_path)
    tree = _snap_with_size(csum, 42, vol.bs.reverser)
    snap = _snap_with_size(csum, 42, vol.bs.reverser)
    assert tree_diff(tree, snap) == []
