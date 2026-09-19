"""
Snapshot / diff / patch round-trip tests.

Group A: applying diff(empty, T) to an empty volume produces T.
Group B: snapdb.write + snapdb.read is lossless.
Group C: patch(T1, diff(T1, T2)) produces a volume equal to T2.
Group D: same as C but diff uses live trees; assertion compares snapshots.
         Isolates tree_diff+tree_patch from snap serialisation bugs.
"""

from typing import cast
import pytest
from hypothesis import given, settings

from farmfs import getvol
from farmfs.volume import mkfs, tree_diff, tree_patch
from farmfs.snapshot import KeySnapshot
from farmfs.fs import Path, DIR, LINK
from tests.conftest import build_blob, build_link, build_dir
from tests.hyp_trees import trees, csum_bytes


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_vol(tmp_path_factory, suffix: str) -> Path:
    """Create a fresh volume rooted at a tmp directory."""
    d = tmp_path_factory.mktemp(suffix)
    root = Path(str(d))
    udd = root.join(".farmfs").join("userdata")
    mkfs(root, udd)
    return root


def _rel(path: Path) -> str:
    """Convert an absolute snapshot path like /a/b to a relative path a/b."""
    s = str(path)
    return s.lstrip("/")


def _build_tree(vol_path: Path, tree: list) -> None:
    """
    Materialise a tree (list of dicts from generate_trees2) into a volume.
    tree items: {"path": Path, "type": DIR|LINK, "csum": str|None}
    csum is a decimal string of the class index; we map it to real bytes via csum_bytes.
    """
    for item in tree:
        itype = item["type"]
        rel = item["path"]
        # Skip the root dir — mkfs already created it
        if str(rel) in ("/", "."):
            continue
        rel_str = _rel(rel)
        if itype == DIR:
            build_dir(vol_path, rel_str)
        elif itype == LINK:
            class_idx = int(item["csum"])
            content = csum_bytes(class_idx)
            real_csum = build_blob(vol_path, content)
            build_link(vol_path, rel_str, real_csum)


def _apply_patch(local_vol_path: Path, remote_vol_path: Path, deltas: list) -> None:
    """Apply a list of SnapDeltas from remote into local."""
    local_vol = getvol(local_vol_path)
    remote_vol = getvol(remote_vol_path)
    for delta in deltas:
        blob_op, tree_op, _ = tree_patch(local_vol, remote_vol, delta)
        blob_op()
        tree_op()


# ---------------------------------------------------------------------------
# Group A: apply diff(empty, T) to empty volume → produces T
# ---------------------------------------------------------------------------

@given(tree=trees())
@settings(deadline=None)
def test_apply_to_empty(tmp_path_factory, tree):
    src_path = _make_vol(tmp_path_factory, "src")
    local_path = _make_vol(tmp_path_factory, "local")

    _build_tree(src_path, tree)

    src_vol = getvol(src_path)
    local_vol = getvol(local_path)

    deltas = tree_diff(local_vol.tree(), src_vol.tree())
    _apply_patch(local_path, src_path, deltas)

    # Re-open vols after mutation
    assert list(getvol(local_path).tree()) == list(getvol(src_path).tree())


# ---------------------------------------------------------------------------
# Group B: snapdb.write + snapdb.read is lossless
# ---------------------------------------------------------------------------

@given(tree=trees())
@settings(deadline=None)
def test_snap_write_read(tmp_path_factory, tree):
    vol_path = _make_vol(tmp_path_factory, "vol")
    _build_tree(vol_path, tree)

    vol = getvol(vol_path)
    vol.snapdb.write("v1", cast(KeySnapshot, vol.tree()), overwrite=True)

    # KeySnapshot can only be iterated once — create fresh instances.
    tree_items = list(getvol(vol_path).tree())
    snap_items = list(vol.snapdb.read("v1"))

    assert tree_items == snap_items


# ---------------------------------------------------------------------------
# Group C: patch(T1, diff(T1, T2)) produces volume equal to T2
# ---------------------------------------------------------------------------

@given(tree1=trees(), tree2=trees())
@settings(deadline=None)
def test_diff_round_trip(tmp_path_factory, tree1, tree2):
    vol1_path = _make_vol(tmp_path_factory, "vol1")
    vol2_path = _make_vol(tmp_path_factory, "vol2")

    _build_tree(vol1_path, tree1)
    _build_tree(vol2_path, tree2)

    vol1 = getvol(vol1_path)
    vol2 = getvol(vol2_path)

    # Write snapshots so we can read them back (tests snap round-trip implicitly)
    vol1.snapdb.write("s1", cast(KeySnapshot, vol1.tree()), overwrite=True)
    vol2.snapdb.write("s2", cast(KeySnapshot, vol2.tree()), overwrite=True)

    snap1 = vol1.snapdb.read("s1")
    snap2 = vol2.snapdb.read("s2")

    deltas = tree_diff(snap1, snap2)
    _apply_patch(vol1_path, vol2_path, deltas)

    assert list(getvol(vol1_path).tree()) == list(getvol(vol2_path).tree())


# ---------------------------------------------------------------------------
# Group D: live-tree diff+patch; assert via snapshots
# diff uses live trees (no snap serialisation), assertion compares fresh snaps
# ---------------------------------------------------------------------------

@given(tree1=trees(), tree2=trees())
@settings(deadline=None)
def test_live_diff_snap_equal(tmp_path_factory, tree1, tree2):
    vol1_path = _make_vol(tmp_path_factory, "vol1")
    vol2_path = _make_vol(tmp_path_factory, "vol2")

    _build_tree(vol1_path, tree1)
    _build_tree(vol2_path, tree2)

    # Diff live trees — no snapshot round-trip on the input side
    vol1 = getvol(vol1_path)
    vol2 = getvol(vol2_path)
    deltas = tree_diff(vol1.tree(), vol2.tree())
    _apply_patch(vol1_path, vol2_path, deltas)

    # Snapshot both sides fresh after the patch and compare
    vol1 = getvol(vol1_path)
    vol2 = getvol(vol2_path)
    vol1.snapdb.write("s1", cast(KeySnapshot, vol1.tree()), overwrite=True)
    vol2.snapdb.write("s2", cast(KeySnapshot, vol2.tree()), overwrite=True)

    assert list(vol1.snapdb.read("s1")) == list(vol2.snapdb.read("s2"))


# ---------------------------------------------------------------------------
# Group E: foreign symlinks are rejected at snapshot time
# ---------------------------------------------------------------------------

def test_foreign_symlink_raises(tmp_path_factory):
    """A symlink that does not point into the blobstore must raise ValueError when iterated."""
    vol_path = _make_vol(tmp_path_factory, "vol")
    vol = getvol(vol_path)

    # Create a regular file in the volume (not a blob link)
    target = vol_path.join("target.txt")
    with target.open("w") as fd:
        fd.write("hello")

    # Create a symlink pointing at that file — not a blobstore path
    link = vol_path.join("foreign.lnk")
    link.symlink(target)

    with pytest.raises(ValueError, match="foreign"):
        list(vol.tree())


def test_symlink_to_tracked_file_raises(tmp_path_factory):
    """A symlink pointing at another tracked (blob-backed) file in the depot,
    rather than directly at its blob, is still not a blobstore path and must raise."""
    vol_path = _make_vol(tmp_path_factory, "vol")
    vol = getvol(vol_path)

    real_csum = build_blob(vol_path, b"hello")
    build_link(vol_path, "a", real_csum)

    # b points at the *tracked file* a, not at a.farmfs/userdata/... blob path.
    link = vol_path.join("b")
    link.symlink(vol_path.join("a"))

    with pytest.raises(ValueError, match="foreign"):
        list(vol.tree())


def test_corrupt_userdata_name_raises(tmp_path_factory):
    """A symlink that resolves inside .farmfs/userdata but whose name doesn't
    parse as a checksum must raise, not silently pass through."""
    vol_path = _make_vol(tmp_path_factory, "vol")
    vol = getvol(vol_path)

    bogus = vol_path.join(".farmfs").join("userdata").join("not-a-blob")
    link = vol_path.join("corrupt.lnk")
    link.symlink(bogus)

    with pytest.raises(ValueError, match="foreign"):
        list(vol.tree())


def test_hanging_blob_symlink_does_not_raise(tmp_path_factory):
    """A symlink shaped like a valid blob path, but whose blob does not
    actually exist on disk, is a well-formed (if broken) blob reference.
    get_blob_csum only checks structure, so this must NOT raise at snapshot
    time -- missing-blob detection is fsck's job, not tree()'s."""
    vol_path = _make_vol(tmp_path_factory, "vol")
    vol = getvol(vol_path)

    fake_csum = "a" * 32
    link = vol_path.join("hanging.lnk")
    link.symlink(vol.bs.blob_path(fake_csum))

    items = list(vol.tree())
    hanging_items = [i for i in items if str(i._path).endswith("hanging.lnk")]
    assert len(hanging_items) == 1
    assert hanging_items[0]._csum == fake_csum


# ---------------------------------------------------------------------------
# Group F: further symlink oddities (dir symlinks, chains, relative targets,
# circular symlinks, plain hanging symlinks). These pin down actually-observed
# behavior; see project_symlink_test_backlog memory for follow-up items.
# ---------------------------------------------------------------------------

def test_symlink_to_directory_raises(tmp_path_factory):
    """A symlink pointing at a directory (in or out of the depot) is not a
    blobstore path and must raise the same "foreign symlink" error as a
    symlink to a regular file. walk() classifies it as LINK (via lstat), so
    it never gets recursed into as a DIR."""
    vol_path = _make_vol(tmp_path_factory, "vol")
    vol = getvol(vol_path)

    real_dir = vol_path.join("realdir")
    real_dir.mkdir()
    link = vol_path.join("dirlink")
    link.symlink(real_dir)

    with pytest.raises(ValueError, match="foreign"):
        list(vol.tree())


def test_symlink_chain_raises(tmp_path_factory):
    """A symlink pointing at another symlink (which itself points at a valid
    blob) must still raise: readlinkat() resolves only one hop, so the outer
    link's immediate target (the inner symlink's path) does not structurally
    match the blobstore and is rejected, even though the chain would
    eventually resolve to real content."""
    vol_path = _make_vol(tmp_path_factory, "vol")
    vol = getvol(vol_path)

    real_csum = build_blob(vol_path, b"hello")
    build_link(vol_path, "a", real_csum)  # a -> blob (valid)

    b = vol_path.join("b")
    b.symlink(vol_path.join("a"))  # b -> a -> blob (chain)

    with pytest.raises(ValueError, match="foreign"):
        list(vol.tree())


def test_relative_symlink_into_blobstore_is_recognized(tmp_path_factory):
    """A symlink using a relative target that resolves into the blobstore
    (rather than the absolute form every farmfs helper currently produces)
    must be recognized as a valid blob link, not rejected."""
    vol_path = _make_vol(tmp_path_factory, "vol")
    vol = getvol(vol_path)

    real_csum = build_blob(vol_path, b"hello")
    blob_path = vol.bs.blob_path(real_csum)

    link = vol_path.join("rel.lnk")
    rel_target = blob_path.relative_to(vol_path)
    link.symlink(Path(str(rel_target), vol_path))

    items = list(vol.tree())
    link_items = [i for i in items if str(i._path).endswith("rel.lnk")]
    assert len(link_items) == 1
    assert link_items[0]._csum == real_csum


def test_circular_symlink_raises_in_tree(tmp_path_factory):
    """Two symlinks pointing at each other: tree() rejects the outer link
    before ever needing to chase the cycle, since readlinkat() only follows
    one hop and that hop already fails the blobstore containment check."""
    vol_path = _make_vol(tmp_path_factory, "vol")
    vol = getvol(vol_path)

    a = vol_path.join("a")
    b = vol_path.join("b")
    a.symlink(b)
    b.symlink(a)

    with pytest.raises(ValueError, match="foreign"):
        list(vol.tree())


def test_circular_symlink_freeze_raises_oserror(tmp_path_factory):
    """Freezing a circular symlink hits the OS's own loop detection (via
    checksum()'s open()) and raises OSError (ELOOP), rather than hanging or
    silently corrupting anything."""
    vol_path = _make_vol(tmp_path_factory, "vol")
    vol = getvol(vol_path)

    a = vol_path.join("a")
    b = vol_path.join("b")
    a.symlink(b)
    b.symlink(a)

    with pytest.raises(OSError):
        vol.freeze(a)


def test_hanging_foreign_symlink_raises(tmp_path_factory):
    """A symlink pointing at a nonexistent path outside the blobstore (a
    plain broken symlink, as opposed to the blob-shaped hanging link in
    test_hanging_blob_symlink_does_not_raise) is rejected the same as any
    other foreign symlink -- existence of the target is never required for
    the containment check to run."""
    vol_path = _make_vol(tmp_path_factory, "vol")
    vol = getvol(vol_path)

    link = vol_path.join("hanging.lnk")
    link.symlink(vol_path.join("does_not_exist.txt"))

    with pytest.raises(ValueError, match="foreign"):
        list(vol.tree())


def test_repair_link_rejects_foreign_symlink(tmp_path_factory):
    """repair_link() must not treat a symlink to a real file outside the
    blobstore as "already fine". oldlink.isfile() would say True (it follows
    symlinks), but that answers "does something resolve here", not "does
    this point directly at a blob" -- so repair_link uses get_blob_csum
    (the same structural check tree() uses) instead."""
    vol_path = _make_vol(tmp_path_factory, "vol")
    vol = getvol(vol_path)

    target = vol_path.join("target.txt")
    with target.open("w") as fd:
        fd.write("hello")
    link = vol_path.join("foreign.lnk")
    link.symlink(target)

    with pytest.raises(ValueError):
        vol.repair_link(link)


def test_repair_link_rejects_symlink_chain(tmp_path_factory):
    """Same root cause as test_repair_link_rejects_foreign_symlink: a symlink
    pointing at another live symlink (not a direct blob link) is not treated
    as already-fine just because isfile() follows the chain to real content."""
    vol_path = _make_vol(tmp_path_factory, "vol")
    vol = getvol(vol_path)

    real_csum = build_blob(vol_path, b"hello")
    build_link(vol_path, "a", real_csum)
    b = vol_path.join("b")
    b.symlink(vol_path.join("a"))

    with pytest.raises(ValueError):
        vol.repair_link(b)


def test_repair_link_leaves_valid_blob_link_alone(tmp_path_factory):
    """A symlink that already points directly at an existing blob is
    correctly recognized as fine and left untouched (returns None)."""
    vol_path = _make_vol(tmp_path_factory, "vol")
    vol = getvol(vol_path)

    real_csum = build_blob(vol_path, b"hello")
    link_path = build_link(vol_path, "a", real_csum)

    result = vol.repair_link(link_path)

    assert result is None
    assert link_path.readlinkat() == vol.bs.blob_path(real_csum)
