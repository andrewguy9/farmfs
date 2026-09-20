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
from hypothesis import assume, given, settings

from farmfs import getvol
from farmfs.volume import mkfs, tree_diff, tree_patch
from farmfs.snapshot import KeySnapshot
from farmfs.fs import Path, ensure_symlink_unsafe
from tests.conftest import build_blob, build_link, build_dir
from tests.hyp_trees import trees, build_tree as _build_tree, EXTERNAL_KINDS


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
# Group E: in-depot symlinks are captured as sub_path links; only symlinks
# resolving outside the depot are rejected as foreign.
# ---------------------------------------------------------------------------

def test_absolute_link_to_plain_file_is_sub_path(tmp_path_factory):
    """A symlink pointing at an ordinary file inside the depot, via an
    absolute on-disk target, is a legitimate interior-absolute link -- it
    must be captured as a sub_path item, not rejected as foreign."""
    vol_path = _make_vol(tmp_path_factory, "vol")
    vol = getvol(vol_path)

    target = vol_path.join("target.txt")
    with target.open("w") as fd:
        fd.write("hello")

    link = vol_path.join("interior.lnk")
    link.symlink(target)

    items = list(vol.tree())
    link_items = [i for i in items if str(i._path).endswith("interior.lnk")]
    assert len(link_items) == 1
    assert link_items[0].csum() is None
    assert link_items[0].sub_path() == "target.txt"
    assert link_items[0].rel_path() is None


def test_absolute_link_to_tracked_file_is_sub_path(tmp_path_factory):
    """A symlink pointing at another tracked (blob-backed) file in the depot,
    via an absolute on-disk target rather than the blob path directly, is
    also a legitimate interior-absolute link, not a blob link itself."""
    vol_path = _make_vol(tmp_path_factory, "vol")
    vol = getvol(vol_path)

    real_csum = build_blob(vol_path, b"hello")
    build_link(vol_path, "a", real_csum)

    link = vol_path.join("b")
    link.symlink(vol_path.join("a"))

    items = list(vol.tree())
    link_items = [i for i in items if str(i._path).endswith("/b") or str(i._path) == "b"]
    assert len(link_items) == 1
    assert link_items[0].csum() is None
    assert link_items[0].sub_path() == "a"


def test_corrupt_userdata_name_is_sub_path(tmp_path_factory):
    """A symlink that resolves inside .farmfs/userdata but whose name doesn't
    parse as a checksum is still inside the depot -- it's captured as a
    sub_path link (faithfully reproducible), not rejected as foreign."""
    vol_path = _make_vol(tmp_path_factory, "vol")
    vol = getvol(vol_path)

    bogus = vol_path.join(".farmfs").join("userdata").join("not-a-blob")
    link = vol_path.join("corrupt.lnk")
    link.symlink(bogus)

    items = list(vol.tree())
    link_items = [i for i in items if str(i._path).endswith("corrupt.lnk")]
    assert len(link_items) == 1
    assert link_items[0].csum() is None
    assert link_items[0].sub_path() == ".farmfs/userdata/not-a-blob"


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
    assert hanging_items[0].csum() == fake_csum


# ---------------------------------------------------------------------------
# Group F: further symlink oddities (dir symlinks, chains, relative targets,
# circular symlinks, plain hanging symlinks). These pin down actually-observed
# behavior; see project_symlink_test_backlog memory for follow-up items.
# ---------------------------------------------------------------------------

def test_symlink_to_directory_is_sub_path(tmp_path_factory):
    """A symlink pointing at a directory inside the depot is a legitimate
    interior-absolute link -- walk() classifies it as LINK (via lstat), so
    it never gets recursed into as a DIR, and it's captured as sub_path."""
    vol_path = _make_vol(tmp_path_factory, "vol")
    vol = getvol(vol_path)

    real_dir = vol_path.join("realdir")
    real_dir.mkdir()
    link = vol_path.join("dirlink")
    link.symlink(real_dir)

    items = list(vol.tree())
    link_items = [i for i in items if str(i._path).endswith("dirlink")]
    assert len(link_items) == 1
    assert link_items[0].sub_path() == "realdir"


def test_symlink_chain_is_sub_path(tmp_path_factory):
    """A symlink pointing at another symlink (which itself points at a valid
    blob) is captured as a sub_path link whose value is the inner symlink's
    path -- readlinkat() resolves only one hop, so the chain is never
    followed or resolved; it's recorded faithfully as-is, one hop at a time."""
    vol_path = _make_vol(tmp_path_factory, "vol")
    vol = getvol(vol_path)

    real_csum = build_blob(vol_path, b"hello")
    build_link(vol_path, "a", real_csum)  # a -> blob (valid)

    b = vol_path.join("b")
    b.symlink(vol_path.join("a"))  # b -> a -> blob (chain)

    items = list(vol.tree())
    link_items = [i for i in items if str(i._path).endswith("/b") or str(i._path) == "b"]
    assert len(link_items) == 1
    assert link_items[0].sub_path() == "a"


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
    assert link_items[0].csum() == real_csum


def test_interior_relative_link_is_rel_path(tmp_path_factory):
    """A relative symlink whose on-disk target string is not blob-shaped, but
    resolves inside the depot when followed from its own location, is a
    legitimate interior-relative link -- captured verbatim as rel_path, not
    rejected. This is the counterpart to sub_path (interior-absolute); it's
    the case Group E's sub_path tests never actually exercised (they all
    write absolute on-disk targets)."""
    vol_path = _make_vol(tmp_path_factory, "vol")
    vol = getvol(vol_path)

    sub_dir = build_dir(vol_path, "sub")
    target = vol_path.join("target.txt")
    with target.open("w") as fd:
        fd.write("hello")

    link = sub_dir.join("rel.lnk")
    ensure_symlink_unsafe(link, "../target.txt")

    items = list(vol.tree())
    link_items = [i for i in items if str(i._path).endswith("rel.lnk")]
    assert len(link_items) == 1
    assert link_items[0].csum() is None
    assert link_items[0].sub_path() is None
    assert link_items[0].rel_path() == "../target.txt"


def test_absolute_link_outside_depot_raises(tmp_path_factory):
    """An absolute symlink pointing entirely outside the depot -- not just
    outside the blobstore, genuinely outside the volume root -- must still
    be rejected as foreign through vol.tree() directly (Group E's existing
    foreign-symlink coverage tests this via repair_link(), not tree()
    itself)."""
    vol_path = _make_vol(tmp_path_factory, "vol")
    vol = getvol(vol_path)

    external_dir = Path(str(tmp_path_factory.mktemp("external")))
    target = external_dir.join("external.txt")
    with target.open("w") as fd:
        fd.write("outside")

    link = vol_path.join("abs_external.lnk")
    link.symlink(target)

    with pytest.raises(ValueError, match="foreign"):
        list(vol.tree())


def test_relative_link_escaping_depot_via_dotdot_raises(tmp_path_factory):
    """A relative symlink that walks up and out of the depot root via ..
    must be rejected as foreign, exactly like an absolute link that escapes
    -- the containment check runs against the *resolved* absolute target
    (which readlinkat() already normalizes), so textual .. segments can't
    be used to sneak past it and leak external data as a rel_path link."""
    vol_path = _make_vol(tmp_path_factory, "vol")
    vol = getvol(vol_path)

    # vol_path's parent is outside the depot -- ../escaped.txt from inside
    # vol_path resolves to a sibling of the volume root, not inside it.
    outside_file = vol_path.parent().join("escaped_%s.txt" % vol_path.name())
    with outside_file.open("w") as fd:
        fd.write("leaked")
    try:
        link = vol_path.join("escape.lnk")
        ensure_symlink_unsafe(link, "../" + outside_file.name())

        with pytest.raises(ValueError, match="foreign"):
            list(vol.tree())
    finally:
        outside_file.unlink()


def test_circular_symlink_captured_as_sub_path(tmp_path_factory):
    """Two symlinks pointing at each other: tree() never needs to chase the
    cycle, since readlinkat() only follows one hop -- each link is captured
    as a sub_path pointing at the other's path, faithfully, with no attempt
    to resolve the chain and no infinite loop."""
    vol_path = _make_vol(tmp_path_factory, "vol")
    vol = getvol(vol_path)

    a = vol_path.join("a")
    b = vol_path.join("b")
    a.symlink(b)
    b.symlink(a)

    items = list(vol.tree())
    by_path = {str(i._path): i for i in items if i.is_link()}
    assert by_path["a"].sub_path() == "b"
    assert by_path["b"].sub_path() == "a"


def test_circular_symlink_freeze_is_a_noop(tmp_path_factory):
    """freeze() no longer dereferences an already-interior symlink at all
    (see tests/test_freeze.py), so it never reaches the OS-level ELOOP that
    used to happen when checksum()'s open() tried to resolve a circular
    pair. classify_link() only needs one hop to recognize `a` as pointing
    at another in-depot path (`b`), regardless of what b itself points at
    -- so freezing a circular symlink is just a no-op, matching how
    TreeSnapshot already treats the pair (captured as two independent
    sub_path items, never resolving the cycle)."""
    vol_path = _make_vol(tmp_path_factory, "vol")
    vol = getvol(vol_path)

    a = vol_path.join("a")
    b = vol_path.join("b")
    a.symlink(b)
    b.symlink(a)

    result = vol.freeze(a)

    assert result is None
    assert a.islink()
    assert a.readlinkat() == b


def test_hanging_interior_symlink_is_sub_path(tmp_path_factory):
    """A symlink pointing at a nonexistent path inside the depot (a plain
    broken symlink, as opposed to the blob-shaped hanging link in
    test_hanging_blob_symlink_does_not_raise) is still captured as a
    sub_path link -- existence of the target is never required for
    classification, only containment. Faithfully reproducing a link that
    was already broken is farmfs doing its job correctly, not a defect."""
    vol_path = _make_vol(tmp_path_factory, "vol")
    vol = getvol(vol_path)

    link = vol_path.join("hanging.lnk")
    link.symlink(vol_path.join("does_not_exist.txt"))

    items = list(vol.tree())
    link_items = [i for i in items if str(i._path).endswith("hanging.lnk")]
    assert len(link_items) == 1
    assert link_items[0].sub_path() == "does_not_exist.txt"


def test_hanging_interior_relative_symlink_is_rel_path(tmp_path_factory):
    """The rel_path counterpart to test_hanging_interior_symlink_is_sub_path:
    a relative symlink pointing at a nonexistent path inside the depot is
    still captured as a rel_path link, verbatim, existence never required."""
    vol_path = _make_vol(tmp_path_factory, "vol")
    vol = getvol(vol_path)

    link = vol_path.join("hanging_rel.lnk")
    ensure_symlink_unsafe(link, "does_not_exist.txt")

    items = list(vol.tree())
    link_items = [i for i in items if str(i._path).endswith("hanging_rel.lnk")]
    assert len(link_items) == 1
    assert link_items[0].rel_path() == "does_not_exist.txt"


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


# ---------------------------------------------------------------------------
# Group G: generator-driven interior/exterior symlink properties, using the
# 8-kind taxonomy from tests/hyp_trees.py (see its module docstring).
# ---------------------------------------------------------------------------

@given(tree=trees(include_interior_links=True, include_broken_links=True))
@settings(deadline=None)
def test_interior_links_round_trip(tmp_path_factory, tree):
    """A tree containing only interior (never external) links -- connected
    or broken, absolute or relative -- must snapshot cleanly (no raise),
    round-trip through snap write/read losslessly, and patch onto a
    *different* volume root with sub_path links correctly rebased (since
    sub_path is depot-root-relative) and rel_path links carried over
    byte-identical (since rel_path is link-location-relative, independent
    of the depot root)."""
    src_path = _make_vol(tmp_path_factory, "src")
    dst_path = _make_vol(tmp_path_factory, "dst")

    _build_tree(src_path, tree)

    src_vol = getvol(src_path)
    items = list(src_vol.tree())
    for item in items:
        if item.is_link():
            fields = (item.csum(), item.sub_path(), item.rel_path())
            assert sum(1 for f in fields if f is not None) == 1

    src_vol.snapdb.write("s1", cast(KeySnapshot, getvol(src_path).tree()), overwrite=True)
    snap_items = list(src_vol.snapdb.read("s1"))
    assert list(getvol(src_path).tree()) == snap_items

    dst_vol = getvol(dst_path)
    deltas = tree_diff(dst_vol.tree(), src_vol.snapdb.read("s1"))
    _apply_patch(dst_path, src_path, deltas)

    assert list(getvol(dst_path).tree()) == list(getvol(src_path).tree())


@given(tree=trees(include_interior_links=True, include_external_links=True, include_broken_links=True))
@settings(deadline=None)
def test_external_links_are_rejected(tmp_path_factory, tree):
    """A tree containing at least one external (foreign) link -- connected
    or broken, absolute or relative -- must always be rejected by tree(),
    and freeze() on that same external link must also raise. TreeSnapshot
    raises on the first offending item it walks to, so this only proves
    "rejected somewhere", matching the existing hand-written Group E/F
    single-external-link coverage -- still valuable here for varied
    depth/shape stress that the hand-written cases don't reach."""
    external_items = [item for item in tree if item.get("link_kind") in EXTERNAL_KINDS]
    assume(external_items)

    vol_path = _make_vol(tmp_path_factory, "vol")
    ext_root = Path(str(tmp_path_factory.mktemp("external")))
    _build_tree(vol_path, tree, ext_root=ext_root)

    vol = getvol(vol_path)
    with pytest.raises(ValueError, match="foreign"):
        list(vol.tree())

    one_external = vol_path.join(_rel(external_items[0]["path"]))
    with pytest.raises(ValueError):
        vol.freeze(one_external)
