"""
Snapshot / diff / patch round-trip tests.

Group A: applying diff(empty, T) to an empty volume produces T.
Group B: snapdb.write + snapdb.read is lossless.
Group C: patch(T1, diff(T1, T2)) produces a volume equal to T2.
"""

from typing import cast

from farmfs import getvol
from farmfs.volume import mkfs, tree_diff, tree_patch
from farmfs.snapshot import KeySnapshot
from farmfs.fs import Path, DIR, LINK
from tests.conftest import build_blob, build_link, build_dir
from tests.trees2 import csum_bytes


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

def test_apply_to_empty(tmp_path_factory, tree2):
    src_path = _make_vol(tmp_path_factory, "src")
    local_path = _make_vol(tmp_path_factory, "local")

    _build_tree(src_path, tree2)

    src_vol = getvol(src_path)
    local_vol = getvol(local_path)

    deltas = list(tree_diff(local_vol.tree(), src_vol.tree()))
    _apply_patch(local_path, src_path, deltas)

    # Re-open vols after mutation
    assert list(getvol(local_path).tree()) == list(getvol(src_path).tree())


# ---------------------------------------------------------------------------
# Group B: snapdb.write + snapdb.read is lossless
# ---------------------------------------------------------------------------

def test_snap_write_read(tmp_path_factory, tree2):
    vol_path = _make_vol(tmp_path_factory, "vol")
    _build_tree(vol_path, tree2)

    vol = getvol(vol_path)
    vol.snapdb.write("v1", cast(KeySnapshot, vol.tree()), overwrite=True)

    # KeySnapshot can only be iterated once — create fresh instances.
    tree_items = list(getvol(vol_path).tree())
    snap_items = list(vol.snapdb.read("v1"))

    assert tree_items == snap_items


# ---------------------------------------------------------------------------
# Group C: patch(T1, diff(T1, T2)) produces volume equal to T2
# ---------------------------------------------------------------------------

def test_diff_round_trip(tmp_path_factory, tree2_pair):
    tree1, tree2 = tree2_pair

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

    deltas = list(tree_diff(snap1, snap2))
    _apply_patch(vol1_path, vol2_path, deltas)

    assert list(getvol(vol1_path).tree()) == list(getvol(vol2_path).tree())
