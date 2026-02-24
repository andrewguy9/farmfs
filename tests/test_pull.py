"""
UX tests for `farmfs pull`.

Tests the full stack: CLI → volume lookup → tree_diff → tree_patch → filesystem.

Group P1: pull a snapshot from remote into an empty local volume.
Group P2: pull transitions — local starts as T1, remote has T2 as a snapshot;
          after pull, local tree matches remote snapshot.
"""

from typing import cast

from farmfs.ui import farmfs_ui
from farmfs.snapshot import KeySnapshot
from farmfs.fs import Path
from farmfs import getvol
from farmfs.volume import mkfs
from .conftest import build_blob, build_link, build_dir
from .trees2 import csum_bytes


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_vol(tmp_path_factory, name: str) -> Path:
    d = tmp_path_factory.mktemp(name)
    root = Path(str(d))
    udd = root.join(".farmfs").join("userdata")
    mkfs(root, udd)
    return root


def _rel(path: Path) -> str:
    return str(path).lstrip("/")


def _build_tree(vol_path: Path, tree: list) -> None:
    from farmfs.fs import DIR, LINK
    for item in tree:
        rel = item["path"]
        if str(rel) in ("/", "."):
            continue
        rel_str = _rel(rel)
        if item["type"] == DIR:
            build_dir(vol_path, rel_str)
        elif item["type"] == LINK:
            content = csum_bytes(int(item["csum"]))
            real_csum = build_blob(vol_path, content)
            build_link(vol_path, rel_str, real_csum)


def _snap_items(vol_path: Path, snap_name: str) -> list:
    vol = getvol(vol_path)
    return list(vol.snapdb.read(snap_name))


def _write_snap(vol_path: Path, snap_name: str) -> None:
    vol = getvol(vol_path)
    vol.snapdb.write(snap_name, cast(KeySnapshot, vol.tree()), overwrite=True)


# ---------------------------------------------------------------------------
# Group P1: pull snapshot into empty local volume
# ---------------------------------------------------------------------------

def test_pull_into_empty(tmp_path_factory, tree2):
    remote_path = _make_vol(tmp_path_factory, "remote")
    local_path = _make_vol(tmp_path_factory, "local")

    _build_tree(remote_path, tree2)
    _write_snap(remote_path, "v1")

    # Register remote using a relative path from local to remote
    r = farmfs_ui(["remote", "add", "origin", str(remote_path)], local_path)
    assert r == 0

    r = farmfs_ui(["pull", "origin", "v1"], local_path)
    assert r == 0

    assert list(getvol(local_path).tree()) == _snap_items(remote_path, "v1")


# ---------------------------------------------------------------------------
# Group P2: pull transitions — local is T1, remote snap is T2
# ---------------------------------------------------------------------------

def test_pull_transition(tmp_path_factory, tree2_pair):
    tree1, tree2 = tree2_pair

    remote_path = _make_vol(tmp_path_factory, "remote")
    local_path = _make_vol(tmp_path_factory, "local")

    _build_tree(remote_path, tree2)
    _write_snap(remote_path, "v2")

    _build_tree(local_path, tree1)

    r = farmfs_ui(["remote", "add", "origin", str(remote_path)], local_path)
    assert r == 0

    r = farmfs_ui(["pull", "origin", "v2"], local_path)
    assert r == 0

    assert list(getvol(local_path).tree()) == _snap_items(remote_path, "v2")
