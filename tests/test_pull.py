"""
UX tests for `farmfs pull` and `farmfs pull-path`.

Tests the full stack: CLI → volume lookup → tree_diff → tree_patch → filesystem.

Group P1: pull a snapshot from remote into an empty local volume.
Group P2: pull transitions — local starts as T1, remote has T2 as a snapshot;
          after pull, local tree matches remote snapshot.
Group P3–P8: pull-path — subtree copy with rebasing.
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


# ---------------------------------------------------------------------------
# Helpers for pull-path tests
# ---------------------------------------------------------------------------

def _tree_paths(vol_path: Path) -> list:
    """Return sorted list of (path_str, type, csum) tuples for the live tree."""
    return [(i._path, i._type, i._csum) for i in getvol(vol_path).tree()]


# ---------------------------------------------------------------------------
# Group P3: pull-path into empty dest subtree
# ---------------------------------------------------------------------------

def test_pull_path_into_empty(tmp_path_factory):
    remote_path = _make_vol(tmp_path_factory, "remote")
    local_path = _make_vol(tmp_path_factory, "local")

    build_dir(remote_path, "src")
    csum_a = build_blob(remote_path, b"file_a_content")
    build_link(remote_path, "src/a.bin", csum_a)
    csum_b = build_blob(remote_path, b"file_b_content")
    build_link(remote_path, "src/b.bin", csum_b)

    r = farmfs_ui(["remote", "add", "origin", str(remote_path)], local_path)
    assert r == 0

    src_path = str(remote_path.join("src"))
    dst_path = str(local_path.join("dest"))
    r = farmfs_ui(["pull-path", "origin", src_path, dst_path], local_path)
    assert r == 0

    tree = _tree_paths(local_path)
    paths = [p for (p, t, c) in tree]
    assert "dest" in paths
    assert "dest/a.bin" in paths
    assert "dest/b.bin" in paths
    # No src/ directory in local
    assert not any(p.startswith("src") for p in paths)


# ---------------------------------------------------------------------------
# Group P4: pull-path overwrites existing dest subtree (full sync)
# ---------------------------------------------------------------------------

def test_pull_path_full_sync(tmp_path_factory):
    remote_path = _make_vol(tmp_path_factory, "remote")
    local_path = _make_vol(tmp_path_factory, "local")

    # Remote: src/a.bin (new content), src/b.bin
    build_dir(remote_path, "src")
    csum1 = build_blob(remote_path, b"new_a_content")
    build_link(remote_path, "src/a.bin", csum1)
    csum2 = build_blob(remote_path, b"b_content")
    build_link(remote_path, "src/b.bin", csum2)

    # Local: dest/a.bin (old content), dest/c.bin
    build_dir(local_path, "dest")
    csum_old = build_blob(local_path, b"old_a_content")
    build_link(local_path, "dest/a.bin", csum_old)
    csum3 = build_blob(local_path, b"c_content")
    build_link(local_path, "dest/c.bin", csum3)

    r = farmfs_ui(["remote", "add", "origin", str(remote_path)], local_path)
    assert r == 0

    src_path = str(remote_path.join("src"))
    dst_path = str(local_path.join("dest"))
    r = farmfs_ui(["pull-path", "origin", src_path, dst_path], local_path)
    assert r == 0

    tree = _tree_paths(local_path)
    by_path = {p: c for (p, t, c) in tree}
    assert by_path.get("dest/a.bin") == csum1
    assert by_path.get("dest/b.bin") == csum2
    assert "dest/c.bin" not in by_path


# ---------------------------------------------------------------------------
# Group P5: pull-path deduplicates blobs already in local blobstore
# ---------------------------------------------------------------------------

def test_pull_path_dedup(tmp_path_factory):
    remote_path = _make_vol(tmp_path_factory, "remote")
    local_path = _make_vol(tmp_path_factory, "local")

    shared_content = b"shared_blob_content"

    # Remote: src/a.bin with shared content
    build_dir(remote_path, "src")
    csum1 = build_blob(remote_path, shared_content)
    build_link(remote_path, "src/a.bin", csum1)

    # Local: other/a.bin with same content (blob already imported)
    build_dir(local_path, "other")
    build_blob(local_path, shared_content)
    build_link(local_path, "other/a.bin", csum1)

    r = farmfs_ui(["remote", "add", "origin", str(remote_path)], local_path)
    assert r == 0

    src_path = str(remote_path.join("src"))
    dst_path = str(local_path.join("dest"))
    r = farmfs_ui(["pull-path", "origin", src_path, dst_path], local_path)
    assert r == 0

    tree = _tree_paths(local_path)
    by_path = {p: c for (p, t, c) in tree}
    # dest/a.bin created pointing to same blob
    assert by_path.get("dest/a.bin") == csum1
    # other/a.bin untouched
    assert by_path.get("other/a.bin") == csum1


# ---------------------------------------------------------------------------
# Group P6: pull-path from named snapshot
# ---------------------------------------------------------------------------

def test_pull_path_from_snap(tmp_path_factory):
    remote_path = _make_vol(tmp_path_factory, "remote")
    local_path = _make_vol(tmp_path_factory, "local")

    build_dir(remote_path, "src")
    csum_a = build_blob(remote_path, b"snap_a_content")
    build_link(remote_path, "src/a.bin", csum_a)
    _write_snap(remote_path, "v1")

    r = farmfs_ui(["remote", "add", "origin", str(remote_path)], local_path)
    assert r == 0

    src_path = str(remote_path.join("src"))
    dst_path = str(local_path.join("dest"))
    r = farmfs_ui(["pull-path", "origin", src_path, dst_path, "v1"], local_path)
    assert r == 0

    tree = _tree_paths(local_path)
    by_path = {p: c for (p, t, c) in tree}
    assert by_path.get("dest/a.bin") == csum_a


# ---------------------------------------------------------------------------
# Group P7: src_path outside remote vol → error
# ---------------------------------------------------------------------------

def test_pull_path_src_outside_remote(tmp_path_factory):
    remote_path = _make_vol(tmp_path_factory, "remote")
    local_path = _make_vol(tmp_path_factory, "local")
    other_path = _make_vol(tmp_path_factory, "other")

    r = farmfs_ui(["remote", "add", "origin", str(remote_path)], local_path)
    assert r == 0

    # src_path points into a completely different volume
    src_path = str(other_path.join("src"))
    dst_path = str(local_path.join("dest"))
    r = farmfs_ui(["pull-path", "origin", src_path, dst_path], local_path)
    assert r != 0


# ---------------------------------------------------------------------------
# Group P8: dest_path outside local vol → error
# ---------------------------------------------------------------------------

def test_pull_path_dest_outside_local(tmp_path_factory):
    remote_path = _make_vol(tmp_path_factory, "remote")
    local_path = _make_vol(tmp_path_factory, "local")
    other_path = _make_vol(tmp_path_factory, "other")

    r = farmfs_ui(["remote", "add", "origin", str(remote_path)], local_path)
    assert r == 0

    src_path = str(remote_path.join("src"))
    # dest_path points outside the local volume
    dst_path = str(other_path.join("dest"))
    r = farmfs_ui(["pull-path", "origin", src_path, dst_path], local_path)
    assert r != 0
