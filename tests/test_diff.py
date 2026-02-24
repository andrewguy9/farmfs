"""
UX tests for `farmfs diff`.

diff is read-only: it prints deltas but does not modify the filesystem.

Group D1: diff empty local against a remote snapshot — all deltas are
          additions; local filesystem is unchanged after the call.
Group D2: diff T1 local against a remote T2 snapshot — local filesystem
          is unchanged after the call regardless of what the diff contains.
"""

from farmfs.ui import farmfs_ui
from farmfs import getvol
from .test_pull import _make_vol, _build_tree, _write_snap


# ---------------------------------------------------------------------------
# Group D1: diff empty local against remote snapshot
# ---------------------------------------------------------------------------

def test_diff_from_empty(tmp_path_factory, tree2):
    remote_path = _make_vol(tmp_path_factory, "remote")
    local_path = _make_vol(tmp_path_factory, "local")

    _build_tree(remote_path, tree2)
    _write_snap(remote_path, "v1")

    r = farmfs_ui(["remote", "add", "origin", str(remote_path)], local_path)
    assert r == 0

    before = list(getvol(local_path).tree())
    r = farmfs_ui(["diff", "origin", "v1"], local_path)
    assert r == 0

    # Filesystem must be unchanged
    assert list(getvol(local_path).tree()) == before


# ---------------------------------------------------------------------------
# Group D2: diff T1 local against remote T2 snapshot
# ---------------------------------------------------------------------------

def test_diff_transition(tmp_path_factory, tree2_pair):
    tree1, tree2 = tree2_pair

    remote_path = _make_vol(tmp_path_factory, "remote")
    local_path = _make_vol(tmp_path_factory, "local")

    _build_tree(remote_path, tree2)
    _write_snap(remote_path, "v2")

    _build_tree(local_path, tree1)

    r = farmfs_ui(["remote", "add", "origin", str(remote_path)], local_path)
    assert r == 0

    before = list(getvol(local_path).tree())
    r = farmfs_ui(["diff", "origin", "v2"], local_path)
    assert r == 0

    # Filesystem must be unchanged
    assert list(getvol(local_path).tree()) == before
