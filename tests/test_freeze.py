"""
Tests for Volume.freeze(), focused on symlink inputs.

freeze() on a plain file hardlinks it into the blobstore and replaces it
with a blob-backed symlink, as always.

freeze() on a path that is ALREADY a symlink no longer dereferences it into
a blob. Snapshots can now represent a symlink faithfully as itself (a blob
link, a sub_path/rel_path interior link, or a rejected foreign link) --
overwriting an existing symlink with a frozen copy of its target's content
would destroy that structure rather than preserve it. So:
  - an interior symlink (already classifiable as blob/sub_path/rel_path) is
    left completely untouched -- freeze() returns None, nothing to do.
  - a foreign symlink (target outside the depot) still raises, exactly like
    TreeSnapshot -- freeze() never silently absorbs external file content
    into the depot as a blob.
"""

import pytest

from farmfs import getvol
from farmfs.volume import mkfs
from farmfs.fs import Path


def _make_vol(tmp_path_factory, suffix: str) -> Path:
    d = tmp_path_factory.mktemp(suffix)
    root = Path(str(d))
    udd = root.join(".farmfs").join("userdata")
    mkfs(root, udd)
    return root


def test_freeze_regular_file(tmp_path_factory):
    """Baseline: freezing a plain file replaces it with a symlink to its blob."""
    vol_path = _make_vol(tmp_path_factory, "vol")
    vol = getvol(vol_path)

    f = vol_path.join("a.txt")
    with f.open("w") as fd:
        fd.write("hello")

    result = vol.freeze(f)

    assert result is not None
    assert f.islink()
    assert f.readlinkat() == vol.bs.blob_path(result["csum"])
    assert not result["was_dup"]


def test_freeze_interior_symlink_is_a_noop(tmp_path_factory):
    """Freezing a symlink whose target is another in-volume file is a no-op:
    it's already faithfully representable in a snapshot (as a sub_path
    link), so freeze() must not touch it -- no dereferencing, no
    conversion to a blob. Returns None to signal nothing was done."""
    vol_path = _make_vol(tmp_path_factory, "vol")
    vol = getvol(vol_path)

    target = vol_path.join("real.txt")
    with target.open("w") as fd:
        fd.write("shared content")
    target_mode_before = target.stat().st_mode

    link = vol_path.join("link.txt")
    link.symlink(target)

    result = vol.freeze(link)

    assert result is None
    assert link.islink()
    assert link.readlinkat() == target  # untouched, still points at real.txt directly

    # The target is completely unaffected -- still a plain writable file
    # with its original content, never touched by freeze() at all.
    assert not target.islink()
    assert target.isfile()
    assert target.stat().st_mode == target_mode_before
    assert target.stat().st_nlink == 1
    with target.open("r") as fd:
        assert fd.read() == "shared content"


def test_freeze_interior_symlink_to_frozen_target_is_still_a_noop(tmp_path_factory):
    """Same no-op behavior even when the interior symlink's target is
    itself already a frozen blob link -- freeze() doesn't care what kind
    of interior link it is, only that it's interior."""
    vol_path = _make_vol(tmp_path_factory, "vol")
    vol = getvol(vol_path)

    target = vol_path.join("real.txt")
    with target.open("w") as fd:
        fd.write("shared content")
    target_result = vol.freeze(target)
    assert target_result is not None

    link = vol_path.join("link.txt")
    link.symlink(target)

    link_result = vol.freeze(link)

    assert link_result is None
    assert link.islink()
    assert link.readlinkat() == target  # still points directly at real.txt, not its blob


def test_freeze_foreign_symlink_raises(tmp_path_factory):
    """Freezing a symlink whose target is outside the depot entirely must
    raise, not silently dereference and absorb external file content into
    the depot as a blob -- consistent with TreeSnapshot's refusal to ever
    leak information from outside the depot."""
    vol_path = _make_vol(tmp_path_factory, "vol")
    vol = getvol(vol_path)

    external_dir = Path(str(tmp_path_factory.mktemp("external")))
    target = external_dir.join("secret.txt")
    with target.open("w") as fd:
        fd.write("sensitive external content")

    link = vol_path.join("external.lnk")
    link.symlink(target)

    with pytest.raises(ValueError, match="foreign"):
        vol.freeze(link)

    # Nothing should have changed -- link still points at the external file.
    assert link.islink()
    assert link.readlinkat() == target
