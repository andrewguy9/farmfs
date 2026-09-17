"""
Tests for Volume.freeze(), focused on symlink inputs.

freeze() computes csum = path.checksum() (follows symlinks). For a regular
file it hardlinks path into the blobstore (import_via_link). For a symlink
it instead copies the bytes via import_via_fd, since hardlinking would
follow through the symlink to the target's inode -- and ensure_readonly
chmod'ing that shared inode would silently mutate an un-frozen target file.
"""

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

    assert f.islink()
    assert f.readlinkat() == vol.bs.blob_path(result["csum"])
    assert not result["was_dup"]


def test_freeze_symlink_to_tracked_target_dedups(tmp_path_factory):
    """Freezing a symlink whose target is another in-volume file that gets
    frozen too: both should collapse to the same blob via dedup, and the
    target file must remain a normal writable file (not corrupted by the
    hardlink trick), since it is frozen independently and explicitly."""
    vol_path = _make_vol(tmp_path_factory, "vol")
    vol = getvol(vol_path)

    target = vol_path.join("real.txt")
    with target.open("w") as fd:
        fd.write("shared content")

    link = vol_path.join("link.txt")
    link.symlink(target)

    link_result = vol.freeze(link)
    target_result = vol.freeze(target)

    assert link_result["csum"] == target_result["csum"]
    assert target_result["was_dup"]  # second freeze of identical content is a dup

    assert link.islink()
    assert target.islink()
    assert link.readlinkat() == vol.bs.blob_path(link_result["csum"])
    assert target.readlinkat() == vol.bs.blob_path(target_result["csum"])


def test_freeze_symlink_does_not_mutate_unfrozen_target(tmp_path_factory):
    """Freezing a symlink whose target is NOT itself frozen: the target file
    must be untouched (still a regular, writable file with its original
    content) -- the hardlink used to import the blob must not leak
    read-only permissions back onto the target via the shared inode."""
    vol_path = _make_vol(tmp_path_factory, "vol")
    vol = getvol(vol_path)

    target = vol_path.join("real.txt")
    with target.open("w") as fd:
        fd.write("shared content")
    target_mode_before = target.stat().st_mode

    link = vol_path.join("link.txt")
    link.symlink(target)

    vol.freeze(link)

    assert link.islink()
    # The un-frozen target should still be a regular file, not a symlink,
    # and its permissions should be unchanged (not flipped read-only by
    # the blobstore's ensure_readonly acting on the shared inode).
    assert not target.islink()
    assert target.isfile()
    assert target.stat().st_mode == target_mode_before
    assert target.stat().st_nlink == 1  # not hardlinked into the blobstore
    with target.open("r") as fd:
        assert fd.read() == "shared content"
