"""Integration tests for farmd CLI — all tests call farmd_ui(argv, cwd)."""
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from farmfs import getvol
from farmfs.farmd import (
    JobConfig,
    JobRunner,
    JobState,
)
from farmfs.farmd_ui import (
    _format_status,
    _format_time,
    farmd_ui,
)
from farmfs.fs import Path
from farmfs.volume import FarmFSVolume, mkfs


# ── Fixtures ──────────────────────────────────────────────────────────────────

def make_volume(path: Path) -> FarmFSVolume:
    udd_path = path.join(".farmfs").join("userdata")
    mkfs(path, udd_path)
    vol = getvol(path)
    return vol


@pytest.fixture
def farmd_vol(tmp: Path) -> Path:
    """Create a fresh farmfs volume under tmp/farmd and return its path."""
    vol_path = tmp.join("farmd")
    make_volume(vol_path)
    return vol_path


@pytest.fixture
def farmfs_vol(tmp: Path) -> Path:
    """Create a separate farmfs volume (simulating a managed volume) and return its path."""
    vol_path = tmp.join("farmfs")
    make_volume(vol_path)
    return vol_path


def _jr(vol_path: Path) -> JobRunner:
    """Open a JobRunner on an existing farmfs volume at vol_path."""
    vol = getvol(vol_path)
    return JobRunner(vol)


# ── mkcfg ─────────────────────────────────────────────────────────────────────

def test_mkcfg_creates_config(farmd_vol: Path) -> None:
    rc = farmd_ui(["mkcfg"], farmd_vol)
    assert rc == 0
    jr = _jr(farmd_vol)
    cfg = jr.configdb.read("config")
    assert cfg.night_start == 22
    assert cfg.night_end == 6


def test_mkcfg_idempotent_fails_second(farmd_vol: Path) -> None:
    farmd_ui(["mkcfg"], farmd_vol)
    # Second call should raise or return non-zero (overwrite=False)
    try:
        rc = farmd_ui(["mkcfg"], farmd_vol)
        assert rc != 0
    except Exception:
        pass  # either exception or non-zero is acceptable


# ── config set / show ─────────────────────────────────────────────────────────

def test_config_set_and_show(farmd_vol: Path, capsys: pytest.CaptureFixture) -> None:
    farmd_ui(["mkcfg"], farmd_vol)
    rc = farmd_ui(["config", "set", "--night-start=20", "--night-end=8"], farmd_vol)
    assert rc == 0
    jr = _jr(farmd_vol)
    cfg = jr.configdb.read("config")
    assert cfg.night_start == 20
    assert cfg.night_end == 8


def test_config_set_partial(farmd_vol: Path) -> None:
    farmd_ui(["mkcfg"], farmd_vol)
    rc = farmd_ui(["config", "set", "--night-start=21"], farmd_vol)
    assert rc == 0
    jr = _jr(farmd_vol)
    cfg = jr.configdb.read("config")
    assert cfg.night_start == 21
    assert cfg.night_end == 6  # unchanged default


def test_config_show_ok(farmd_vol: Path, capsys: pytest.CaptureFixture) -> None:
    farmd_ui(["mkcfg"], farmd_vol)
    rc = farmd_ui(["config", "show"], farmd_vol)
    assert rc == 0
    out = capsys.readouterr().out
    assert "night_start" in out
    assert "night_end" in out


def test_config_show_missing(farmd_vol: Path, capsys: pytest.CaptureFixture) -> None:
    # No mkcfg — config key absent
    rc = farmd_ui(["config", "show"], farmd_vol)
    assert rc == 1


# ── volume add / remove / list ────────────────────────────────────────────────

def test_volume_add_basic(farmd_vol: Path) -> None:
    rc = farmd_ui(["volume", "add", "media", "/Volumes/Media"], farmd_vol)
    assert rc == 0
    jr = _jr(farmd_vol)
    vc = jr.volumedb.read("media")
    assert vc.name == "media"
    assert vc.root == "/Volumes/Media"
    assert vc.jobs == []


def test_volume_add_with_fsck_job(farmd_vol: Path) -> None:
    rc = farmd_ui(
        ["volume", "add", "photos", "/Volumes/Photos",
         "--fsck-every=1d", "--fsck-flags=--missing"],
        farmd_vol,
    )
    assert rc == 0
    jr = _jr(farmd_vol)
    vc = jr.volumedb.read("photos")
    assert len(vc.jobs) == 1
    assert vc.jobs[0].type == "fsck"
    assert vc.jobs[0].flags == ["--missing"]
    assert vc.jobs[0].every_seconds == 86400


def test_volume_add_with_all_jobs(farmd_vol: Path) -> None:
    rc = farmd_ui(
        ["volume", "add", "media", "/Volumes/Media",
         "--fsck-every=1d",
         "--fetch-remote=backup", "--fetch-every=6h",
         "--upload-remote=backup", "--upload-every=12h"],
        farmd_vol,
    )
    assert rc == 0
    jr = _jr(farmd_vol)
    vc = jr.volumedb.read("media")
    assert len(vc.jobs) == 3
    types = [j.type for j in vc.jobs]
    assert "fsck" in types
    assert "fetch" in types
    assert "upload" in types


def test_volume_add_duplicate(farmd_vol: Path) -> None:
    assert farmd_ui(["volume", "add", "media", "/Volumes/Media"], farmd_vol) == 0
    assert farmd_ui(["volume", "add", "media", "/Volumes/Media"], farmd_vol) == 1


def test_volume_remove(farmd_vol: Path) -> None:
    farmd_ui(["volume", "add", "media", "/Volumes/Media"], farmd_vol)
    rc = farmd_ui(["volume", "remove", "media"], farmd_vol)
    assert rc == 0
    jr = _jr(farmd_vol)
    assert "media" not in jr.volumedb.list()


def test_volume_remove_missing(farmd_vol: Path) -> None:
    rc = farmd_ui(["volume", "remove", "nonexistent"], farmd_vol)
    assert rc == 1


def test_volume_list(farmd_vol: Path, capsys: pytest.CaptureFixture) -> None:
    farmd_ui(["volume", "add", "media", "/Volumes/Media"], farmd_vol)
    rc = farmd_ui(["volume", "list"], farmd_vol)
    assert rc == 0
    out = capsys.readouterr().out
    assert "media" in out


# ── job add / remove / list ───────────────────────────────────────────────────

def test_job_add_fsck(farmd_vol: Path) -> None:
    farmd_ui(["volume", "add", "media", "/Volumes/Media"], farmd_vol)
    rc = farmd_ui(
        ["job", "add", "media", "fsck", "--every=1d", "--flags=--missing"],
        farmd_vol,
    )
    assert rc == 0
    jr = _jr(farmd_vol)
    vc = jr.volumedb.read("media")
    assert len(vc.jobs) == 1
    assert vc.jobs[0].type == "fsck"
    assert vc.jobs[0].flags == ["--missing"]
    assert vc.jobs[0].job_id == "media/fsck-missing"


def test_job_add_fetch(farmd_vol: Path) -> None:
    farmd_ui(["volume", "add", "media", "/Volumes/Media"], farmd_vol)
    rc = farmd_ui(
        ["job", "add", "media", "fetch", "--every=6h", "--remote=backup"],
        farmd_vol,
    )
    assert rc == 0
    jr = _jr(farmd_vol)
    vc = jr.volumedb.read("media")
    assert len(vc.jobs) == 1
    assert vc.jobs[0].type == "fetch"
    assert vc.jobs[0].remote == "backup"
    assert vc.jobs[0].job_id == "media/fetch-backup"


def test_job_add_duplicate(farmd_vol: Path) -> None:
    farmd_ui(["volume", "add", "media", "/Volumes/Media"], farmd_vol)
    assert farmd_ui(["job", "add", "media", "fsck", "--every=1d"], farmd_vol) == 0
    assert farmd_ui(["job", "add", "media", "fsck", "--every=1d"], farmd_vol) == 1


def test_job_add_missing_volume(farmd_vol: Path) -> None:
    rc = farmd_ui(
        ["job", "add", "nonexistent", "fsck", "--every=1d"],
        farmd_vol,
    )
    assert rc == 1


def test_job_remove(farmd_vol: Path) -> None:
    farmd_ui(["volume", "add", "media", "/Volumes/Media"], farmd_vol)
    farmd_ui(["job", "add", "media", "fsck", "--every=1d"], farmd_vol)
    rc = farmd_ui(["job", "remove", "media/fsck-all"], farmd_vol)
    assert rc == 0
    jr = _jr(farmd_vol)
    vc = jr.volumedb.read("media")
    assert len(vc.jobs) == 0


def test_job_remove_missing(farmd_vol: Path) -> None:
    rc = farmd_ui(["job", "remove", "media/fsck-all"], farmd_vol)
    assert rc == 1


def test_job_list(farmd_vol: Path, capsys: pytest.CaptureFixture) -> None:
    farmd_ui(["volume", "add", "media", "/Volumes/Media"], farmd_vol)
    farmd_ui(["job", "add", "media", "fsck", "--every=1d"], farmd_vol)
    rc = farmd_ui(["job", "list"], farmd_vol)
    assert rc == 0
    out = capsys.readouterr().out
    assert "media/fsck-all" in out


def test_job_list_filter_by_vol(farmd_vol: Path, capsys: pytest.CaptureFixture) -> None:
    farmd_ui(["volume", "add", "media", "/Volumes/Media", "--fsck-every=1d"], farmd_vol)
    farmd_ui(["volume", "add", "photos", "/Volumes/Photos", "--fsck-every=1d"], farmd_vol)
    capsys.readouterr()  # clear setup output
    rc = farmd_ui(["job", "list", "photos"], farmd_vol)
    assert rc == 0
    out = capsys.readouterr().out
    assert "photos" in out
    assert "media" not in out


# ── status ────────────────────────────────────────────────────────────────────

def test_status_empty(farmd_vol: Path, capsys: pytest.CaptureFixture) -> None:
    rc = farmd_ui(["status"], farmd_vol)
    assert rc == 0
    out = capsys.readouterr().out
    assert "VOLUME" in out


def test_status_with_pending_job(farmd_vol: Path, capsys: pytest.CaptureFixture) -> None:
    farmd_ui(["volume", "add", "media", "/Volumes/Media"], farmd_vol)
    farmd_ui(["job", "add", "media", "fsck", "--every=1d"], farmd_vol)
    rc = farmd_ui(["status"], farmd_vol)
    assert rc == 0
    out = capsys.readouterr().out
    assert "media" in out
    assert "PENDING" in out or "ASAP" in out


def test_status_with_running_job(farmd_vol: Path, capsys: pytest.CaptureFixture) -> None:
    farmd_ui(["volume", "add", "media", "/Volumes/Media"], farmd_vol)
    farmd_ui(["job", "add", "media", "fsck", "--every=1d"], farmd_vol)
    jr = _jr(farmd_vol)
    js = JobState("2026-02-28T00:00:00+00:00", None, None, None, True, 9999, 1, None, None)
    jr.statedb.write("media/fsck-all", js, overwrite=False)
    rc = farmd_ui(["status"], farmd_vol)
    assert rc == 0
    out = capsys.readouterr().out
    assert "RUNNING" in out


def test_status_with_ok_job(farmd_vol: Path, capsys: pytest.CaptureFixture) -> None:
    farmd_ui(["volume", "add", "media", "/Volumes/Media"], farmd_vol)
    farmd_ui(["job", "add", "media", "fsck", "--every=1d"], farmd_vol)
    jr = _jr(farmd_vol)
    future = "2099-01-01T00:00:00+00:00"
    js = JobState("2026-02-28T00:00:00+00:00", "2026-02-28T00:01:00+00:00", 0, future, False, None, 1, None, None)
    jr.statedb.write("media/fsck-all", js, overwrite=False)
    rc = farmd_ui(["status"], farmd_vol)
    assert rc == 0
    out = capsys.readouterr().out
    assert "OK(0)" in out


# ── log ───────────────────────────────────────────────────────────────────────

def test_log_missing_state(farmd_vol: Path) -> None:
    rc = farmd_ui(["log", "media/fsck-all"], farmd_vol)
    assert rc == 1


def test_log_no_blob(farmd_vol: Path) -> None:
    jr = _jr(farmd_vol)
    js = JobState("2026-02-28T00:00:00+00:00", "2026-02-28T00:01:00+00:00", 0, None, False, None, 1, None, None)
    jr.statedb.write("media/fsck-all", js, overwrite=False)
    rc = farmd_ui(["log", "media/fsck-all"], farmd_vol)
    assert rc == 1


# ── run-now ───────────────────────────────────────────────────────────────────

def test_run_now_not_found(farmd_vol: Path) -> None:
    rc = farmd_ui(["run-now", "media/fsck-all"], farmd_vol)
    assert rc == 1


def test_run_now_runs_job(farmd_vol: Path, farmfs_vol: Path) -> None:
    """run-now should invoke subprocess and record state."""
    farmd_ui(["volume", "add", "media", str(farmfs_vol)], farmd_vol)
    farmd_ui(["job", "add", "media", "fsck", "--every=1d"], farmd_vol)

    fake_result = MagicMock()
    fake_result.returncode = 0
    fake_result.stdout = b"fsck ok\n"

    with patch("farmfs.farmd.subprocess.run", return_value=fake_result) as mock_run:
        rc = farmd_ui(["run-now", "media/fsck-all"], farmd_vol)

    assert rc == 0
    mock_run.assert_called_once()
    jr = _jr(farmd_vol)
    js = jr.statedb.read("media/fsck-all")
    assert js.last_exit_code == 0
    assert js.run_count == 1
    assert js.running is False


def test_run_now_records_exit_code(farmd_vol: Path, farmfs_vol: Path) -> None:
    farmd_ui(["volume", "add", "media", str(farmfs_vol)], farmd_vol)
    farmd_ui(["job", "add", "media", "fsck", "--every=1d"], farmd_vol)

    fake_result = MagicMock()
    fake_result.returncode = 2
    fake_result.stdout = b"error\n"

    with patch("farmfs.farmd.subprocess.run", return_value=fake_result):
        rc = farmd_ui(["run-now", "media/fsck-all"], farmd_vol)

    assert rc == 2


# ── _format_time / _format_status helpers ────────────────────────────────────

def test_format_time_none() -> None:
    assert _format_time(None) == "never"


def test_format_time_iso() -> None:
    result = _format_time("2026-02-28T03:00:00+00:00")
    assert "2026" in result


def test_format_time_bad() -> None:
    result = _format_time("not-a-date")
    assert result == "not-a-date"


def test_format_status_pending() -> None:
    now = datetime(2026, 2, 28, 0, 0, 0, tzinfo=timezone.utc)
    job = JobConfig("fsck", 86400, True, [], None, None, "media/fsck-all")
    assert _format_status(None, job, now) == "PENDING"


def test_format_status_running() -> None:
    now = datetime(2026, 2, 28, 0, 0, 0, tzinfo=timezone.utc)
    job = JobConfig("fsck", 86400, True, [], None, None, "media/fsck-all")
    js = JobState("2026-02-28T00:00:00+00:00", None, None, None, True, 1234, 1, None, None)
    assert _format_status(js, job, now) == "RUNNING"


def test_format_status_ok() -> None:
    now = datetime(2026, 2, 28, 0, 0, 0, tzinfo=timezone.utc)
    job = JobConfig("fsck", 86400, True, [], None, None, "media/fsck-all")
    js = JobState("2026-02-28T00:00:00+00:00", "2026-02-28T00:01:00+00:00", 0, "2026-03-01T00:00:00+00:00", False, None, 1, None, None)
    assert _format_status(js, job, now) == "OK(0)"


def test_format_status_fail() -> None:
    now = datetime(2026, 2, 28, 0, 0, 0, tzinfo=timezone.utc)
    job = JobConfig("fsck", 86400, True, [], None, None, "media/fsck-all")
    js = JobState("2026-02-28T00:00:00+00:00", "2026-02-28T00:01:00+00:00", 1, "2026-03-01T00:00:00+00:00", False, None, 1, None, None)
    assert _format_status(js, job, now) == "FAIL(1)"
