"""Integration tests for farmd CLI — all tests call farmd_ui(argv, cwd)."""
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from farmfs import getvol
from farmfs.farmd import (
    ALWAYS_SCHEDULE_NAME,
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


# ── schedule add / remove / list ──────────────────────────────────────────────

def test_schedule_add(farmd_vol: Path) -> None:
    rc = farmd_ui(["schedule", "add", "nightly", "--cron=0 22 * * *"], farmd_vol)
    assert rc == 0
    jr = _jr(farmd_vol)
    sc = jr.scheduledb.read("nightly")
    assert sc.cron == "0 22 * * *"


def test_schedule_add_duplicate(farmd_vol: Path) -> None:
    farmd_ui(["schedule", "add", "nightly", "--cron=0 22 * * *"], farmd_vol)
    rc = farmd_ui(["schedule", "add", "nightly", "--cron=0 22 * * *"], farmd_vol)
    assert rc == 1


def test_schedule_remove(farmd_vol: Path) -> None:
    farmd_ui(["schedule", "add", "nightly", "--cron=0 22 * * *"], farmd_vol)
    rc = farmd_ui(["schedule", "remove", "nightly"], farmd_vol)
    assert rc == 0
    jr = _jr(farmd_vol)
    assert "nightly" not in jr.scheduledb.list()


def test_schedule_remove_missing(farmd_vol: Path) -> None:
    rc = farmd_ui(["schedule", "remove", "nonexistent"], farmd_vol)
    assert rc == 1


def test_schedule_remove_always_fails(farmd_vol: Path) -> None:
    rc = farmd_ui(["schedule", "remove", "always"], farmd_vol)
    assert rc == 1


def test_schedule_list(farmd_vol: Path, capsys: pytest.CaptureFixture) -> None:
    farmd_ui(["schedule", "add", "nightly", "--cron=0 22 * * *"], farmd_vol)
    rc = farmd_ui(["schedule", "list"], farmd_vol)
    assert rc == 0
    out = capsys.readouterr().out
    assert "always" in out
    assert "nightly" in out


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
    assert vc.jobs[0].schedule == ALWAYS_SCHEDULE_NAME


def test_volume_add_with_fsck_schedule(farmd_vol: Path) -> None:
    farmd_ui(["schedule", "add", "nightly", "--cron=0 22 * * *"], farmd_vol)
    rc = farmd_ui(
        ["volume", "add", "photos", "/Volumes/Photos",
         "--fsck-every=1d", "--fsck-schedule=nightly"],
        farmd_vol,
    )
    assert rc == 0
    jr = _jr(farmd_vol)
    vc = jr.volumedb.read("photos")
    assert vc.jobs[0].schedule == "nightly"


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
    assert vc.jobs[0].schedule == ALWAYS_SCHEDULE_NAME


def test_job_add_with_schedule(farmd_vol: Path) -> None:
    farmd_ui(["volume", "add", "media", "/Volumes/Media"], farmd_vol)
    farmd_ui(["schedule", "add", "nightly", "--cron=0 22 * * *"], farmd_vol)
    rc = farmd_ui(
        ["job", "add", "media", "fsck", "--every=1d", "--schedule=nightly"],
        farmd_vol,
    )
    assert rc == 0
    jr = _jr(farmd_vol)
    vc = jr.volumedb.read("media")
    assert vc.jobs[0].schedule == "nightly"


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
    assert "always" in out


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
    assert "always" in out


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


def _make_mock_proc(returncode: int = 0) -> MagicMock:
    """Return a mock subprocess.Popen object that exits immediately."""
    mock_proc = MagicMock()
    mock_proc.pid = 12345
    mock_proc.returncode = returncode
    # wait(timeout=...) returns None immediately (fast exit, no cancel needed)
    mock_proc.wait.return_value = None
    return mock_proc


def test_run_now_runs_job(farmd_vol: Path, farmfs_vol: Path) -> None:
    """run-now should invoke subprocess and record state."""
    farmd_ui(["volume", "add", "media", str(farmfs_vol)], farmd_vol)
    farmd_ui(["job", "add", "media", "fsck", "--every=1d"], farmd_vol)

    mock_proc = _make_mock_proc(returncode=0)

    with patch("farmfs.farmd.subprocess.Popen", return_value=mock_proc) as mock_popen:
        rc = farmd_ui(["run-now", "media/fsck-all"], farmd_vol)

    assert rc == 0
    mock_popen.assert_called_once()
    jr = _jr(farmd_vol)
    js = jr.statedb.read("media/fsck-all")
    assert js.last_exit_code == 0
    assert js.run_count == 1
    assert js.running is False


def test_run_now_records_exit_code(farmd_vol: Path, farmfs_vol: Path) -> None:
    farmd_ui(["volume", "add", "media", str(farmfs_vol)], farmd_vol)
    farmd_ui(["job", "add", "media", "fsck", "--every=1d"], farmd_vol)

    mock_proc = _make_mock_proc(returncode=2)

    with patch("farmfs.farmd.subprocess.Popen", return_value=mock_proc):
        rc = farmd_ui(["run-now", "media/fsck-all"], farmd_vol)

    assert rc == 2


# ── requeue ───────────────────────────────────────────────────────────────────

def test_requeue_no_state(farmd_vol: Path) -> None:
    rc = farmd_ui(["requeue", "media/fsck-all"], farmd_vol)
    assert rc == 1


def test_requeue_clears_next_run(farmd_vol: Path) -> None:
    jr = _jr(farmd_vol)
    future = "2099-01-01T00:00:00+00:00"
    js = JobState("2026-02-28T00:00:00+00:00", "2026-02-28T00:01:00+00:00", 0, future, False, None, 1, None, None)
    jr.statedb.write("media/fsck-all", js, overwrite=False)
    rc = farmd_ui(["requeue", "media/fsck-all"], farmd_vol)
    assert rc == 0
    assert jr.statedb.read("media/fsck-all").next_run is None


def test_requeue_running_job(farmd_vol: Path) -> None:
    jr = _jr(farmd_vol)
    js = JobState("2026-02-28T00:00:00+00:00", None, None, None, True, 9999, 1, None, None)
    jr.statedb.write("media/fsck-all", js, overwrite=False)
    rc = farmd_ui(["requeue", "media/fsck-all"], farmd_vol)
    assert rc == 1


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
    job = JobConfig("fsck", 86400, True, [], None, None, "media/fsck-all", ALWAYS_SCHEDULE_NAME)
    assert _format_status(None, job, now) == "PENDING"


def test_format_status_running() -> None:
    now = datetime(2026, 2, 28, 0, 0, 0, tzinfo=timezone.utc)
    job = JobConfig("fsck", 86400, True, [], None, None, "media/fsck-all", ALWAYS_SCHEDULE_NAME)
    js = JobState("2026-02-28T00:00:00+00:00", None, None, None, True, 1234, 1, None, None)
    assert _format_status(js, job, now) == "RUNNING"


def test_format_status_ok() -> None:
    now = datetime(2026, 2, 28, 0, 0, 0, tzinfo=timezone.utc)
    job = JobConfig("fsck", 86400, True, [], None, None, "media/fsck-all", ALWAYS_SCHEDULE_NAME)
    js = JobState("2026-02-28T00:00:00+00:00", "2026-02-28T00:01:00+00:00", 0, "2026-03-01T00:00:00+00:00", False, None, 1, None, None)
    assert _format_status(js, job, now) == "OK(0)"


def test_format_status_fail() -> None:
    now = datetime(2026, 2, 28, 0, 0, 0, tzinfo=timezone.utc)
    job = JobConfig("fsck", 86400, True, [], None, None, "media/fsck-all", ALWAYS_SCHEDULE_NAME)
    js = JobState("2026-02-28T00:00:00+00:00", "2026-02-28T00:01:00+00:00", 1, "2026-03-01T00:00:00+00:00", False, None, 1, None, None)
    assert _format_status(js, job, now) == "FAIL(1)"


def test_format_status_cancelled() -> None:
    now = datetime(2026, 2, 28, 0, 0, 0, tzinfo=timezone.utc)
    job = JobConfig("fsck", 86400, True, [], None, None, "media/fsck-all", ALWAYS_SCHEDULE_NAME)
    js = JobState("2026-02-28T00:00:00+00:00", "2026-02-28T00:01:00+00:00", -15,
                  "2026-03-01T00:00:00+00:00", False, None, 1, None, None)
    assert _format_status(js, job, now) == "CANCELLED(-15)"
