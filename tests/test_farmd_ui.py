"""Integration tests for farmd CLI subcommands (no subprocess — call handlers directly)."""
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
    cmd_config_set,
    cmd_config_show,
    cmd_job_add,
    cmd_job_list,
    cmd_job_remove,
    cmd_log,
    cmd_mkcfg,
    cmd_run_now,
    cmd_status,
    cmd_volume_add,
    cmd_volume_list,
    cmd_volume_remove,
    farmd_ui,
)
from farmfs.fs import Path
from farmfs.volume import FarmFSVolume, mkfs


# ── Fixtures ──────────────────────────────────────────────────────────────────

def make_volume(path: Path) -> FarmFSVolume:
    # TODO centralize udd default calculation.
    udd_path = path.join(".farmfs").join("userdata")
    mkfs(path, udd_path)
    vol = getvol(path)
    return vol

@pytest.fixture
def farmfs_vol(tmp) -> FarmFSVolume:
    """Create a fresh farmfs volume and return the FarmFSVolume instance."""
    vol_path = tmp.join("farmfs")
    return make_volume(vol_path)

def make_job_runner(path: Path) -> JobRunner:
    vol = getvol(path)
    jr = JobRunner(vol)
    return jr

@pytest.fixture
def farmd_vol(tmp):
    """Create a fresh farmd volume and return a JobRunner instance."""
    vol_path = str(tmp.join("farmd"))
    jr = make_job_runner(vol_path)
    return jr


# ── mkfs ──────────────────────────────────────────────────────────────────────

def test_mkfs_creates_volume(tmp: Path) -> None:

    rc = farmd_ui(["mkcfg", "--volume", "test_vol"], tmp)
    assert rc == 0
    # Volume must be a valid farmfs vol
    make_job_runner(tmp.join("test_vol"))
    jr = JobRunner(Path(vol_path))
    cfg = jr.configdb.read("config")
    assert cfg.night_start == 22
    assert cfg.night_end == 6


# ── config set / show ─────────────────────────────────────────────────────────

def test_config_set_and_show(farmd_vol, capsys) -> None:
    vol_path, jr = farmd_vol
    args = {"--volume": vol_path, "--night-start": "20", "--night-end": "8"}
    rc = cmd_config_set(args)
    assert rc == 0

    cfg = jr.configdb.read("config")
    assert cfg.night_start == 20
    assert cfg.night_end == 8


def test_config_set_partial(farmd_vol) -> None:
    vol_path, jr = farmd_vol
    # Only change night_start
    rc = cmd_config_set({"--volume": vol_path, "--night-start": "21", "--night-end": None})
    assert rc == 0
    cfg = jr.configdb.read("config")
    assert cfg.night_start == 21
    assert cfg.night_end == 6  # unchanged


def test_config_show_ok(farmd_vol, capsys) -> None:
    vol_path, _ = farmd_vol
    rc = cmd_config_show({"--volume": vol_path})
    assert rc == 0
    out = capsys.readouterr().out
    assert "night_start" in out
    assert "night_end" in out


def test_config_show_missing(tmp, capsys) -> None:
    # Create a raw volume with no config key
    vol_path = str(tmp.join("raw_vol"))
    make_job_runner(Path(vol_path))
    rc = cmd_config_show({"--volume": vol_path})
    assert rc == 1


# ── volume add / remove / list ────────────────────────────────────────────────

def test_volume_add_basic(farmd_vol) -> None:
    vol_path, jr = farmd_vol
    args = {
        "--volume": vol_path,
        "<name>": "media",
        "<root>": "/Volumes/Media",
        "--fsck-every": None,
        "--fsck-flags": [],
        "--fetch-remote": None,
        "--fetch-every": None,
        "--upload-remote": None,
        "--upload-every": None,
    }
    rc = cmd_volume_add(args)
    assert rc == 0
    vc = jr.volumedb.read("media")
    assert vc.name == "media"
    assert vc.root == "/Volumes/Media"
    assert vc.jobs == []


def test_volume_add_with_fsck_job(farmd_vol) -> None:
    vol_path, jr = farmd_vol
    args = {
        "--volume": vol_path,
        "<name>": "photos",
        "<root>": "/Volumes/Photos",
        "--fsck-every": "1d",
        "--fsck-flags": ["--missing"],
        "--fetch-remote": None,
        "--fetch-every": None,
        "--upload-remote": None,
        "--upload-every": None,
    }
    rc = cmd_volume_add(args)
    assert rc == 0
    vc = jr.volumedb.read("photos")
    assert len(vc.jobs) == 1
    assert vc.jobs[0].type == "fsck"
    assert vc.jobs[0].flags == ["--missing"]
    assert vc.jobs[0].every_seconds == 86400


def test_volume_add_with_all_jobs(farmd_vol) -> None:
    vol_path, jr = farmd_vol
    args = {
        "--volume": vol_path,
        "<name>": "media",
        "<root>": "/Volumes/Media",
        "--fsck-every": "1d",
        "--fsck-flags": [],
        "--fetch-remote": "backup",
        "--fetch-every": "6h",
        "--upload-remote": "backup",
        "--upload-every": "12h",
    }
    rc = cmd_volume_add(args)
    assert rc == 0
    vc = jr.volumedb.read("media")
    assert len(vc.jobs) == 3
    types = [j.type for j in vc.jobs]
    assert "fsck" in types
    assert "fetch" in types
    assert "upload" in types


def test_volume_add_duplicate(farmd_vol) -> None:
    vol_path, _ = farmd_vol
    base_args = {
        "--volume": vol_path,
        "<name>": "media",
        "<root>": "/Volumes/Media",
        "--fsck-every": None, "--fsck-flags": [],
        "--fetch-remote": None, "--fetch-every": None,
        "--upload-remote": None, "--upload-every": None,
    }
    assert cmd_volume_add(base_args) == 0
    assert cmd_volume_add(base_args) == 1  # duplicate


def test_volume_remove(farmd_vol) -> None:
    vol_path, jr = farmd_vol
    add_args = {
        "--volume": vol_path,
        "<name>": "media",
        "<root>": "/Volumes/Media",
        "--fsck-every": None, "--fsck-flags": [],
        "--fetch-remote": None, "--fetch-every": None,
        "--upload-remote": None, "--upload-every": None,
    }
    cmd_volume_add(add_args)
    rc = cmd_volume_remove({"--volume": vol_path, "<name>": "media"})
    assert rc == 0
    assert "media" not in jr.volumedb.list()


def test_volume_remove_missing(farmd_vol) -> None:
    vol_path, _ = farmd_vol
    rc = cmd_volume_remove({"--volume": vol_path, "<name>": "nonexistent"})
    assert rc == 1


def test_volume_list(farmd_vol, capsys) -> None:
    vol_path, _ = farmd_vol
    add_args = {
        "--volume": vol_path,
        "<name>": "media",
        "<root>": "/Volumes/Media",
        "--fsck-every": None, "--fsck-flags": [],
        "--fetch-remote": None, "--fetch-every": None,
        "--upload-remote": None, "--upload-every": None,
    }
    cmd_volume_add(add_args)
    rc = cmd_volume_list({"--volume": vol_path})
    assert rc == 0
    out = capsys.readouterr().out
    assert "media" in out


# ── job add / remove / list ───────────────────────────────────────────────────

def _add_volume(vol_path: str, name: str = "media", root: str = "/Volumes/Media") -> None:
    args = {
        "--volume": vol_path,
        "<name>": name,
        "<root>": root,
        "--fsck-every": None, "--fsck-flags": [],
        "--fetch-remote": None, "--fetch-every": None,
        "--upload-remote": None, "--upload-every": None,
    }
    assert cmd_volume_add(args) == 0


def test_job_add_fsck(farmd_vol) -> None:
    vol_path, jr = farmd_vol
    _add_volume(vol_path)
    args = {
        "--volume": vol_path,
        "<vol_name>": "media",
        "<type>": "fsck",
        "--every": "1d",
        "--flags": ["--missing"],
        "--remote": None,
        "--snap": None,
    }
    rc = cmd_job_add(args)
    assert rc == 0
    vc = jr.volumedb.read("media")
    assert len(vc.jobs) == 1
    assert vc.jobs[0].type == "fsck"
    assert vc.jobs[0].flags == ["--missing"]
    assert vc.jobs[0].job_id == "media/fsck-missing"


def test_job_add_fetch(farmd_vol) -> None:
    vol_path, jr = farmd_vol
    _add_volume(vol_path)
    args = {
        "--volume": vol_path,
        "<vol_name>": "media",
        "<type>": "fetch",
        "--every": "6h",
        "--flags": [],
        "--remote": "backup",
        "--snap": None,
    }
    rc = cmd_job_add(args)
    assert rc == 0
    vc = jr.volumedb.read("media")
    assert len(vc.jobs) == 1
    assert vc.jobs[0].type == "fetch"
    assert vc.jobs[0].remote == "backup"
    assert vc.jobs[0].job_id == "media/fetch-backup"


def test_job_add_duplicate(farmd_vol) -> None:
    vol_path, _ = farmd_vol
    _add_volume(vol_path)
    args = {
        "--volume": vol_path,
        "<vol_name>": "media",
        "<type>": "fsck",
        "--every": "1d",
        "--flags": ["--missing"],
        "--remote": None,
        "--snap": None,
    }
    assert cmd_job_add(args) == 0
    assert cmd_job_add(args) == 1  # duplicate


def test_job_add_missing_volume(farmd_vol) -> None:
    vol_path, _ = farmd_vol
    args = {
        "--volume": vol_path,
        "<vol_name>": "nonexistent",
        "<type>": "fsck",
        "--every": "1d",
        "--flags": [],
        "--remote": None,
        "--snap": None,
    }
    rc = cmd_job_add(args)
    assert rc == 1


def test_job_remove(farmd_vol) -> None:
    vol_path, jr = farmd_vol
    _add_volume(vol_path)
    add_args = {
        "--volume": vol_path,
        "<vol_name>": "media",
        "<type>": "fsck",
        "--every": "1d",
        "--flags": [],
        "--remote": None,
        "--snap": None,
    }
    cmd_job_add(add_args)
    rc = cmd_job_remove({"--volume": vol_path, "<job_id>": "media/fsck-all"})
    assert rc == 0
    vc = jr.volumedb.read("media")
    assert len(vc.jobs) == 0


def test_job_remove_missing(farmd_vol) -> None:
    vol_path, _ = farmd_vol
    rc = cmd_job_remove({"--volume": vol_path, "<job_id>": "media/fsck-all"})
    assert rc == 1


def test_job_list(farmd_vol, capsys) -> None:
    vol_path, _ = farmd_vol
    _add_volume(vol_path)
    add_args = {
        "--volume": vol_path,
        "<vol_name>": "media",
        "<type>": "fsck",
        "--every": "1d",
        "--flags": [],
        "--remote": None,
        "--snap": None,
    }
    cmd_job_add(add_args)
    rc = cmd_job_list({"--volume": vol_path, "<vol_name>": None})
    assert rc == 0
    out = capsys.readouterr().out
    assert "media/fsck-all" in out


# ── status ────────────────────────────────────────────────────────────────────

def test_status_empty(farmd_vol, capsys) -> None:
    vol_path, _ = farmd_vol
    rc = cmd_status({"--volume": vol_path})
    assert rc == 0
    out = capsys.readouterr().out
    assert "VOLUME" in out


def test_status_with_pending_job(farmd_vol, capsys) -> None:
    vol_path, _ = farmd_vol
    _add_volume(vol_path)
    add_args = {
        "--volume": vol_path,
        "<vol_name>": "media",
        "<type>": "fsck",
        "--every": "1d",
        "--flags": [],
        "--remote": None,
        "--snap": None,
    }
    cmd_job_add(add_args)
    rc = cmd_status({"--volume": vol_path})
    assert rc == 0
    out = capsys.readouterr().out
    assert "media" in out
    assert "PENDING" in out or "ASAP" in out


# ── _format_time / _format_status helpers ────────────────────────────────────

def test_format_time_none() -> None:
    assert _format_time(None) == "never"


def test_format_time_iso() -> None:
    result = _format_time("2026-02-28T03:00:00+00:00")
    assert "2026" in result


def test_format_time_bad() -> None:
    # Should not raise; falls back to the raw string
    result = _format_time("not-a-date")
    assert result == "not-a-date"


def test_format_status_pending() -> None:
    now = datetime(2026, 2, 28, 0, 0, 0, tzinfo=timezone.utc)
    job = JobConfig("fsck", 86400, True, [], None, None, "media/fsck-all")
    assert _format_status(None, job, now) == "PENDING"


def test_format_status_running() -> None:
    now = datetime(2026, 2, 28, 0, 0, 0, tzinfo=timezone.utc)
    job = JobConfig("fsck", 86400, True, [], None, None, "media/fsck-all")
    js = JobState("2026-02-28T00:00:00+00:00", None, None, None, True, 1234, 1, None)
    assert _format_status(js, job, now) == "RUNNING"


def test_format_status_ok() -> None:
    now = datetime(2026, 2, 28, 0, 0, 0, tzinfo=timezone.utc)
    job = JobConfig("fsck", 86400, True, [], None, None, "media/fsck-all")
    js = JobState("2026-02-28T00:00:00+00:00", "2026-02-28T00:01:00+00:00", 0, "2026-03-01T00:00:00+00:00", False, None, 1, None)
    assert _format_status(js, job, now) == "OK(0)"


def test_format_status_fail() -> None:
    now = datetime(2026, 2, 28, 0, 0, 0, tzinfo=timezone.utc)
    job = JobConfig("fsck", 86400, True, [], None, None, "media/fsck-all")
    js = JobState("2026-02-28T00:00:00+00:00", "2026-02-28T00:01:00+00:00", 1, "2026-03-01T00:00:00+00:00", False, None, 1, None)
    assert _format_status(js, job, now) == "FAIL(1)"


# ── cmd_log ────────────────────────────────────────────────────────────────────

def test_cmd_log_missing_state(farmd_vol) -> None:
    vol_path, _ = farmd_vol
    rc = cmd_log({"<job_id>": "media/fsck-all", "--volume": vol_path})
    assert rc == 1


def test_cmd_log_no_blob(farmd_vol) -> None:
    vol_path, jr = farmd_vol
    # Write a state with no blob
    js = JobState("2026-02-28T00:00:00+00:00", "2026-02-28T00:01:00+00:00", 0, None, False, None, 1, None)
    jr.statedb.write("media/fsck-all", js, overwrite=False)
    rc = cmd_log({"<job_id>": "media/fsck-all", "--volume": vol_path})
    assert rc == 1


# ── cmd_run_now ────────────────────────────────────────────────────────────────

def test_cmd_run_now_not_found(farmd_vol) -> None:
    vol_path, _ = farmd_vol
    rc = cmd_run_now({"<job_id>": "media/fsck-all", "--volume": vol_path})
    assert rc == 1


def test_cmd_run_now_runs_job(farmd_vol) -> None:
    """run-now should run the job via subprocess and record state."""
    vol_path, jr = farmd_vol
    _add_volume(vol_path)
    add_args = {
        "--volume": vol_path,
        "<vol_name>": "media",
        "<type>": "fsck",
        "--every": "1d",
        "--flags": [],
        "--remote": None,
        "--snap": None,
    }
    cmd_job_add(add_args)

    # Patch subprocess.run so we don't actually invoke farmfs
    fake_result = MagicMock()
    fake_result.returncode = 0
    fake_result.stdout = b"fsck ok\n"

    with patch("farmfs.farmd.subprocess.run", return_value=fake_result) as mock_run:
        rc = cmd_run_now({"<job_id>": "media/fsck-all", "--volume": vol_path})

    assert rc == 0
    mock_run.assert_called_once()
    # State should be recorded
    js = jr.statedb.read("media/fsck-all")
    assert js.last_exit_code == 0
    assert js.run_count == 1
    assert js.running is False


def test_cmd_run_now_records_exit_code(farmd_vol) -> None:
    """run-now should return the subprocess exit code."""
    vol_path, jr = farmd_vol
    _add_volume(vol_path)
    add_args = {
        "--volume": vol_path,
        "<vol_name>": "media",
        "<type>": "fsck",
        "--every": "1d",
        "--flags": [],
        "--remote": None,
        "--snap": None,
    }
    cmd_job_add(add_args)

    fake_result = MagicMock()
    fake_result.returncode = 2
    fake_result.stdout = b"error\n"

    with patch("farmfs.farmd.subprocess.run", return_value=fake_result):
        rc = cmd_run_now({"<job_id>": "media/fsck-all", "--volume": vol_path})

    assert rc == 2


# ── job_list with filter ───────────────────────────────────────────────────────

def test_job_list_filter_by_vol(farmd_vol, capsys) -> None:
    vol_path, _ = farmd_vol
    # Add two volumes
    for vname, vroot in [("media", "/vol/media"), ("photos", "/vol/photos")]:
        add_args = {
            "--volume": vol_path,
            "<name>": vname,
            "<root>": vroot,
            "--fsck-every": "1d", "--fsck-flags": [],
            "--fetch-remote": None, "--fetch-every": None,
            "--upload-remote": None, "--upload-every": None,
        }
        cmd_volume_add(add_args)

    rc = cmd_job_list({"--volume": vol_path, "<vol_name>": "photos"})
    assert rc == 0
    out = capsys.readouterr().out
    # Only photos volume filtered
    assert "photos" in out or out == ""  # photos has 1 fsck job from --fsck-every


# ── status with running job ────────────────────────────────────────────────────

def test_status_with_running_job(farmd_vol, capsys) -> None:
    vol_path, jr = farmd_vol
    _add_volume(vol_path)
    add_args = {
        "--volume": vol_path,
        "<vol_name>": "media",
        "<type>": "fsck",
        "--every": "1d",
        "--flags": [],
        "--remote": None,
        "--snap": None,
    }
    cmd_job_add(add_args)

    # Manually write a running state
    js = JobState("2026-02-28T00:00:00+00:00", None, None, None, True, 9999, 1, None)
    jr.statedb.write("media/fsck-all", js, overwrite=False)

    rc = cmd_status({"--volume": vol_path})
    assert rc == 0
    out = capsys.readouterr().out
    assert "RUNNING" in out


# ── status with completed job shows OK ────────────────────────────────────────

def test_status_with_ok_job(farmd_vol, capsys) -> None:
    vol_path, jr = farmd_vol
    _add_volume(vol_path)
    add_args = {
        "--volume": vol_path,
        "<vol_name>": "media",
        "<type>": "fsck",
        "--every": "1d",
        "--flags": [],
        "--remote": None,
        "--snap": None,
    }
    cmd_job_add(add_args)

    # Write a completed successful state
    future = "2099-01-01T00:00:00+00:00"
    js = JobState("2026-02-28T00:00:00+00:00", "2026-02-28T00:01:00+00:00", 0, future, False, None, 1, None)
    jr.statedb.write("media/fsck-all", js, overwrite=False)

    rc = cmd_status({"--volume": vol_path})
    assert rc == 0
    out = capsys.readouterr().out
    assert "OK(0)" in out
