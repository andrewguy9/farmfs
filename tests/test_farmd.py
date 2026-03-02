"""Unit tests for farmfs/farmd.py — pure functions only (no subprocess, no I/O)."""
from datetime import datetime, timezone
from typing import Any, Dict

import pytest

import os
import socket
import tempfile
import threading

from farmfs.farmd import (
    ALWAYS_CRON,
    ALWAYS_SCHEDULE_NAME,
    JobConfig,
    JobState,
    ScheduleConfig,
    VolumeConfig,
    _serve_socket,
    build_farmfs_argv,
    check_daemon,
    compute_next_run,
    decode_job_state,
    decode_schedule_config,
    decode_volume_config,
    encode_job_state,
    encode_schedule_config,
    encode_volume_config,
    is_job_due,
    is_schedule_active,
    make_job_id,
    parse_every,
)
from farmfs.util import parse_utc


# ── parse_every ───────────────────────────────────────────────────────────────

def test_parse_every_hours() -> None:
    assert parse_every("1h") == 3600
    assert parse_every("6h") == 21600
    assert parse_every("24h") == 86400


def test_parse_every_days() -> None:
    assert parse_every("1d") == 86400
    assert parse_every("7d") == 604800


def test_parse_every_weeks() -> None:
    assert parse_every("1w") == 604800
    assert parse_every("2w") == 1209600


def test_parse_every_months() -> None:
    assert parse_every("1m") == 2592000


def test_parse_every_invalid() -> None:
    with pytest.raises(ValueError):
        parse_every("bad")
    with pytest.raises(ValueError):
        parse_every("1x")
    with pytest.raises(ValueError):
        parse_every("1")
    with pytest.raises(ValueError):
        parse_every("")


# ── make_job_id ───────────────────────────────────────────────────────────────

def test_make_job_id_fsck_no_flags() -> None:
    raw: Dict[str, Any] = {"type": "fsck", "flags": []}
    assert make_job_id("media", raw) == "media/fsck-all"


def test_make_job_id_fsck_no_flags_none() -> None:
    raw: Dict[str, Any] = {"type": "fsck", "flags": None}
    assert make_job_id("media", raw) == "media/fsck-all"


def test_make_job_id_fsck_flags_sorted() -> None:
    raw: Dict[str, Any] = {"type": "fsck", "flags": ["--checksums", "--missing"]}
    assert make_job_id("media", raw) == "media/fsck-checksums_missing"


def test_make_job_id_fsck_flags_single() -> None:
    raw: Dict[str, Any] = {"type": "fsck", "flags": ["--missing"]}
    assert make_job_id("photos", raw) == "photos/fsck-missing"


def test_make_job_id_fetch_remote() -> None:
    raw: Dict[str, Any] = {"type": "fetch", "remote": "backup", "snap": None}
    assert make_job_id("media", raw) == "media/fetch-backup"


def test_make_job_id_fetch_remote_with_snap() -> None:
    raw: Dict[str, Any] = {"type": "fetch", "remote": "backup", "snap": "daily"}
    assert make_job_id("media", raw) == "media/fetch-backup+daily"


def test_make_job_id_fetch_no_remote() -> None:
    raw: Dict[str, Any] = {"type": "fetch", "remote": None, "snap": None}
    assert make_job_id("media", raw) == "media/fetch-all"


def test_make_job_id_upload_remote() -> None:
    raw: Dict[str, Any] = {"type": "upload", "remote": "backup"}
    assert make_job_id("media", raw) == "media/upload-backup"


def test_make_job_id_upload_no_remote() -> None:
    raw: Dict[str, Any] = {"type": "upload", "remote": None}
    assert make_job_id("media", raw) == "media/upload-all"


def test_make_job_id_unknown_type() -> None:
    raw: Dict[str, Any] = {"type": "unknown"}
    with pytest.raises(ValueError):
        make_job_id("media", raw)


# ── is_schedule_active ────────────────────────────────────────────────────────

def test_is_schedule_active_always() -> None:
    sc = ScheduleConfig(name=ALWAYS_SCHEDULE_NAME, cron=ALWAYS_CRON)
    for h in range(24):
        now = datetime(2026, 2, 28, h, 0, 0, tzinfo=timezone.utc)
        assert is_schedule_active(sc, now) is True


def test_is_schedule_active_hourly_at_boundary() -> None:
    # "0 * * * *" fires at the top of every hour
    sc = ScheduleConfig(name="hourly", cron="0 * * * *")
    # Exactly at the top of the hour → active
    now_on = datetime(2026, 2, 28, 3, 0, 0, tzinfo=timezone.utc)
    assert is_schedule_active(sc, now_on) is True
    # 1 minute past → not active
    now_off = datetime(2026, 2, 28, 3, 1, 0, tzinfo=timezone.utc)
    assert is_schedule_active(sc, now_off) is False


def test_is_schedule_active_nightly() -> None:
    # "0 22 * * *" fires daily at 22:00 UTC
    sc = ScheduleConfig(name="nightly", cron="0 22 * * *")
    now_on = datetime(2026, 2, 28, 22, 0, 0, tzinfo=timezone.utc)
    assert is_schedule_active(sc, now_on) is True
    now_off = datetime(2026, 2, 28, 10, 0, 0, tzinfo=timezone.utc)
    assert is_schedule_active(sc, now_off) is False


# ── is_job_due ────────────────────────────────────────────────────────────────

def test_is_job_due_no_next_run() -> None:
    js = JobState(None, None, None, None, False, None, 0, None, None)
    now = datetime(2026, 2, 28, 0, 0, 0, tzinfo=timezone.utc)
    assert is_job_due(js, now) is True


def test_is_job_due_past() -> None:
    past = "2026-02-27T00:00:00+00:00"
    js = JobState(None, None, None, past, False, None, 1, None, None)
    now = datetime(2026, 2, 28, 0, 0, 0, tzinfo=timezone.utc)
    assert is_job_due(js, now) is True


def test_is_job_due_future() -> None:
    future = "2026-03-01T00:00:00+00:00"
    js = JobState(None, None, None, future, False, None, 1, None, None)
    now = datetime(2026, 2, 28, 0, 0, 0, tzinfo=timezone.utc)
    assert is_job_due(js, now) is False


def test_is_job_due_exact_now() -> None:
    ts = "2026-02-28T00:00:00+00:00"
    js = JobState(None, None, None, ts, False, None, 1, None, None)
    now = datetime(2026, 2, 28, 0, 0, 0, tzinfo=timezone.utc)
    assert is_job_due(js, now) is True


# ── compute_next_run ──────────────────────────────────────────────────────────

def test_compute_next_run() -> None:
    last = datetime(2026, 2, 28, 0, 0, 0, tzinfo=timezone.utc)
    result = compute_next_run(last, 3600)
    parsed = parse_utc(result)
    assert parsed == datetime(2026, 2, 28, 1, 0, 0, tzinfo=timezone.utc)


# ── build_farmfs_argv ─────────────────────────────────────────────────────────

def _make_job(type: str, flags: list, remote: str | None = None, snap: str | None = None, schedule: str = ALWAYS_SCHEDULE_NAME) -> JobConfig:
    return JobConfig(
        type=type,  # type: ignore[arg-type]
        every_seconds=3600,
        enabled=True,
        flags=flags,
        remote=remote,
        snap=snap,
        job_id="test/job",
        schedule=schedule,
    )


def test_build_farmfs_argv_fsck_no_flags() -> None:
    job = _make_job("fsck", [])
    assert build_farmfs_argv(job) == ["fsck"]


def test_build_farmfs_argv_fsck_with_flags() -> None:
    job = _make_job("fsck", ["--missing", "--checksums"])
    assert build_farmfs_argv(job) == ["fsck", "--missing", "--checksums"]


def test_build_farmfs_argv_fetch_with_remote() -> None:
    job = _make_job("fetch", [], remote="backup")
    assert build_farmfs_argv(job) == ["fetch", "backup"]


def test_build_farmfs_argv_fetch_no_remote() -> None:
    job = _make_job("fetch", [])
    assert build_farmfs_argv(job) == ["fetch"]


def test_build_farmfs_argv_fetch_with_snap() -> None:
    job = _make_job("fetch", [], remote="backup", snap="daily")
    assert build_farmfs_argv(job) == ["fetch", "backup", "daily"]


def test_build_farmfs_argv_upload_with_remote() -> None:
    job = _make_job("upload", [], remote="backup")
    assert build_farmfs_argv(job) == ["upload", "backup"]


def test_build_farmfs_argv_upload_no_remote() -> None:
    job = _make_job("upload", [])
    assert build_farmfs_argv(job) == ["upload"]


# ── Encode / decode round-trips ───────────────────────────────────────────────

def test_schedule_config_roundtrip() -> None:
    sc = ScheduleConfig(name="nightly", cron="0 22 * * *")
    encoded = encode_schedule_config(sc)
    decoded = decode_schedule_config(encoded, "nightly")
    assert decoded == sc


def test_job_state_roundtrip() -> None:
    js = JobState(
        last_run_start="2026-02-28T00:00:00+00:00",
        last_run_end="2026-02-28T00:01:00+00:00",
        last_exit_code=0,
        next_run="2026-02-29T00:00:00+00:00",
        running=False,
        running_pid=None,
        run_count=5,
        last_log_blob="abc123",
        live_log_path=None,
    )
    encoded = encode_job_state(js)
    decoded = decode_job_state(encoded, "media/fsck-all")
    assert decoded == js


def test_job_state_roundtrip_defaults() -> None:
    js = JobState(None, None, None, None, False, None, 0, None, None)
    encoded = encode_job_state(js)
    decoded = decode_job_state(encoded, "media/fsck-all")
    assert decoded == js


def test_volume_config_roundtrip() -> None:
    job = JobConfig(
        type="fsck",
        every_seconds=86400,
        enabled=True,
        flags=["--missing"],
        remote=None,
        snap=None,
        job_id="media/fsck-missing",
        schedule=ALWAYS_SCHEDULE_NAME,
    )
    vc = VolumeConfig(name="media", root="/Volumes/Media/farmfs", jobs=[job])
    encoded = encode_volume_config(vc)
    decoded = decode_volume_config(encoded, "media")
    assert decoded == vc


def test_volume_config_roundtrip_no_jobs() -> None:
    vc = VolumeConfig(name="photos", root="/Volumes/Photos", jobs=[])
    encoded = encode_volume_config(vc)
    decoded = decode_volume_config(encoded, "photos")
    assert decoded == vc


# ── check_daemon / _serve_socket ──────────────────────────────────────────────

def _start_test_server(sock_path: str) -> threading.Event:
    """Bind a socket at sock_path, start _serve_socket in a thread, return shutdown event."""
    srv = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    srv.bind(sock_path)
    srv.listen(4)
    shutdown = threading.Event()
    t = threading.Thread(target=_serve_socket, args=(srv, shutdown), daemon=True)
    t.start()
    return shutdown


def test_check_daemon_stopped(tmp_path: Any) -> None:
    """No socket file → stopped."""
    # Use a MagicMock-like stand-in for JobRunner with just vol.root
    class FakeJR:
        class vol:
            class root:
                @staticmethod
                def join(name: str) -> Any:
                    class P:
                        def __str__(self) -> str:
                            return str(tmp_path / name)
                    return P()
    state, pid = check_daemon(FakeJR())  # type: ignore[arg-type]
    assert state == "stopped"
    assert pid is None


def test_check_daemon_running() -> None:
    """Live server → running with correct PID."""
    # Use tempfile.mkdtemp() — pytest's tmp_path can exceed AF_UNIX's 104-char limit on macOS
    tmpdir = tempfile.mkdtemp()
    sock_path = os.path.join(tmpdir, ".farmd.sock")

    class FakeJR:
        class vol:
            class root:
                @staticmethod
                def join(name: str) -> Any:
                    class P:
                        def __str__(self) -> str:
                            return sock_path
                    return P()

    shutdown = _start_test_server(sock_path)
    try:
        state, pid = check_daemon(FakeJR())  # type: ignore[arg-type]
        assert state == "running"
        assert pid == os.getpid()
    finally:
        shutdown.set()


def test_check_daemon_crashed(tmp_path: Any) -> None:
    """Socket file exists but no listener → crashed."""
    sock_path = str(tmp_path / ".farmd.sock")
    # Create a socket file with nothing listening
    open(sock_path, "w").close()

    class FakeJR:
        class vol:
            class root:
                @staticmethod
                def join(name: str) -> Any:
                    class P:
                        def __str__(self) -> str:
                            return sock_path
                    return P()

    state, pid = check_daemon(FakeJR())  # type: ignore[arg-type]
    assert state == "crashed"
    assert pid is None
