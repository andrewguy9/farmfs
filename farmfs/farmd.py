"""FarmFS Maintenance Daemon — core logic.

All config dataclasses, encode/decode, JobRunner, helpers, and daemon loop.
"""
from __future__ import annotations

import os
import re
import signal
import subprocess
import tempfile
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Iterator, List, Literal, Optional

from farmfs.keydb import KeyDBFactory, KeyDBWindow
from farmfs.util import add_seconds, format_utc, is_past, parse_utc
from farmfs.volume import FarmFSVolume

POLL_INTERVAL_SECONDS = 60

JOB_TYPES = Literal["fsck", "fetch", "upload"]

# ── Config dataclasses ────────────────────────────────────────────────────────


@dataclass
class DaemonConfig:
    night_start: int   # 0–23
    night_end: int     # 0–23


@dataclass
class JobConfig:
    type: JOB_TYPES
    every_seconds: int
    enabled: bool
    flags: List[str]           # fsck only
    remote: Optional[str]      # fetch / upload
    snap: Optional[str]        # fetch only
    job_id: str                # derived, stable


@dataclass
class VolumeConfig:
    name: str
    root: str                  # absolute path; Path() created at use-site
    jobs: List[JobConfig]


@dataclass
class JobState:
    last_run_start: Optional[str]   # ISO UTC
    last_run_end: Optional[str]
    last_exit_code: Optional[int]
    next_run: Optional[str]
    running: bool
    running_pid: Optional[int]
    run_count: int
    last_log_blob: Optional[str]    # checksum in JobRunner's vol blobstore
    live_log_path: Optional[str]    # absolute path to in-progress log file


# ── Encode / decode ───────────────────────────────────────────────────────────

def encode_daemon_config(c: DaemonConfig) -> Dict[str, Any]:
    return {"night_start": c.night_start, "night_end": c.night_end}


def decode_daemon_config(d: Dict[str, Any], key: str) -> DaemonConfig:
    return DaemonConfig(night_start=int(d["night_start"]), night_end=int(d["night_end"]))


def encode_job_config(j: JobConfig) -> Dict[str, Any]:
    return {
        "type": j.type,
        "every_seconds": j.every_seconds,
        "enabled": j.enabled,
        "flags": j.flags,
        "remote": j.remote,
        "snap": j.snap,
        "job_id": j.job_id,
    }


def decode_job_config(d: Dict[str, Any]) -> JobConfig:
    return JobConfig(
        type=d["type"],
        every_seconds=int(d["every_seconds"]),
        enabled=bool(d["enabled"]),
        flags=list(d.get("flags") or []),
        remote=d.get("remote"),
        snap=d.get("snap"),
        job_id=d["job_id"],
    )


def encode_volume_config(v: VolumeConfig) -> Dict[str, Any]:
    return {
        "name": v.name,
        "root": v.root,
        "jobs": [encode_job_config(j) for j in v.jobs],
    }


def decode_volume_config(d: Dict[str, Any], key: str) -> VolumeConfig:
    return VolumeConfig(
        name=d["name"],
        root=d["root"],
        jobs=[decode_job_config(j) for j in d.get("jobs", [])],
    )


def encode_job_state(s: JobState) -> Dict[str, Any]:
    return {
        "last_run_start": s.last_run_start,
        "last_run_end": s.last_run_end,
        "last_exit_code": s.last_exit_code,
        "next_run": s.next_run,
        "running": s.running,
        "running_pid": s.running_pid,
        "run_count": s.run_count,
        "last_log_blob": s.last_log_blob,
        "live_log_path": s.live_log_path,
    }


def decode_job_state(d: Dict[str, Any], key: str) -> JobState:
    return JobState(
        last_run_start=d.get("last_run_start"),
        last_run_end=d.get("last_run_end"),
        last_exit_code=d.get("last_exit_code"),
        next_run=d.get("next_run"),
        running=bool(d.get("running", False)),
        running_pid=d.get("running_pid"),
        run_count=int(d.get("run_count", 0)),
        last_log_blob=d.get("last_log_blob"),
        live_log_path=d.get("live_log_path"),
    )


# ── Helpers ───────────────────────────────────────────────────────────────────

EVERY_RE = re.compile(r'^(\d+)(h|d|w|m)$')

_EVERY_MULTIPLIERS = {'h': 3600, 'd': 86400, 'w': 604800, 'm': 2592000}


def parse_every(s: str) -> int:
    """'1h'→3600  '1d'→86400  '1w'→604800  '1m'→2592000. Raises ValueError."""
    m = EVERY_RE.match(s)
    if not m:
        raise ValueError(f"Invalid every value: {s!r}. Expected format like '1h', '2d', '1w', '1m'.")
    n, unit = int(m.group(1)), m.group(2)
    return n * _EVERY_MULTIPLIERS[unit]


def make_job_id(vol_name: str, raw_job: Dict[str, Any]) -> str:
    """Derive stable human-readable job ID from volume name + raw job dict."""
    job_type = raw_job["type"]
    if job_type == "fsck":
        flags: List[str] = list(raw_job.get("flags") or [])
        if flags:
            stripped = sorted(f.lstrip("-") for f in flags)
            discriminator = "_".join(stripped)
        else:
            discriminator = "all"
        return f"{vol_name}/fsck-{discriminator}"
    elif job_type == "fetch":
        remote = raw_job.get("remote") or "all"
        snap = raw_job.get("snap")
        if snap:
            return f"{vol_name}/fetch-{remote}+{snap}"
        return f"{vol_name}/fetch-{remote}"
    elif job_type == "upload":
        remote = raw_job.get("remote") or "all"
        return f"{vol_name}/upload-{remote}"
    else:
        raise ValueError(f"Unknown job type: {job_type!r}")


def is_job_due(js: JobState, now: datetime) -> bool:
    """True if next_run is None or is_past(parse_utc(js.next_run), now)."""
    if js.next_run is None:
        return True
    return is_past(parse_utc(js.next_run), now)


def compute_next_run(last: datetime, every_seconds: int) -> str:
    """Return format_utc(add_seconds(last, every_seconds))."""
    return format_utc(add_seconds(last, every_seconds))


def is_pid_alive(pid: int) -> bool:
    """os.kill(pid, 0) — True if process is alive."""
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


# ── JobRunner ─────────────────────────────────────────────────────────────────

class JobRunner:
    """Thin wrapper around FarmFSVolume adding scheduler-specific KeyDB windows."""

    def __init__(self, vol: FarmFSVolume) -> None:
        self.vol: FarmFSVolume = vol
        json_db = self.vol.keydb
        self.configdb: KeyDBFactory[DaemonConfig] = KeyDBFactory(
            KeyDBWindow("scheduler/config", json_db),
            encode_daemon_config,
            decode_daemon_config,
        )
        self.volumedb: KeyDBFactory[VolumeConfig] = KeyDBFactory(
            KeyDBWindow("scheduler/volumes", json_db),
            encode_volume_config,
            decode_volume_config,
        )
        self.statedb: KeyDBFactory[JobState] = KeyDBFactory(
            KeyDBWindow("scheduler/state", json_db),
            encode_job_state,
            decode_job_state,
        )


# ── Scheduling helpers ────────────────────────────────────────────────────────

def is_night_window(config: DaemonConfig, local_hour: int) -> bool:
    """Return True if local_hour falls in the configured night window."""
    s, e = config.night_start, config.night_end
    if s < e:
        return s <= local_hour < e
    # Wrap-around: e.g. night_start=22, night_end=6
    return local_hour >= s or local_hour < e


def build_farmfs_argv(job: JobConfig) -> List[str]:
    """Return argv for subprocess call (without 'farmfs' prefix).

    fsck   → ['fsck', '--missing', ...]
    fetch  → ['fetch', 'backup']  or  ['fetch']
    upload → ['upload', 'backup']
    """
    if job.type == "fsck":
        argv: List[str] = ["fsck"]
        argv.extend(job.flags)
        return argv
    elif job.type == "fetch":
        if job.remote:
            argv = ["fetch", job.remote]
        else:
            argv = ["fetch"]
        if job.snap:
            argv.append(job.snap)
        return argv
    elif job.type == "upload":
        if job.remote:
            return ["upload", job.remote]
        return ["upload"]
    else:
        raise ValueError(f"Unknown job type: {job.type!r}")


def clear_stale_running(jr: JobRunner) -> None:
    """For each job state with running=True, check PID; clear if dead."""
    for job_id in jr.statedb.list():
        try:
            js = jr.statedb.read(job_id)
        except FileNotFoundError:
            continue
        if js.running:
            pid = js.running_pid
            if pid is None or not is_pid_alive(pid):
                js.running = False
                js.running_pid = None
                jr.statedb.write(job_id, js, overwrite=True)


# ── Job runner ────────────────────────────────────────────────────────────────

def run_job(jr: JobRunner, vol_cfg: VolumeConfig, job: JobConfig, now: datetime) -> None:
    """Run a single job synchronously.

    1. Write state: running=True, running_pid=os.getpid(), last_run_start=now
    2. subprocess.run(["farmfs","--quiet"] + argv, cwd=vol_cfg.root, stdout=PIPE, stderr=STDOUT)
    3. Import captured output as a blob into jr.vol.bs → get checksum
    4. Write state: running=False, pid=None, exit_code, next_run, last_log_blob, run_count+1
    """
    job_id = job.job_id
    try:
        prev_state = jr.statedb.read(job_id)
        run_count = prev_state.run_count
    except FileNotFoundError:
        run_count = 0

    from farmfs.fs import Path as FsPath

    start_str = format_utc(now)

    # Write subprocess output directly to a temp file so it can be tailed live
    log_fd, log_path_str = tempfile.mkstemp(prefix="farmd-", suffix=".log", dir=str(jr.vol.bs.tmp_dir))
    log_path = FsPath(log_path_str)
    try:
        running_state = JobState(
            last_run_start=start_str,
            last_run_end=None,
            last_exit_code=None,
            next_run=None,
            running=True,
            running_pid=os.getpid(),
            run_count=run_count,
            last_log_blob=None,
            live_log_path=log_path_str,
        )
        jr.statedb.write(job_id, running_state, overwrite=True)

        argv = ["farmfs", "--quiet"] + build_farmfs_argv(job)
        print(f"{now.astimezone().strftime('%Y-%m-%d %H:%M:%S')} Starting {job_id}")
        with os.fdopen(log_fd, "wb") as log_fh:
            log_fd = -1  # ownership transferred to log_fh
            result = subprocess.run(
                argv,
                cwd=vol_cfg.root,
                stdout=log_fh,
                stderr=subprocess.STDOUT,
            )
        exit_code: int = result.returncode

        end_now = datetime.now(timezone.utc)
        end_str = format_utc(end_now)
        print(f"{end_now.astimezone().strftime('%Y-%m-%d %H:%M:%S')} Finished {job_id} exit={exit_code}")
        next_run_str = compute_next_run(end_now, job.every_seconds)

        # Import log file into blobstore via hardlink (zero-copy on same fs)
        log_blob: Optional[str] = None
        if log_path.stat().st_size > 0:
            csum = log_path.checksum()
            jr.vol.bs.import_via_link(log_path, csum)
            log_blob = csum

        done_state = JobState(
            last_run_start=start_str,
            last_run_end=end_str,
            last_exit_code=exit_code,
            next_run=next_run_str,
            running=False,
            running_pid=None,
            run_count=run_count + 1,
            last_log_blob=log_blob,
            live_log_path=None,
        )
        jr.statedb.write(job_id, done_state, overwrite=True)
    finally:
        if log_fd != -1:
            os.close(log_fd)  # close if subprocess never ran
        if log_path.exists():
            log_path.unlink()


# ── Daemon loop ───────────────────────────────────────────────────────────────

def daemon_loop(jr: JobRunner) -> None:
    """Poll every POLL_INTERVAL_SECONDS. On each tick:
      - re-read config from KeyDB (picks up changes within one cycle)
      - clear stale running markers
      - if in night window: find one due job, run it synchronously
    Handles SIGTERM/SIGINT cleanly (waits for current job to finish).
    """
    shutdown = threading.Event()
    signal.signal(signal.SIGTERM, lambda *_: shutdown.set())
    signal.signal(signal.SIGINT, lambda *_: shutdown.set())

    last_in_window: Optional[bool] = None

    while not shutdown.is_set():
        now = datetime.now(timezone.utc)
        try:
            config = jr.configdb.read("config")
        except FileNotFoundError:
            # No config written yet; use sensible defaults (always active)
            config = DaemonConfig(night_start=0, night_end=0)

        clear_stale_running(jr)

        local_hour = now.astimezone().hour
        in_window = is_night_window(config, local_hour)
        if in_window != last_in_window:
            if in_window:
                print(f"{now.astimezone().strftime('%Y-%m-%d %H:%M:%S')} Night mode")
            else:
                print(f"{now.astimezone().strftime('%Y-%m-%d %H:%M:%S')} Day mode")
            last_in_window = in_window

        if in_window:
            volume_names = jr.volumedb.list()
            ran_job = False
            for vol_name in volume_names:
                if ran_job:
                    break
                try:
                    vol_cfg = jr.volumedb.read(vol_name)
                except FileNotFoundError:
                    continue
                for job in vol_cfg.jobs:
                    if not job.enabled:
                        continue
                    try:
                        js = jr.statedb.read(job.job_id)
                    except FileNotFoundError:
                        js = JobState(None, None, None, None, False, None, 0, None, None)
                    if js.running:
                        continue
                    if is_job_due(js, now):
                        run_job(jr, vol_cfg, job, now)
                        ran_job = True
                        break

        shutdown.wait(timeout=POLL_INTERVAL_SECONDS)


# ── Log retrieval ─────────────────────────────────────────────────────────────

def read_log_blob(jr: JobRunner, log_blob: str) -> Iterator[bytes]:
    """Read a log blob from the JobRunner's blobstore, yielding chunks."""
    with jr.vol.bs.read_handle(log_blob) as fh:
        while True:
            chunk = fh.read(16 * 1024)
            if not chunk:
                break
            yield chunk
