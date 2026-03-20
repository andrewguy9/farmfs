"""FarmFS Maintenance Daemon — core logic.

All config dataclasses, encode/decode, JobRunner, helpers, and daemon loop.
"""
from __future__ import annotations

import json
import os
import re
import signal
import socket
import subprocess
import tempfile
import threading
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterator, List, Literal, Optional, Tuple

from croniter import croniter

from farmfs.keydb import KeyDBFactory, KeyDBWindow
from farmfs.util import add_seconds, format_utc, is_past, parse_utc
from farmfs.volume import FarmFSVolume

POLL_INTERVAL_SECONDS = 60

JOB_TYPES = Literal["fsck", "fetch", "upload", "gc"]

ALWAYS_SCHEDULE_NAME = "always"
ALWAYS_CRON = "* * * * *"

# ── SmartAlert dataclass ──────────────────────────────────────────────────────


@dataclass
class SmartAlert:
    device: str              # e.g. /dev/sda
    fail_type: str           # SMARTD_FAILTYPE: Health, ErrorCount, SelfTest, …
    message: str             # SMARTD_MESSAGE: one-sentence summary
    full_message: str        # SMARTD_FULLMESSAGE: complete report
    device_info: str         # SMARTD_DEVICEINFO: brief identity line
    received_at: str         # ISO UTC timestamp when farmd recorded the alert
    prevcnt: int             # SMARTD_PREVCNT: how many prior alerts for this device


def encode_smart_alert(a: SmartAlert) -> Dict[str, Any]:
    return {
        "device": a.device,
        "fail_type": a.fail_type,
        "message": a.message,
        "full_message": a.full_message,
        "device_info": a.device_info,
        "received_at": a.received_at,
        "prevcnt": a.prevcnt,
    }


def decode_smart_alert(d: Dict[str, Any], key: str) -> SmartAlert:
    return SmartAlert(
        device=d["device"],
        fail_type=d.get("fail_type", ""),
        message=d.get("message", ""),
        full_message=d.get("full_message", ""),
        device_info=d.get("device_info", ""),
        received_at=d.get("received_at", ""),
        prevcnt=int(d.get("prevcnt", 0)),
    )


# ── Config dataclasses ────────────────────────────────────────────────────────


@dataclass
class ScheduleConfig:
    name: str
    cron: str


@dataclass
class JobConfig:
    type: JOB_TYPES
    every_seconds: int
    enabled: bool
    flags: List[str]           # fsck only
    remote: Optional[str]      # fetch / upload
    snap: Optional[str]        # fetch only
    job_id: str                # derived, stable
    schedule: str              # schedule name; "always" means always active


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

def encode_schedule_config(s: ScheduleConfig) -> Dict[str, Any]:
    return {"name": s.name, "cron": s.cron}


def decode_schedule_config(d: Dict[str, Any], key: str) -> ScheduleConfig:
    return ScheduleConfig(name=d["name"], cron=d["cron"])


def encode_job_config(j: JobConfig) -> Dict[str, Any]:
    return {
        "type": j.type,
        "every_seconds": j.every_seconds,
        "enabled": j.enabled,
        "flags": j.flags,
        "remote": j.remote,
        "snap": j.snap,
        "job_id": j.job_id,
        "schedule": j.schedule,
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
        schedule=d.get("schedule", ALWAYS_SCHEDULE_NAME),
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
    elif job_type == "gc":
        return f"{vol_name}/gc"
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


def is_schedule_active(schedule: ScheduleConfig, now: datetime) -> bool:
    """Return True if the cron schedule fires during the current minute.

    Computes the next fire time after (now truncated to minute - 1 second)
    and checks if it equals the current minute. This correctly handles
    boundaries and works with any valid cron expression.
    """
    now_min = now.replace(second=0, microsecond=0)
    start = now_min - timedelta(seconds=1)
    cron = croniter(schedule.cron, start)
    return cron.get_next(datetime) == now_min


# ── JobRunner ─────────────────────────────────────────────────────────────────

class JobRunner:
    """Thin wrapper around FarmFSVolume adding scheduler-specific KeyDB windows."""

    def __init__(self, vol: FarmFSVolume) -> None:
        self.vol: FarmFSVolume = vol
        json_db = self.vol.keydb
        self.scheduledb: KeyDBFactory[ScheduleConfig] = KeyDBFactory(
            KeyDBWindow("scheduler/schedules", json_db),
            encode_schedule_config,
            decode_schedule_config,
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
        self.smartdb: KeyDBFactory[SmartAlert] = KeyDBFactory(
            KeyDBWindow("scheduler/smart", json_db),
            encode_smart_alert,
            decode_smart_alert,
        )


# ── Scheduling helpers ────────────────────────────────────────────────────────

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
    elif job.type == "gc":
        return ["gc"]
    else:
        raise ValueError(f"Unknown job type: {job.type!r}")


def _resolve_schedule(jr: JobRunner, schedule_name: str) -> ScheduleConfig:
    """Look up a named schedule; return ALWAYS_SCHEDULE if not found or if name is 'always'."""
    if schedule_name == ALWAYS_SCHEDULE_NAME:
        return ScheduleConfig(name=ALWAYS_SCHEDULE_NAME, cron=ALWAYS_CRON)
    try:
        return jr.scheduledb.read(schedule_name)
    except FileNotFoundError:
        return ScheduleConfig(name=ALWAYS_SCHEDULE_NAME, cron=ALWAYS_CRON)


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

CANCEL_POLL_SECS = 5


def run_job(jr: JobRunner, vol_cfg: VolumeConfig, job: JobConfig, now: datetime,
            cancel: Optional[threading.Event] = None) -> None:
    """Run a single job synchronously.

    1. Write state: running=True, running_pid=proc.pid, last_run_start=now
    2. subprocess.Popen(["farmfs","--quiet"] + argv, cwd=vol_cfg.root, ...)
       Poll every CANCEL_POLL_SECS; send SIGTERM if cancel event is set.
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
        argv = ["farmfs", "--quiet"] + build_farmfs_argv(job)
        print(f"{now.astimezone().strftime('%Y-%m-%d %H:%M:%S')} Starting {job_id}")
        proc: Optional[subprocess.Popen[bytes]] = None
        with os.fdopen(log_fd, "wb") as log_fh:
            log_fd = -1  # ownership transferred to log_fh
            try:
                proc = subprocess.Popen(
                    argv,
                    cwd=vol_cfg.root,
                    stdout=log_fh,
                    stderr=subprocess.STDOUT,
                )
            except OSError as e:
                log_fh.write(f"farmd: failed to launch job: {e}\n".encode())

        if proc is not None:
            # Store real child PID so stale-running detection works correctly
            running_state = JobState(
                last_run_start=start_str,
                last_run_end=None,
                last_exit_code=None,
                next_run=None,
                running=True,
                running_pid=proc.pid,
                run_count=run_count,
                last_log_blob=None,
                live_log_path=log_path_str,
            )
            jr.statedb.write(job_id, running_state, overwrite=True)

            # Poll loop: check cancel event every CANCEL_POLL_SECS
            while True:
                try:
                    proc.wait(timeout=CANCEL_POLL_SECS)
                    break  # exited naturally
                except subprocess.TimeoutExpired:
                    if cancel is not None and cancel.is_set():
                        proc.terminate()   # SIGTERM
                        proc.wait()        # wait for child to honour it
                        break

            exit_code: int = proc.returncode
        else:
            exit_code = -1

        end_now = datetime.now(timezone.utc)
        end_str = format_utc(end_now)
        print(f"{end_now.astimezone().strftime('%Y-%m-%d %H:%M:%S')} Finished {job_id} exit={exit_code}")
        next_run_str = compute_next_run(end_now, job.every_seconds)

        # Import log file into blobstore via hardlink (zero-copy on same fs)
        log_blob: Optional[str] = None
        if log_path.stat().st_size > 0:
            csum = log_path.checksum()
            with jr.vol.bs.session() as sess:
                sess.import_via_link(log_path, csum)
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


# ── Daemon socket ─────────────────────────────────────────────────────────────

def socket_path(jr: JobRunner) -> str:
    """Absolute path to the Unix domain socket for this depot."""
    return str(jr.vol.root.join(".farmfs").join("locks").join("farmd.sock"))


def check_daemon(jr: JobRunner) -> Tuple[str, Optional[int]]:
    """Probe the daemon socket.

    Returns:
        ("running", pid)       — socket exists and accepted a connection
        ("crashed", None)      — socket file exists but connection refused
        ("stopped", None)      — no socket file
    """
    path = socket_path(jr)
    if not os.path.exists(path):
        return ("stopped", None)
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        sock.settimeout(2)
        sock.connect(path)
        data = b""
        while True:
            chunk = sock.recv(256)
            if not chunk:
                break
            data += chunk
        msg = json.loads(data.decode())
        return ("running", int(msg["pid"]))
    except (ConnectionRefusedError, FileNotFoundError):
        return ("crashed", None)
    except Exception:
        return ("crashed", None)
    finally:
        sock.close()


def _serve_socket(sock: socket.socket, shutdown: threading.Event) -> None:
    """Background thread: accept connections and reply with daemon PID."""
    sock.setblocking(False)
    while not shutdown.is_set():
        try:
            conn, _ = sock.accept()
        except BlockingIOError:
            shutdown.wait(timeout=1)
            continue
        try:
            conn.sendall(json.dumps({"pid": os.getpid()}).encode())
        finally:
            conn.close()


# ── Daemon loop ───────────────────────────────────────────────────────────────

def daemon_loop(jr: JobRunner) -> None:
    """Poll every POLL_INTERVAL_SECONDS. On each tick:
      - re-read config from KeyDB (picks up changes within one cycle)
      - clear stale running markers
      - find one due job whose schedule is active, run it in a thread
        (cancels the job if the schedule window closes while it is running)
    Handles SIGTERM/SIGINT cleanly (signals running job, waits for it to finish).
    Binds a Unix domain socket so farmd status can detect the daemon.
    """
    shutdown = threading.Event()
    signal.signal(signal.SIGTERM, lambda *_: shutdown.set())
    signal.signal(signal.SIGINT, lambda *_: shutdown.set())

    sock_path = socket_path(jr)
    # Check for an existing socket — refuse to start if another daemon is live
    state, existing_pid = check_daemon(jr)
    if state == "running":
        raise RuntimeError(f"A farmd daemon is already running on this depot (pid {existing_pid})")
    if state == "crashed":
        # Stale socket from a previous crash — safe to remove
        os.unlink(sock_path)

    srv = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    srv.bind(sock_path)
    srv.listen(4)
    sock_thread = threading.Thread(target=_serve_socket, args=(srv, shutdown), daemon=True)
    sock_thread.start()

    try:
        while not shutdown.is_set():
            now = datetime.now(timezone.utc)

            clear_stale_running(jr)

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
                    if not is_job_due(js, now):
                        continue
                    schedule = _resolve_schedule(jr, job.schedule)
                    if not is_schedule_active(schedule, now):
                        continue

                    cancel = threading.Event()
                    t = threading.Thread(
                        target=run_job,
                        args=(jr, vol_cfg, job, now, cancel),
                        daemon=True,
                    )
                    t.start()
                    while t.is_alive():
                        t.join(timeout=POLL_INTERVAL_SECONDS)
                        if t.is_alive():
                            if shutdown.is_set():
                                cancel.set()
                            else:
                                now2 = datetime.now(timezone.utc)
                                sched2 = _resolve_schedule(jr, job.schedule)
                                if not is_schedule_active(sched2, now2):
                                    ts = now2.astimezone().strftime('%Y-%m-%d %H:%M:%S')
                                    print(f"{ts} Cancelling {job.job_id} (schedule window closed)")
                                    cancel.set()

                    ran_job = True
                    break

            shutdown.wait(timeout=POLL_INTERVAL_SECONDS)
    finally:
        srv.close()
        try:
            os.unlink(sock_path)
        except FileNotFoundError:
            pass


# ── Log retrieval ─────────────────────────────────────────────────────────────

def read_log_blob(jr: JobRunner, log_blob: str) -> Iterator[bytes]:
    """Read a log blob from the JobRunner's blobstore, yielding chunks."""
    with jr.vol.bs.read_handle(log_blob) as fh:
        while True:
            chunk = fh.read(16 * 1024)
            if not chunk:
                break
            yield chunk
