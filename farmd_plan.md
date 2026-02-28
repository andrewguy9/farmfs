# FarmFS Maintenance Daemon — Implementation Plan

## Context

FarmFS volumes need periodic maintenance: integrity checks (`fsck`), blob fetches, and uploads. Across many volumes this becomes error-prone to run manually. We need a daemon that runs these jobs automatically on a schedule, restricted to night hours to avoid contention, with a status CLI for visibility.

The **JobRunner volume is the authority** — systemd/launchd points `farmd start --volume /path/to/vol` at it, and all config, state, and logs live inside that volume's KeyDB/blobstore. No separate config file. The volume can be inspected and backed up with standard farmfs tools.

**Decisions:**
- Config + state: in a dedicated farmfs volume (via KeyDB, JSON)
- Execution: long-running daemon, fully sequential (one job at a time)
- Status: terminal table via `farmd status --volume <path>`
- Location: inside farmfs repo, new `farmd` entry point
- Volume path: user-specified (`--volume` flag); created with `farmd mkfs --volume <path>`

---

## Architecture Overview

```
systemd/launchd unit
  └── farmd start --volume /srv/farmd
                              │
              JobRunner Volume (.farmfs/ inside)
              ┌──────────────────────────────────────────────┐
              │  KeyDB keys/                                  │
              │    scheduler/config          → DaemonConfig  │
              │    scheduler/volumes/<name>  → VolumeConfig  │
              │    scheduler/state/<job_id>  → JobState      │
              │                                               │
              │  Blobstore userdata/                          │
              │    (log blobs — each run's output)            │
              └──────────────────────────────────────────────┘
                         │
              subprocess farmfs ... cwd=vol_root  → manages → /mnt/media, /mnt/photos, ...
```

---

## Files to Create / Modify

| File | Action | Purpose |
|------|--------|---------|
| `farmfs/farmd.py` | Create | All dataclasses, encode/decode, `JobRunner`, daemon loop, job runner |
| `farmfs/farmd_ui.py` | Create | docopt CLI: mkfs, start, status, run-now, config/volume/job subcommands. **Kept separate from `ui.py`** following the `farmapi`/`api.py` precedent — `farmd` manages a JobRunner volume, not a farmfs working tree, so it shares no helpers with `farmfs_ui`. `ui.py` is already 1221 lines. |
| `tests/test_farmd.py` | Create | Unit tests for config/state encode-decode, `parse_every`, `make_job_id`, `is_night_window`, `build_farmfs_argv`, `is_job_due` |
| `pyproject.toml` | Modify | Add `farmd` entry point |

---

## KeyDB Schema (inside the JobRunner volume)

All keys via `JsonKeyDB` + `KeyDBWindow` namespacing.

### `scheduler/config` — global daemon settings

```json
{
  "night_start": 22,
  "night_end": 6
}
```

### `scheduler/volumes/<name>` — per-volume job list

```json
{
  "root": "/Volumes/Media/farmfs",
  "jobs": [
    { "type": "fsck",   "flags": ["--missing", "--checksums"], "every": "1d",  "enabled": true },
    { "type": "fetch",  "remote": "backup", "snap": null,      "every": "6h",  "enabled": true },
    { "type": "upload", "remote": "backup",                    "every": "12h", "enabled": true }
  ]
}
```

### `scheduler/state/<job_id>` — per-job runtime state

```json
{
  "last_run_start": "2026-02-27T23:01:05+00:00",
  "last_run_end":   "2026-02-27T23:04:11+00:00",
  "last_exit_code": 0,
  "next_run":       "2026-02-28T23:01:05+00:00",
  "running":        false,
  "running_pid":    null,
  "run_count":      14,
  "last_log_blob":  "a1b2c3d4e5f6..."
}
```

`last_log_blob` is a checksum in the JobRunner volume's blobstore. Log blobs accumulate; `farmfs gc` on the JobRunner volume removes ones no longer referenced.

### Job ID derivation (stable, human-readable)

Format: `<volume_name>/<type>-<discriminator>`

- `fsck`: discriminator = sorted flags joined by `_` with `--` stripped, or `all` if no flags
  - e.g. `media/fsck-checksums_missing`, `photos/fsck-all`
- `fetch`: remote name or `all` + optional `+<snap>`
  - e.g. `media/fetch-backup`
- `upload`: remote name — e.g. `media/upload-backup`

---

## Time utilities in `farmfs/util.py`

Pure time functions, added to the existing `util.py` and tested in `tests/test_util.py` (alongside existing util tests). No `datetime.now()` calls inside — callers pass `now` in.

```python
# farmfs/util.py additions

from datetime import datetime, timezone, timedelta

ISO_FORMAT = "%Y-%m-%dT%H:%M:%S%z"

def parse_utc(s: str) -> datetime:
    """Parse an ISO 8601 UTC string → aware datetime (UTC).
    Accepts strings produced by format_utc()."""
    return datetime.fromisoformat(s)

def format_utc(dt: datetime) -> str:
    """Format an aware datetime → ISO 8601 string with UTC offset (+00:00).
    Suitable for JSON storage."""
    return dt.astimezone(timezone.utc).isoformat()

def add_seconds(dt: datetime, seconds: int) -> datetime:
    """Return dt + seconds as a new datetime (same timezone)."""
    return dt + timedelta(seconds=seconds)

def is_past(dt: datetime, now: datetime) -> bool:
    """Return True if dt <= now (i.e. the deadline has passed)."""
    return dt <= now
```

These replace the `now_iso: str` passing pattern in `farmd.py` — callers get `now = datetime.now(timezone.utc)` once at the top of each tick and pass it through. Encode/decode uses `format_utc`/`parse_utc`.

**Tests to add in `tests/test_util.py`:**

```python
def test_parse_format_roundtrip():
    now = datetime(2026, 2, 28, 3, 0, 0, tzinfo=timezone.utc)
    assert parse_utc(format_utc(now)) == now

def test_add_seconds():
    t = datetime(2026, 2, 28, 0, 0, 0, tzinfo=timezone.utc)
    assert add_seconds(t, 3600) == datetime(2026, 2, 28, 1, 0, 0, tzinfo=timezone.utc)

def test_is_past_true():
    past = datetime(2026, 2, 27, 0, 0, 0, tzinfo=timezone.utc)
    now  = datetime(2026, 2, 28, 0, 0, 0, tzinfo=timezone.utc)
    assert is_past(past, now) is True

def test_is_past_false():
    future = datetime(2026, 3, 1, 0, 0, 0, tzinfo=timezone.utc)
    now    = datetime(2026, 2, 28, 0, 0, 0, tzinfo=timezone.utc)
    assert is_past(future, now) is False

def test_is_past_equal():
    t = datetime(2026, 2, 28, 0, 0, 0, tzinfo=timezone.utc)
    assert is_past(t, t) is True   # equal counts as past (deadline reached)
```

---

## Config change detection

The daemon re-reads config from KeyDB at the **start of every poll tick** (`jr.configdb.read("config")`, `jr.volumedb.list()` etc.). Changes take effect within one 60-second cycle — no daemon restart, no signal, no inotify required. Add a volume or job, wait up to 60 seconds, it's live.

## Night window crossing policy

The night window governs when jobs **start**. A job that is already running when the window closes is **allowed to finish**. Rationale:

- farmfs operations are safe to interrupt at the blobstore level (tmp-file → atomic rename), but a long fetch or fsck represents significant work that would be wasted if killed mid-run.
- The purpose of the window is to avoid *starting* contention-heavy operations during business hours, not to hard-stop them.
- Jobs that routinely run longer than the window should be scheduled less frequently or broken into smaller sub-checks (e.g. `--missing` separately from `--checksums`).

Farmfs operations are constructed as pipelines where blob imports and re-linking are atomic (write to tmp, then rename/symlink), so mid-run interruption is generally safe — no partial blobs or broken symlinks will be left behind. **Cancellation is out of scope for v1.** The daemon does not kill running subprocesses when the window closes. On SIGTERM/SIGINT the daemon sets a shutdown flag, waits for the current `run_job` call to return (subprocess to finish), then exits cleanly.

---

## Module: `farmfs/farmd.py`

All config dataclasses, encode/decode, `JobRunner`, and daemon logic live here.

```python
from __future__ import annotations
import os, re, signal, subprocess, threading
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Any, Callable, Dict, List, Literal, Optional

from farmfs.fs import Path
from farmfs.keydb import KeyDBFactory, KeyDBWindow
from farmfs.volume import FarmFSVolume, mkfs

POLL_INTERVAL_SECONDS = 60
JOB_TYPES = Literal["fsck", "fetch", "upload"]

# ── Config dataclasses ──────────────────────────────────────────────────────

@dataclass
class DaemonConfig:
    night_start: int   # 0–23
    night_end: int     # 0–23

@dataclass
class JobConfig:
    type: JOB_TYPES
    every_seconds: int
    enabled: bool
    flags: List[str]          # fsck only
    remote: Optional[str]     # fetch / upload
    snap: Optional[str]       # fetch only
    job_id: str               # derived, stable

@dataclass
class VolumeConfig:
    name: str
    root: str                 # absolute path; Path() created at use-site
    jobs: List[JobConfig]

@dataclass
class JobState:
    last_run_start: Optional[str]   # ISO UTC
    last_run_end:   Optional[str]
    last_exit_code: Optional[int]
    next_run:       Optional[str]
    running:        bool
    running_pid:    Optional[int]
    run_count:      int
    last_log_blob:  Optional[str]   # checksum in JobRunner's vol blobstore

# ── Encode / decode (for KeyDBFactory) ──────────────────────────────────────

def encode_daemon_config(c: DaemonConfig) -> Dict[str, Any]: ...
def decode_daemon_config(d: Dict[str, Any], key: str) -> DaemonConfig: ...
def encode_volume_config(v: VolumeConfig) -> Dict[str, Any]: ...
def decode_volume_config(d: Dict[str, Any], key: str) -> VolumeConfig: ...
def encode_job_state(s: JobState) -> Dict[str, Any]: ...
def decode_job_state(d: Dict[str, Any], key: str) -> JobState: ...

# ── Helpers ──────────────────────────────────────────────────────────────────

EVERY_RE = re.compile(r'^(\d+)(h|d|w|m)$')

def parse_every(s: str) -> int:
    """'1h'→3600  '1d'→86400  '1w'→604800  '1m'→2592000. Raises ValueError."""
    ...

def make_job_id(vol_name: str, raw_job: Dict[str, Any]) -> str:
    """Derive stable human-readable job ID from volume name + raw job dict."""
    ...

def is_job_due(js: JobState, now: datetime) -> bool:
    """True if next_run is None or is_past(parse_utc(js.next_run), now)."""
    ...

def compute_next_run(last: datetime, every_seconds: int) -> str:
    """Return format_utc(add_seconds(last, every_seconds))."""
    ...

def is_pid_alive(pid: int) -> bool:
    """os.kill(pid, 0) — True if process is alive."""
    ...

# ── JobRunner ─────────────────────────────────────────────────────────────

class JobRunner:
    """Thin wrapper around FarmFSVolume adding scheduler-specific KeyDB windows."""
    def __init__(self, root: Path) -> None:
        self.vol: FarmFSVolume = FarmFSVolume(root)
        json_db = self.vol.keydb
        self.configdb: KeyDBFactory[DaemonConfig] = KeyDBFactory(
            KeyDBWindow("scheduler/config", json_db),
            encode_daemon_config, decode_daemon_config,
        )
        self.volumedb: KeyDBFactory[VolumeConfig] = KeyDBFactory(
            KeyDBWindow("scheduler/volumes", json_db),
            encode_volume_config, decode_volume_config,
        )
        self.statedb: KeyDBFactory[JobState] = KeyDBFactory(
            KeyDBWindow("scheduler/state", json_db),
            encode_job_state, decode_job_state,
        )

def make_job_runner(root: Path) -> None:
    """Create a new farmfs volume to back a JobRunner (farmfs mkfs equivalent)."""
    mkfs(root, root.join("userdata"))

# ── Scheduling helpers ───────────────────────────────────────────────────────

def is_night_window(config: DaemonConfig, local_hour: int) -> bool:
    s, e = config.night_start, config.night_end
    if s < e:
        return s <= local_hour < e
    return local_hour >= s or local_hour < e   # wrap-around e.g. 22–06

def build_farmfs_argv(job: JobConfig) -> List[str]:
    """Return argv for subprocess call (without 'farmfs' prefix).
    fsck  → ["fsck", "--missing", ...]
    fetch → ["fetch", "backup"]  or  ["fetch"]
    upload→ ["upload", "backup"]
    """
    ...

def clear_stale_running(jr: JobRunner) -> None:
    """For each job state with running=True, check PID; clear if dead."""
    ...

# ── Job runner ───────────────────────────────────────────────────────────────

def run_job(jr: JobRunner, vol_cfg: VolumeConfig, job: JobConfig, now: datetime) -> None:
    """
    1. Write state: running=True, running_pid=os.getpid(), last_run_start=now
    2. subprocess.run(["farmfs","--quiet"] + argv, cwd=vol_cfg.root, stdout=PIPE, stderr=STDOUT)
    3. Import captured output as a blob into jr.vol.bs → get checksum
    4. Write state: running=False, pid=None, exit_code, next_run, last_log_blob, run_count+1
    """
    ...

# ── Daemon loop ──────────────────────────────────────────────────────────────

def daemon_loop(jr: JobRunner) -> None:
    """
    Poll every POLL_INTERVAL_SECONDS. On each tick:
      - clear stale running markers
      - if in night window: find one due job, run it synchronously
    Handles SIGTERM/SIGINT cleanly.
    """
    shutdown = threading.Event()
    signal.signal(signal.SIGTERM, lambda *_: shutdown.set())
    signal.signal(signal.SIGINT,  lambda *_: shutdown.set())

    while not shutdown.is_set():
        now = datetime.now(timezone.utc)
        config = jr.configdb.read("config")
        clear_stale_running(jr)

        if is_night_window(config, now.astimezone().hour):
            volumes = [jr.volumedb.read(n) for n in jr.volumedb.list()]
            for vol_cfg in volumes:
                for job in vol_cfg.jobs:
                    if not job.enabled:
                        continue
                    try:
                        js = jr.statedb.read(job.job_id)
                    except FileNotFoundError:
                        js = JobState(None, None, None, None, False, None, 0, None)
                    if js.running:
                        continue
                    if is_job_due(js, now):
                        run_job(jr, vol_cfg, job, now)
                        break   # one job per tick; re-evaluate on next wake-up
                else:
                    continue
                break

        shutdown.wait(timeout=POLL_INTERVAL_SECONDS)
```

---

## Module: `farmfs/farmd_ui.py`

```python
FARMD_USAGE = """
FarmFS Maintenance Daemon

Usage:
  farmd mkfs --volume=<vol>
  farmd start --volume=<vol>
  farmd status --volume=<vol>
  farmd log <job_id> --volume=<vol>
  farmd run-now <job_id> --volume=<vol>
  farmd config set --volume=<vol> [--night-start=<h>] [--night-end=<h>]
  farmd config show --volume=<vol>
  farmd volume add --volume=<vol> <name> <root>
               [--fsck-every=<e>] [--fsck-flags=<f>...]
               [--fetch-remote=<r>] [--fetch-every=<e>]
               [--upload-remote=<r>] [--upload-every=<e>]
  farmd volume remove --volume=<vol> <name>
  farmd volume list --volume=<vol>
  farmd job add --volume=<vol> <vol_name> <type> [--flags=<f>...] [--remote=<r>] [--snap=<s>] --every=<e>
  farmd job remove --volume=<vol> <job_id>
  farmd job list --volume=<vol> [<vol_name>]
  farmd -h | --help

Options:
  --volume=<vol>   Path to the farmd farmfs volume.
  -h --help        Show help.
"""

def farmd_main() -> Never: ...
```

### `volume add` convenience flags

`farmd volume add` accepts optional flags to auto-create common jobs at registration time. This avoids needing separate `job add` calls for the typical case of enabling fsck + replication to one remote:

```bash
# Register a volume with fsck every day and full replication to "backup" every 6h
farmd volume add --volume /srv/farmd/main media /Volumes/Media/farmfs \
    --fsck-every 1d \
    --fetch-remote backup --fetch-every 6h \
    --upload-remote backup --upload-every 12h
```

If the convenience flags are omitted, no jobs are created (user must add them with `farmd job add`). If multiple remotes or custom fsck flags are needed, use `job add` directly. The convenience flags map to a single job each — `--fsck-every` creates one fsck job with no extra flags (use `job add --flags` for `--missing`, `--checksums`, etc.).

### Status output format

```
VOLUME    JOB                      LAST RUN              STATUS     NEXT RUN
--------  -----------------------  --------------------  ---------  --------------------
media     fsck-checksums_missing   2026-02-27 23:01:05   OK(0)      2026-02-28 23:01:05
media     fetch-backup             2026-02-28 02:00:11   FAIL(1)    2026-02-28 08:00:11
photos    fsck-all                 never                 PENDING    ASAP
```

Pure stdlib string formatting (`str.ljust`).

---

## `pyproject.toml` Change

```toml
[project.scripts]
farmd = "farmfs.farmd_ui:farmd_main"
```

No new dependencies. All data is JSON via the existing `JsonKeyDB` layer.

---

## Deployment & systemd

### Volume placement

| Scenario | Volume path | Run as |
|----------|-------------|--------|
| System service | `/var/lib/farmd/<name>` | dedicated `farmd` user |
| Single-user desktop | `~/.local/share/farmd/<name>` | your user |

Create with: `farmd mkfs --volume <path>`

### Permissions issues

1. **Managed volume access**: Run the daemon as the same user who owns the farmfs volumes. If running as a different user, add that user to a shared group and ensure volumes are group-readable.

2. **`farmfs` binary path**: systemd's `PATH` often excludes `~/.local/bin`. Use an absolute path in `ExecStart` or set `Environment=PATH=...`.

3. **systemd sandboxing**: If using `ProtectSystem=strict` or similar, explicitly list all volume paths in `ReadWritePaths=`.

### System service unit

```ini
[Unit]
Description=FarmFS Maintenance Daemon
After=network.target local-fs.target

[Service]
Type=simple
User=farmd
ExecStart=/usr/local/bin/farmd start --volume /var/lib/farmd/main
Restart=on-failure
RestartSec=30
ReadWritePaths=/var/lib/farmd/main /mnt/media /mnt/photos
Environment=PATH=/usr/local/bin:/usr/bin:/bin

[Install]
WantedBy=multi-user.target
```

### User service unit (recommended for desktops)

```ini
# ~/.config/systemd/user/farmd.service
[Unit]
Description=FarmFS Maintenance Daemon
After=default.target

[Service]
ExecStart=%h/.local/bin/farmd start --volume %h/.local/share/farmd/main
Restart=on-failure

[Install]
WantedBy=default.target
```

`systemctl --user enable --now farmd` — runs as you, no permission issues. Use `loginctl enable-linger <user>` to start at boot.

---

## Implementation Order

1. **`farmfs/util.py`** — add `parse_utc`, `format_utc`, `add_seconds`, `is_past`
   **`tests/test_util.py`** — add time utility tests
   → `make check`

2. **`farmfs/farmd.py`** — dataclasses, encode/decode, helpers, `JobRunner`, `is_night_window`, `build_farmfs_argv`, `run_job`, `daemon_loop`
   **`tests/test_farmd.py`** — unit tests for all of the above
   → `make check`

3. **`farmfs/farmd_ui.py`** — all CLI subcommands + `farmd_main`
   **`pyproject.toml`** — add `farmd` entry point
   → `make dev && farmd --help` → `make check`

---

## Verification

- `make check` passes after each phase.
- `farmd mkfs --volume /tmp/test-farmd` creates a valid farmfs volume (`farmfs fsck` passes on it).
- `farmd volume add --volume /tmp/test-farmd media /some/farmfs-vol` stores key in KeyDB.
- `farmd job add --volume /tmp/test-farmd media fsck --every 1d` adds a job; `farmd job list` shows it.
- `farmd run-now media/fsck-all --volume /tmp/test-farmd` runs fsck, stores log blob, updates state.
- `farmd status --volume /tmp/test-farmd` prints the table.
- `farmd log media/fsck-all --volume /tmp/test-farmd` prints the last run's output.
