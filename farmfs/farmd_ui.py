"""FarmFS Maintenance Daemon — CLI entry point."""
from __future__ import annotations

import sys
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from docopt import docopt

from farmfs import cwd, getvol

from farmfs.farmd import (
    DaemonConfig,
    JobConfig,
    JobRunner,
    JobState,
    VolumeConfig,
    build_farmfs_argv,
    daemon_loop,
    is_job_due,
    make_job_id,
    parse_every,
    read_log_blob,
    run_job,
)
from farmfs.fs import Path
from farmfs.volume import FarmFSVolume

FARMD_USAGE = """
FarmFS Maintenance Daemon

Usage:
  farmd mkcfg
  farmd start
  farmd status
  farmd log <job_id>
  farmd run-now <job_id>
  farmd config set  [--night-start=<h>] [--night-end=<h>]
  farmd config show
  farmd volume add  <name> <root>
               [--fsck-every=<e>] [--fsck-flags=<f>...]
               [--fetch-remote=<r>] [--fetch-every=<e>]
               [--upload-remote=<r>] [--upload-every=<e>]
  farmd volume remove  <name>
  farmd volume list
  farmd job add  <vol_name> <type> [--flags=<f>...] [--remote=<r>] [--snap=<s>] --every=<e>
  farmd job remove  <job_id>
  farmd job list  [<vol_name>]
  farmd -h | --help

Options:
  --night-start=<h>     Night window start hour (0-23).
  --night-end=<h>       Night window end hour (0-23).
  --fsck-every=<e>      Schedule fsck job (e.g. 1d).
  --fsck-flags=<f>      Flags for the fsck job (e.g. --missing --checksums).
  --fetch-remote=<r>    Remote name for fetch job.
  --fetch-every=<e>     Schedule fetch job.
  --upload-remote=<r>   Remote name for upload job.
  --upload-every=<e>    Schedule upload job.
  --every=<e>           Schedule interval (e.g. 1h, 6h, 1d, 1w).
  --flags=<f>           Flags for the job (fsck only).
  --remote=<r>          Remote name for fetch/upload jobs.
  --snap=<s>            Snapshot name for fetch jobs.
  -h --help             Show help.
"""

_DEFAULT_NIGHT_START = 22
_DEFAULT_NIGHT_END = 6


def _open_jr(vol: FarmFSVolume) -> JobRunner:
    return JobRunner(vol)


def _format_time(iso: Optional[str]) -> str:
    if iso is None:
        return "never"
    try:
        dt = datetime.fromisoformat(iso).astimezone()
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return iso


def _format_status(js: Optional[JobState], job: JobConfig, now: datetime) -> str:
    if js is None:
        return "PENDING"
    if js.running:
        return "RUNNING"
    if js.last_exit_code is None:
        return "PENDING"
    if js.last_exit_code == 0:
        return "OK(0)"
    return f"FAIL({js.last_exit_code})"


def _format_next(js: Optional[JobState], job: JobConfig, now: datetime) -> str:
    if js is None or js.next_run is None:
        return "ASAP"
    if is_job_due(js, now):
        return "ASAP"
    return _format_time(js.next_run)


# ── Command handlers ──────────────────────────────────────────────────────────

def _find_vol(cwd: Path) -> FarmFSVolume:
    return getvol(cwd)

def cmd_mkcfg(jr: JobRunner) -> int:
    # Write default config
    cfg = DaemonConfig(night_start=_DEFAULT_NIGHT_START, night_end=_DEFAULT_NIGHT_END)
    jr.configdb.write("config", cfg, overwrite=False)
    print(f"Created farmd config in volume {jr.vol.root.relative_to(cwd)}")
    return 0


def cmd_start(jr: JobRunner) -> int:
    print(f"Starting farmd daemon on volume {jr.vol.root.relative_to(cwd)}")
    daemon_loop(jr)
    return 0


def cmd_status(jr: JobRunner) -> int:
    now = datetime.now(timezone.utc)

    col_vol = 10
    col_job = 25
    col_last = 22
    col_status = 11
    col_next = 22

    header = (
        "VOLUME".ljust(col_vol)
        + "JOB".ljust(col_job)
        + "LAST RUN".ljust(col_last)
        + "STATUS".ljust(col_status)
        + "NEXT RUN".ljust(col_next)
    )
    sep = (
        f'{"-" * col_vol}'
        f'  {"-" * (col_job - 2)}'
        f'  {"-" * (col_last - 2)}'
        f'  {"-" * (col_status - 2)}'
        f'  {"-" * col_next}'
    )
    print(header)
    print(sep)

    volume_names = jr.volumedb.list()
    for vol_name in sorted(volume_names):
        try:
            vol_cfg = jr.volumedb.read(vol_name)
        except FileNotFoundError:
            continue
        for job in vol_cfg.jobs:
            try:
                js: Optional[JobState] = jr.statedb.read(job.job_id)
            except FileNotFoundError:
                js = None
            last_run = _format_time(js.last_run_start if js else None)
            status = _format_status(js, job, now)
            next_run = _format_next(js, job, now)
            job_short = job.job_id.split("/", 1)[-1] if "/" in job.job_id else job.job_id
            print(
                vol_name.ljust(col_vol)
                + job_short.ljust(col_job)
                + last_run.ljust(col_last)
                + status.ljust(col_status)
                + next_run.ljust(col_next)
            )
    return 0


def cmd_log(jr: JobRunner, args: dict) -> int:
    job_id = args["<job_id>"]
    try:
        js = jr.statedb.read(job_id)
    except FileNotFoundError:
        print(f"No state found for job {job_id!r}", file=sys.stderr)
        return 1
    if js.last_log_blob is None:
        print(f"No log blob for job {job_id!r}", file=sys.stderr)
        return 1
    for chunk in read_log_blob(jr, js.last_log_blob):
        sys.stdout.buffer.write(chunk)
    return 0


def cmd_run_now(jr: JobRunner, args: dict) -> int:
    job_id = args["<job_id>"]
    now = datetime.now(timezone.utc)

    # Find the job in the volumedb
    for vol_name in jr.volumedb.list():
        try:
            vol_cfg = jr.volumedb.read(vol_name)
        except FileNotFoundError:
            continue
        for job in vol_cfg.jobs:
            if job.job_id == job_id:
                print(f"Running job {job_id} now ...")
                run_job(jr, vol_cfg, job, now)
                try:
                    js = jr.statedb.read(job_id)
                    code = js.last_exit_code
                except FileNotFoundError:
                    code = None
                print(f"Job {job_id} completed with exit code {code}")
                return code if code is not None else 0

    print(f"Job {job_id!r} not found", file=sys.stderr)
    return 1


def cmd_config_set(jr: JobRunner, args: dict) -> int:
    try:
        cfg = jr.configdb.read("config")
    except FileNotFoundError:
        cfg = DaemonConfig(night_start=_DEFAULT_NIGHT_START, night_end=_DEFAULT_NIGHT_END)

    if args.get("--night-start") is not None:
        cfg.night_start = int(args["--night-start"])
    if args.get("--night-end") is not None:
        cfg.night_end = int(args["--night-end"])

    jr.configdb.write("config", cfg, overwrite=True)
    print(f"Config updated: night_start={cfg.night_start} night_end={cfg.night_end}")
    return 0


def cmd_config_show(jr: JobRunner) -> int:
    try:
        cfg = jr.configdb.read("config")
        print(f"night_start: {cfg.night_start}")
        print(f"night_end:   {cfg.night_end}")
    except FileNotFoundError:
        print("No config found (daemon not initialised — run farmd mkfs)", file=sys.stderr)
        return 1
    return 0


def cmd_volume_add(jr: JobRunner, args: dict) -> int:
    name = args["<name>"]
    root = args["<root>"]

    # Check if volume already exists
    existing = jr.volumedb.list()
    if name in existing:
        print(f"Volume {name!r} already exists", file=sys.stderr)
        return 1

    jobs: list[JobConfig] = []

    # --fsck-every → create an fsck job
    fsck_every = args.get("--fsck-every")
    raw: Dict[str, Any] = {}
    if fsck_every:
        fsck_flags = list(args.get("--fsck-flags") or [])
        raw = {"type": "fsck", "flags": fsck_flags}
        job_id = make_job_id(name, raw)
        jobs.append(JobConfig(
            type="fsck",
            every_seconds=parse_every(fsck_every),
            enabled=True,
            flags=fsck_flags,
            remote=None,
            snap=None,
            job_id=job_id,
        ))

    # --fetch-remote + --fetch-every → create a fetch job
    fetch_remote = args.get("--fetch-remote")
    fetch_every = args.get("--fetch-every")
    if fetch_remote and fetch_every:
        raw = {"type": "fetch", "remote": fetch_remote, "snap": None}
        job_id = make_job_id(name, raw)
        jobs.append(JobConfig(
            type="fetch",
            every_seconds=parse_every(fetch_every),
            enabled=True,
            flags=[],
            remote=fetch_remote,
            snap=None,
            job_id=job_id,
        ))

    # --upload-remote + --upload-every → create an upload job
    upload_remote = args.get("--upload-remote")
    upload_every = args.get("--upload-every")
    if upload_remote and upload_every:
        raw = {"type": "upload", "remote": upload_remote}
        job_id = make_job_id(name, raw)
        jobs.append(JobConfig(
            type="upload",
            every_seconds=parse_every(upload_every),
            enabled=True,
            flags=[],
            remote=upload_remote,
            snap=None,
            job_id=job_id,
        ))

    vc = VolumeConfig(name=name, root=root, jobs=jobs)
    jr.volumedb.write(name, vc, overwrite=False)
    print(f"Added volume {name!r} at {root} with {len(jobs)} job(s)")
    return 0


def cmd_volume_remove(jr: JobRunner, args: dict) -> int:
    name = args["<name>"]
    try:
        jr.volumedb.delete(name)
        print(f"Removed volume {name!r}")
    except FileNotFoundError:
        print(f"Volume {name!r} not found", file=sys.stderr)
        return 1
    return 0


def cmd_volume_list(jr: JobRunner) -> int:
    volume_names = sorted(jr.volumedb.list())
    if not volume_names:
        print("No volumes configured")
    for name in volume_names:
        try:
            vc = jr.volumedb.read(name)
            print(f"{name}  {vc.root}  ({len(vc.jobs)} job(s))")
        except FileNotFoundError:
            print(f"{name}  (error reading config)")
    return 0


def cmd_job_add(jr: JobRunner, args: dict) -> int:
    vol_name = args["<vol_name>"]
    job_type = args["<type>"]
    every_str = args["--every"]
    flags = list(args.get("--flags") or [])
    remote = args.get("--remote")
    snap = args.get("--snap")

    try:
        vol_cfg = jr.volumedb.read(vol_name)
    except FileNotFoundError:
        print(f"Volume {vol_name!r} not found. Add it first with 'farmd volume add'.", file=sys.stderr)
        return 1

    raw = {"type": job_type, "flags": flags, "remote": remote, "snap": snap}
    job_id = make_job_id(vol_name, raw)

    # Check for duplicate job_id
    for existing_job in vol_cfg.jobs:
        if existing_job.job_id == job_id:
            print(f"Job {job_id!r} already exists", file=sys.stderr)
            return 1

    new_job = JobConfig(
        type=job_type,  # type: ignore[arg-type]
        every_seconds=parse_every(every_str),
        enabled=True,
        flags=flags,
        remote=remote,
        snap=snap,
        job_id=job_id,
    )
    vol_cfg.jobs.append(new_job)
    jr.volumedb.write(vol_name, vol_cfg, overwrite=True)
    print(f"Added job {job_id!r} to volume {vol_name!r}")
    return 0


def cmd_job_remove(jr: JobRunner, args: dict) -> int:
    job_id = args["<job_id>"]

    removed = False
    for vol_name in jr.volumedb.list():
        try:
            vol_cfg = jr.volumedb.read(vol_name)
        except FileNotFoundError:
            continue
        new_jobs = [j for j in vol_cfg.jobs if j.job_id != job_id]
        if len(new_jobs) < len(vol_cfg.jobs):
            vol_cfg.jobs[:] = new_jobs
            jr.volumedb.write(vol_name, vol_cfg, overwrite=True)
            removed = True

    if removed:
        # Remove state too
        try:
            jr.statedb.delete(job_id)
        except FileNotFoundError:
            pass
        print(f"Removed job {job_id!r}")
        return 0
    else:
        print(f"Job {job_id!r} not found", file=sys.stderr)
        return 1


def cmd_job_list(jr: JobRunner, args: dict) -> int:
    filter_vol = args.get("<vol_name>")
    now = datetime.now(timezone.utc)

    volume_names = sorted(jr.volumedb.list())
    for vol_name in volume_names:
        if filter_vol and vol_name != filter_vol:
            continue
        try:
            vol_cfg = jr.volumedb.read(vol_name)
        except FileNotFoundError:
            continue
        for job in vol_cfg.jobs:
            try:
                js: Optional[JobState] = jr.statedb.read(job.job_id)
            except FileNotFoundError:
                js = None
            enabled = "enabled" if job.enabled else "disabled"
            next_run = _format_next(js, job, now)
            argv_str = " ".join(["farmfs", "--quiet"] + build_farmfs_argv(job))
            print(f"{job.job_id}  [{enabled}]  every {job.every_seconds}s  next={next_run}  cmd={argv_str}")
    return 0


# ── Main entry point ──────────────────────────────────────────────────────────

def farmd_ui(argv: list[str], cwd: Path) -> int:
    args = docopt(FARMD_USAGE, argv=argv)

    vol = _find_vol(cwd)
    jr = _open_jr(vol)

    if args["mkcfg"]:
        code = cmd_mkcfg(jr)
    elif args["start"]:
        code = cmd_start(jr)
    elif args["status"]:
        code = cmd_status(jr)
    elif args["log"]:
        code = cmd_log(jr, args)
    elif args["run-now"]:
        code = cmd_run_now(jr, args)
    elif args["config"] and args["set"]:
        code = cmd_config_set(jr, args)
    elif args["config"] and args["show"]:
        code = cmd_config_show(jr)
    elif args["volume"] and args["add"]:
        code = cmd_volume_add(jr, args)
    elif args["volume"] and args["remove"]:
        code = cmd_volume_remove(jr, args)
    elif args["volume"] and args["list"]:
        code = cmd_volume_list(jr)
    elif args["job"] and args["add"]:
        code = cmd_job_add(jr, args)
    elif args["job"] and args["remove"]:
        code = cmd_job_remove(jr, args)
    elif args["job"] and args["list"]:
        code = cmd_job_list(jr, args)
    else:
        print(FARMD_USAGE)
        code = 0
    return code


def farmd_main():
    return farmd_ui(sys.argv[1:], cwd)
