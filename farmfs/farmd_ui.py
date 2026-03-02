"""FarmFS Maintenance Daemon — CLI entry point."""
from __future__ import annotations

import os
import sys
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from docopt import docopt
from tabulate import tabulate

from farmfs import cwd, getvol

from farmfs.farmd import (
    ALWAYS_CRON,
    ALWAYS_SCHEDULE_NAME,
    JobConfig,
    JobRunner,
    JobState,
    ScheduleConfig,
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
  farmd start
  farmd status
  farmd log <job_id>
  farmd run-now <job_id>
  farmd requeue <job_id>
  farmd schedule add <name> --cron=<expr>
  farmd schedule remove <name>
  farmd schedule list
  farmd volume add  <name> <root>
               [--fsck-every=<e>] [--fsck-flags=<f>...] [--fsck-schedule=<s>]
               [--fetch-remote=<r>] [--fetch-every=<e>] [--fetch-schedule=<s>]
               [--upload-remote=<r>] [--upload-every=<e>] [--upload-schedule=<s>]
  farmd volume remove  <name>
  farmd volume list
  farmd job add  <vol_name> <type> [--flags=<f>...] [--remote=<r>] [--snap=<s>] --every=<e> [--schedule=<s>]
  farmd job remove  <job_id>
  farmd job list  [<vol_name>]
  farmd -h | --help

Options:
  --cron=<expr>         Cron expression (e.g. "0 22 * * *" for 10pm daily).
  --fsck-every=<e>      Schedule fsck job (e.g. 1d).
  --fsck-flags=<f>      Flags for the fsck job (e.g. --missing --checksums).
  --fsck-schedule=<s>   Schedule name for fsck job [default: always].
  --fetch-remote=<r>    Remote name for fetch job.
  --fetch-every=<e>     Schedule fetch job.
  --fetch-schedule=<s>  Schedule name for fetch job [default: always].
  --upload-remote=<r>   Remote name for upload job.
  --upload-every=<e>    Schedule upload job.
  --upload-schedule=<s> Schedule name for upload job [default: always].
  --every=<e>           Schedule interval (e.g. 1h, 6h, 1d, 1w).
  --flags=<f>           Flags for the job (fsck only).
  --remote=<r>          Remote name for fetch/upload jobs.
  --snap=<s>            Snapshot name for fetch jobs.
  --schedule=<s>        Schedule name for job [default: always].
  -h --help             Show help.
"""


def _open_jr(vol: FarmFSVolume) -> JobRunner:
    return JobRunner(vol)


def _find_vol(cwd: Path) -> FarmFSVolume:
    return getvol(cwd)


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
    if js.last_exit_code < 0:
        return f"CANCELLED({js.last_exit_code})"
    if js.last_exit_code == 0:
        return "OK(0)"
    return f"FAIL({js.last_exit_code})"


def _format_every(seconds: int) -> str:
    for unit, secs in [("w", 604800), ("d", 86400), ("h", 3600)]:
        if seconds % secs == 0:
            return f"{seconds // secs}{unit}"
    return f"{seconds}s"


def _format_next(js: Optional[JobState], job: JobConfig, now: datetime) -> str:
    if js is None or js.next_run is None:
        return "ASAP"
    if is_job_due(js, now):
        return "ASAP"
    return _format_time(js.next_run)


# ── Command handlers ──────────────────────────────────────────────────────────

def cmd_start(jr: JobRunner) -> int:
    print(f"Starting farmd daemon on volume {jr.vol.root.relative_to(cwd)}")
    daemon_loop(jr)
    return 0


def _format_duration(js: Optional[JobState], now: datetime) -> str:
    if js is None or js.last_run_start is None:
        return "-"
    try:
        start = datetime.fromisoformat(js.last_run_start)
        if js.running:
            secs = int((now - start).total_seconds())
        elif js.last_run_end is not None:
            secs = int((datetime.fromisoformat(js.last_run_end) - start).total_seconds())
        else:
            return "-"
    except Exception:
        return "-"
    if secs < 60:
        return f"{secs}s"
    if secs < 3600:
        return f"{secs // 60}m{secs % 60:02d}s"
    return f"{secs // 3600}h{(secs % 3600) // 60:02d}m"


def cmd_status(jr: JobRunner) -> int:
    now = datetime.now(timezone.utc)

    headers = ["VOLUME", "JOB", "SCHEDULE", "LAST RUN", "DURATION", "STATUS", "NEXT RUN"]
    rows = []

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
            job_short = job.job_id.split("/", 1)[-1] if "/" in job.job_id else job.job_id
            rows.append([
                vol_name,
                job_short,
                job.schedule,
                _format_time(js.last_run_start if js else None),
                _format_duration(js, now),
                _format_status(js, job, now),
                _format_next(js, job, now),
            ])

    print(tabulate(rows, headers=headers, tablefmt="simple"))
    return 0


def cmd_log(jr: JobRunner, args: dict) -> int:
    job_id = args["<job_id>"]
    try:
        js = jr.statedb.read(job_id)
    except FileNotFoundError:
        print(f"No state found for job {job_id!r}", file=sys.stderr)
        return 1
    if js.running and js.live_log_path is not None:
        os.execvp("tail", ["tail", "-f", js.live_log_path])
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


def cmd_requeue(jr: JobRunner, args: dict) -> int:
    job_id = args["<job_id>"]
    try:
        js = jr.statedb.read(job_id)
    except FileNotFoundError:
        print(f"No state found for job {job_id!r} — it will run ASAP already", file=sys.stderr)
        return 1
    if js.running:
        print(f"Job {job_id!r} is currently running", file=sys.stderr)
        return 1
    js.next_run = None
    jr.statedb.write(job_id, js, overwrite=True)
    print(f"Requeued {job_id!r} — will run on next daemon tick")
    return 0


def cmd_schedule_add(jr: JobRunner, args: dict) -> int:
    name = args["<name>"]
    cron_expr = args["--cron"]
    sc = ScheduleConfig(name=name, cron=cron_expr)
    try:
        jr.scheduledb.write(name, sc, overwrite=False)
    except Exception:
        print(f"Schedule {name!r} already exists", file=sys.stderr)
        return 1
    print(f"Added schedule {name!r} with cron {cron_expr!r}")
    return 0


def cmd_schedule_remove(jr: JobRunner, args: dict) -> int:
    name = args["<name>"]
    if name == ALWAYS_SCHEDULE_NAME:
        print(f"Cannot remove built-in schedule {name!r}", file=sys.stderr)
        return 1
    try:
        jr.scheduledb.delete(name)
        print(f"Removed schedule {name!r}")
    except FileNotFoundError:
        print(f"Schedule {name!r} not found", file=sys.stderr)
        return 1
    return 0


def cmd_schedule_list(jr: JobRunner) -> int:
    rows = [[ALWAYS_SCHEDULE_NAME, ALWAYS_CRON, "(built-in)"]]
    for name in sorted(jr.scheduledb.list()):
        try:
            sc = jr.scheduledb.read(name)
            rows.append([sc.name, sc.cron, ""])
        except FileNotFoundError:
            rows.append([name, "(error)", ""])
    print(tabulate(rows, headers=["NAME", "CRON", "NOTE"], tablefmt="simple"))
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
        fsck_schedule = args.get("--fsck-schedule") or ALWAYS_SCHEDULE_NAME
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
            schedule=fsck_schedule,
        ))

    # --fetch-remote + --fetch-every → create a fetch job
    fetch_remote = args.get("--fetch-remote")
    fetch_every = args.get("--fetch-every")
    if fetch_remote and fetch_every:
        fetch_schedule = args.get("--fetch-schedule") or ALWAYS_SCHEDULE_NAME
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
            schedule=fetch_schedule,
        ))

    # --upload-remote + --upload-every → create an upload job
    upload_remote = args.get("--upload-remote")
    upload_every = args.get("--upload-every")
    if upload_remote and upload_every:
        upload_schedule = args.get("--upload-schedule") or ALWAYS_SCHEDULE_NAME
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
            schedule=upload_schedule,
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
        return 0
    rows = []
    for name in volume_names:
        try:
            vc = jr.volumedb.read(name)
            rows.append([name, vc.root, len(vc.jobs)])
        except FileNotFoundError:
            rows.append([name, "(error reading config)", ""])
    print(tabulate(rows, headers=["VOLUME", "ROOT", "JOBS"], tablefmt="simple"))
    return 0


def cmd_job_add(jr: JobRunner, args: dict) -> int:
    vol_name = args["<vol_name>"]
    job_type = args["<type>"]
    every_str = args["--every"]
    flags = list(args.get("--flags") or [])
    remote = args.get("--remote")
    snap = args.get("--snap")
    schedule = args.get("--schedule") or ALWAYS_SCHEDULE_NAME

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
        schedule=schedule,
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

    rows = []
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
            argv_str = " ".join(["farmfs", "--quiet"] + build_farmfs_argv(job))
            rows.append([
                job.job_id,
                "enabled" if job.enabled else "disabled",
                job.schedule,
                _format_every(job.every_seconds),
                _format_next(js, job, now),
                argv_str,
            ])
    print(tabulate(rows, headers=["JOB", "STATE", "SCHEDULE", "EVERY", "NEXT RUN", "CMD"], tablefmt="simple"))
    return 0


# ── Main entry point ──────────────────────────────────────────────────────────

def farmd_ui(argv: list[str], cwd: Path) -> int:
    args = docopt(FARMD_USAGE, argv=argv)

    vol = _find_vol(cwd)
    jr = _open_jr(vol)

    if args["start"]:
        code = cmd_start(jr)
    elif args["status"]:
        code = cmd_status(jr)
    elif args["log"]:
        code = cmd_log(jr, args)
    elif args["run-now"]:
        code = cmd_run_now(jr, args)
    elif args["requeue"]:
        code = cmd_requeue(jr, args)
    elif args["schedule"] and args["add"]:
        code = cmd_schedule_add(jr, args)
    elif args["schedule"] and args["remove"]:
        code = cmd_schedule_remove(jr, args)
    elif args["schedule"] and args["list"]:
        code = cmd_schedule_list(jr)
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
