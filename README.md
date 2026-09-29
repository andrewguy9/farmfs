farmfs
======

Content-addressed storage for archiving, backing up, and distributing large binary files, with cheap snapshots and automatic deduplication.

## What is FarmFS

FarmFS is a content-addressed archive built on top of an ordinary filesystem. When you freeze a file, FarmFS stores its contents once in an immutable blob store and replaces the original file with a symlink to that blob. Two files with identical contents end up as two symlinks pointing at the same stored bytes.

Snapshots record directory structure and the checksum of every frozen file, rather than copying file contents again. This gives FarmFS inexpensive snapshots, deduplication, integrity checking, and efficient replication between volumes.

FarmFS is intended for large collections of files that usually stop changing once created — photographs, videos, ML datasets, disk images, archives — not for files you expect to keep editing in place.

FarmFS doesn't mount a filesystem, require FUSE, or change how applications open files. It manages an ordinary directory using ordinary symlinks; any program that can open a file can open a frozen one.

### Good fit / poor fit

| Good fits | Likely poor fits |
|---|---|
| Photo and video collections | Databases with frequent in-place updates |
| Datasets and ML models | Frequently modified source trees |
| Archives and immutable build artifacts | VM disk images that change continuously |
| Large collections containing duplicates | Applications that expect to modify files in place |
| Local archives and mounted-volume replication | Requirements for mature encrypted Internet backup |

A static disk image fits the model even when an actively-changing one doesn't — the dividing line is whether the file, once written, is done changing.

### Vocabulary

| Term | Meaning |
|---|---|
| Volume | A directory tree managed by FarmFS (created with `farmfs mkfs`). |
| Blob | Immutable file contents, identified by their checksum. |
| Blob store | Where a volume keeps its blobs, under `<volume>/.farmfs/userdata/`. |
| Frozen file | A visible pathname backed by a blob — a symlink into the blob store. |
| Snapshot | A named record of paths, structure, and content identities, taken with `snap make`. |
| Remote | Another FarmFS volume, registered with `farmfs remote add`, that you can `pull`/`diff`/`fetch` against. |
| Depot | A FarmFS volume used to hold `farmd`'s own job configuration and logs — a normal volume, just one `farmd` manages itself rather than one holding your files. |

## Why FarmFS

### How is this different from `cp`?

`cp` just copies bytes from one place to another — it's the right tool for that, and FarmFS uses it too, under the hood, the first time it stores a file. The difference is everything FarmFS remembers afterward that `cp` has no way to: it recognizes when two files are byte-for-byte identical and stores that content once no matter how many places reference it, it can tell you later if any of that stored content has silently corrupted, and it lets you name a point in time (a snapshot) and return your whole tree to exactly that state.

### How is this different from `rsync`?

`rsync` reconciles two trees at the moment you run it — it walks both sides, compares them, and copies over whatever differs. It has no memory between runs; going back to how things looked yesterday is something you arrange yourself, and deduplication across unrelated files is something you engineer around it, not something it does natively.

FarmFS keeps that memory as a first-class thing: a snapshot is a real, named point in history, comparing two snapshots never touches file bytes, and dedup falls out of content addressing everywhere. What `rsync` still does better: efficient partial-file transfer of one large file that changed a little, and mature, battle-tested network transport — FarmFS's own remote/pull story is currently strongest between local and mounted volumes (see [Limitations](#limitations)). rsync remains a fine transport to run underneath a FarmFS remote; the two aren't mutually exclusive.

### How is this different from `git`?

Not as much as you'd think structurally — git also content-addresses blobs by hash and dedupes identical content, the same idea FarmFS is built on. The difference is what each one is *for*. Git is built around small text files and meaningful diffs: it delta-encodes blobs against each other, and its whole workflow assumes the content is diffable. Pointing git at a directory of large binaries works technically, but every version gets stored close to in full (delta compression barely helps on already-compressed binary data), there's no way to "diff" two photos in any way that means anything, and the repository just grows and grows.

FarmFS assumes the opposite: your files are opaque and immutable, and the only meaningful relationship is "identical or not." That's a better fit for photos, video, audio, disk images, ML model weights — anything where "the file changed" is the whole diff you're ever going to get anyway.

## Limitations

FarmFS has been in daily use by its author for more than 12 years, across many drives and volumes, with no known data-loss incidents. Local archival storage, snapshots, integrity checking, and replication between local or mounted volumes are the most exercised, longest-running parts of the system.

What it doesn't have yet is a mature offsite replication story. `farmfs pull`/`fetch`/`remote` work well between local and mounted volumes, but if you're relying on FarmFS itself to get your only copy safely offsite, that path is less mature than everything else — keep an independent offsite backup until you've built and tested that replication workflow yourself.

**Snapshots only ever contain frozen files and tracked structure.** An ordinary, unfrozen file is invisible to a snapshot — `farmfs status` will point it out, but `snap make` silently leaves it out. Run `farmfs status` and freeze everything you intend to preserve before taking a snapshot; there is no "untracked" entry inside a snapshot to warn you afterward.

**`farmd`'s scheduled `upload` job is currently broken.** It builds a `farmfs upload` command, but `farmfs` has no `upload` subcommand — a scheduled upload job will fail every time it runs. `fsck` and `fetch` jobs are unaffected.

## Installation

### To use FarmFS

From PyPI: `pip install farmfs`

From GitHub: `pip install git+https://github.com/andrewguy9/farmfs.git@master`

### To hack on FarmFS

```
git clone https://github.com/andrewguy9/farmfs.git
cd farmfs
make dev
```

## Quick Start

Create a volume:

```
mkdir myfarm
cd myfarm
farmfs mkfs
```

`farmfs mkfs` initializes FarmFS metadata inside the directory (a `.farmfs/` subdirectory). It does not format the underlying filesystem or erase existing files — it's safe to run against a directory that already has files in it.

Add a couple of photos — including one you've saved in two places, the way photo libraries often end up:

```
mkdir -p photos/2025/trip photos/favorites
cp ~/IMG_1234.jpg photos/2025/trip/
cp ~/IMG_1234.jpg photos/favorites/
```

(This example assumes you have some image file to copy in; any file works.)

`farmfs status` shows files FarmFS doesn't know about yet:

```
$ farmfs status
photos/2025/trip/IMG_1234.jpg
photos/favorites/IMG_1234.jpg
```

**Freeze** them — move their contents into the blob store and replace each pathname with a symlink to it:

```
$ farmfs freeze
Imported photos/2025/trip/IMG_1234.jpg with checksum 2272e053e6c180f98803a6c4be5aabe3
Imported photos/favorites/IMG_1234.jpg with checksum 2272e053e6c180f98803a6c4be5aabe3 was a duplicate
```

Both paths have identical bytes, so FarmFS stores one blob and both paths reference it — the second freeze found the content already there and only added the symlink:

```
$ ls -l photos/2025/trip/ photos/favorites/
photos/2025/trip/IMG_1234.jpg -> .farmfs/userdata/227/2e0/53e/6c180f98803a6c4be5aabe3
photos/favorites/IMG_1234.jpg -> .farmfs/userdata/227/2e0/53e/6c180f98803a6c4be5aabe3
```

Now that everything's frozen, take a snapshot — a named point you can always come back to:

```
farmfs snap make backup
```

**Thaw** a file to edit it — this materializes it as a normal, writable file again:

```
$ farmfs thaw photos/2025/trip/IMG_1234.jpg
Exported photos/2025/trip/IMG_1234.jpg
```

Suppose you (or something else) deletes a file by mistake:

```
rm photos/favorites/IMG_1234.jpg
```

`snap restore` puts the volume back exactly as the snapshot recorded it — including re-linking the deleted favorite and re-freezing the thawed trip copy, without you having to remember what you changed:

```
$ farmfs snap restore backup
diff: link photos/2025/trip/IMG_1234.jpg 2272e053e6c180f98803a6c4be5aabe3
Apply mklink photos/2025/trip/IMG_1234.jpg -> 2272e053e6c180f98803a6c4be5aabe3
diff: link photos/favorites/IMG_1234.jpg 2272e053e6c180f98803a6c4be5aabe3
Apply mklink photos/favorites/IMG_1234.jpg -> 2272e053e6c180f98803a6c4be5aabe3
```

Now build a second volume and pull your work into it — the way you'd replicate onto another drive:

```
cd ..
mkdir backup_copy
cd backup_copy
farmfs mkfs
farmfs remote add origin ../myfarm
```

```
$ farmfs pull origin
diff: dir photos None
Apply mkdir photos
diff: dir photos/2025 None
Apply mkdir photos/2025
diff: dir photos/2025/trip None
Apply mkdir photos/2025/trip
diff: link photos/2025/trip/IMG_1234.jpg 2272e053e6c180f98803a6c4be5aabe3
Apply mklink photos/2025/trip/IMG_1234.jpg -> 2272e053e6c180f98803a6c4be5aabe3
diff: dir photos/favorites None
Apply mkdir photos/favorites
diff: link photos/favorites/IMG_1234.jpg 2272e053e6c180f98803a6c4be5aabe3
Apply mklink photos/favorites/IMG_1234.jpg -> 2272e053e6c180f98803a6c4be5aabe3
```

Every path in that output is relative to the volume it's being applied to (`backup_copy`), not an absolute filesystem path — `pull` is only ever changing things inside the volume you ran it from.

## How it works

A FarmFS volume only ever tracks two kinds of things: **content** and **structure**. Knowing which is which tells you what to expect when you archive, snapshot, or distribute a tree.

* **Content** is frozen bytes, identified by checksum rather than by path. The same bytes anywhere in your tree are stored once, corruption is detectable (a blob's contents always have to match its checksum), and the file is read-only until you `thaw` it back.
* **Structure** is everything else: directories, and symlinks that already existed in your tree and point somewhere inside the volume. Structure has no bytes of its own to store or deduplicate, but FarmFS still remembers it exactly, so a snapshot can put it back exactly.

**A snapshot is nothing more than a list of what's content and what's structure, at every path in your tree, at one point in time.** It never contains file bytes. Snapshots scale with the number of paths, not the total size of the files — creating or comparing a snapshot never rereads or copies a single stored blob, only the small per-path metadata (O(number of entries), not O(total bytes)). Transferring a genuinely missing blob during a `pull` is the one operation whose cost still depends on the bytes involved.

The snapshot itself is stored the same way as any other content — checksummed at rest, so it's covered by the same corruption checks as your files, and replicated (`farmfs fetch`) by comparing checksums and moving it only when it's actually changed.

The same content/structure split is what makes distribution efficient: pulling a snapshot from a remote volume only ever transfers the content you don't already have, by checksum, and replays the structure locally. Garbage collection is the mirror image — a piece of content is only ever removed once nothing in your live tree or any snapshot you've kept still needs it.

## Symlinks

Frozen files appear in the working tree as symlinks into the blob store — FarmFS treats those entries as content references, not as structure. A symlink that already existed in your tree independently of FarmFS, and that points somewhere inside the volume, is structure: FarmFS preserves it as faithfully as a directory, whether it's relative, absolute, points at a directory, or is currently broken, and reproduces the same absolute-vs-relative form on restore or pull. A symlink pointing *outside* the volume isn't content or structure FarmFS can vouch for, so FarmFS refuses to snapshot it rather than silently absorb someone else's file or silently drop the link.

**Known gap:** neither a bare `farmfs freeze` walk nor `farmfs status` currently looks at symlinks at all — both only consider regular files, so a foreign symlink sitting in your tree is invisible to them, and the rejection above only surfaces later, when you run `snap make`. If you're archiving a tree that might contain foreign symlinks, run `snap make` (or `farmdbg walk root`) early to find out, rather than trusting a clean `farmfs status`.

You can inspect how a given link was classified with `farmdbg walk`, which prints `blob` (a frozen file's checksum), `sub_path`, or `rel_path` for each link entry, with the target path rendered relative to your current directory (except for `blob`, where the checksum itself — not its location in the blobstore — is the file's identity):

```
farmdbg walk root
.               dir
sub             dir
sub/real.txt    link   blob        b1946ac92492d2347c6235b4d2611184
link_to_file    link   rel_path    sub/real.txt
link_dir        link   rel_path    sub
```

Full detail on relative/absolute/broken/circular link behavior and the exact rules for what counts as "inside the volume" is covered by the tests in `tests/test_snap.py` and `tests/test_freeze.py`, and by `classify_link()` in `farmfs/snapshot.py`.

## Maintenance

### fsck

`farmfs fsck` checks the integrity of your FarmFS volume: every blob's checksum, blob permissions, snapshot metadata, and whether any frozen file matches a `.farmignore` pattern it shouldn't. Run it periodically or after hardware events to catch corruption early.

```
farmfs fsck                # run all checks
farmfs fsck --checksums    # just re-verify blob content against its checksum
farmfs fsck --fix          # detect and repair what can be safely corrected without data loss
```

Running `farmfs fsck` with no flags runs all checks; individual checks can be selected with `--missing`, `--frozen-ignored`, `--blob-permissions`, `--checksums`, and `--keydb`. `--fix` repairs whatever that check found — downloading a missing or corrupt blob from a remote, thawing a frozen-but-ignored file, restoring blob permissions, or migrating/rewriting keydb metadata into canonical form, depending on which check flagged the problem. Exit code is 0 when no problems are found, non-zero otherwise.

Each check's exact output format, and the three-level storage/JSON/semantic structure of `--keydb`, are documented alongside the checker functions in `farmfs/ui.py` and `farmfs/fsck_types.py`.

## farmd — Maintenance Daemon

`farmd` is an optional scheduling daemon that runs `farmfs` maintenance jobs — periodic integrity checks, scheduled replica transfers, and drive health monitoring via smartd — across one or more volumes on cron-style schedules, so they happen on their own instead of whenever you remember to run them. It manages one or more FarmFS volumes from a central **depot**, itself a FarmFS volume that holds job configuration and run logs.

Quick start:

```
farmd mkfs ~/.local/share/farmd/main --register
farmd volume add media /Volumes/Media/farmfs --fsck-every=1d --fetch-remote=backup --fetch-every=6h
farmd start
farmd status
```

`--register` appends the depot path to `~/.config/farmd/config.json` so every subsequent `farmd` command finds it automatically. `farmd volume add` also accepts `--upload-remote`/`--upload-every` to schedule replication the other direction, but that job type is currently broken — see [Limitations](#limitations) — use `--fetch-*` for now.

`farmd` also supports multiple depot replicas for its own high availability, restricting jobs to named cron windows (see [Schedules](#schedules) below), and running as a systemd/launchd service. These, plus the full smartd drive-health integration, are documented in detail further down this README in case you need them, but the quick start above and `farmd --help` / `farmd status` are enough to get going.

### Depot discovery

Every `farmd` command needs to locate the depot. The lookup order is:

| Priority | Source |
|----------|--------|
| 1 | `--config=<path>` flag (reads the single-depot key `farmd_root` from a JSON file) |
| 2 | `FARMD_VOLUME` environment variable (direct path to depot root) |
| 3 | the multi-depot list `farmd_roots` in `~/.config/farmd/config.json` |
| 4 | the multi-depot list `farmd_roots` in `/etc/farmd/config.json` |
| 5 | Current working directory (fallback) |

`farmd_root` (singular) and `farmd_roots` (plural) are two different, deliberate schemas, not a typo: `--config`/`FARMD_VOLUME` point at exactly one depot directly, while the `~/.config`/`/etc` config files hold a priority-ordered list for automatic failover between replicas. The first reachable depot in that list wins; unreachable paths (unmounted drives, missing directories) are skipped silently, so a drive failure automatically falls through to the next entry.

`~/.config/farmd/config.json` (user) and `/etc/farmd/config.json` (system) format:

```json
{
  "farmd_roots": [
    "/Volumes/Primary/farmd",
    "/Volumes/Backup/farmd",
    "/mnt/nas/farmd"
  ]
}
```

Only the depot path list lives here. All job configuration, schedules, and run state live inside the depot's own keydb, where they're checksummed and can be replicated with `farmfs fetch`.

### High-availability: multiple depot replicas

Because the depot is a FarmFS volume, you can replicate it across drives. List all replicas in `farmd_roots` in priority order — primary first:

```json
{
  "farmd_roots": [
    "/Volumes/Primary/farmd",
    "/Volumes/Mirror/farmd"
  ]
}
```

If the primary drive is unavailable, `farmd` falls through to the mirror automatically. Sync the replicas with `farmfs fetch`.

### Schedules

A job has two independent settings that control when it runs: `--every` (how often it's *due* — e.g. `1d`, `6h`) and `--schedule` (a *window* it's only allowed to run within). Both have to hold at the same time for the daemon to start a job: if a daily job is due but its schedule's window hasn't opened yet, it waits; if the window opens but the job isn't due yet, nothing happens either.

A schedule is a named cron expression, defined once with `farmd schedule add <name> --cron="<expr>"` and then referenced by name from any job via `--schedule=<name>`. Every volume/job starts out on the built-in `always` schedule (`* * * * *`, matching every minute), which is really "no window restriction" — `--every` alone then fully controls its cadence. A named schedule like `--cron="0 22 * * *"` narrows that: the job is only eligible during the single minute each day that expression matches (10pm here), so in practice it runs once a day, at whatever the next 10pm is after it becomes due.

Because a schedule's window is that narrow, a job can still be running when the window closes — see [Job cancellation](#job-cancellation) below for what happens then.

### Managing jobs

```
# Add a named cron schedule (optional — jobs default to "always")
farmd schedule add overnight --cron="0 22 * * *"

# Add a volume with jobs attached to the overnight schedule
farmd volume add photos /Volumes/Photos/farmfs \
    --fsck-every=1d --fsck-schedule=overnight

# Add a job to an existing volume
farmd job add media fsck --every=1d --flags=--checksums --schedule=overnight

# List all jobs
farmd job list

# Force a job to run immediately
farmd run-now media/fsck-all

# Reset a job's next-run time so it runs on the next daemon tick
farmd requeue media/fsck-all

# View the last run's log
farmd log media/fsck-all
```

### Status output

```
farmd status
```

| Column | Meaning |
|--------|---------|
| JOB | Full job ID (`volume/type-discriminator`) — copy-paste ready |
| SCHEDULE | Named cron schedule or `always` |
| LAST RUN | Local time of the most recent run start |
| DURATION | Wall-clock time of the last (or current) run |
| STATUS | `PENDING`, `RUNNING`, `OK(0)`, `FAIL(N)`, or `CANCELLED(-15)` |
| NEXT RUN | When the job will next be eligible, or `ASAP` if overdue |

Color is enabled automatically when stdout is a terminal. Disable it with `--no-color` or by setting the `NO_COLOR` environment variable.

### Job cancellation

A cron schedule like `0 22 * * *` is only active for the one minute it matches each day (see [Schedules](#schedules) above) — a job that's still running once that minute has passed has outlived its window. When that happens, `farmd` sends `SIGTERM` to the child process and records the exit code as negative (e.g. `-15`). The status column will show `CANCELLED(-15)`. A job on the `always` schedule is never cancelled this way, since its window never closes.

farmfs operations are atomic at the blob level (write to tmp → rename/symlink), so mid-run cancellation is safe — no partial blobs or broken symlinks are left behind.

### Running as a system service

**systemd (Linux)**

```ini
# /etc/systemd/system/farmd.service
[Unit]
Description=FarmFS Maintenance Daemon
After=network.target local-fs.target

[Service]
Type=simple
User=farmd
ExecStart=/usr/local/bin/farmd start
Restart=on-failure
RestartSec=30
Environment=PATH=/usr/local/bin:/usr/bin:/bin

[Install]
WantedBy=multi-user.target
```

```
systemctl enable --now farmd
```

**systemd user service (desktop)**

```ini
# ~/.config/systemd/user/farmd.service
[Unit]
Description=FarmFS Maintenance Daemon
After=default.target

[Service]
ExecStart=%h/.local/bin/farmd start
Restart=on-failure

[Install]
WantedBy=default.target
```

```
systemctl --user enable --now farmd
# Start at login (even without a graphical session):
loginctl enable-linger $USER
```

**launchd (macOS)**

```xml
<!-- ~/Library/LaunchAgents/com.farmfs.farmd.plist -->
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN"
  "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>             <string>com.farmfs.farmd</string>
  <key>ProgramArguments</key>
  <array>
    <string>/usr/local/bin/farmd</string>
    <string>start</string>
  </array>
  <key>RunAtLoad</key>         <true/>
  <key>KeepAlive</key>         <true/>
  <key>StandardOutPath</key>   <string>/tmp/farmd.log</string>
  <key>StandardErrorPath</key> <string>/tmp/farmd.err</string>
</dict>
</plist>
```

```
launchctl load ~/Library/LaunchAgents/com.farmfs.farmd.plist
```

### Device health monitoring (smartd)

`farmd` integrates with [smartmontools](https://www.smartmontools.org/) to record S.M.A.R.T. device warnings into the depot. When a drive backing one of your volumes reports a problem — failing health check, rising error count, bad self-test — the alert appears in `farmd status` and persists until you clear it.

#### How it works

smartd's `-M exec` directive calls a script whenever it detects a problem. The `smartd-runner` helper (default on Debian/Ubuntu) runs every script placed in `/etc/smartmontools/smartd_warning.d/`. You install a small wrapper there that activates your virtualenv and calls `farmd --config=<config-file> smart record`. That command reads the environment variables smartd sets (`SMARTD_DEVICE`, `SMARTD_FAILTYPE`, `SMARTD_MESSAGE`, etc.) and stores the alert in the depot keyed by device name.

#### Installation

`bin/smartd_farmd_warning` is the core script, but smartd runs as root with a minimal environment — it won't know about your virtualenv or depot location. Create a site-specific wrapper that provides those two things:

```bash
sudo tee /etc/smartmontools/smartd_warning.d/10farmd > /dev/null <<'EOF'
#!/bin/sh
. /path/to/venv/bin/activate
exec farmd --config=/etc/farmd/config.json smart record
EOF
sudo chmod +x /etc/smartmontools/smartd_warning.d/10farmd
```

Replace `/path/to/venv` with the virtualenv that has farmfs installed. `/etc/farmd/config.json` should contain `{"farmd_root": "/path/to/depot"}` — the single-depot key, per [Depot discovery](#depot-discovery) above.

No changes to `/etc/smartd.conf` are needed when using the Debian default:

```
DEVICESCAN -d removable -n standby -m root -M exec /usr/share/smartmontools/smartd-runner
```

`smartd-runner` will call `10farmd` alongside any existing mail scripts.

#### Viewing alerts

Alerts appear automatically in `farmd status` whenever any are present:

```
Daemon: STOPPED

JOB                    SCHEDULE  LAST RUN  DURATION  STATUS  NEXT RUN
...

DEVICE    FAIL TYPE   ALERT TIME           MESSAGE
--------  ----------  -------------------  --------------------------------
/dev/sda  Health      2026-03-04 12:00:00  Device failure: /dev/sda
  Use 'farmd smart list' for full reports; 'farmd smart clear <device>' to dismiss.
```

For the full smartd report on each device:

```
farmd smart list
```

#### Dismissing an alert

Once you have replaced or confirmed a drive is healthy:

```
farmd smart clear /dev/sda
```

The alert is removed and will no longer appear in `farmd status`. smartd will re-record it if the device reports another problem.

#### Identifying which volume a device backs

smartd warns per-device; FarmFS volumes are per-path. Use `lsblk` to map devices to mount points:

```
lsblk -o NAME,MOUNTPOINT,MODEL,SERIAL
```

Cross-reference the `MODEL` and `SERIAL` columns with the `DEVICE INFO` column in `farmd smart list` (sourced from `SMARTD_DEVICEINFO`) to find which volume is at risk.

## Command reference

`farmfs --help` always prints the current, authoritative command grammar:

```
$ farmfs --help
FarmFS

Usage:
  farmfs mkfs [options] [--root <root>] [--data <data>]
  farmfs (status|freeze|thaw) [options] [<path>...]
  farmfs snap list [options]
  farmfs snap (make|read|delete|restore|diff) [options] [--force] <snap>
  farmfs fsck [options] [--remote=<remote>] [--missing --frozen-ignored --blob-permissions --checksums --keydb] [--fix]
  farmfs count [options]
  farmfs similarity [options] <dir_a> <dir_b>
  farmfs gc [options] [--noop]
  farmfs remote add [options] [--force] <remote> <root>
  farmfs remote remove [options] <remote>
  farmfs remote list [options] [<remote>]
  farmfs pull [options] <remote> [<snap>]
  farmfs pull-path [options] <src_path> <dest_path> [<snap>]
  farmfs diff [options] <remote> [<snap>]
  farmfs fetch [options] [--force] [<remote>] [<snap>]

Options:
  --quiet  Disable progress bars.
```

`farmdbg` is a lower-level debugging and repair tool — dumping parts of the keystore or blobstore, walking and repairing links, and syncing blobs directly against S3/HTTP/file-backed stores outside the `farmfs remote` model. `farmdbg --help` lists its full grammar.

## Development

```
git clone https://github.com/andrewguy9/farmfs.git
cd farmfs
make dev
```

Run `make check` (tests, type checking, and linting) before committing — all three must pass. `make test` runs the regression suite alone; coverage must stay above 80%. Performance tests live under `perf/` and are run with `make perf` or `pytest -s perf/your_test.py`; they inform development decisions and aren't part of `make check`.

`farmdbg` (see [Command reference](#command-reference) above) is the primary tool for low-level debugging during development.

### A note on function composition style

The codebase prefers `compose()` over `pipeline()` where both are viable — fewer wrapper functions means less per-call overhead:

```
cincs = compose(*incs)
timeit(lambda: cincs(0))
0.45056812500001797

pincs = pipeline(*incs)
timeit(lambda: pincs(0))
0.8594365409999227
```

For chained iterators the two perform the same — pulling from an iterator dominates the cost either way:

```
csum = compose(fmap(inc), fmap(inc), fmap(inc), sum)
timeit(lambda: csum(range(1000)), number=10000)
1.2722054580000304

psum = pipeline(fmap(inc), fmap(inc), fmap(inc), sum)
timeit(lambda: psum(range(1000)), number=10000)
1.273805500000094
```

(Benchmarks above are indicative, not current measurements — no date, hardware, or FarmFS version recorded for them.)

### PyPy3

FarmFS is a pure Python program and runs under PyPy3, but PyPy3 has historically performed *worse* than CPython for FarmFS: the code is iterator-heavy rather than loop-heavy, which limits how much PyPy's JIT can help, and the iterator overhead itself dominates. If you're optimizing FarmFS's performance, caching, I/O parallelization, and reducing small string allocations are more promising directions than switching interpreters.
