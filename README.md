farmfs
======

Archive, back up, and distribute your files with cheap snapshots and automatic deduplication.

## What is FarmFS

FarmFS is a git-like content management system for photo and video collections, ML datasets, saved disk images, and archives. These are a good fit because their contents often stay unchanged while you organize them, keep backups, or share them. FarmFS stores each distinct file's contents once, so keeping the same content under multiple names or in multiple snapshots costs little extra space.

It manages an ordinary directory on your existing filesystem, with snapshots and remotes; there's no filesystem to mount or FUSE layer to install.

When you freeze a file, FarmFS stores its contents once in an immutable blob store and replaces the original file with a symlink to that blob. Two files with identical contents end up as two symlinks pointing at the same stored bytes. Applications can read frozen files through their usual paths.

While Git has commits, FarmFS has snapshots: you choose when to capture the state of your directory, and therefore how finely to record its changes. Snapshots let you roll back to a saved state or diff against it to see what changed. Each snapshot records directory structure and the checksum of every frozen file without copying the file contents again, so keeping those states is inexpensive.

### Good fit / poor fit

| Good fits | Likely poor fits |
|---|---|
| Photo and video collections | Databases with frequent in-place updates |
| Datasets and ML models | Frequently modified source trees |
| Archives and immutable build artifacts | VM disk images that change continuously |
| Large collections containing duplicates | Applications that expect to modify files in place |
| Local archives and mounted-volume replication | Requirements for mature encrypted Internet backup |

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

Every one of the use cases below is a normal thing you'd already reach `cp`, `rsync`, `scp`, or git for. FarmFS doesn't replace those tools — a frozen file is a real symlink, so they all still work on it. What changes is that once your files are content-addressed, the same operations become cheaper, replication becomes diff-based rather than full-tree, and you get consistency guarantees none of those tools offer on their own: a copy either has the exact content it's supposed to, verifiable by its checksum, or FarmFS can tell you it doesn't.

### "I want to copy some files"

Once a file is frozen, its symlink *is* a complete, self-contained reference to its content — copying that reference (`cp -P`, or anything else that preserves symlinks) is O(1) work regardless of the file's size, because there's no content in the copy to move.

`farmfs pull` takes this further at the volume level: it diffs two volumes' snapshots first (an O(files) comparison of paths and checksums, no bytes touched), and transfers only the blobs the destination is actually missing — O(delta). If the destination already has 999 of your 1000 photos, pulling only moves the one it doesn't have.

### "I want to replicate to another host, or to S3"

A frozen file's symlink target is an absolute path rooted at its own volume, so replicating a tree by copying the raw symlinks somewhere else only works if the destination reconstructs the same paths — otherwise a copied symlink can point at a path that doesn't exist on the new host. `farmfs pull`/`fetch` sidestep this by replicating what the symlink *means* (this path is a reference to blob X) rather than its literal on-disk bytes, rebasing the reference onto wherever the destination volume actually lives.

FarmFS blobs are immutable and named by checksum, so replication can rely on stable content identities: comparing the checksums recorded in frozen files' symlink targets or snapshots tells it which content each tree references, without rereading the file bytes. Ordinary POSIX files can change in place. To decide what to transfer by comparing their contents, `rsync --checksum` reads and hashes source files and their same-size destination counterparts on each run; its default quick check uses size and modification time instead ([rsync manual](https://download.samba.org/pub/rsync/rsync.1#opt--checksum)). FarmFS can compare the recorded identities and transfer only missing blobs. `farmdbg s3 upload` applies the same principle by comparing local and remote blob names.

Those checksums also provide a durable integrity reference: `farmfs fsck --checksums` can read a blob and verify that it still contains the bytes its name promises. Replication relies on the blobs remaining immutable; integrity checking detects corruption that breaks that assumption.

`farmdbg s3` is a lower-level tool for this blob-level sync, separate from `farmfs remote`/`pull`/`fetch` (which talk to other FarmFS volumes on local or mounted paths) — see [Limitations](#limitations) for where the offsite/`farmd`-managed replication story is still maturing.

### "I have a content depot, but I need to iterate on it or replicate it"

Git can version a directory of large files, and content-addresses them the same way FarmFS does — but a git checkout is a real second copy of the bytes, separate from what git stores in `.git/objects`, so iterating on or cloning a large depot in git costs you that duplication every time. A checked-out frozen file in FarmFS *is* the one stored blob, so there's nothing to duplicate, and `pull`/`fetch` replicate the same way, by reference.

<details>
<summary>Why a git checkout costs double, verified</summary>

Confirmed by committing a 1MB incompressible (random-byte) file to a fresh git repo and measuring both copies on disk: the working-tree file and the compressed object in `.git/objects` each take close to the full 1MB, ~2MB total for one file, one version, before any history exists. Delta compression barely helps here since the data's already effectively incompressible, which is typical of large binaries (photos, video, model weights).

This isn't a shortcoming specific to git — it's what any tool built around diffable, delta-compressible history costs you when pointed at content that isn't diffable. FarmFS just doesn't try to diff file content at all, so it never pays that cost.

</details>

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
farmd volume add media /Volumes/Media/farmfs
farmd job add fsck media --every=1w --checksums
farmd job add fetch media --every=1d backup
farmd start
farmd status
```

`--register` appends the depot path to `~/.config/farmd/config.json` so every subsequent `farmd` command finds it automatically. `farmd volume add` just registers the volume; jobs are added separately with `farmd job add <type> <vol> --every=<interval> [options]` — `fsck`, `fetch`, `gc`, and `upload` are the four job types, though `upload` is currently broken (see [Limitations](#limitations)) — use `fetch` for replication instead.

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

Every job has a **period** (`--every=<interval>` — `1d`, `6h`, `1w`, ...) that says how often it should run: weekly, daily, every six hours. Once a job's period has elapsed since its last run *finished*, it's *due* — and it stays due, waiting, until the daemon actually runs it.

A job can also have a **schedule** (`--schedule=<name>`), which doesn't control how often the job runs — only *when it's allowed to start*. A schedule is a named cron expression (`farmd schedule add <name> --cron="<expr>"`) matching a specific window, like overnight or over the weekend. This is what lets you keep I/O-heavy jobs — a checksum-verifying `fsck --checksums`, a multi-terabyte `fetch` — from competing with active use of the machine: run them after hours instead of whenever they happen to become due. Jobs default to the built-in `always` schedule, which matches every minute — no restriction on when they can start.

Period and schedule combine like this: the daemon checks every due job against its schedule, and only starts one that's both due *and* currently inside its schedule's window. A job can be due long before its schedule allows it to run — it just waits. For example, a weekly `fsck` scheduled for the weekend might become due Monday morning; it stays due, but the daemon won't start it until Saturday, when the schedule check finally passes too.

**Make schedules wide, not a single instant.** The daemon polls once a minute and checks the schedule fresh each time, so a cron expression like `0 3 * * 6` (exactly 3:00am Saturday) is only a match for that one minute — if the poll doesn't land inside it, the job waits another week, and if the job is still running one minute later, the schedule no longer matches and it gets cancelled (see below) whether or not it was actually done. Write the cron as a range covering however long the job realistically needs instead:

```
farmd schedule add overnight --cron="0-59 1-5 * * *"    # 1am-6am every day
farmd schedule add weekend   --cron="0-59 3-8 * * 6"    # 3am-9am Saturday

farmd job add fsck media --every=1w --checksums --schedule=weekend
farmd job add fetch media --every=1d --schedule=overnight backup
```

This runs a full integrity check once a week, sometime in the 3am-9am Saturday window after it's due, and a replication sync once a day, sometime in the 1am-6am window after it's due — with several hours of room to actually finish rather than one narrow minute to both start and complete in.

A schedule's window can close before a running job finishes — see [Job cancellation](#job-cancellation) below for what happens then.

### Managing jobs

```
# Register a volume (no job configuration yet)
farmd volume add photos /Volumes/Photos/farmfs

# Add jobs to it -- schedule is optional, defaults to "always"
farmd job add fsck photos --every=1d --schedule=overnight
farmd job add fetch photos --every=6h backup

# List all jobs
farmd job list

# Force a job to run immediately
farmd run-now photos/fsck-all

# Reset a job's next-run time so it runs on the next daemon tick
farmd requeue photos/fsck-all

# View the last run's log
farmd log photos/fsck-all
```

Job IDs (`photos/fsck-all`, `photos/fetch-backup`, ...) are derived automatically from the volume name, job type, and its flags/remote — `farmd job list` always shows you the current ones to use with `run-now`/`requeue`/`log`.

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

A job that's still running once its schedule no longer matches the current minute has outlived its window (see [Schedules](#schedules) above, including why a narrow cron expression like `0 22 * * *` makes this likely rather than an edge case). When that happens, `farmd` sends `SIGTERM` to the child process and records the exit code as negative (e.g. `-15`). The status column will show `CANCELLED(-15)`. A job on the `always` schedule is never cancelled this way, since its window never closes.

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
