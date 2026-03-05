farmfs
======

Tool for creating / distributing / maintaining symlink farms.

## Warning
FarmFS is still very early stage software. 

Please do not keep anything in it which you are not willing to lose.

## Installation

### To use Farmfs

pip install git+https://github.com/andrewguy9/farmfs.git@master

### To hack on Farmfs
```
git clone https://github.com/andrewguy9/farmfs.git
cd farmfs
make dev
```

## Usage:
```
FarmFS

Usage:
  farmfs mkfs [--root <root>] [--data <data>]
  farmfs (status|freeze|thaw) [<path>...]
  farmfs snap list
  farmfs snap (make|read|delete|restore|diff) [--force] <snap>
  farmfs fsck [--missing] [--frozen-ignored] [--blob-permissions] [--checksums] [--keydb] [--fix]
  farmfs count
  farmfs similarity <dir_a> <dir_b>
  farmfs gc [--noop]
  farmfs remote add [--force] <remote> <root>
  farmfs remote remove <remote>
  farmfs remote list [<remote>]
  farmfs pull <remote> [<snap>]
  farmfs diff <remote> [<snap>]
  farmfs fetch [--force] [<remote>] [<snap>]

Options:
  --quiet  Disable progress bars.
```
## What is FarmFS

Farmfs is a git style interface to non text, usually immutable, sometimes large files.
It takes your files and puts them into an immutable blob store then builds symlinks from the file names into the store.

### Why would you do that?
* You can snapshot your directory structure BIG_O(num_files).
* You can diff two different farmfs stores with BIG_O(num_files) rather than BIG_O(sum(file_sizes))
* You can identify corruption of your files because all entries in the blob store are checksumed.
* If the same file contents appear in multiple places you only have to put it in the blob store once. (deduplication)

## Getting Started

Create a Farmfs store

```
mkdir myfarm
cd myfarm
farmfs mkfs
```

Make some files

```
mkdir -p 1/2/3/4/5
mkdir -p a/b/c/d/e
echo "value1" > 1/2/3/4/5/v1
echo "value1" > a/b/c/d/e/v1
```

Status can show us unmanged files.

```
farmfs status
/Users/andrewguy9/Downloads/readme/1/2/3/4/5/v1
/Users/andrewguy9/Downloads/readme/a/b/c/d/e/v1
```

Add the untracked files to the blob store.
Notice it only needs to store "value1" once.

```
farmfs freeze
Processing /Users/andrewguy9/Downloads/readme/1/2/3/4/5/v1 with csum /Users/andrewguy9/Downloads/readme/.farmfs/userdata
Putting link at /Users/andrewguy9/Downloads/readme/.farmfs/userdata/238/851/a91/77b60af767ca431ed521e55
Processing /Users/andrewguy9/Downloads/readme/a/b/c/d/e/v1 with csum /Users/andrewguy9/Downloads/readme/.farmfs/userdata
Found a copy of file already in userdata, skipping copy
```

Edit a file.
First we need to thaw it, then we can change it.

```
farmfs thaw 1/2/3/4/5/v1

farmfs status
/Users/andrewguy9/Downloads/readme/1/2/3/4/5/v1

echo "value2" > 1/2/3/4/5/v1

farmfs freeze 1/2/3/4/5/v1
Processing /Users/andrewguy9/Downloads/readme/1/2/3/4/5/v1 with csum /Users/andrewguy9/Downloads/readme/.farmfs/userdata
Putting link at /Users/andrewguy9/Downloads/readme/.farmfs/userdata/4ca/8c5/ae5/e759e237bfb80c51940de7a

farmfs status
```

We don't want to loose our progress, so lets make a snapshot.

```
farmfs snap make mysnap
```

Now create more stuff

```
echo "oops" > mistake.txt

farmfs freeze mistake.txt
Processing /Users/andrewguy9/Downloads/readme/mistake.txt with csum /Users/andrewguy9/Downloads/readme/.farmfs/userdata
Putting link at /Users/andrewguy9/Downloads/readme/.farmfs/userdata/38a/f5c/549/26b620264ab1501150cf189
```

Well that was a mistake, lets roll back to the old snap.

```
farmfs snap restore mysnap
Removing /mistake.txt
```

Now that we have our files built, lets build another depot.

```
cd ..
mkdir copy
cd copy
farmfs mkfs
```

We want to add our prior depot as a remote.

```
farmfs remote add origin ../myfarm
```

Now lets copy our work from before.

```
farmfs pull origin
mkdir /1
mkdir /1/2
mkdir /1/2/3
mkdir /1/2/3/4
mkdir /1/2/3/4/5
mklink /1/2/3/4/5/v1 -> /4ca/8c5/ae5/e759e237bfb80c51940de7a
Blob missing from local, copying
*** /Users/andrewguy9/Downloads/copy/.farmfs/userdata/4ca/8c5/ae5/e759e237bfb80c51940de7a /Users/andrewguy9/Downloads/myfarm/.farmfs/userdata/4ca/8c5/ae5/e759e237bfb80c51940de7a
mkdir /a
mkdir /a/b
mkdir /a/b/c
mkdir /a/b/c/d
mkdir /a/b/c/d/e
mklink /a/b/c/d/e/v1 -> /238/851/a91/77b60af767ca431ed521e55
Blob missing from local, copying
*** /Users/andrewguy9/Downloads/copy/.farmfs/userdata/238/851/a91/77b60af767ca431ed521e55 /Users/andrewguy9/Downloads/myfarm/.farmfs/userdata/238/851/a91/77b60af767ca431ed521e55
```

Lets see whats in our new depot:

```
find *
1
1/2
1/2/3
1/2/3/4
1/2/3/4/5
1/2/3/4/5/v1
a
a/b
a/b/c
a/b/c/d
a/b/c/d/e
a/b/c/d/e/v1
```
## Maintenance

### fsck

`farmfs fsck` checks the integrity of your FarmFS volume. Run it periodically or after hardware
events to catch corruption early. Use `--fix` to automatically repair problems that can be safely
corrected without data loss.

```
farmfs fsck [--missing] [--frozen-ignored] [--blob-permissions] [--checksums] [--keydb] [--fix]
```

Running `farmfs fsck` with no flags runs all checks. Individual checks can be selected with flags.

| Flag | What it checks |
|------|----------------|
| `--missing` | Frozen files (symlinks) whose blob is absent from the blobstore |
| `--frozen-ignored` | Frozen files that match `.farmignore` patterns |
| `--blob-permissions` | Blobs that are writable (all blobs should be read-only) |
| `--checksums` | Blobs whose content does not match their stored checksum |
| `--keydb` | Metadata key/value store integrity (see below) |

#### `--missing`

Walks the live tree and all snapshots, looking for link entries whose blob is not present in the
local blobstore. Each missing blob is printed along with every snapshot and file path that
references it:

```
a1b2c3d4e5f6...
    mysnap    photos/vacation/img001.jpg
    mysnap    photos/vacation/img001_copy.jpg
```

With `--fix <remote>`: downloads the missing blob from the named remote.

#### `--frozen-ignored`

Walks the live tree looking for frozen files (symlinks into the blobstore) that match patterns in
`.farmignore`. These files should not be frozen — they were probably frozen before the ignore rule
was added. Each offending path is printed:

```
Ignored file frozen: build/output.o
```

With `--fix`: thaws each frozen-ignored file back to a regular file (copies the blob content out
and removes the symlink).

#### `--blob-permissions`

Walks every blob in the blobstore and checks that it is read-only. Blobs are immutable by design;
a writable blob indicates the permissions were changed externally and is a risk for accidental
modification. Each writable blob is printed:

```
writable blob: a1b2c3d4e5f6...
```

With `--fix`: restores read-only permissions on each writable blob.

#### `--checksums`

Re-hashes every blob in the blobstore and compares the result against the blob's filename (which
is its checksum). A mismatch indicates the blob content has been corrupted. Each corrupt blob is
printed:

```
CORRUPTION checksum mismatch in blob a1b2c3d4e5f6... got 000000000000...
```

With `--fix <remote>`: if the remote copy of the blob has the correct checksum, downloads it to
replace the corrupt local copy. If the remote copy is also corrupt, reports that it cannot be
repaired.

#### `--keydb`

The keydb stores snapshots and remote configuration. `--keydb` runs three levels of checks:

1. **Storage** — every key must be blob-backed (symlink into the blobstore) and its blob must
   checksum correctly. Legacy file-backed keys from old versions of FarmFS are reported as `LEGACY`
   and can be migrated with `--fix`.

2. **JSON** — the stored bytes must be canonical JSON (deterministic key ordering, UTF-8 encoding).
   Non-canonical entries are reported with a diff showing where the encoding differs.

3. **Semantic** — snapshot entries are decoded and re-encoded through the `SnapshotItem` type,
   which normalises legacy absolute paths (`/foo`) to relative form (`foo`). If the re-encoded
   form differs from what is stored the key needs a rewrite.

`--fix` repairs all three classes of issue without data loss:
- Migrates file-backed keys to blob-backed
- Rewrites non-canonical JSON in canonical form
- Rewrites snapshots with normalised (relative) paths

```
farmfs fsck --keydb            # detect problems
farmfs fsck --keydb --fix      # detect and repair
```

Exit code is 0 when no problems are found, non-zero otherwise.

## farmd — Maintenance Daemon

`farmd` is a scheduling daemon that runs `farmfs` jobs (fsck, fetch, upload)
on a timed basis. It manages one or more farmfs volumes from a central
**depot** — itself a farmfs volume that stores job configuration and run logs
in its keydb.

### Installation

`farmd` is installed alongside `farmfs`:

```
pip install farmfs
```

### First-time setup

**1. Create a depot**

```
farmd mkfs ~/.local/share/farmd/main --register
```

`--register` appends the path to `~/.config/farmd/config.json` so every
subsequent `farmd` command finds the depot automatically.

**2. Register a farmfs volume and add jobs**

```
farmd volume add media /Volumes/Media/farmfs \
    --fsck-every=1d \
    --fetch-remote=backup --fetch-every=6h \
    --upload-remote=backup --upload-every=12h
```

**3. Start the daemon**

```
farmd start
```

### Depot discovery

Every `farmd` command needs to locate the depot. The lookup order is:

| Priority | Source |
|----------|--------|
| 1 | `--volume=<path>` flag |
| 2 | `FARMD_VOLUME` environment variable |
| 3 | `farmd_roots` list in `~/.config/farmd/config.json` |
| 4 | `farmd_roots` list in `/etc/farmd/config.json` |
| 5 | Current working directory (fallback) |

The first reachable depot wins. Unreachable paths (unmounted drives, missing
directories) are skipped silently, so a drive failure automatically falls
through to the next entry in the list.

### Config file format

`~/.config/farmd/config.json` (user) and `/etc/farmd/config.json` (system):

```json
{
  "farmd_roots": [
    "/Volumes/Primary/farmd",
    "/Volumes/Backup/farmd",
    "/mnt/nas/farmd"
  ]
}
```

Only the depot path list lives here. All job configuration, schedules, and
run state live inside the depot's keydb where they are checksummed and can
be replicated with `farmfs fetch`/`farmfs upload`.

### High-availability: multiple depot replicas

Because the depot is a farmfs volume, you can replicate it across drives.
List all replicas in `farmd_roots` in priority order — primary first:

```json
{
  "farmd_roots": [
    "/Volumes/Primary/farmd",
    "/Volumes/Mirror/farmd"
  ]
}
```

If the primary drive is unavailable, `farmd` falls through to the mirror
automatically. Sync the replicas with standard `farmfs fetch`/`farmfs upload`.

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

Colour is enabled automatically when stdout is a terminal. Disable it with
`--no-color` or by setting the `NO_COLOR` environment variable.

### Job cancellation

If a job is running under a windowed schedule (e.g. `0 22 * * *`) and the
schedule window closes before the job finishes, `farmd` sends `SIGTERM` to
the child process and records the exit code as negative (e.g. `-15`). The
status column will show `CANCELLED(-15)`.

farmfs operations are atomic at the blob level (write to tmp → rename/symlink),
so mid-run cancellation is safe — no partial blobs or broken symlinks are left
behind.

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

`farmd` integrates with [smartmontools](https://www.smartmontools.org/) to record
S.M.A.R.T. device warnings into the depot. When a drive backing one of your
volumes reports a problem — failing health check, rising error count, bad
self-test — the alert appears in `farmd status` and persists until you clear it.

#### How it works

smartd's `-M exec` directive calls a script whenever it detects a problem.
The `smartd-runner` helper (default on Debian/Ubuntu) runs every script placed
in `/etc/smartmontools/smartd_warning.d/`. FarmFS ships a script,
`bin/smartd_farmd_warning`, that calls `farmd smart record`. That command reads
the environment variables smartd sets (`SMARTD_DEVICE`, `SMARTD_FAILTYPE`,
`SMARTD_MESSAGE`, etc.) and stores the alert in the depot keyed by device name.

#### Installation

```bash
sudo cp bin/smartd_farmd_warning /etc/smartmontools/smartd_warning.d/10farmd
sudo chmod +x /etc/smartmontools/smartd_warning.d/10farmd
```

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

The alert is removed and will no longer appear in `farmd status`. smartd will
re-record it if the device reports another problem.

#### Identifying which volume a device backs

smartd warns per-device; FarmFS volumes are per-path. Use `lsblk` to map
devices to mount points:

```
lsblk -o NAME,MOUNTPOINT,MODEL,SERIAL
```

Cross-reference the `MODEL` and `SERIAL` columns with the `DEVICE INFO` column
in `farmd smart list` (sourced from `SMARTD_DEVICEINFO`) to find which volume
is at risk.

## Development:

### Before Committing

Always run the full validation suite before committing changes:

```
make check
```

This runs tests (with coverage), type checking, and linting in one step. All three must pass.

### Testing:

#### Regression Testing:
Regression tests can be run with `make test` or `pytest` directly.
Tests are kept in the `tests` directory, which will be detected by `pytest` automatically.
Coverage must remain above 80%.

#### Performance Optimization:
Performance testing cases are stored under the `perf` directory. These are useful for making development decisions and are not generally useful as ongoing tests.

To run:
```
make perf
```

Or for a specific test/pattern:
```
pytest -s perf/your_test.py [-k case_pattern]
```

Note: `-s` is required to get a printout of the results.

Example: `pytest -s perf/transducer.py -k transducers`

### Debugging

farmfs comes with a useful debugging tool `farmdbg`.

```
farmdbg
Usage:
  farmdbg reverse <csum>
  farmdbg key read <key>
  farmdbg key write <key> <value>
  farmdbg key delete <key>
  farmdbg key list [<key>]
  farmdbg walk (keys|userdata|root|snap <snapshot>)
  farmdbg checksum <path>...
  farmdbg fix link <file> <target>
  farmdbg rewrite-links <target>
```

`farmdbg` can be used to dump parts of the keystore or blobstore, as well as walk and repair links.

# Compose vs Pipeline performance

Compose has less function call overhead than pipeline because we flatten the call chain. There are fewer wrapper functions.

```
cincs = compose(*incs)
timeit(lambda: cincs(0))
0.45056812500001797

pincs = pipeline(*incs)
timeit(lambda: pincs(0))
0.8594365409999227

```

When dealing with chained iterators, pipeline and compose have the same performance.
Pulling from an iterator is faster than mixing in composed function calls, even with fmap overhead.

```
csum = compose(fmap(inc), fmap(inc), fmap(inc), sum)
timeit(lambda: csum(range(1000)), number=10000)
1.2722054580000304

csum2 = compose(fmap(compose(inc, inc, inc)), sum)
timeit(lambda: csum2(range(1000)), number=10000)
2.0529240829999935

psum = pipeline(fmap(inc), fmap(inc), fmap(inc), sum)
timeit(lambda: psum(range(1000)), number=10000)
1.273805500000094

psum2 = pipeline(fmap(pipeline(inc, inc, inc)), sum)
timeit(lambda: psum2(range(1000)), number=10000)
2.7146950840000272
```

# Pypy3 support:

farmfs is a pure python program, and has support for pypy3.

However, performance of pypy3 is actually worse than cPython due
to farmfs uses iterators over loops, negating the benefits of most
of the JITs optimizations. To improve performance consider
improvements to caching, IO parallelization and reducing small
string allocations.

python3.9.2
```
time farmfs snap make --force test_snap
real    0m2.387s
user    0m2.010s
sys     0m0.319s

time farmfs snap make --force test_snap
real    0m2.305s
user    0m1.991s
sys     0m0.312s

time farmfs snap make --force test_snap
real    0m2.258s
user    0m1.939s
sys     0m0.317s
```

pypy3
```
time farmfs snap make --force test_snap
real    0m6.363s
user    0m5.850s
sys     0m0.512s

time farmfs snap make --force test_snap
real    0m6.177s
user    0m5.730s
sys     0m0.449s

time farmfs snap make --force test_snap
real    0m6.201s
user    0m5.731s
sys     0m0.455s
```
