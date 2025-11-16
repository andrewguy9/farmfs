# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Quick Reference

### Common Commands

**Installation & Setup:**
```bash
python setup.py install          # Install the package with dependencies
pip install -e .                 # Install in editable mode
```

**Running:**
```bash
farmfs <command> [options]       # Main CLI tool
farmdbg <command> [options]      # Debugging tool
farmapi                          # Flask REST API server
```

**Testing:**
```bash
pytest                           # Run all tests (regression suite)
pytest tests/test_file.py        # Run specific test file
pytest -k test_name              # Run test by pattern
pytest -s perf/test_file.py      # Run performance tests with output

tox                              # Full test suite (py37, py39, pypy3, flake8)
tox -e py39                      # Run tests in specific Python version
tox -e py37-perf -- -k pattern   # Run perf tests in specific environment
```

**Linting & Formatting:**
```bash
tox -e lint                      # Check code formatting
yapf -d --recursive farmfs       # Show formatting diffs
isort --check-only --recursive farmfs  # Check import sorting
flake8 farmfs tests perf         # Lint check
```

**Coverage:**
```bash
pytest --cov farmfs --cov-report=html  # Generate HTML coverage report
coverage report -m                      # Show coverage summary
coverage html -i                        # Generate HTML coverage
```

## Project Architecture

FarmFS is a **Git-like content-addressable filesystem tool** for managing large, immutable binary files with deduplication. It treats large files the way Git treats source code.

### Core Design Principles

1. **Content Addressing**: Every file is identified by its MD5 checksum, not path
2. **Deduplication**: Identical file contents stored only once in blob store
3. **Immutability**: Frozen files are read-only symlinks to blobs
4. **Snapshot Versioning**: Point-in-time snapshots of directory trees (O(num_files), not O(file_sizes))
5. **Functional Style**: Heavy use of function composition, lazy evaluation, and higher-order functions

### Module Organization

| Module | Role |
|--------|------|
| **ui.py** | CLI entry point using docopt; implements all user commands |
| **volume.py** | Core orchestrator; manages blobstore, snapshots, and diffs |
| **blobstore.py** | Content-addressable blob storage abstraction (supports FileBlobstore, S3Blobstore, HttpBlobstore) |
| **snapshot.py** | Snapshot data structures and tree diffing algorithm |
| **keydb.py** | Key-value database for metadata (keys, status, remotes) |
| **fs.py** | Filesystem abstraction (Path wrapper, file operations) |
| **util.py** | Functional programming utilities (composition, mapping, progress bars) |
| **compose.py** | Pre-generated function composition utilities (up to 8 functions) |
| **transducer3.py** | Transducer pattern for lazy evaluation |
| **api.py** | Flask REST API for blob CRUD operations |

### Key Data Structures

**SnapshotItem**: `(path, type, csum?)`
- `type`: DIR, LINK, or FILE
- `csum`: Only present for LINK type (points to blob)

**SnapDelta**: `(path, mode, csum)`
- `mode`: REMOVED, DIR, or LINK
- Used to describe changes between snapshots

**Blob Storage Path**: `.farmfs/userdata/3AB/C12/DEF/<rest-of-checksum>`
- Hierarchical directory structure based on checksum

### Critical Patterns

**Type System** (Python 2/3 compatibility):
```python
safetype = str      # Unicode/str for all internal strings
rawtype = bytes     # Raw bytes for file I/O
ingest(raw) → safetype    # Convert bytes to string
egest(safe) → rawtype     # Convert string to bytes
```

**Path Abstraction**:
- Always use `Path` class from fs.py, not raw strings
- Supports methods: `.join()`, `.parent()`, `.isfile()`, `.isdir()`, `.checksum()`

**Ignore Patterns**:
- `.farmignore` specifies patterns to skip (like `.gitignore`)
- Uses fnmatch for pattern matching

**Blob Reverser**:
- `reverser(path_in_blobstore)` extracts checksum from path via regex
- Handles both legacy and new path formats

**Progress Bars**:
- Custom implementations: `csum_pbar()`, `tree_pbar()`, `list_pbar()`
- Disabled with `--quiet` flag

### Data Flow Example: Pull Operation

```
farmfs pull origin snapshot
  ↓
Volume(local) + Volume(remote)
  ↓
tree_diff(remote_snap, local_tree) → yields SnapDeltas
  ↓
tree_patcher applies each delta:
  - LINK: import blob via blobstore.read_handle()
  - DIR: create directory
  - REMOVED: delete file/dir
  ↓
fs.ensure_symlink() creates link to blob
  ↓
Local tree now matches remote snapshot
```

## Common Development Tasks

### Adding a New CLI Command

1. Add command to `UI_USAGE` string in ui.py
2. Create handler function that takes docopt args dict
3. Implement using Volume API (volume.py)
4. Add tests in tests/ directory

### Working with Snapshots

1. Create: `volume.snap_make(name)` → stores TreeSnapshot in keydb
2. Read: `volume.snap_read(name)` → returns TreeSnapshot
3. Diff: `tree_diff(old_snapshot, new_snapshot)` → yields SnapDeltas
4. Patch: `tree_patcher(volume, remote_volume)` → applies deltas

### Testing Patterns

- **Unit tests**: Use fixtures in conftest.py
- **Regression tests**: Use pytest in tests/ directory
- **Performance tests**: Use pytest -s in perf/ directory
- **Coverage**: Run `pytest --cov` to generate reports

### Debugging Tools

`farmdbg` provides low-level inspection:
```bash
farmdbg reverse <checksum>           # Find blob by checksum
farmdbg key read|write|delete <key>  # Inspect keydb
farmdbg walk keys|userdata|snap      # Walk data structures
farmdbg fix link <file> <target>     # Repair broken links
farmdbg rewrite-links <target>       # Batch fix links
```

## Testing Strategy

**Regression Suite**: Core functionality tests
- Run with: `pytest`
- Located in: `tests/`

**Performance Tests**: Decision-making and benchmarking
- Run with: `pytest -s perf/` or `tox -e py37-perf`
- Located in: `perf/`
- Not run in CI by default

**CI/CD**: Full coverage across Python versions
- Run with: `tox`
- Tests py37, py39, pypy3
- Includes linting (flake8)
- Generates coverage reports

## Important Notes

### Performance Considerations

- Use `compose()` over `pipeline()` for function composition (less call overhead)
- For chained iterators, pipeline and compose have equal performance
- PyPy3 is slower than cPython for FarmFS due to iterator-heavy code and JIT limitations

### Code Style

- Line length limit: 160 characters
- Flake8 ignores: E731 (lambda), E302 (blank lines after function), E306 (blank lines before nested def)
- Use yapf and isort for formatting
- Run `tox -e lint` before committing

### Immutability Design

- **Frozen files**: Read-only symlinks to blobs (never modify directly)
- **Thaw operation**: Copy blob to working directory as regular file
- **Freeze operation**: Move file to blobstore, create symlink back

### Deduplication

- Identical files (by MD5) automatically share blob
- Saves disk space without explicit dedup logic
- Tree operations check blob existence before copying

### Branch Context

Currently on `progress_bar` branch. Recent work includes fsck overhaul and snapshot improvements.
