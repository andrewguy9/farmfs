"""
Handle Pipelines: Scoped Lifetimes and Automatic Retries

This demo explores a pattern for composing IO operations into lazy pipelines
where file handles (or network connections) have scoped lifetimes and
transient failures are retried automatically.

The core idea is separating the *description* of an IO resource from its
*acquisition*. Instead of passing open file handles through a pipeline, we
pass "thunks" — zero-argument functions that produce a handle when called.
This gives us two properties:

  1. Scoped lifetimes: Handles are only open for the duration of the
     operation that needs them. The consumer calls the thunk inside a
     `with` block, so the handle is opened, used, and closed in a tight
     scope. No handle is held open while the pipeline pulls the next item
     upstream.

  2. Automatic retries: Because retryFdIo2 receives thunks rather than
     open handles, it can reconstruct fresh handles on each retry attempt.
     If a transient IOError occurs mid-copy, both src and dst handles are
     closed (via the `with` block exiting), then brand new handles are
     opened for the next attempt. For writes, re-opening with 'wb'
     truncates the destination, making the retry idempotent.

The pipeline is demand-driven (pull-based). The consumer at the end pulls
one value, which cascades back through all stages via Python generators.
Each file is fully processed (listed, mapped, opened, copied, closed)
before the next file is even discovered from the directory listing. This
means memory usage is O(1) in the number of files — only one file's
handles are open at any time.

Pipeline stages:
  list_dirs          Enumerate directory entries (concatMap over input dirs)
  files_only         Filter to regular files, skip directories
  dst_path_factory   Map each src path to a (src, dst) pair via relative_to
  thunk_pair_factory Convert path pairs into (dst_path, src_thunk, dst_thunk)
  copy_with_retry    Call retryFdIo2 with the thunk pair and copyfileobj

The same thunk pattern generalizes to network connections: swap path.open()
for acquiring a connection from a pool or opening an HTTP/S3 stream, and
the retry + scoped lifetime machinery works unchanged.

Run with FARMFS_DEBUG=1 to see retry attempts and exponential backoff.
"""

from farmfs.fs import Path
from farmfs import cwd
from farmfs.util import pipeline, concatMap, retryFdIo2
from hashlib import md5
from random import random
from shutil import copyfileobj
from tempfile import mkdtemp
from typing import Callable, Generator, Iterator, Literal, IO, Tuple

def list_dir(path: Path) -> Generator[Path, None, None]:
    for item in path.dir_list():
        print("Found path: %s" % item)
        yield item

list_dirs = concatMap(list_dir)

Mode = Literal['r', 'w', 'rb', 'wb']
HandleThunk = Callable[[], IO]
def handle_thunk(path: Path, mode: Mode) -> HandleThunk:
    print("Creating thunk for %s with mode %s" % (path, mode))
    def thunk():
        print("Opening %s with mode %s" % (path, mode))
        return path.open(mode)
    return thunk

def io_factory(mode: Mode):
    def file_pipeline(paths: Iterator[Path]) -> Generator[HandleThunk, None, None]:
        for path in paths:
            thunk = handle_thunk(path, mode)
            yield thunk
    return file_pipeline

def checksum_pipeline(items: Iterator[HandleThunk]) -> Generator[str, None, None]:
    for thunk in items:
        with thunk() as f:
            print("Computing checksum for %s" % f.name)
            hasher = md5()
            while True:
                chunk = f.read(8192)
                if not chunk:
                    break
                hasher.update(chunk)
            yield hasher.hexdigest()

def unique_pipeline(items: Iterator[str]) -> Generator[str, None, None]:
    seen = set()
    for item in items:
        if item not in seen:
            print("New value: %s" % item)
            yield item
            seen.add(item)
        else:
            print("Duplicate value: %s" % item)

def files_only(paths: Iterator[Path]) -> Generator[Path, None, None]:
    for path in paths:
        if path.isfile():
            print("Yielding file: %s" % path)
            yield path
        else:
            print("Skipping non-file: %s" % path)

# For each input directory path, list the files, open them, compute their checksums and return the unique checksums.
checksum_dir_pipeline = pipeline(
    list_dirs,
    files_only,
    io_factory('rb'),
    checksum_pipeline,
    unique_pipeline)

# Build destination paths by mapping src paths relative to a source root into a dest root.
def dst_path_factory(src_root: Path, dst_root: Path):
    def make_dst_paths(src_paths: Iterator[Path]) -> Generator[Tuple[Path, Path], None, None]:
        for src_path in src_paths:
            relative = src_path.relative_to(src_root)
            dst_path = dst_root.join(relative)
            print("Mapped %s -> %s" % (src_path, dst_path))
            yield (src_path, dst_path)
    return make_dst_paths

# Produce (dst_path, src_thunk, dst_thunk) triples from (src_path, dst_path) pairs.
# dst_path is carried through so downstream stages can report what was written.
def thunk_pair_factory(pairs: Iterator[Tuple[Path, Path]]) -> Generator[Tuple[Path, HandleThunk, HandleThunk], None, None]:
    for src_path, dst_path in pairs:
        src_thunk = handle_thunk(src_path, 'rb')
        dst_thunk = handle_thunk(dst_path, 'wb')
        yield (dst_path, src_thunk, dst_thunk)

# Copy src to dst using retryFdIo2, yielding dst path on success.
def copy_with_retry(pairs: Iterator[Tuple[Path, HandleThunk, HandleThunk]]) -> Generator[Path, None, None]:
    for dst_path, src_thunk, dst_thunk in pairs:
        print("Copying with retry...")
        def flaky_copy(src, dst):
            if random() < 0.2:
                raise IOError("Simulated transient IO failure")
            copyfileobj(src, dst)
        retryFdIo2(src_thunk, dst_thunk, flaky_copy, retry_exception=lambda e: isinstance(e, IOError))
        print("Copy succeeded.")
        yield dst_path

# Pipeline: list dir -> files only -> pair with dst paths -> make thunk pairs -> copy with retry
tmp_dir = Path(mkdtemp(prefix="farmfs_demo_"))
print("Temp dir: %s" % tmp_dir)
copy_dir_pipeline = pipeline(
    list_dirs,
    files_only,
    dst_path_factory(cwd, tmp_dir),
    thunk_pair_factory,
    copy_with_retry)
print("Consuming copy pipeline...")
for result in copy_dir_pipeline([cwd]):
    print("Copied: %s" % result)
print("Done.")
