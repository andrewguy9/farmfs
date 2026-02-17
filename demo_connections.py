"""
Handle Pipelines: Scoped Lifetimes, Connection Reuse, Parallel Downloads,
and Automatic Retries

## TL;DR

Don't pass open connections through a pipeline — pass functions that *create*
connections ("thunks"). This lets you:
  - Close connections immediately after each operation (scoped lifetimes)
  - Return connections to a pool for reuse (no redundant TCP handshakes)
  - Retry failed operations with fresh connections (automatic retries)
  - Run multiple operations in parallel with bounded concurrency

This demo downloads S3 objects through a lazy generator pipeline. Only 3 TCP
connections serve all requests: 1 for listing, 2 for parallel downloads.
Connections are pooled and reused, not reopened. If a download fails, the
thunks reconstruct fresh handles and retry automatically.

---

This demo explores a pattern for composing IO operations into lazy pipelines
where connections have scoped lifetimes, are reused via pooling, downloads
run in parallel, and transient failures are retried automatically.

It downloads objects from an S3 bucket to a local temp directory using
S3Lib's ConnectionPool and pfmaplazy for bounded parallelism. The pipeline
is built from composable Iterator -> Iterator stages wired together with
pipeline().

## Composable building blocks

The IO concerns — scoped lifetimes, retries, and parallelism — are
decomposed into small, single-responsibility functions that compose:

  - withHandles2(get_src, get_dst, ioFn): Opens two context managers via
    their thunks, calls ioFn with the unwrapped values, and ensures both
    are closed when done. Owns scoped lifetimes only — no retry logic.

  - withHandles2Thunk(get_src, get_dst, ioFn): Higher-order version of withHandles2
    that returns a zero-argument thunk. This composes more naturally with retry,
    letting you write:
        retry(withHandles2Thunk(src, dst, ioFn), pred)
    instead of needing a lambda wrapper.

  - retry(fn, retry_exception, tries): Calls a zero-argument callable up
    to `tries` times with exponential backoff. Handled exceptions (where
    retry_exception returns True) are retried; unhandled exceptions are
    re-raised immediately. Owns retry + backoff only — knows nothing
    about handles or connections.

  - pfmaplazy(fn, workers, buffer_size): Applies fn across a stream in
    parallel with bounded concurrency and backpressure. Owns parallelism
    only.

These compose at the call site:

    retry(
        lambda: withHandles2(src_thunk, dst_thunk, s3_download(bucket, key)),
        is_s3_exception,
    )

retry calls the lambda on each attempt. The lambda calls withHandles2,
which opens fresh handles (or leases pooled connections) via the thunks,
runs the download, and closes/returns handles. If the download fails with
a handled exception, withHandles2's `with` block exits (returning the
connection to the pool), and retry sleeps then tries again with fresh
handles.

## Thunks: separating description from acquisition

The core idea is separating the *description* of an IO resource from its
*acquisition*. Instead of passing open handles through a pipeline, we pass
"thunks" — zero-argument functions that produce a context manager when
called. This gives us:

  1. Scoped lifetimes: withHandles2 calls the thunk inside a `with` block,
     so the handle is opened, used, and closed in a tight scope. No handle
     is held open while the pipeline pulls the next item upstream.

  2. Connection reuse: The S3 ConnectionPool manages a set of TCP
     connections. Each pool.lease() call acquires an existing connection
     (MRU strategy) or creates a new one if needed. When the lease's
     `with` block exits, the connection is returned to the pool — not
     closed. Subsequent operations reuse the warm connection, avoiding
     TCP handshake overhead. The thunk pattern makes this transparent:
     the pipeline stage just calls its thunk and gets a connection,
     without knowing whether it's fresh or reused.

  3. Automatic retries: Because retry receives a thunk that calls
     withHandles2, each attempt reconstructs fresh handles. If a transient
     error occurs mid-transfer, both handles are closed (via the `with`
     block exiting — returning the connection to the pool), then brand new
     handles are acquired for the next attempt. For local file writes,
     re-opening with 'wb' truncates the destination, making the retry
     idempotent.

## Parallel downloads with bounded concurrency

The download stage uses pfmaplazy(download_one(...), workers=2) to run
up to 2 downloads concurrently in a thread pool. pfmaplazy applies an
element-wise function across the stream with backpressure — it won't
greedily consume the entire input iterator, keeping memory bounded.

The ConnectionPool's max_connections=3 matches the concurrency: 1
connection held by the listing generator + 2 for the parallel download
workers. Each worker leases a connection from the pool via its thunk,
uses it for one GET, and returns it. The pool's MRU strategy means hot
connections are reused first.

The @uncurry decorator on download_one unpacks the (key, src_thunk,
dst_thunk) tuple into positional arguments, keeping the function
signature explicit rather than manually destructuring a tuple.

## Demand-driven evaluation

The pipeline is demand-driven (pull-based) up to the pfmaplazy stage.
Upstream stages (list_keys, take, thunk factories) are lazy generators
that only produce values when pulled. pfmaplazy introduces bounded
eagerness: it pulls up to workers + buffer_size items ahead to keep
the thread pool fed, but no more. Downstream of pfmaplazy, results
are yielded as futures complete.

In practice, only 3 TCP connections are created (1 for listing, 2 for
downloads) regardless of how many objects are transferred.

## Pipeline stages

  list_keys              List S3 keys from bucket (via pooled connection)
  take(n)                Limit to first n keys (for experimentation)
  s3_read_thunk_factory  Produce (key, src_thunk) where src_thunk leases
                           a pooled connection and calls get_object
  dst_thunk_factory      Produce (key, src_thunk, dst_thunk) where dst_thunk
                           opens a local file for writing
  pfmaplazy(             Apply download_one in parallel across 2 worker
    download_one,          threads with backpressure; each invocation
    workers=2)             composes retry + withHandles2 + s3_download

## Running

  python demo_connections.py               # normal output
  FARMFS_DEBUG=1 python demo_connections.py  # with retry logging and
                                             # S3 request/socket details

With FARMFS_DEBUG=1, observe that:
  - The socket fd and local_port are reused across GET requests,
    confirming TCP connection reuse via the pool.
  - Download log lines interleave, confirming parallel execution
    (e.g. "Downloading X..." and "Downloading Y..." appear before
    either "Downloaded X" or "Downloaded Y").
  - Pool stats show in_use hitting 3 during peak concurrency (1 list
    + 2 downloads) and returning to available: 3 at the end.
"""

from collections.abc import Callable
from contextlib import AbstractContextManager as ContextManager
from farmfs.blobstore import is_s3_exception
from farmfs.fs import Path
from farmfs.util import RetriesExhausted, pipeline, take, pfmaplazy, uncurry
import time
from s3lib import Connection
from s3lib.pool import ConnectionPool
from s3lib.ui import load_creds
from shutil import copyfileobj
from tempfile import mkdtemp
from typing import Generator, Iterator, IO, Tuple

def handle_thunk(path: Path, mode: str) -> Callable[[], IO]:
    print("  Creating file thunk for %s" % path)
    def thunk():
        print("  Opening %s with mode %s" % (path, mode))
        return path.open(mode)
    return thunk

# List S3 keys from a bucket using a leased connection from the pool.
def list_keys(pool, bucket):
    def lister(prefixes: Iterator[str]) -> Generator[str, None, None]:
        for prefix in prefixes:
            with pool.lease() as conn:
                print("Listing bucket=%s prefix=%s (conn reused from pool)" % (bucket, prefix))
                for key in conn.list_bucket(bucket, prefix=prefix):
                    print("  Found key: %s" % key)
                    yield key
    return lister

# For each S3 key, produce a (key, src_thunk) pair.
# The src_thunk leases a connection from the pool and calls get_object.
def s3_read_thunk_factory(pool, bucket):
    def factory(keys: Iterator[str]) -> Generator[Tuple[str, Callable[[], ContextManager[Connection]]], None, None]:
        for key in keys:
            def make_src_thunk(k):
                def src_thunk():
                    print("  Leasing connection for GET %s/%s" % (bucket, k))
                    return pool.lease()
                return src_thunk
            yield (key, make_src_thunk(key))
    return factory

# For each (key, src_thunk), add a dst_thunk that opens a local file for writing.
def dst_thunk_factory(dst_root: Path):
    def factory(pairs: Iterator[Tuple[str, Callable[[], ContextManager[Connection]]]]) -> Generator[Tuple[str, Callable[[], ContextManager[Connection]], Callable[[], IO[bytes]]], None, None]:
        for key, src_thunk in pairs:
            # Use just the filename portion of the key for the local path.
            name = key.rsplit("/", 1)[-1] if "/" in key else key
            dst_path = dst_root.join(name)
            dst_thunk = handle_thunk(dst_path, 'wb')
            yield (key, src_thunk, dst_thunk)
    return factory

# retry: retries a zero-argument callable up to `tries` times.
# Handled exceptions (where retry_exception returns True) are retried with
# exponential backoff. Unhandled exceptions are re-raised immediately.
# Raises RetriesExhausted if all attempts fail with handled exceptions.
def retry[Z](fn: Callable[[], Z],
             retry_exception: Callable[[Exception], bool],
             tries: int = 3) -> Z:
    if tries < 1:
        raise ValueError("tries must be at least 1")
    failed_attempts = []
    for attempt in range(tries):
        try:
            return fn()
        except Exception as e:
            if not retry_exception(e):
                raise
            failed_attempts.append((attempt + 1, e))
            if attempt < tries - 1:
                sleep_time = 4 ** (attempt + 1)
                time.sleep(sleep_time)
    raise RetriesExhausted("retry operation failed", failed_attempts)

# A handle thunk is a zero-argument function that produces a context manager use in an IO operation.
type HandleFn[T] = Callable[[], ContextManager[T]]

def withHandles2[X, Y, Z](get_src: HandleFn[X], get_dst: HandleFn[Y], ioFn: Callable[[X, Y], Z]) -> Z:
    """
    withHandles2: scoped lifetime for two handles.
    Opens both handles via their thunks, calls ioFn with the opened handles,
    and ensures both are closed when done (via context manager exit).
    This is the "single attempt" core that retryFdIo2 used to inline.
    """
    with get_src() as src, get_dst() as dst:
        return ioFn(src, dst)
    
def withHandles2Thunk[X, Y, Z](get_src: HandleFn[X], get_dst: HandleFn[Y], ioFn: Callable[[X, Y], Z]) -> Callable[[], Z]:
    """
    Higher-order version of withHandles2 that returns a zero-argument thunk.
    This composes more naturally with retry, letting you write:
        retry(withHandles2Thunk(src, dst, ioFn), pred)
    instead of needing a lambda wrapper.
    """
    def thunk():
        with get_src() as src, get_dst() as dst:
            return ioFn(src, dst)
    return thunk
    
# Download S3 object to local file using retryFdIo2.
# src is a ConnectionLease, dst is a local file handle.
def s3_download(bucket, key):
    def download(src_conn, dst_file):
        print("  s3_download: type of src_conn: %s, type of dst_file: %s" % (type(src_conn), type(dst_file)))
        print("  Downloading %s/%s ..." % (bucket, key))
        resp = src_conn.get_object(bucket, key)
        if resp.status != 200:
            raise RuntimeError("S3 returned status %d for %s" % (resp.status, key))
        copyfileobj(resp, dst_file)
        print("  Downloaded %s/%s -> %s" % (bucket, key, dst_file.name))
    return download

# Element-wise download function for use with pfmaplazy.
# @uncurry unpacks the (key, src_thunk, dst_thunk) tuple into positional args.
# Composes retry (retries with backoff) and withHandles2 (scoped lifetimes).
def download_one(bucket: str):
    @uncurry
    def download(key: str, src_thunk: HandleFn[Connection], dst_thunk: HandleFn[IO[bytes]]) -> str:
        print("download_one: type of src_thunk: %s, type of dst_thunk: %s" % (type(src_thunk), type(dst_thunk)))
        print("Downloading %s with retry..." % key)
        retry(
            withHandles2Thunk(src_thunk, dst_thunk, s3_download(bucket, key)),
            is_s3_exception,
        )
        print("Download succeeded: %s" % key)
        return key
    return download

# --- Main ---
access_id, secret = load_creds(None)
tmp_dir = Path(mkdtemp(prefix="farmfs_demo_"))
print("Temp dir: %s" % tmp_dir)

BUCKET = "chomsonforms"
LIMIT = 5

with ConnectionPool(access_id, secret, max_connections=3) as pool:
    print("Pool stats: %s" % pool.stats())
    s3_to_local_pipeline = pipeline(
        list_keys(pool, BUCKET),
        take(LIMIT),
        s3_read_thunk_factory(pool, BUCKET),
        dst_thunk_factory(tmp_dir),
        pfmaplazy(download_one(BUCKET), workers=2, buffer_size=2))
    print("Consuming S3 download pipeline...")
    for key in s3_to_local_pipeline([None]):
        print("Done: %s" % key)
        print("Pool stats: %s" % pool.stats())
    print("Final pool stats: %s" % pool.stats())
print("Done.")
