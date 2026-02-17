"""
Managed IO Pipelines

Don't pass open connections through a pipeline — pass thunks that *create*
them. A thunk is a zero-argument function that returns a context manager.
This separates the description of a resource from its acquisition, giving
you scoped lifetimes, connection reuse, retries, and parallelism — all
from composing a few small functions.

The building blocks:

  withHandles2Thunk(src, dst, io) -> thunk
      Bracket two resources around an IO action. Returns a thunk that,
      when called, opens both handles, runs io, and closes them.

  retry(thunk, predicate)
      Call a thunk up to N times with backoff. Knows nothing about handles.

  pfmaplazy(fn, workers)
      Apply fn across a stream in parallel with backpressure.

These compose directly:

    retry(withHandles2Thunk(src, dst, io), is_transient)

Each retry attempt gets fresh handles. If a connection fails mid-transfer,
the with-block exits (returning it to the pool), and the next attempt
leases a new one. Connection pooling is transparent — the src thunk
calls pool.lease(), so the pipeline gets reuse without knowing about it.

This demo downloads S3 objects to a temp directory through a lazy pipeline
with a synthetic 30% failure rate injected into the IO function. A sample
run shows all three properties working together:

  Parallelism — two downloads run concurrently:

    Downloading chomsonforms/00b949e8...
    Downloading chomsonforms/011bf656...

  Retries with fresh handles — 0408430e fails on attempt 1, succeeds on
  attempt 2 with a freshly leased connection and reopened file:

    Leasing connection for GET chomsonforms/0408430e...   <- attempt 1
    ...
    Leasing connection for GET chomsonforms/0408430e...   <- attempt 2
    Downloading chomsonforms/0408430e...
    Downloaded chomsonforms/0408430e...
    Download succeeded: 0408430e

  Connection pooling — 3 TCP connections serve all 5 downloads. At peak
  concurrency all 3 are in use (1 listing + 2 download workers). At the
  end all 3 are returned to the pool, none leaked:

    Pool stats: {total: 3, available: 0, in_use: 3}   <- peak
    Final pool stats: {total: 3, available: 3, in_use: 0}

  python demo_connections.py               # normal output
  FARMFS_DEBUG=1 python demo_connections.py  # connection reuse details
"""

from collections.abc import Callable
from contextlib import AbstractContextManager as ContextManager
from farmfs.blobstore import is_s3_exception
from farmfs.fs import Path
from farmfs.util import RetriesExhausted, pipeline, take, pfmaplazy
import random
import time
from s3lib import Connection
from s3lib.pool import ConnectionPool
from s3lib.ui import load_creds
from shutil import copyfileobj
from tempfile import mkdtemp
from typing import Generator, Iterator, IO

# A handle fn is a zero-argument function that produces a context manager for use in an IO operation.
type HandleFn[T] = Callable[[], ContextManager[T]]

# TODO handle is too generic. This is a "file_thunk"?
def handle_thunk(path: Path, mode: str) -> Callable[[], IO]:
    print("  Creating file thunk for %s" % path)
    def thunk():
        print("  Opening %s with mode %s" % (path, mode))
        return path.open(mode)
    return thunk

# List S3 keys from a bucket using a leased connection from the pool.
# TODO is this s3_list_keys
def list_keys(pool, bucket) -> Generator[str, None, None]:
    with pool.lease() as conn:
        print("Listing bucket=%s (conn reused from pool)" % (bucket))
        for key in conn.list_bucket(bucket):
            print("  Found key: %s" % key)
            yield key

# key -> HandleFn factories: given a key, return a thunk that produces a handle.

# TODO this isn't a reader, its a "connection getter"
def s3_bucket_reader(pool, bucket) -> Callable[[str], HandleFn[Connection]]:
    """Given a key, return a thunk that leases a pooled S3 connection."""
    def factory(key: str) -> HandleFn[Connection]:
        def thunk():
            print("  Leasing connection for GET %s/%s" % (bucket, key))
            return pool.lease()
        return thunk
    return factory

def directory_writer(dst_root: Path) -> Callable[[str], HandleFn[IO[bytes]]]:
    """Given a key, return a thunk that opens a local file for writing."""
    def factory(key: str) -> HandleFn[IO[bytes]]:
        name = key.rsplit("/", 1)[-1] if "/" in key else key
        dst_path = dst_root.join(name)
        return handle_thunk(dst_path, 'wb')
    return factory

# TODO convert comments to docstrings.
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
    # TODO withHandles2Thunk should be implemented in terms of withHandles2 to avoid behaviroal drift.
    def thunk():
        with get_src() as src, get_dst() as dst:
            return ioFn(src, dst)
    return thunk
    
# S3-specific io factory: given a bucket, returns a key->ioFn that
# GETs the object from S3 and copies it to a writable file.
# TODO s3_object_copier is really a s3_bucket_reader
def s3_object_copier(bucket: str, fail_rate: float = 0.0) -> Callable[[str], Callable[[Connection, IO[bytes]], None]]:
    # TODO s3_object_copy is really a s3_object_reader
    def s3_object_copy(key: str) -> Callable[[Connection, IO[bytes]], None]:
        # TODO s3_do_copy_io is really s3_object_read
        def s3_do_copy_io(src_conn: Connection, dst_file: IO[bytes]) -> None:
            if random.random() < fail_rate:
                raise RuntimeError("Synthetic failure for %s/%s" % (bucket, key))
            print("  Downloading %s/%s ..." % (bucket, key))
            resp = src_conn.get_object(bucket, key)
            if resp.status != 200:
                raise RuntimeError("S3 returned status %d for %s" % (resp.status, key))
            copyfileobj(resp, dst_file)
            print("  Downloaded %s/%s -> %s" % (bucket, key, dst_file.name))
        return s3_do_copy_io
    return s3_object_copy

# --- Main ---
access_id, secret = load_creds(None)
tmp_dir = Path(mkdtemp(prefix="farmfs_demo_"))
print("Temp dir: %s" % tmp_dir)

BUCKET = "chomsonforms"
LIMIT = 5

with ConnectionPool(access_id, secret, max_connections=3) as pool:
    print("Pool stats: %s" % pool.stats())

    # Make a lazy list of keys to process
    keys: Iterator[str] = list_keys(pool, BUCKET)

    # Composable parts:
    #   src: key -> thunk that leases a pooled S3 connection
    #   dst: key -> thunk that opens a local file for writing
    #   io:  key -> function that GETs the S3 object and copies to file
    src_factory = s3_bucket_reader(pool, BUCKET)
    dst_factory = directory_writer(tmp_dir)
    io_factory = s3_object_copier(BUCKET, fail_rate=0.3)

    # TODO download is really download to dir.
    def download(key: str) -> str:
        print("Downloading %s with retry..." % key)
        thunk = withHandles2Thunk(src_factory(key), dst_factory(key), io_factory(key))
        retry(thunk, is_s3_exception)
        print("Download succeeded: %s" % key)
        return key

    # Pipeline: limit keys, then download in parallel.
    s3_to_local_pipeline = pipeline(
        take(LIMIT),
        pfmaplazy(download, workers=2, buffer_size=2))
    print("Consuming S3 download pipeline...")
    for key in s3_to_local_pipeline(keys):
        print("Done: %s" % key)
        print("Pool stats: %s" % pool.stats())
    print("Final pool stats: %s" % pool.stats())
print("Done.")
