"""
# Managed IO Pipelines: S3 Demo (v2)

Built from the same primitives as the file handle demo.
The S3 side, file side, and IO side each stay in their own lane.
"""

from collections.abc import Callable
from contextlib import contextmanager, AbstractContextManager as ContextManager
from farmfs.blobstore import is_s3_exception
from farmfs.fs import Path
from farmfs.util import RetriesExhausted, pipeline, fmap, pfmaplazy, take
from shutil import copyfileobj
from tempfile import mkdtemp
from typing import IO, Iterator, Tuple
import random
import time

from s3lib.pool import ConnectionPool
from s3lib.ui import load_creds

# ---------------------------------------------------------------------------
# Core primitives (same as file demo)
# ---------------------------------------------------------------------------

type HandleFn[T] = Callable[[], ContextManager[T]]
type Thunk[T] = Callable[[], T]


def withHandles2[X, Y, Z](get_src: HandleFn[X], get_dst: HandleFn[Y], io_fn: Callable[[X, Y], Z]) -> Z:
    with get_src() as src, get_dst() as dst:
        return io_fn(src, dst)


def withHandles2Thunk[X, Y, Z](get_src: HandleFn[X], get_dst: HandleFn[Y], io_fn: Callable[[X, Y], Z]) -> Thunk[Z]:
    def thunk():
        return withHandles2(get_src, get_dst, io_fn)
    return thunk


def retry[Z](fn: Thunk[Z], retry_exception: Callable[[Exception], bool], tries: int = 3) -> Z:
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
                time.sleep(4 ** (attempt + 1))
    raise RetriesExhausted("retry operation failed", failed_attempts)


def retryThunk[Z](fn: Thunk[Z], retry_exception: Callable[[Exception], bool], tries: int = 3) -> Thunk[Z]:
    def thunk():
        return retry(fn, retry_exception, tries)
    return thunk


def uncurry[A, B, Z](fn: Callable[[A, B], Z]) -> Callable[[Tuple[A, B]], Z]:
    def uncurried(pair):
        a, b = pair
        return fn(a, b)
    return uncurried


# ---------------------------------------------------------------------------
# File side — knows about paths, knows nothing about S3 or what data is copied
# ---------------------------------------------------------------------------

def file_thunk(path: Path, mode: str) -> HandleFn[IO]:
    def thunk():
        return path.open(mode)
    return thunk


# ---------------------------------------------------------------------------
# IO side — knows about file descriptors, knows nothing about paths or S3
# ---------------------------------------------------------------------------

def copy_bytes(src_fd: IO[bytes], dst_fd: IO[bytes]) -> None:
    copyfileobj(src_fd, dst_fd)


# ---------------------------------------------------------------------------
# S3 side — knows about S3, knows nothing about local paths or copying
# ---------------------------------------------------------------------------

@contextmanager
def _s3_get(pool: ConnectionPool, bucket: str, key: str, fail_rate: float = 0.0):
    """Lease a connection, call get_object, yield the response as a readable handle."""
    with pool.lease() as conn:
        print("  Leasing connection for GET %s/%s" % (bucket, key))
        if random.random() < fail_rate:
            raise RuntimeError("Synthetic failure for %s/%s" % (bucket, key))
        resp = conn.get_object(bucket, key)
        if resp.status != 200:
            raise RuntimeError("S3 returned status %d for %s/%s" % (resp.status, bucket, key))
        print("  Downloading %s/%s ..." % (bucket, key))
        yield resp
        print("  Downloaded %s/%s" % (bucket, key))


def s3_get_thunk(pool: ConnectionPool, bucket: str, key: str, fail_rate: float = 0.0) -> HandleFn:
    """S3 src thunk: fully encapsulates pool, bucket, key. Returns a readable handle."""
    def thunk():
        return _s3_get(pool, bucket, key, fail_rate)
    return thunk


def s3_list_keys(pool: ConnectionPool, bucket: str) -> Iterator[str]:
    with pool.lease() as conn:
        for key in conn.list_bucket(bucket):
            yield key


# ---------------------------------------------------------------------------
# Wiring — combines the three concerns without any leaking across them
# ---------------------------------------------------------------------------

def download_with_retry(get_src: HandleFn, get_dst: HandleFn) -> None:
    """Given a src thunk and a dst thunk, copy with retries. Knows nothing about S3 or paths."""
    return retryThunk(withHandles2Thunk(get_src, get_dst, copy_bytes), is_s3_exception)()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

access_id, secret = load_creds(None)
tmp_dir = Path(mkdtemp(prefix="farmfs_demo_"))
print("Temp dir: %s" % tmp_dir)

BUCKET = "chomsonforms"
LIMIT = 5
FAIL_RATE = 0.3

with ConnectionPool(access_id, secret, max_connections=3) as pool:
    print("Pool stats: %s" % pool.stats())

    keys: Iterator[str] = s3_list_keys(pool, BUCKET)

    # S3 side: key -> src thunk
    get_src = lambda key: s3_get_thunk(pool, BUCKET, key, FAIL_RATE)

    # File side: key -> dst thunk
    dst_path = lambda key: tmp_dir.join(key.rsplit("/", 1)[-1] if "/" in key else key)
    get_dst = lambda key: file_thunk(dst_path(key), 'wb')

    # Pair them up, then download in parallel
    src_dst_pair = lambda key: (get_src(key), get_dst(key))

    parallel_download = pfmaplazy(uncurry(download_with_retry), workers=2, buffer_size=2)

    s3_to_local = pipeline(
        take(LIMIT),
        fmap(src_dst_pair),
        parallel_download,
    )

    print("Consuming S3 download pipeline...")
    for result in s3_to_local(keys):
        print("Pool stats: %s" % pool.stats())

    print("Final pool stats: %s" % pool.stats())

print("Done.")
