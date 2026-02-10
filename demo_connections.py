"""
Handle Pipelines: Scoped Lifetimes, Connection Reuse, and Automatic Retries

This demo explores a pattern for composing IO operations into lazy pipelines
where connections have scoped lifetimes, are reused via pooling, and
transient failures are retried automatically.

It downloads objects from an S3 bucket to a local temp directory using
S3Lib's ConnectionPool and farmfs's retryFdIo2. The pipeline is built
from composable Iterator -> Iterator stages wired together with pipeline().

## Thunks: separating description from acquisition

The core idea is separating the *description* of an IO resource from its
*acquisition*. Instead of passing open handles through a pipeline, we pass
"thunks" — zero-argument functions that produce a handle when called. This
gives us three properties:

  1. Scoped lifetimes: Handles are only open for the duration of the
     operation that needs them. The consumer calls the thunk inside a
     `with` block, so the handle is opened, used, and closed in a tight
     scope. No handle is held open while the pipeline pulls the next item
     upstream.

  2. Connection reuse: The S3 ConnectionPool manages a set of TCP
     connections. Each pool.lease() call acquires an existing connection
     (MRU strategy) or creates a new one if needed. When the lease's
     `with` block exits, the connection is returned to the pool — not
     closed. Subsequent operations reuse the warm connection, avoiding
     TCP handshake overhead. In practice, only 2 TCP connections are
     created (one for listing, one for downloads) regardless of how many
     objects are transferred. The thunk pattern makes this transparent:
     the pipeline stage just calls its thunk and gets a connection,
     without knowing whether it's fresh or reused.

  3. Automatic retries: Because retryFdIo2 receives thunks rather than
     open handles, it can reconstruct fresh handles on each retry attempt.
     If a transient error occurs mid-transfer, both src and dst handles
     are closed (via the `with` block exiting — returning the connection
     to the pool), then brand new handles are acquired for the next
     attempt. For local file writes, re-opening with 'wb' truncates the
     destination, making the retry idempotent.

## Demand-driven evaluation

The pipeline is demand-driven (pull-based). The consumer at the end pulls
one value, which cascades back through all stages via Python generators.
Each object is fully processed (listed, downloaded, written, closed)
before the next object is even discovered from the bucket listing. This
means resource usage is O(1) in the number of objects — only one
connection lease and one file handle are active at any time.

## Pipeline stages

  list_keys              List S3 keys from bucket (via pooled connection)
  take(n)                Limit to first n keys (for experimentation)
  s3_read_thunk_factory  Produce (key, src_thunk) where src_thunk leases
                           a pooled connection and calls get_object
  dst_thunk_factory      Produce (key, src_thunk, dst_thunk) where dst_thunk
                           opens a local file for writing
  download_with_retry    Call retryFdIo2 with the thunk pair and s3_download;
                           on transient failure, both thunks reconstruct
                           fresh handles and retry

## Running

  python demo_connections.py               # normal output
  FARMFS_DEBUG=1 python demo_connections.py  # with retry logging and
                                             # S3 request/socket details

With FARMFS_DEBUG=1, observe that the socket fd and local_port are reused
across GET requests — confirming TCP connection reuse via the pool.
"""

from farmfs.fs import Path
from farmfs.util import pipeline, retryFdIo2, take
from farmfs.blobstore import s3_exceptions
from s3lib.pool import ConnectionPool
from s3lib.ui import load_creds
from shutil import copyfileobj
from tempfile import mkdtemp
from typing import Callable, Generator, Iterator, IO, Tuple

BUCKET = "chomsonforms"

HandleThunk = Callable[[], IO]

def handle_thunk(path: Path, mode: str) -> HandleThunk:
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
    def factory(keys: Iterator[str]) -> Generator[Tuple[str, HandleThunk], None, None]:
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
    def factory(pairs: Iterator[Tuple[str, HandleThunk]]) -> Generator[Tuple[str, HandleThunk, HandleThunk], None, None]:
        for key, src_thunk in pairs:
            # Use just the filename portion of the key for the local path.
            name = key.rsplit("/", 1)[-1] if "/" in key else key
            dst_path = dst_root.join(name)
            dst_thunk = handle_thunk(dst_path, 'wb')
            yield (key, src_thunk, dst_thunk)
    return factory

# Download S3 object to local file using retryFdIo2.
# src is a ConnectionLease, dst is a local file handle.
def s3_download(bucket, key):
    def download(src_conn, dst_file):
        print("  Downloading %s/%s ..." % (bucket, key))
        resp = src_conn.get_object(bucket, key)
        if resp.status != 200:
            raise RuntimeError("S3 returned status %d for %s" % (resp.status, key))
        copyfileobj(resp, dst_file)
        print("  Downloaded %s/%s -> %s" % (bucket, key, dst_file.name))
    return download

def download_with_retry(bucket):
    def stage(triples: Iterator[Tuple[str, HandleThunk, HandleThunk]]) -> Generator[str, None, None]:
        for key, src_thunk, dst_thunk in triples:
            print("Downloading %s with retry..." % key)
            retryFdIo2(src_thunk, dst_thunk, s3_download(bucket, key), s3_exceptions)
            print("Download succeeded: %s" % key)
            yield key
    return stage

# --- Main ---
access_id, secret = load_creds(None)
tmp_dir = Path(mkdtemp(prefix="farmfs_demo_"))
print("Temp dir: %s" % tmp_dir)

with ConnectionPool(access_id, secret, max_connections=2) as pool:
    print("Pool stats: %s" % pool.stats())
    s3_to_local_pipeline = pipeline(
        list_keys(pool, BUCKET),
        take(2),
        s3_read_thunk_factory(pool, BUCKET),
        dst_thunk_factory(tmp_dir),
        download_with_retry(BUCKET))
    print("Consuming S3 download pipeline...")
    for key in s3_to_local_pipeline([None]):
        print("Done: %s" % key)
        print("Pool stats: %s" % pool.stats())
    print("Final pool stats: %s" % pool.stats())
print("Done.")
