from __future__ import annotations

from functools import partial as functools_partial
from collections import defaultdict
from collections.abc import Callable, Sequence
import functools
import logging
import os
from farmfs.pipeline import pipeline, then  # noqa: F401,E402 - re-exported for callers
import sys
import time
from datetime import datetime, timezone, timedelta
from typing import (Any, Concatenate, ContextManager, IO, Dict,
                    Iterable, Iterator, List, Optional, ParamSpec, Protocol,
                    Tuple, TypeVar, TypeVarTuple, cast, overload)

from concurrent.futures import ThreadPoolExecutor, wait, Future, FIRST_COMPLETED
from concurrent.futures.thread import _threads_queues

# Configure module-level logger
logger = logging.getLogger(__name__)

# Enable debug logging via FARMFS_DEBUG environment variable
if os.environ.get('FARMFS_DEBUG'):
    logging.basicConfig(
        level=logging.DEBUG,
        format='[%(asctime)s] %(levelname)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        stream=sys.stderr
    )
    logger.setLevel(logging.DEBUG)


class RetriesExhausted(Exception):
    """Raised when all retry attempts have been exhausted.

    Captures the underlying exceptions from each failed attempt along with
    their context for debugging purposes.
    """

    def __init__(self, message: str, attempts: list[tuple[int, Exception]]):
        """
        Args:
            message: Description of the operation that failed
            attempts: List of (attempt_number, exception) tuples from each failed attempt
        """
        self.attempts = attempts
        self.message = message
        super().__init__(self._format_message())

    def _format_message(self):
        lines = [f"{self.message} after {len(self.attempts)} attempts:"]
        for attempt_num, exc in self.attempts:
            lines.append(f"  Attempt {attempt_num}: {type(exc).__name__}: {exc}")
        return "\n".join(lines)


X = TypeVar("X")
Y = TypeVar("Y")
Z = TypeVar("Z")

def bytes2str(b: bytes) -> str:
    return b.decode("utf-8")
def str2bytes(s: str) -> bytes:
    return s.encode("utf-8")

def ingest(d: bytes | str) -> str:
    """
    Convert bytes  to str
    """
    if isinstance(d, bytes):
        return bytes2str(d)
    elif isinstance(d, str):
        return d
    else:
        raise TypeError("Can't ingest data of type %s" % type(d))


def egest(s: str | bytes) -> bytes:
    """
    Convert str to bytes
    """
    if isinstance(s, bytes):
        return s
    elif isinstance(s, str):  # On python 2 str is bytes.
        return str2bytes(s)
    else:
        raise TypeError("Can't egest data of type %s" % type(s))


def empty_default(xs: Iterable[X], default: Iterable[X]) -> list[X]:
    """ "
    If zero length array is passed, returns default.
    Otherwise returns the origional array.
    """
    xs = list(xs)
    if len(xs) == 0:
        return list(default)
    else:
        return xs


def compose(f: Callable[[Y], X], g: Callable[..., Y]) -> Callable[..., X]:
    fn = lambda *args, **kwargs: f(g(*args, **kwargs))
    fn.__name__ = f.__name__ + "_" + g.__name__
    return fn


def composeFunctor(f: Callable[[Y], X], g: Callable[[Y], Y]) -> Callable[[Y], X]:
    out = lambda x: f(g(x))
    out.__name__ = "compose_functor_" + f.__name__ + "_" + g.__name__
    return out


P = ParamSpec("P")
R = TypeVar("R")
A = TypeVar("A")
B = TypeVar("B")
C = TypeVar("C")

@overload
def partial(fn: Callable[P, R], /) -> Callable[P, R]: ...
@overload
def partial(fn: Callable[Concatenate[A, P], R], a: A, /) -> Callable[P, R]: ...
@overload
def partial(fn: Callable[Concatenate[A, B, P], R], a: A, b: B, /) -> Callable[P, R]: ...
@overload
def partial(fn: Callable[Concatenate[A, B, C, P], R], a: A, b: B, c: C, /) -> Callable[P, R]: ...

def partial(fn: Callable[..., R], /, *args: object, **kwargs: object) -> Callable[..., R]:
    out = functools_partial(fn, *args, **kwargs)
    # functools.partial returns a "partial" object; type checkers don't track the narrowed callable well.
    # out = cast(Callable[..., R], out)
    setattr(out, "__name__", "partial_" + getattr(fn, "__name__", "callable"))
    return out


def concat(xss: Iterable[Iterable[X]]) -> Iterator[X]:
    """
    Flatten one level of nesting.

    Takes an iterable of iterables and yields each element from each
    inner iterable in order.

    This is equivalent to:

        (x for xs in xss for x in xs)

    Only one level is flattened; inner elements are yielded as-is.

    Example:
        concat([[1, 2], [3], [], [4, 5]])
        -> 1, 2, 3, 4, 5
    """
    for xs in xss:
        for x in xs:
            yield x


def fmap(func: Callable[[X], Y]) -> Callable[[Iterable[X]], Iterator[Y]]:
    """
    Lift a function into iterable space.

    Returns a function that applies `func` to each element of an input
    iterable, producing a lazy iterator of results.

    Equivalent to:

        lambda xs: map(func, xs)

    or:

        lambda xs: (func(x) for x in xs)

    This is useful for building iterable pipelines via composition,
    since it transforms a value-level function (X -> Y) into an
    iterable-level function (Iterable[X] -> Iterator[Y]).

    Example:
        inc = lambda x: x + 1
        f = fmap(inc)
        list(f([1, 2, 3]))
        -> [2, 3, 4]

    The returned iterator is lazy; `func` is applied as elements are
    consumed.
    """
    def mapped(collection: Iterable[X]) -> Iterator[Y]:
        return map(func, collection)

    mapped.__name__ = "mapped_" + func.__name__
    return mapped


Head = TypeVar("Head")
MappedHead = TypeVar("MappedHead")
Tail = TypeVarTuple("Tail")

def mapFirst(
    func: Callable[[Head], MappedHead],
) -> Callable[[tuple[Head, *Tail]], tuple[MappedHead, *Tail]]:
    def mapped(t: tuple[Head, *Tail]) -> tuple[MappedHead, *Tail]:
        head, *tail = t
        return (func(head), *cast(tuple[*Tail], tuple(tail)))
    return mapped

def mapSecond(func):
    def mapped(t):
        a, b = t
        return (a, func(b))

def concatMap(func: Callable[[X], Iterable[Y]]) -> Callable[[Iterable[X]], Iterator[Y]]:
    """
    Map and flatten (a.k.a. flatMap / bind).

    Returns a function that:
        1. Applies `func` to each element of an input iterable.
        2. Expects `func(x)` to return an iterable.
        3. Flattens the resulting iterables into a single iterator.

    Equivalent to:

        lambda xs: (y for x in xs for y in func(x))

    or:
        compose(concat, fmap(func))

    or:
        lambda xs: concat(map(func, xs))

    Example:
        f = concatMap(lambda x: range(x))
        f([1, 3, 2])
        -> 0, 0, 1, 2, 0, 1

    This is commonly known as flatMap in functional programming.
    """
    return compose(concat, fmap(func))

def pfmap(func: Callable[..., X], workers: int = 8):
    if workers < 1:
        raise ValueError("workers must be at least 1")

    def parallel_mapped(collection):
        with ThreadPoolExecutor(max_workers=workers) as executor:
            try:
                # Enqueue all work from collection.
                # XXX this is greedy, so we fully consume the input before
                # starting to produce output.
                for result in executor.map(func, collection):
                    yield result
            except KeyboardInterrupt as e:
                executor.shutdown(wait=False)
                executor._threads.clear()
                _threads_queues.clear()
                raise e

    parallel_mapped.__name__ = "pmapped_" + func.__name__
    return parallel_mapped

def pfmaplazy(
    func: Callable[[X], Y],
    workers: int = 8,
    buffer_size: int = 16,
) -> Callable[[Iterable[X]], Iterator[Y]]:
    if workers < 1:
        raise ValueError("workers must be at least 1")
    if buffer_size < 1:
        raise ValueError("buffer_size must be at least 1")

    max_in_flight = workers + buffer_size

    @functools.wraps(func)
    def parallel_mapped_lazy(collection: Iterable[X]) -> Iterator[Y]:
        # NOTE: This yields results in completion order.
        with ThreadPoolExecutor(max_workers=workers) as ex:
            in_flight: set[Future[Y]] = set()

            def drain_one() -> Iterator[Y]:
                done, _ = wait(in_flight, return_when=FIRST_COMPLETED)
                for fut in done:
                    in_flight.remove(fut)
                    yield fut.result()

            try:
                for item in collection:
                    in_flight.add(ex.submit(func, item))
                    if len(in_flight) >= max_in_flight:
                        yield from drain_one()

                while in_flight:
                    yield from drain_one()

            except BaseException:
                # Try to stop quickly; don't depend on private internals.
                for fut in in_flight:
                    fut.cancel()
                ex.shutdown(wait=False, cancel_futures=True)
                raise

    parallel_mapped_lazy.__name__ = "pfmaplazy_" + getattr(func, "__name__", "fn")
    return parallel_mapped_lazy

def ffilter(func: Callable[[X], bool]) -> Callable[[Iterable[X]], Iterator[X]]:
    def filtered(collection: Iterable[X]) -> Iterator[X]:
        return filter(func, collection)

    return filtered


def identity(x: X) -> X:
    return x


def groupby[K, V](func: Callable[[V], K], ls: Iterable[V]) -> List[Tuple[K, List[V]]]:
    groups: Dict[K, List[V]] = defaultdict(list)
    for i in ls:
        groups[func(i)].append(i)
    return list(groups.items())

def fgroupby[K, V](func: Callable[[V], K]) -> Callable[[Iterable[V]], List[Tuple[K, List[V]]]]:
    def grouper(ls: Iterable[V]) -> List[Tuple[K, List[V]]]:
        return groupby(func, ls)
    return grouper

def take(count: int) -> Callable[[Iterable[X]], Iterator[X]]:
    def taker(collection: Iterable[X]) -> Iterator[X]:
        remaining = count
        i = iter(collection)
        try:
            while remaining > 0:
                try:
                    yield next(i)
                except StopIteration:
                    return
                remaining = remaining - 1
        finally:
            # Close the upstream iterator if it supports it. Generators and
            # other resource-holding iterators implement .close(), which triggers
            # their finally/with blocks and releases resources (e.g. connection
            # pool leases, file handles). Plain iterators (list, range) don't
            # have .close() and don't need cleanup.
            # This matters because take is the only pipeline stage that
            # intentionally stops pulling before the input is exhausted —
            # without this, upstream generators would stay suspended inside
            # their with blocks until GC collects them (non-deterministic,
            # and never on non-refcounting runtimes like PyPy).
            if hasattr(i, 'close'):
                i.close()

    return taker


def consume(collection: Iterable[X]) -> None:
    for _ in collection:
        pass


def uniq(ls: Iterable[X]) -> Iterator[X]:
    seen = set()
    for i in ls:
        if i in seen:
            continue
        else:
            seen.add(i)
            yield i


def irange(start: int, increment: int) -> Iterator[int]:
    while True:
        yield start
        start += increment


def invert(v: Any) -> bool:
    return not v


def finvert(f: Callable[..., Any]) -> Callable[..., bool]:
    def inverted(*args, **kwargs):
        return invert(f(*args, **kwargs))

    return inverted


# TODO why not len?
def count(iterator: Iterable[X]) -> int:
    c = 0
    for v in iterator:
        c += 1
    return c


Ts = TypeVarTuple("Ts")

def uncurry(func: Callable[[*Ts], R]) -> Callable[[tuple[*Ts]], R]:
    """Wrap func so it takes a single tuple of positional args."""
    def uncurried(args: tuple[*Ts], /) -> R:
        return func(*args)
    unwrapped_name = f"uncurried_{func.__name__}"
    uncurried.__name__ = unwrapped_name
    uncurried.__qualname__ = unwrapped_name

    return uncurried

def curry(func: Callable[[tuple[*Ts]], R]) -> Callable[[*Ts], R]:
    """Wrap func so it takes positional args and packs them into one tuple."""
    def curried(*args: *Ts) -> R:
        return func(args)
    return curried


def identify(func: Callable[[X], Any]) -> Callable[[X], X]:
    """Wrap func so that it returns what comes in."""

    def identified(arg: X) -> X:
        func(arg)
        return arg

    return identified

def zipFrom(a: X, bs: Iterable[Y]) -> Iterator[tuple[X, Y]]:
    """Converts a value and list into a list of tuples: a -> [b] -> [(a,b)]"""
    for b in bs:
        yield (a, b)

def nth(n: int) -> Callable[[Sequence[X]], X]:
    def nth_getter(lst: Sequence[X]) -> X:
        return lst[n]

    return nth_getter


first = nth(0)
second = nth(1)


def maybe(default: X, v: Optional[X]) -> X:
    if v:
        return v
    else:
        return default


def every(predicate: Callable[[X], bool], coll: Iterable[X]) -> bool:
    for x in coll:
        if not predicate(x):
            return False
    return True

# TODO missing tests
def every_pred(*predicates: Callable[[X], bool]) -> Callable[[X], bool]:
    preds = tuple(predicates)
    def all_predicates(x: X) -> bool:
        for p in preds:
            if not p(x):
                return False
        return True
    return all_predicates

def jaccard_similarity(a: set[X], b: set[X]) -> float:
    return float(len(a.intersection(b))) / float(len(a.union(b)))


# TODO this is not used.
def dethrow(function, catch_predicate, error_encoder=identity):
    """
    Converts a function which raises exceptions to a function which returns either a result or an error code.
    catch_predicate is a function which takes an exception e, and returns whether the exception should be caught, or
    raised. Error encoder takes a caught exception e returns the error value for the wrapped function.
    """

    def dethrow_wrapper(*args, **kwargs):
        """
        Wrapper of a function passed to dethrow. Some if its exceptions are converted to error codes
        """
        try:
            return function(*args, **kwargs)
        except Exception as e:
            if catch_predicate(e):
                return error_encoder(e)
            else:
                raise e

    return dethrow_wrapper


# TODO do the fsck fixers need to use this?
ACC = TypeVar("ACC")
INC = TypeVar("INC")


@overload
def reducefileobj(reducer: Callable[[bytes, bytes], bytes],
                  fsrc: Readable,
                  initial: None,
                  length: int = ...) -> bytes: ...


@overload
def reducefileobj(reducer: Callable[[ACC, INC], ACC],
                  fsrc: Readable,
                  initial: ACC,
                  length: int = ...) -> ACC: ...


def reducefileobj(reducer: Any,
                  fsrc: Readable,
                  initial: Any = None,
                  length: int = 16 * 1024) -> Any:
    if initial is None:
        acc = fsrc.read(length)
    else:
        acc = initial
    while 1:
        buf = fsrc.read(length)
        if not buf:
            break
        acc = reducer(acc, buf)
    return acc


def _writebuf(dst: Writable, buf: bytes) -> Writable:
    dst.write(buf)
    return dst


# TODO do the fsck fixers need to use this?
def copyfileobj(fsrc: Readable, fdst: Writable, length: int = 16 * 1024) -> None:
    """copy data from file-like object fsrc to file-like object fdst"""
    reducefileobj(_writebuf, fsrc, fdst, length)


# TODO do the fsck fixers need to use this?
# TODO this is not used
# TODO this the type annotation is really hard. Copy from storywriter.
def fork(*fns):
    """
    Return a function, which calls all the functions in fns.
    The return values of these functions are collated into a tuple and returned.
    """

    def forked(*args, **kwargs):
        return tuple([fn(*args, **kwargs) for fn in fns])

    return forked


# Readable is the minimal protocol required of a blob read handle.
# IO[bytes], HTTPResponse, _S3HandleWrapper, BytesIO, and Werkzeug's LimitedStream all satisfy it.
# The context-manager constraint is expressed separately via HandleThunk[Readable] =
# Callable[[], ContextManager[Readable]], relying on ContextManager's covariance.
class Readable(Protocol):
    def read(self, n: int = -1, /) -> bytes: ...


# Writable is the minimal protocol required of a blob write handle.
# IO[bytes], SafeBinaryOutput, and any bytes-mode file-like object satisfy it.
# Same covariance story as Readable for context-manager use via HandleThunk[Writable].
class Writable(Protocol):
    def write(self, data: bytes, /) -> int: ...


# Handles are any object which can be used as a context manager. There are many types of handles.
# We use X as the type variable for what is returned by __enter__.
type Handle[T] = ContextManager[T]
# HandleThunk is a function which takes no arguments and returns a Handle of a given type.
# This is useful for retrying idempotent operations on handles.
type HandleThunk[T] = Callable[[], Handle[T]]
# OneHandleIoFn is a function which takes a single handle and performs some io operation using that handle
# returning a value of type Y. This is useful for retrying idempotent operations on handles and limits the life
# scope of the handle.
type OneHandleIoFn[X, Y] = Callable[[X], Y]
# Exception predicate is a function which takes an exception and returns true if this is an expected
# failure mode, and we should retry, or false if this is an unexpected failure mode, and we should re-raise.
ExceptionPredicate = Callable[[Exception], bool]
# A Thunk is a zero-argument callable that produces a value.
type Thunk[T] = Callable[[], T]


class HasOpen(Protocol):
    def open(self, mode: str) -> ContextManager[IO]: ...


def withHandles2[X, Y, Z](
        get_src: HandleThunk[X],
        get_dst: HandleThunk[Y],
        io_fn: Callable[[X, Y], Z],
) -> Z:
    with get_src() as src, get_dst() as dst:
        return io_fn(src, dst)


def withHandles2Thunk[X, Y, Z](
        get_src: HandleThunk[X],
        get_dst: HandleThunk[Y],
        io_fn: Callable[[X, Y], Z],
) -> Thunk[Z]:
    def thunk() -> Z:
        return withHandles2(get_src, get_dst, io_fn)
    return thunk


def retry[Z](
        fn: Thunk[Z],
        retry_exception: ExceptionPredicate,
        tries: int = 3,
) -> Z:
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
            logger.debug(
                "\nretry attempt %d/%d failed with %s: %.200s\n",
                attempt + 1, tries, type(e).__name__, str(e)
            )
            if attempt < tries - 1:
                sleep_time = 4 ** (attempt + 1)
                logger.debug("Sleeping %ds before retry...", sleep_time)
                time.sleep(sleep_time)
            else:
                logger.debug("All %d retry attempts exhausted", tries)
    raise RetriesExhausted("retry operation failed", failed_attempts)


def retryThunk[Z](
        fn: Thunk[Z],
        retry_exception: ExceptionPredicate,
        tries: int = 3,
) -> Thunk[Z]:
    def thunk() -> Z:
        return retry(fn, retry_exception, tries)
    return thunk


def file_thunk(path: HasOpen, mode: str) -> HandleThunk[IO]:
    def thunk() -> ContextManager[IO]:
        return path.open(mode)
    return thunk


def retryFdIo1[X, Y](
        get_fd: HandleThunk[X],
        ioFn: OneHandleIoFn[X, Y],
        retry_exception: ExceptionPredicate,
        tries: int = 3) -> Y:
    if tries < 1:
        raise ValueError("tries must be at least 1")
    raise NotImplementedError()


# A TwoHandleIoFn is a function of two handles, which performs
# some io operation using those handles, returning a value of type Z.
# This is useful for retrying idempotent operations on pairs of handles and limits the life
# scope of the handles.
type TwoHandleIoFn[X, Y, Z] = Callable[[X, Y], Z]

def retryFdIo2[X, Y, Z](
        get_src: HandleThunk[X],
        get_dst: HandleThunk[Y],
        ioFn: TwoHandleIoFn[X, Y, Z],
        retry_exception: ExceptionPredicate,
        tries: int = 3) -> Z:
    """
    Attempts idepotent ioFn with 2 file handles. Retries up to `tries` times.

    get_src: a function which recives no arguments and returns a file like object which will be read (by convention).
    get_dst: a function which recives no arguments and returns a file
            like object which will be written to (by convention).
    io: a function which is called with src, dst as its arguments.
        Failures should result in throws. Return value is returned on completion.
    retry_exception: a predicate function which recives raised
                     exceptions. If it returns true, this is an expected failure mode, and we will retry.

    If retry_exception returns False, the exception is re-raised.

    Raises:
        RetriesExhausted: When all retry attempts fail, containing details of each failed attempt.
    """
    if tries < 1:
        raise ValueError("tries must be at least 1")
    failed_attempts = []
    for attempt in range(tries):
        try:
            with get_src() as src, get_dst() as dst:
                return ioFn(src, dst)
        except Exception as e:
            if not retry_exception(e):
                raise

            failed_attempts.append((attempt + 1, e))
            # Log retry (newline for progress bars)
            logger.debug(
                "\nretryFdIo2 attempt %d/%d failed with %s: %.200s\n",
                attempt + 1, tries, type(e).__name__, str(e)
            )

            # If this was not the last attempt, sleep with exponential backoff
            if attempt < tries - 1:
                # Exponential backoff: 4s, 16s, 64s, etc.
                sleep_time = 4 ** (attempt + 1)
                logger.debug("Sleeping %ds before retry...", sleep_time)
                time.sleep(sleep_time)
            else:
                # Last attempt failed
                logger.debug("All %d retry attempts exhausted", tries)

    # All retries exhausted, raise with details of all attempts
    raise RetriesExhausted("retryFdIo2 operation failed", failed_attempts)


S = TypeVar("S")

def runState(x: X, state: S, stateFn: Callable[[S, X], tuple[S, R]]) -> tuple[S, R]:
    """
    Ticks the state with x. Returns (state, result).
    """
    return stateFn(state, x)


def mapM(
        xs: Iterable[X],
        m: Callable[[X, S, Callable[[S, X], Tuple[S, R]]], Tuple[S, R]],
        ctx: S,
        fn: Callable[[S, X], tuple[S, R]]):
    for x in xs:
        ctx, r = m(x, ctx, fn)
        yield r


def csum_pct(csum: str) -> float:
    """
    Takes a hex md5 checksum digest string. Returns a float between 0.0 and 1.0 representing what
    lexographic percentile of the checksum.
    """
    assert len(csum) == 32
    max_value = int("f" * 32, 16)
    csum_int = int(csum, 16)
    return csum_int / max_value


def tree_pct(item: str) -> float:
    """
    Takes a tree item, and returns a float between 0.0 and 1.0 representing what lexographic percentile of the item.
    """
    # TODO impossible.
    return 1.0


def cardinality(seen: int, pct: float) -> int:
    """
    Estimate the number of items in a progressive set based on how far we've iterated over the set,
    and how many items we've seen so far.
    """
    if pct < 0.00001:
        pct = 0.00001
    return int(seen / pct)


# ── Time utilities ────────────────────────────────────────────────────────────

def parse_utc(s: str) -> datetime:
    """Parse an ISO 8601 string → aware datetime. Accepts strings produced by format_utc()."""
    return datetime.fromisoformat(s)


def format_utc(dt: datetime) -> str:
    """Format an aware datetime → ISO 8601 string with UTC offset (+00:00)."""
    return dt.astimezone(timezone.utc).isoformat()


def add_seconds(dt: datetime, seconds: int) -> datetime:
    """Return dt + seconds as a new datetime (same timezone)."""
    return dt + timedelta(seconds=seconds)


def is_past(dt: datetime, now: datetime) -> bool:
    """Return True if dt <= now (i.e. the deadline has passed or been reached)."""
    return dt <= now
