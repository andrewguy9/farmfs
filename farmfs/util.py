from functools import partial as functools_partial
from collections import defaultdict
import logging
import os
import sys
import time
from typing import IO, Any, Callable, Iterable, Iterator, Optional, Tuple, TypeVar

from concurrent.futures import ThreadPoolExecutor, as_completed
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


def partial(fn: Callable[..., X], *args, **kwargs) -> Callable[..., X]:
    out: Callable[..., X] = functools_partial(fn, *args, **kwargs)
    out.__name__ = "partial_" + fn.__name__
    return out


def concat(ls: Iterable[Iterable[X]]) -> Iterator[X]:
    for sublist in ls:
        for item in sublist:
            yield item


def concatMap(func: Callable[[X], Iterable[Y]]) -> Callable[[Iterable[X]], Iterator[Y]]:
    return compose(concat, partial(map, func))


def fmap(func: Callable[[X], Y]) -> Callable[[Iterable[X]], Iterator[Y]]:
    def mapped(collection: Iterable[X]) -> Iterator[Y]:
        return map(func, collection)

    mapped.__name__ = "mapped_" + func.__name__
    return mapped


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

def pfmaplazy(func: Callable[[X], Y], workers: int = 8, buffer_size: int = 16):
    if workers < 1:
        raise ValueError("workers must be at least 1")
    if buffer_size < 1:
        raise ValueError("buffer_size must be at least 1")

    def parallel_mapped_lazy(collection: Iterable[X]) -> Iterator[Y]:
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = []
            try:
                for item in collection:
                    future = executor.submit(func, item)
                    futures.append(future)
                    # Ensure the number of futures doesn't exceed workers + buffer_size
                    if len(futures) >= (workers + buffer_size):
                        for completed_future in as_completed(futures):
                            yield completed_future.result()
                            futures.remove(completed_future)
                            # Break once we're below the buffer size
                            if len(futures) < (workers + buffer_size):
                                break
                # Ensure all remaining futures are processed
                for future in as_completed(futures):
                    yield future.result()
            except KeyboardInterrupt as e:
                # TODO bugs might have been fixed in python 3.9+ which make this work without the _threads_queues hack. Should test and remove if so.
                executor.shutdown(wait=False)
                executor._threads.clear()
                _threads_queues.clear()
                raise e

    parallel_mapped_lazy.__name__ = "pmapped_" + func.__name__
    return parallel_mapped_lazy


def ffilter(func: Callable[[X], bool]) -> Callable[[Iterable[X]], Iterator[X]]:
    def filtered(collection: Iterable[X]) -> Iterator[X]:
        return filter(func, collection)

    return filtered


def identity(x: X) -> X:
    return x


def groupby(func, ls):
    groups = defaultdict(list)
    for i in ls:
        groups[func(i)].append(i)
    return list(groups.items())


def take(count: int) -> Callable[[Iterable[X]], Iterator[X]]:
    def taker(collection: Iterable[X]) -> Iterator[X]:
        remaining = count
        i = iter(collection)
        while remaining > 0:
            try:
                yield next(i)
            except StopIteration:
                return
            remaining = remaining - 1

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


def uncurry(func: Callable[..., X]) -> Callable[[tuple], X]:
    """Wraps func so that the first arg is expanded into tuple args."""

    def uncurried(tuple_args: tuple, **kwargs) -> X:
        return func(*tuple_args, **kwargs)

    return uncurried


def curry(func: Callable[..., X]) -> Callable[..., X]:
    """
    Wraps func so that a series of args are turned into a single arg list.
    """

    def curried(*args, **kwargs) -> X:
        return func(args, **kwargs)

    return curried


def identify(func: Callable[[X], Any]) -> Callable[[X], X]:
    """Wrap func so that it returns what comes in."""

    def identified(arg: X) -> X:
        func(arg)
        return arg

    return identified


# TODO the type annotation for this is really hard. Copy from storywriter.
def pipeline(*funcs):
    if funcs:
        foo = funcs[0]
        rest = funcs[1:]
        if rest:
            next_hop = pipeline(*rest)

            def pipe(*args, **kwargs):
                return next_hop(foo(*args, **kwargs))

            return pipe
        else:  # no rest, foo is final function.
            return foo
    else:  # no funcs at all.
        return fmap(identity)


def zipFrom(a: X, bs: Iterable[Y]) -> Iterator[tuple[X, Y]]:
    """Converts a value and list into a list of tuples: a -> [b] -> [(a,b)]"""
    for b in bs:
        yield (a, b)


def nth(n: int) -> Callable[[list[X]], X]:
    def nth_getter(lst: list[X]) -> X:
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
def reducefileobj(reducer: Callable[[ACC, INC], ACC],
                  fsrc: IO,
                  initial: Optional[ACC] = None,
                  length: int = 16 * 1024) -> ACC:
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


def _writebuf(dst: IO, buf: bytes) -> IO:
    dst.write(buf)
    return dst


# TODO do the fsck fixers need to use this?
def copyfileobj(fsrc: IO, fdst: IO, length: int = 16 * 1024) -> None:
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

HandleThunk = Callable[[], IO]
OneHandleIoFn = Callable[[IO], X]
ExceptionPredicate = Callable[[BaseException], bool]

def retryFdIo1(get_fd: HandleThunk,
               ioFn: OneHandleIoFn[X],
               retry_exception: ExceptionPredicate,
               tries=3) -> X:
    raise NotImplementedError()


TwoHandleIoFn = Callable[[IO, IO], X]

def retryFdIo2(get_src: HandleThunk,
               get_dst: HandleThunk,
               ioFn: TwoHandleIoFn[X],
               retry_exception: ExceptionPredicate,
               tries: int = 3) -> X:
    """
    Attempts idepotent ioFn with 2 file handles. Retries up to `tries` times.
    get_src is a function which recives no arguments and returns a file like object which will be read (by convention).
    get_dst is a function which recives no arguments and returns a file like object which will be written to (by convention).
    io is a function which is called with src, dst as its arguments. Failures should result in throws. Return value is returned on completion.
    retry_exception is a predicate function which recives raised exceptions. If it returns true, this is an expected failure mode, and we will retry.
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
                # Exponential backoff: 1s, 2s, 4s, 8s, etc.
                sleep_time = 2 ** attempt
                logger.debug("Sleeping %ds before retry...", sleep_time)
                time.sleep(sleep_time)
            else:
                # Last attempt failed
                logger.debug("All %d retry attempts exhausted", tries)

    # All retries exhausted, raise with details of all attempts
    raise RetriesExhausted("retryFdIo2 operation failed", failed_attempts)

S = TypeVar("S")
R = TypeVar("R")

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
