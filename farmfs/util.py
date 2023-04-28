from functools import partial as functools_partial
from collections import defaultdict
from time import time, sleep
from itertools import count as itercount
import sys
if sys.version_info >= (3, 0):
    from concurrent.futures import ThreadPoolExecutor
    from concurrent.futures.thread import _threads_queues
    # In python3, map is now lazy.
    imap = map
    # In python3, map is now lazy.
    ifilter = filter
    rawtype = bytes
    safetype = str
    raw2str = lambda r: r.decode('utf-8')
    str2raw = lambda s: s.encode('utf-8')
else:
    # python2
    from itertools import imap
    from itertools import ifilter
    rawtype = str
    safetype = unicode  # noqa: F821
    raw2str = lambda r: r.decode('utf-8')
    str2raw = lambda s: s.encode('utf-8')

def ingest(d):
    """
    Convert rawtype (str py27 or bytes py3x) to safetype
    (unicode py27 or str py3x)
    """
    if isinstance(d, rawtype):
        return raw2str(d)
    elif isinstance(d, safetype):
        return d
    else:
        raise TypeError("Can't ingest data of type %s" % type(d))

def egest(s):
    """
    Convert safetype (unicode py27, str py3x) to rawtype
    (str py27 or bytes py3x)
    """
    if isinstance(s, rawtype):
        return s
    elif isinstance(s, safetype):  # On python 2 str is bytes.
        return str2raw(s)
    else:
        raise TypeError("Can't egest data of type %s" % type(s))

def empty_default(xs, default):
    """"
    If zero length array is passed, returns default.
    Otherwise returns the origional array.
    """
    xs = list(xs)
    if len(xs) == 0:
        return list(default)
    else:
        return xs

def compose(f, g):
    fn = lambda *args, **kwargs: f(g(*args, **kwargs))
    fn.__name__ = f.__name__ + "_" + g.__name__
    return fn

def composeFunctor(f, g):
    out = lambda x: f(g(x))
    out.__name__ = "compose_functor_" + f.__name__ + "_" + g.__name__
    return out

def partial(fn, *args, **kwargs):
    out = functools_partial(fn, *args, **kwargs)
    out.__name__ = "partial_" + fn.__name__
    return out

def concat(ls):
    for sublist in ls:
        for item in sublist:
            yield item

def concatMap(func):
    return compose(concat, partial(imap, func))

def fmap(func):
    def mapped(collection):
        return imap(func, collection)
    mapped.__name__ = "mapped_" + func.__name__
    return mapped


if sys.version_info >= (3, 0):
    def pfmap(func, workers=8):
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
else:
    def pfmap(func, workers=8):
        """concurrent futures are not supported on py2x. Fallbac to fmap."""
        return fmap(func)

def ffilter(func):
    def filtered(collection):
        return ifilter(func, collection)
    return filtered

def identity(x):
    return x

def groupby(func, ls):
    groups = defaultdict(list)
    for i in ls:
        groups[func(i)].append(i)
    return list(groups.items())

def take(count):
    def taker(collection):
        remaining = count
        i = iter(collection)
        while remaining > 0:
            try:
                yield next(i)
            except StopIteration:
                return
            remaining = remaining - 1
    return taker

def consume(collection):
    for _ in collection:
        pass

def uniq(ls):
    seen = set()
    for i in ls:
        if i in seen:
            continue
        else:
            seen.add(i)
            yield i

def irange(start, increment):
    while True:
        yield start
        start += increment

def invert(v):
    return not v

def finvert(f):
    def inverted(*args, **kwargs):
        return invert(f(*args, **kwargs))
    return inverted

# TODO why not len?
def count(iterator):
    c = 0
    for v in iterator:
        c += 1
    return c

def uncurry(func):
    """Wraps func so that the first arg is expanded into list args."""
    def uncurried(list_args, **kwargs):
        return func(*list_args, **kwargs)
    return uncurried

def curry(func):
    """"
    Wraps func so that a series of args are turned into a single arg list.
    """
    def curried(*args, **kwargs):
        return func(args, **kwargs)
    return curried

def identify(func):
    """Wrap func so that it returns what comes in."""
    def identified(arg):
        func(arg)
        return arg
    return identified

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

def zipFrom(a, bs):
    """Converts a value and list into a list of tuples: a -> [b] -> [(a,b)]"""
    for b in bs:
        yield (a, b)

def dot(fn):
    """Reverses the dot syntax (object.attr), so you can do dot(attr)(obj)."""
    def access(obj):
        return getattr(obj, fn)
    return access

def nth(n):
    def nth_getter(lst):
        return lst[n]
    return nth_getter


first = nth(0)
second = nth(1)

def maybe(default, v):
    if v:
        return v
    else:
        return default

def every(predicate, coll):
    for x in coll:
        if not predicate(x):
            return False
    return True

def repeater(
        callback,
        period=0,
        max_tries=None,
        max_time=None,
        predicate=identity,
        catch_predicate=lambda e: False):
    def repeat_worker(*args, **kwargs):
        if max_time is not None:
            deadline = time() + max_time
        else:
            deadline = None
        if max_tries is None:
            r = itercount()
        else:
            r = range(0, max_tries)
        for i in r:
            start_time = time()
            threw = False
            try:
                ret = callback(*args, **kwargs)
            except Exception as e:
                # An exception was caught, so we failed.
                if catch_predicate(e):
                    # This exception was expected. So we failed, but might need retry.
                    threw = True
                else:
                    # This exception was unexpected, lets re-throw.
                    raise
            if not threw and predicate(ret):
                # We didn't throw, and got a success! Exit.
                return True
            if deadline is not None and time() > deadline:
                return False
            end_time = time()
            sleep_time = max(0.0, period - (end_time - start_time))
            sleep(sleep_time)
        # We fell through to here, fail.
        return False
    return repeat_worker

def jaccard_similarity(a, b):
    return float(len(a.intersection(b))) / float(len(a.union(b)))
