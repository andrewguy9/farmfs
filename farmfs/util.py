from functools import partial as functools_partial
from collections import defaultdict
from time import time, sleep
from itertools import count as itercount
import sys
import re
if sys.version_info >= (3, 0):
    import concurrent.futures
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
    safetype = unicode
    raw2str = lambda r: r.decode('utf-8')
    str2raw = lambda s: s.encode('utf-8')

def ingest(d):
  """Convert rawtype (str py27 or bytes py3x) to safetype (unicode py27 or str py3x)"""
  if isinstance(d, rawtype):
    return raw2str(d)
  elif isinstance(d, safetype):
    return d
  else:
    raise TypeError("Can't ingest data of type %s" % type(d))

def egest(s):
  """Convert safetype (unicode py27, str py3x) to rawtype (str py27 or bytes py3x)"""
  if isinstance(s, rawtype):
    return s
  elif isinstance(s, safetype): # On python 2 str is bytes.
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

def composeFunctor(f,g):
    out = lambda x: f(g(x))
    out.__name__ = "compose_functor_" + f.__name__ + "_" + g.__name__
    return out

def partial(fn, *args, **kwargs):
  out = functools_partial(fn, *args, **kwargs)
  out.__name__ = "partial_" + fn.__name__
  return out

def concat(l):
  for sublist in l:
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
            with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
                try:
                    # Enqueue all work from collection.
                    # XXX this is greedy, so we fully consume the input before starting to produce output.
                    for result in executor.map(func, collection):
                        yield result
                except KeyboardInterrupt as e:
                    executor.shutdown(wait=False)
                    executor._threads.clear()
                    concurrent.futures.thread._threads_queues.clear()
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

def groupby(func, l):
  groups = defaultdict(list)
  for i in l:
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

def uniq(l):
  seen = set()
  for i in l:
    if i in seen:
      continue
    else:
      seen.add(i)
      yield i

def irange(start, increment):
  while True :
    yield start
    start += increment

def invert(v):
    return not(v)

#TODO why not len?
def count(iterator):
    c = 0
    for v in iterator:
        c+=1
    return c

def uncurry(func):
  """Wraps func so that the first arg is expanded into list args."""
  def uncurried(list_args, **kwargs):
    return func(*list_args, **kwargs)
  return uncurried

def curry(func):
  """"Wraps func so that a series of args are turned into a single arg list."""
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
    else: # no rest, foo is final function.
      return foo
  else: # no funcs at all.
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

def repeater(callback, period=0, max_tries=None, max_time=None, predicate = identity, catch_predicate = lambda e: False):
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
    return float(len(a.intersection(b))) / float(len (a.union(b)))

def prefix_generator(alphabet, width):
    if not isinstance(alphabet, str):
        raise ValueError("alphabet must be a str")
    if len(alphabet) < 2:
        raise ValueError("Alphabet must be at least base 2")
    if width < 0:
        raise ValueError("width must be positive")
    if width == 0:
        return [""]
    if width == 1:
        return list(alphabet)
    prefixes = prefix_generator(alphabet, width-1)
    return [letter+prefix for letter in alphabet for prefix in prefixes]

def circle(shards, alphabet = '0123456789abcdef', width = 32):
    if shards < 1:
        raise ValueError("shard count must be positive")
    base = len(alphabet)
    try:
        prefix_width = [i for i in range(width+1) if base**i % shards == 0][0]
    except IndexError:
        raise ValueError("Cannot divide base %s into %s shards" % (base, shards))
    suffix_width=width-prefix_width
    prefixes = prefix_generator(alphabet, prefix_width)
    #print("prefixes", prefixes)
    assert(len(prefixes) % shards == 0)

    step = int(base/shards)
    prefix_groups = [prefixes[i*step:(i+1)*step] for i in range(shards)]
    #print("prefix groups", prefix_groups)

    patterns = []
    for prefixes in prefix_groups:
        pattern=""
        for i in range(prefix_width):
            options = set([prefix[i] for prefix in prefixes])
            slice = "[" + re.escape("".join(options)) + "]"
            pattern+=slice
        patterns.append(pattern)

    any = "["+re.escape("".join(alphabet))+"]"
    shard_pats = [re.compile("^"+pattern+any+"{"+str(suffix_width)+"}$") for pattern in patterns]
    #print("patterns", shard_pats)
    return shard_pats

def placer(shards):
    def find_shard(hash):
        for shard,test in enumerate(shards):
            if test.match(hash):
                return shard
        raise ValueError("Unable to match %s with a shard" % hash)
    return find_shard

