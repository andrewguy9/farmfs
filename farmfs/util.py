from functools import partial
from collections import defaultdict
try:
    from itertools import imap
except ImportError:
    # In python3, map is now lazy.
    imap = map

try:
  #Python2
  rawtype = str
  safetype = unicode
  raw2str = lambda r: r.decode('utf-8')
  str2raw = lambda s: s.encode('utf-8')
except:
  #Python3
  rawtype = bytes
  safetype = str
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

""""
If zero length array is passed, returns ["."].
Otherwise returns the origional array.
Useful for dealing with lists of files, or not.
"""
def empty2dot(paths):
  if len(paths) == 0:
    return ["."]
  else:
    return paths

def compose(f, g):
  return lambda *args, **kwargs: f(g(*args, **kwargs))

def composeFunctor(f,g):
    return lambda x: f(g(x))

def concat(l):
  for sublist in l:
    for item in sublist:
      yield item

def concatMap(func):
  return compose(concat, partial(imap, func))

def fmap(func):
  def mapped(collection):
    return imap(func, collection)
  return mapped

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
