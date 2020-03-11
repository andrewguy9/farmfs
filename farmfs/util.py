from functools import partial
from collections import defaultdict
from itertools import imap

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
  return groups.items()

def take(count):
  def taker(collection):
    remaining = count
    i = iter(collection)
    while remaining > 0:
      yield i.next()
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
