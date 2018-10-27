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
  def composition(*args, **kwargs):
      return f(g(*args, **kwargs))
  return composition

def concat(l):
  for sublist in l:
    for item in sublist:
      yield item

def concatMap(func):
  return compose(concat, partial(map, func))

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

def transduce(*funcs):
  if funcs:
    foo = funcs[0]
    rest = funcs[1:]
    if rest:
      next_hop = transduce(*rest)
      def transducer(*args, **kwargs):
        return next_hop(foo(*args, **kwargs))
      return transducer
    else: # no rest, foo is final function.
      return foo
  else: # no funcs at all.
    return fmap(identity)
