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

def test_empty2dot():
  assert empty2dot([]) == ["."]
  l = [1,2,3]
  assert empty2dot(l) == l

def compose(f, g):
  def composition(*args, **kwargs):
      return f(g(*args, **kwargs))
  return composition

def transduce(*funcs):
  def transducer(collection):
    old = collection
    for func in funcs:
      new = func(old)
      old = new
    return new
  return transducer

def fmap(func):
  def mapped(collection):
    return map(func, collection)
  return mapped

def take(count):
  def taker(collection):
    remaining = count
    i = iter(collection)
    while remaining > 0:
      yield i.next()
      remaining = remaining - 1
  return taker
