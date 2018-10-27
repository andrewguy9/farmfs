from farmfs.util import empty2dot, compose, concat, concatMap, fmap, identity, irange, invert, take, uniq, groupby, transduce
import functools
from itertools import ifilter

def add(x,y): return x+y
assert add(1,2) == 3

def inc(x): return x+1
assert inc(1) == 2

def even(x): return x%2==0
assert even(2) == True
assert even(1) == False

even_list = functools.partial(ifilter, even)
assert list(even_list([1,2,3,4])) == [2, 4]

def test_empty2dot():
  assert empty2dot([]) == ["."]
  l = [1,2,3]
  assert empty2dot(l) == l

def test_compose():
  inc_add = compose(inc, add)
  assert inc_add(1,2) == 4

def test_concat():
  assert list(concat([[1,2,3],[4,5,6]])) == [1,2,3,4,5,6]
  assert list(concat([[1],[2,3,4]])) == [1,2,3,4]
  assert list(concat([[1,2,3],[4]])) == [1,2,3,4]
  assert list(concat([[1],[1,2],[1,2,3]])) == [1,1,2,1,2,3]

def test_concatMap():
  assert list(concatMap (lambda x:x*[x])([0,1,2,3,3])) == [1, 2, 2, 3, 3, 3, 3, 3, 3]

def test_fmap():
  inc_iter = fmap(inc)
  assert list(inc_iter([1,2,3,4])) == [2, 3, 4, 5]

def test_identity():
  assert identity(5) == 5

def test_groupby():
  #TODO group by may not order results consistenly for asserts.
  assert groupby(even, [1,2,3,4,5,6]) == [(False,[1,3,5]), (True,[2,4,6])]

def test_take():
  assert list(take(3)([1,2,3,4,5])) == [1,2,3]
  assert list(take(3)([1,2])) == [1,2]

def test_uniq():
  assert list(uniq([1,2,3,4])) == [1,2,3,4]
  assert list(uniq([1,2,2,4])) == [1,2,4]
  assert list(uniq([1,2,3,2])) == [1,2,3]

def test_irange():
  assert list(take(3)(irange(0,1))) == [0,1,2]
  assert list(take(3)(irange(0,-1))) == [0,-1,-2]

def test_invert():
  assert invert(1) == False
  assert invert(True) == False
  assert invert(0) == True
  assert invert([]) == True
  assert invert([1]) == False

def test_transduce():
  identity_transducer = transduce()
  assert identity_transducer([1,2,3]).next, "identity_transducer should be an iterator"
  assert list(identity_transducer([1,2,3])) == [1,2,3]

  inc_transducer = transduce(fmap(inc))
  assert inc_transducer([1,2,3]).next, "inc_transducer should be an iterator."
  assert list(inc_transducer([1,2,3])) == [2,3,4]

  inc_list_transducer = transduce(fmap(inc), list)
  assert isinstance(inc_list_transducer([1,2,3]), list), "inc_list_transducer should return a list"
  assert inc_list_transducer([1,2,3]) == [2,3,4]

  range_transducer = transduce(irange, even_list, take(3), list)
  assert range_transducer(0,1) == [0,2,4]
