import sys
from farmfs.util import empty_default, compose, concat, concatMap, fmap, ffilter, identity, irange, invert, count, take, uniq, groupby, curry, uncurry, identify, pipeline, zipFrom, dot, nth, first, second, every, repeater, pfmap
import functools
from collections import Iterator
from farmfs.util import ingest, egest, safetype, rawtype
import pytest
from time import time

try:
    from unittest.mock import Mock
except ImportError:
    pass

def add(x,y): return x+y
assert add(1,2) == 3

def inc(x): return x+1
assert inc(1) == 2

def even(x): return x%2==0
assert even(2) == True
assert even(1) == False

even_list = ffilter(even)
assert list(even_list([1,2,3,4])) == [2, 4]

def test_empty_default():
  # Test empty behavior
  assert empty_default([], [1]) == [1]
  # Test non empty behavior
  l = [1,2,3]
  assert empty_default(l, [4]) == l
  # Test iterators work
  assert empty_default(iter([1,2,3]), iter([4])) == [1,2,3]
  # Test output is a copy
  i = [1,2,3]
  d = [5,6,7]
  o = empty_default(i, d)
  i.append(4)
  d.append(8)
  assert i == [1,2,3,4]
  assert d == [5,6,7,8]
  assert o == [1,2,3]
  # Test default is a copy
  i = []
  d = [5,6,7]
  o = empty_default(i, d)
  i.append(4)
  d.append(8)
  assert i == [4]
  assert d == [5,6,7,8]
  assert o == [5,6,7]



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

def test_ffilter():
    even_iter = ffilter(even)
    assert list(even_iter([1,2,3,4])) == [2, 4]

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

def test_count():
  assert count(iter([])) == 0
  assert count(iter([1,2,3])) == 3

def test_curries():
  unadd = uncurry(add)
  assert unadd([1,2]) == 3
  readd = curry(unadd)
  assert readd(1,2) == 3

@pytest.mark.skipif(sys.version_info < (3, 3), reason="requires python3.3 or higher")
def test_identify():
  mock = Mock(return_value=1)
  foo = identify(mock)
  result = foo(5)
  assert result == 5
  mock.assert_called_once_with(5)

def test_pipeline():
  identity_pipeline = pipeline()
  assert isinstance(identity_pipeline([1,2,3]), Iterator), "identity_pipeline should be an iterator"
  assert list(identity_pipeline([1,2,3])) == [1,2,3]

  inc_pipeline = pipeline(fmap(inc))
  assert isinstance(inc_pipeline([1,2,3]), Iterator), "inc_pipeline should be an iterator."
  assert list(inc_pipeline([1,2,3])) == [2,3,4]

  inc_list_pipeline = pipeline(fmap(inc), list)
  assert isinstance(inc_list_pipeline([1,2,3]), list), "inc_list_pipeline should return a list"
  assert inc_list_pipeline([1,2,3]) == [2,3,4]

  range_pipeline = pipeline(irange, even_list, take(3), list)
  assert range_pipeline(0,1) == [0,2,4]

def test_zipFrom():
  assert list(zipFrom(1, [2,3,4])) == [(1,2), (1,3), (1,4)]
  assert list(zipFrom(1, [])) == []

def test_ingest():
    assert isinstance(ingest('abc'), safetype)
    assert ingest('abc') == 'abc'
    assert isinstance(ingest(b'abc'), safetype)
    assert ingest(b'abc') == 'abc'
    assert isinstance(ingest(u'abc'), safetype)
    assert ingest(u'abc') == 'abc'
    with pytest.raises(TypeError):
        assert ingest(5)

def test_egest():
    assert isinstance(egest('abc'), rawtype)
    assert egest('abc') == b'abc'
    assert isinstance(egest(b'abc'), rawtype)
    assert egest(b'abc') == b'abc'
    assert isinstance(egest(u'abc'), rawtype)
    assert egest(u'abc') == b'abc'
    with pytest.raises(TypeError):
        assert egest(5)

def test_ingest_egest():
    byte_str = b'I\xc3\xb1t\xc3\xabrn\xc3\xa2ti\xc3\xb4n\xc3\xa0li\xc5\xbe\xc3\xa6ti\xc3\xb8n\n'
    s = ingest(byte_str)
    b = egest(s)
    assert byte_str == b

def test_egest_ingest():
    tst_str = u'abc'
    b = egest(tst_str)
    s = ingest(b)
    assert tst_str == s

def test_dot():
    assert dot("upper")("abc")() == "ABC"

def test_nth():
    lst = [1,2,3]
    assert nth(0)(lst) == 1
    assert nth(1)(lst) == 2
    assert first(lst) == 1
    assert second(lst) == 2

def test_every():
    assert every(even, [2,4,6])
    assert not every(even, [2,3,4])
    assert every(even, [])

def test_repeater():
    context = dict(value=0)
    def increment_value(returns):
        if isinstance(returns, bool):
          returns = [returns]
        returns = iter(returns)
        context['value'] += 1
        ret = next(returns)
        if isinstance(ret, Exception):
            raise ret
        else:
            return ret
    always_true  = lambda x: True
    always_false = lambda x: False

    # On success run once.
    # TODO Retire use of context using nonlocal when we drop py2X support.
    context = dict(value=0)
    r = repeater(increment_value)
    o = r([True])
    assert(context['value'] == 1)
    assert(o == True)
    o = r(True)
    assert(context['value'] == 2)
    assert(o == True)
    # On failure, retry.
    context = dict(value=0)
    r = repeater(increment_value)
    o = r(iter([False]*10 + [True]))
    assert(context['value'] == 11)
    assert(o == True)
    # Stop after max tries
    context = dict(value=0)
    r = repeater(increment_value, max_tries=2)
    o = r(iter([False, False, True]))
    assert(context['value'] == 2)
    assert(o == False)
    # Test period sleeping
    # TODO switch to a test function varient which record the time in array and we check the spacing.
    context = dict(value=0)
    start_time = time()
    r = repeater(increment_value, period=.1)
    o = r(iter([False, True]))
    end_time = time()
    elapsed = end_time-start_time
    assert(context['value'] == 2)
    assert(o == True)
    assert(elapsed >= .1)
    # Test max_time
    context = dict(value=0)
    start_time = time()
    r = repeater(increment_value, period=.1, max_time=.15)
    o = r(iter([False, False, False]))
    end_time = time()
    elapsed = end_time-start_time
    assert(context['value'] == 3)
    assert(o == False)
    assert(elapsed >= .1)
    # Test Predicate
    context = dict(value=0)
    r = repeater(increment_value, predicate=even)
    o = r(iter([1, 3, 4]))
    assert(o == True)
    assert(context['value'] == 3)
    # Test throw expected
    context = dict(value=0)
    r = repeater(increment_value, catch_predicate=lambda e: isinstance(e, ValueError))
    o = r(iter([ValueError("bad value"), True]))
    assert(o == True)
    assert(context['value'] == 2)
    # Test throw unexpected
    context = dict(value=0)
    with pytest.raises(NotImplementedError):
        r = repeater(increment_value, catch_predicate=lambda e: isinstance(e, ValueError))
        o = r(iter([NotImplementedError("Oops"), True]))

def test_pfmap():
    increment = lambda x: x+1
    p_increment = pfmap(increment)
    list(p_increment(range(1,100))) == list(range(2,101))

