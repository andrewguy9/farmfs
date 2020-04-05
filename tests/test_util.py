from farmfs.util import empty2dot, compose, concat, concatMap, fmap, identity, irange, invert, count, take, uniq, groupby, curry, uncurry, identify, pipeline, zipFrom
import functools
from collections import Iterator
from farmfs.util import ingest, egest, safetype, rawtype
import pytest

try:
    from itertools import ifilter
except ImportError:
    ifilter=filter

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

def test_count():
  assert count(iter([])) == 0
  assert count(iter([1,2,3])) == 3

def test_curries():
  unadd = uncurry(add)
  assert unadd([1,2]) == 3
  readd = curry(unadd)
  assert readd(1,2) == 3

def test_identify():
  id_inc = identify(inc)
  assert id_inc(1) == 1
  #TODO i didn't test that inc got called.

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
