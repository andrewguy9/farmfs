from datetime import datetime, timezone
from typing import Callable, Iterable, List, Tuple
from farmfs.util import (
    parse_utc,
    format_utc,
    add_seconds,
    is_past,
)
from farmfs.util import (
    compose,
    concat,
    concatMap,
    count,
    copyfileobj,
    curry,
    empty_default,
    every,
    ffilter,
    first,
    finvert,
    fmap,
    fork,
    groupby,
    identify,
    identity,
    invert,
    irange,
    jaccard_similarity,
    mapM,
    nth,
    pfmap,
    pfmaplazy,
    pipeline,
    retry,
    RetriesExhausted,
    retryFdIo2,
    retryThunk,
    runState,
    second,
    take,
    uncurry,
    uniq,
    withHandles2,
    withHandles2Thunk,
    file_thunk,
    zipFrom,
)
from collections.abc import Iterator
from farmfs.util import ingest, egest
import pytest
import io

try:
    from unittest.mock import Mock
except ImportError:
    pass


def add(x: int, y: int) -> int:
    return x + y


assert add(1, 2) == 3


def inc(x: int) -> int:
    return x + 1


assert inc(1) == 2


def even(x: int) -> bool:
    return x % 2 == 0


assert even(2) is True
assert even(1) is False

even_list = ffilter(even)
assert list(even_list([1, 2, 3, 4])) == [2, 4]


def test_empty_default() -> None:
    # Test empty behavior
    assert empty_default([], [1]) == [1]
    # Test non empty behavior
    ls = [1, 2, 3]
    assert empty_default(ls, [4]) == ls
    # Test iterators work
    assert empty_default(iter([1, 2, 3]), iter([4])) == [1, 2, 3]
    # Test output is a copy
    i = [1, 2, 3]
    d = [5, 6, 7]
    o = empty_default(i, d)
    i.append(4)
    d.append(8)
    assert i == [1, 2, 3, 4]
    assert d == [5, 6, 7, 8]
    assert o == [1, 2, 3]
    # Test default is a copy
    i = []
    d = [5, 6, 7]
    o = empty_default(i, d)
    i.append(4)
    d.append(8)
    assert i == [4]
    assert d == [5, 6, 7, 8]
    assert o == [5, 6, 7]


def test_compose() -> None:
    inc_add = compose(inc, add)
    assert inc_add(1, 2) == 4


def test_concat() -> None:
    assert list(concat([[1, 2, 3], [4, 5, 6]])) == [1, 2, 3, 4, 5, 6]
    assert list(concat([[1], [2, 3, 4]])) == [1, 2, 3, 4]
    assert list(concat([[1, 2, 3], [4]])) == [1, 2, 3, 4]
    assert list(concat([[1], [1, 2], [1, 2, 3]])) == [1, 1, 2, 1, 2, 3]


def test_concatMap() -> None:
    lst = list(range(4))
    def expand(x: int) -> list[int]:
        return [x] * x
    assert list(concatMap(expand)(lst)) == [
        1, 2, 2, 3, 3, 3,
    ]

def test_fmap() -> None:
    inc_iter = fmap(inc)
    assert list(inc_iter([1, 2, 3, 4])) == [2, 3, 4, 5]


def test_ffilter() -> None:
    even_iter = ffilter(even)
    assert list(even_iter([1, 2, 3, 4])) == [2, 4]


def test_identity() -> None:
    assert identity(5) == 5


def test_groupby() -> None:
    # TODO group by may not order results consistenly for asserts.
    assert groupby(even, [1, 2, 3, 4, 5, 6]) == [(False, [1, 3, 5]), (True, [2, 4, 6])]


def test_take() -> None:
    assert list(take(3)([1, 2, 3, 4, 5])) == [1, 2, 3]
    assert list(take(3)([1, 2])) == [1, 2]


def test_uniq() -> None:
    assert list(uniq([1, 2, 3, 4])) == [1, 2, 3, 4]
    assert list(uniq([1, 2, 2, 4])) == [1, 2, 4]
    assert list(uniq([1, 2, 3, 2])) == [1, 2, 3]


def test_irange() -> None:
    assert list(take(3)(irange(0, 1))) == [0, 1, 2]
    assert list(take(3)(irange(0, -1))) == [0, -1, -2]


def test_invert() -> None:
    assert invert(1) is False
    assert invert(True) is False
    assert invert(0) is True
    assert invert([]) is True
    assert invert([1]) is False


def test_finvert() -> None:
    assert finvert(lambda x: x + 1)(1) is False
    assert finvert(lambda x: x == 1)(1) is False
    assert finvert(lambda x: x)(0) is True
    assert finvert(list)() is True
    assert finvert(lambda x: [x])(1) is False


def test_count() -> None:
    assert count(iter([])) == 0
    assert count(iter([1, 2, 3])) == 3


def test_curries() -> None:
    unadd = uncurry(add)
    assert unadd((1, 2)) == 3
    readd = curry(unadd)
    assert readd(1, 2) == 3


def test_identify() -> None:
    mock = Mock(return_value=1)
    foo = identify(mock)
    result = foo(5)
    assert result == 5
    mock.assert_called_once_with(5)


def test_pipeline() -> None:
    inc_pipeline = pipeline(fmap(inc))
    assert isinstance(inc_pipeline([1, 2, 3]), Iterator), (
        "inc_pipeline should be an iterator."
    )
    assert list(inc_pipeline([1, 2, 3])) == [2, 3, 4]

    inc_list_pipeline = pipeline(fmap(inc), list)
    assert isinstance(inc_list_pipeline([1, 2, 3]), list), (
        "inc_list_pipeline should return a list"
    )
    assert inc_list_pipeline([1, 2, 3]) == [2, 3, 4]

    def two_over(x: int) -> float: return 2 / x
    def print_ratio(r: float) -> float: return print("ratio:", r) or r
    def list_float(xs: Iterable[float]) -> List[float]: return list(xs)
    print_ratios: Callable[[Iterable[int]], List[float]] = pipeline(
        fmap(two_over),
        fmap(print_ratio),
        list)
    with pytest.raises(ZeroDivisionError):
        print_ratios([2, 1, 0, -1, 2])

    range_pipeline: Callable[[int, int], List[int]] = pipeline(irange, even_list, take(3), list)
    assert range_pipeline(0, 1) == [0, 2, 4]


def test_zipFrom() -> None:
    assert list(zipFrom(1, [2, 3, 4])) == [(1, 2), (1, 3), (1, 4)]
    assert list(zipFrom(1, [])) == []


def test_ingest() -> None:
    assert isinstance(ingest("abc"), str)
    assert ingest("abc") == "abc"
    assert isinstance(ingest(b"abc"), str)
    assert ingest(b"abc") == "abc"
    assert isinstance(ingest("abc"), str)
    assert ingest("abc") == "abc"
    with pytest.raises(TypeError):
        assert ingest(5)  # type: ignore


def test_egest() -> None:
    assert isinstance(egest("abc"), bytes)
    assert egest("abc") == b"abc"
    assert isinstance(egest(b"abc"), bytes)
    assert egest(b"abc") == b"abc"
    assert isinstance(egest("abc"), bytes)
    assert egest("abc") == b"abc"
    with pytest.raises(TypeError):
        assert egest(5)  # type: ignore


def test_ingest_egest() -> None:
    byte_str = b"I\xc3\xb1t\xc3\xabrn\xc3\xa2ti\xc3\xb4n\xc3\xa0li\xc5\xbe\xc3\xa6ti\xc3\xb8n\n"
    s = ingest(byte_str)
    b = egest(s)
    assert byte_str == b


def test_egest_ingest() -> None:
    tst_str = "abc"
    b = egest(tst_str)
    s = ingest(b)
    assert tst_str == s


def test_nth() -> None:
    lst = [1, 2, 3]
    assert nth(0)(lst) == 1
    assert nth(1)(lst) == 2
    assert first(lst) == 1
    assert second(lst) == 2


def test_every() -> None:
    assert every(even, [2, 4, 6])
    assert not every(even, [2, 3, 4])
    assert every(even, [])


@pytest.mark.parametrize("pfmap_func", [pfmap, pfmaplazy])
def test_pfmap(pfmap_func) -> None:
    increment = lambda x: x + 1
    p_increment = pfmap_func(increment, workers=4)
    limit = 100
    assert sorted(p_increment(range(1, limit))) == sorted(range(2, limit + 1))


def test_jaccard_similarity() -> None:
    a = set([1, 2, 3])
    b = set([1, 2, 4, 5])
    similarity = jaccard_similarity(a, b)
    assert similarity == 0.4


def test_fork() -> None:
    inc = lambda x: x + 1
    sq = lambda x: x**2

    def fail(x):
        raise ValueError(x)

    assert fork()(5) == tuple()
    assert fork(inc, sq)(5) == (6, 25)
    with pytest.raises(ValueError):
        fork(inc, sq, fail)(5)


def test_retryFdIo2_write_file(tmp) -> None:
    src_fn = lambda: io.StringIO("foo")
    dst_path = tmp.join("b")
    dst_fn = lambda: dst_path.open("w")
    always_raise = lambda e: False
    retryFdIo2(src_fn, dst_fn, copyfileobj, always_raise, tries=3)
    with dst_path.open("r") as f:
        verify = f.read()
    assert verify == "foo"


def test_retryFdIo2_safe_output(tmp) -> None:
    src_fn = lambda: io.StringIO("foo")
    dst_path = tmp.join("b")
    dst_fn = lambda: dst_path.safeopen("w")
    always_raise = lambda e: False
    retryFdIo2(src_fn, dst_fn, copyfileobj, always_raise, tries=3)
    with dst_path.open("r") as f:
        verify = f.read()
    assert verify == "foo"


def countedSum(state: Tuple[int, int], x: int) -> Tuple[Tuple[int, int], int]:
    """
    Tracks the sum of the numbers passed to it, and
    the number of times it has been called.
    """
    total, n = state
    total += x
    n += 1
    return (total, n), total


def test_runState() -> None:
    """
    Test the runState function to tick the state by hand.
    """
    # TODO default state would be easier if the signature was (x, state)
    state0 = (0, 0)
    state1, result1 = runState(1, state0, countedSum)
    state2, result2 = runState(2, state1, countedSum)
    state3, result3 = runState(3, state2, countedSum)
    assert (result1, result2, result3) == (1, 3, 6)
    assert state1 == (1, 1)
    assert state2 == (3, 2)
    assert state3 == (6, 3)


def test_runStateMapM() -> None:
    """
    Combine the runState function with the mapM function to tick the state
    with an iterable of updates.
    """
    # Monad m => (a -> m b) -> [a] -> m [b]
    # State   => (a -> State b) -> [a] -> State [b]

    vals = [1, 2, 3]
    state0 = (0, 0)
    assert list(mapM(vals, runState, state0, countedSum)) == [1, 3, 6]


# --- withHandles2 / withHandles2Thunk / retry / retryThunk / file_thunk ---

def test_withHandles2_copies(tmp) -> None:
    src_fn = lambda: io.StringIO("hello")
    dst_path = tmp.join("out.txt")
    dst_fn = lambda: dst_path.open("w")
    withHandles2(src_fn, dst_fn, copyfileobj)
    with dst_path.open("r") as f:
        assert f.read() == "hello"


def test_withHandles2Thunk_is_deferred() -> None:
    called = []

    def get_src():
        called.append("src")
        return io.StringIO("x")

    def get_dst():
        called.append("dst")
        return io.StringIO()

    thunk = withHandles2Thunk(get_src, get_dst, copyfileobj)
    assert called == [], "thunk should not execute until called"
    thunk()
    assert "src" in called and "dst" in called


def test_retry_succeeds_immediately() -> None:
    call_count = [0]

    def fn() -> int:
        call_count[0] += 1
        return 42

    result = retry(fn, lambda e: True, tries=3)
    assert result == 42
    assert call_count[0] == 1


def test_retry_retries_then_succeeds() -> None:
    call_count = [0]

    def fn() -> int:
        call_count[0] += 1
        if call_count[0] < 3:
            raise ValueError("transient")
        return 99

    result = retry(fn, lambda e: isinstance(e, ValueError), tries=3)
    assert result == 99
    assert call_count[0] == 3


def test_retry_raises_retries_exhausted() -> None:
    def fn() -> int:
        raise ValueError("always fails")

    with pytest.raises(RetriesExhausted):
        retry(fn, lambda e: isinstance(e, ValueError), tries=3)


def test_retry_reraises_non_retryable() -> None:
    def fn() -> int:
        raise TypeError("not retryable")

    with pytest.raises(TypeError):
        retry(fn, lambda e: isinstance(e, ValueError), tries=3)


def test_retryThunk_returns_thunk() -> None:
    call_count = [0]

    def fn() -> str:
        call_count[0] += 1
        return "done"

    thunk = retryThunk(fn, lambda e: True, tries=3)
    assert callable(thunk)
    assert call_count[0] == 0, "retryThunk should not call fn until thunk is invoked"
    result = thunk()
    assert result == "done"
    assert call_count[0] == 1


def test_file_thunk_reads(tmp) -> None:
    p = tmp.join("hello.txt")
    with p.open("w") as f:
        f.write("world")

    thunk = file_thunk(p, "r")
    with thunk() as f:
        content = f.read()
    assert content == "world"


# ── Time utility tests ────────────────────────────────────────────────────────

def test_parse_format_roundtrip() -> None:
    now = datetime(2026, 2, 28, 3, 0, 0, tzinfo=timezone.utc)
    assert parse_utc(format_utc(now)) == now


def test_add_seconds() -> None:
    t = datetime(2026, 2, 28, 0, 0, 0, tzinfo=timezone.utc)
    assert add_seconds(t, 3600) == datetime(2026, 2, 28, 1, 0, 0, tzinfo=timezone.utc)


def test_is_past_true() -> None:
    past = datetime(2026, 2, 27, 0, 0, 0, tzinfo=timezone.utc)
    now = datetime(2026, 2, 28, 0, 0, 0, tzinfo=timezone.utc)
    assert is_past(past, now) is True


def test_is_past_false() -> None:
    future = datetime(2026, 3, 1, 0, 0, 0, tzinfo=timezone.utc)
    now = datetime(2026, 2, 28, 0, 0, 0, tzinfo=timezone.utc)
    assert is_past(future, now) is False


def test_is_past_equal() -> None:
    t = datetime(2026, 2, 28, 0, 0, 0, tzinfo=timezone.utc)
    assert is_past(t, t) is True
