from __future__ import print_function
import timeit
from tabulate import tabulate
from farmfs.util import *
from farmfs.transduce import transduce, arrayOf
from farmfs.transduce import compose as comp
from farmfs.transduce import map as transmap
from farmfs.blobstore import fast_reverser, old_reverser

def inc_square_comprehension(nums):
  return [(num+1)*(num+1) for num in nums]

def inc_square_loop(nums):
  out = []
  for n in nums:
    out.append((n+1)*(n+1))
  return out

def inc_square_iter(nums):
  for n in nums:
    yield (n+1) * (n+1)

def inc_square(x):
    return (x+1)*(x+1)

def inc_square_map(nums):
  list(map(inc_square, nums))


def inc_square_map_lambda(nums):
  list(map(lambda x: (x+1)*(x+1), nums))

def inc(x):
    return x+1

def square(x):
    return x*x

squares = transmap(lambda x: x*x)
incs = transmap(lambda x: x+1)

inc_square_fmap = fmap(inc_square)

inc_square_compose = fmap(compose(inc, square))

inc_square_composeFunctor = fmap(composeFunctor(inc, square))

inc_square_pipeline = pipeline(fmap(inc), fmap(square))

def inc_square_transduce_compose(lst):
    transduce(comp(incs, squares), arrayOf, [], lst)

hundredK = range(100000)

def performance_case(name, *args, **kwargs):
    return (name, args, kwargs)

def performance_compare(cases):
    lowest = None
    results = {}
    for name, args, kwargs in cases:
        time = timeit.timeit(*args, **kwargs)
        if lowest is None or time < lowest:
            lowest = time
        results[name] = time
    table = [ (name, time, "%.1f" % (time / lowest)) for (name, time) in results.items()]
    print(tabulate(table, headers = ['case', 'time', 'scale']))

def test_traditional():
    traditional = [
            performance_case("inc_square_comprehension", partial(inc_square_comprehension, hundredK), number=1000),
            performance_case("inc_square_loop", partial(inc_square_loop, hundredK), number=1000),
            performance_case("inc_square_iter", compose(list, partial(inc_square_iter,hundredK)), number=1000)
            ]
    performance_compare(traditional)

def test_maps():
    maps = [
            performance_case("inc_square_map", partial(inc_square_map, hundredK), number=1000),
            performance_case("inc_square_map_lambda", partial(inc_square_map_lambda, hundredK), number=1000),
            performance_case("inc_square_fmap", compose(list, partial(inc_square_fmap, hundredK)), number=1000)
            ]
    performance_compare(maps)

def test_compose():
    composes = [
    performance_case("inc_square_compose", compose(consume, partial(inc_square_compose, hundredK)), number=1000),
    performance_case("inc_square_composeFunctor", compose(consume, partial(inc_square_composeFunctor, hundredK)), number=1000)
    ]
    performance_compare(composes)

def test_transducers():
    transducers = [
            performance_case("inc_square_pipeline", compose(consume, partial(inc_square_pipeline, hundredK)), number=1000),
            performance_case("inc_square_transduce_compose", partial(inc_square_transduce_compose, hundredK), number=1000)
            ]
    performance_compare(transducers)

def test_reverser():
    old_fn = old_reverser()
    fast_fn = fast_reverser()
    sample = "/tmp/perftest/.farmfs/userdata/d41/d8c/d98/f00b204e9800998ecf8427e"
    reversers = [
            performance_case("reverser_old",  compose(consume, partial(fmap(old_fn),  [sample]*10000)), number=1000),
            performance_case("reverser_fast", compose(consume, partial(fmap(fast_fn), [sample]*10000)), number=1000),
            ]
    performance_compare(reversers)

def test_parallelism_short():
    maps = [
            performance_case("inc_square_pipeline", compose(consume, partial( fmap(inc), hundredK)), number=10),
            performance_case("inc_square_parallel_pipeline", compose(consume, partial(pfmap(inc), hundredK)), number=10),
            ]
    performance_compare(maps)

def test_parallelism_cpu_bound():
    maps = [
            performance_case("sum_pipeline",          compose(consume, partial( fmap(sum), [range(1000000) for _ in range(10)])), number=10),
            performance_case("sum_parallel_pipeline", compose(consume, partial(pfmap(sum), [range(1000000) for _ in range(10)])), number=10),
            ]
    performance_compare(maps)

def test_parallelism_io():
    maps = [
            performance_case("io_pipeline",          compose(consume, partial( fmap(sleep), [.1]*40)), number=10),
            performance_case("io_parallel_pipeline", compose(consume, partial(pfmap(sleep), [.1]*40)), number=10),
            ]
    performance_compare(maps)
