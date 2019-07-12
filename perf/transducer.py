import timeit
from tabulate import tabulate
from farmfs.util import *
from farmfs.transduce import transduce, arrayOf
from farmfs.transduce import compose as comp
from farmfs.transduce import map as transmap

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
  map(inc_square, nums)


def inc_square_map_lambda(nums):
  map(lambda x: (x+1)*(x+1), nums)

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
    print tabulate(table, headers = ['case', 'time', 'scale'])

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

