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

if __name__ == '__main__':
    traditional = [
            performance_case("inc_square_comprehension", 'inc_square_comprehension(hundredK)', setup="from __main__ import inc_square_comprehension, hundredK", number=1000),
            performance_case("inc_square_loop", 'inc_square_loop(hundredK)', setup="from __main__ import inc_square_loop, hundredK", number=1000),
            performance_case("inc_square_iter", 'list(inc_square_iter(hundredK))', setup="from __main__ import inc_square_iter, hundredK", number=1000)
            ]
    performance_compare(traditional)
    maps = [
            performance_case("inc_square_map", 'inc_square_map(hundredK)', setup="from __main__ import inc_square_map, hundredK", number=1000),
            performance_case("inc_square_map_lambda", 'inc_square_map_lambda(hundredK)', setup="from __main__ import inc_square_map_lambda, hundredK", number=1000),
            performance_case("inc_square_fmap", 'list(inc_square_fmap(hundredK))', setup="from __main__ import inc_square_fmap, hundredK", number=1000)
            ]
    performance_compare(maps)
    composes = [
    performance_case("inc_square_compose", 'consume(inc_square_compose(hundredK))', setup="from farmfs.util import consume; from __main__ import inc_square_compose, hundredK", number=1000),
    performance_case("inc_square_composeFunctor", 'consume(inc_square_composeFunctor(hundredK))', setup="from farmfs.util import consume; from __main__ import inc_square_composeFunctor, hundredK", number=1000)
    ]
    performance_compare(composes)
    transducers = [
            performance_case("inc_square_pipeline", 'consume(inc_square_pipeline(hundredK))', setup="from farmfs.util import consume; from __main__ import inc_square_pipeline, hundredK", number=1000),
            performance_case("inc_square_transduce_compose", '(inc_square_transduce_compose(hundredK))', setup="from farmfs.util import consume; from __main__ import inc_square_transduce_compose, hundredK", number=1000)
            ]
    performance_compare(transducers)
