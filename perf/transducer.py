import timeit
from farmfs.util import *

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

inc_square_fmap = fmap(inc_square)

inc_square_compose = fmap(compose(inc, square))

inc_square_composeFunctor = fmap(composeFunctor(inc, square))

inc_square_transducer = transduce(fmap(inc), fmap(square))

hundredK = range(100000)

if __name__ == '__main__':
  import timeit
  print("inc_square_comprehension:", timeit.timeit('inc_square_comprehension(hundredK)', setup="from __main__ import inc_square_comprehension, hundredK", number=1000))
  print("inc_square_loop:", timeit.timeit('inc_square_loop(hundredK)', setup="from __main__ import inc_square_loop, hundredK", number=1000))
  print("inc_square_map:", timeit.timeit('inc_square_map(hundredK)', setup="from __main__ import inc_square_map, hundredK", number=1000))
  print("inc_square_map_lambda:", timeit.timeit('inc_square_map_lambda(hundredK)', setup="from __main__ import inc_square_map_lambda, hundredK", number=1000))
  print("inc_square_iter:", timeit.timeit('consume(inc_square_iter(hundredK))', setup="from farmfs.util import consume; from __main__ import inc_square_iter, hundredK", number=1000))
  print("inc_square_fmap:", timeit.timeit('consume(inc_square_fmap(hundredK))', setup="from farmfs.util import consume; from __main__ import inc_square_fmap, hundredK", number=1000))
  print("inc_square_compose:", timeit.timeit('consume(inc_square_compose(hundredK))', setup="from farmfs.util import consume; from __main__ import inc_square_compose, hundredK", number=1000))
  print("inc_square_composeFunctor:", timeit.timeit('consume(inc_square_composeFunctor(hundredK))', setup="from farmfs.util import consume; from __main__ import inc_square_composeFunctor, hundredK", number=1000))
  print("inc_square_transducer:", timeit.timeit('consume(inc_square_transducer(hundredK))', setup="from farmfs.util import consume; from __main__ import inc_square_transducer, hundredK", number=1000))

