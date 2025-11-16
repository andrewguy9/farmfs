import functools as f
from timeit import timeit

def plus(x, y):
    return x + y

def test_reduce():
    f.reduce(plus, range(10000), 0)


timeit(test_reduce)
456.74985525000005

def loop_reduce(fn, coll, init):
    i = iter(coll)
    acc = fn(init, next(i))
    for v in i:
        acc = fn(acc, v)
    return acc

def test_loop():
    loop_reduce(plus, range(10000), 0)


timeit(test_loop)
550.6917261249998