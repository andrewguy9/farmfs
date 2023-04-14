from __future__ import print_function
import timeit
from tabulate import tabulate
from farmfs.util import pipeline, ffilter, partial, fmap, compose, composeFunctor, consume, pfmap, sleep
from farmfs.transduce import transduce, arrayOf
from farmfs.transduce import compose as comp
from farmfs.transduce import map as transmap
from farmfs.blobstore import fast_reverser, old_reverser

def isEven(n):
    return n % 2 == 0

def sum_even_loop(ns):
    total = 0
    for n in ns:
        if isEven(n):
            total += n
    return total

def sum_even_comprehension(ns):
    return sum([n for n in ns if isEven(n)])

def sum_even_filter(ns):
    return sum(filter(isEven, ns))

def sum_even_pipeline(ns):
    return pipeline(ffilter(isEven), sum)(ns)

def isPrime(x):
    for test in range(2, int((x ** .5) + 1)):
        if x % test == 0:
            return False
    return True

def test_is_prime():
    assert isPrime(1) is True
    assert isPrime(2) is True
    assert isPrime(3) is True
    assert isPrime(4) is False
    assert isPrime(5) is True
    assert isPrime(6) is False
    assert isPrime(7) is True

def sum_primes_comprehension(ns):
    return sum([n for n in ns if isPrime(n)])

def sum_primes_loop(ns):
    total = 0
    for n in ns:
        if isPrime(n):
            total += n
    return total

def sum_primes_filter(ns):
    return sum(filter(isPrime, ns))

def sum_primes_pipeline(ns):
    pipeline(ffilter(isPrime), sum)(ns)

# args example: partial(inc_square_comprehension, hundredK)
# kwargs example number=1000
def performance_compare(*cases, case_args=[], timeit_kwargs={}):
    results = {}
    for case in cases:
        name = case.__name__
        case = partial(case, *case_args)
        # TODO make number configurable.
        time = timeit.timeit(case, **timeit_kwargs)
        results[name] = time
    lowest = min([time for time in results.values()])
    table = [(name, time, "%.2f" % (time / lowest)) for (name, time) in results.items()]
    print(tabulate(table, headers=['case', 'time', 'scale']))

def test_primes_sum():
    performance_compare(
        sum_primes_comprehension,
        sum_primes_loop,
        sum_primes_filter,
        sum_primes_pipeline,
        case_args=[list(range(1000))],
        timeit_kwargs={'number': 1000})

def inc_square_comprehension(nums):
    return [(num + 1) * (num + 1) for num in nums]

def inc_square_loop(nums):
    out = []
    for n in nums:
        out.append((n + 1) * (n + 1))
    return out

def inc_square_iter(nums):
    for n in nums:
        yield (n + 1) * (n + 1)

def inc_square(x):
    return (x + 1) * (x + 1)

def inc_square_map(nums):
    list(map(inc_square, nums))


def inc_square_map_lambda(nums):
    list(map(lambda x: (x + 1) * (x + 1), nums))

def inc(x):
    return x + 1

def square(x):
    return x * x


squares = transmap(lambda x: x * x)
incs = transmap(lambda x: x + 1)

inc_square_fmap = fmap(inc_square)

inc_square_compose = fmap(compose(inc, square))

inc_square_composeFunctor = fmap(composeFunctor(inc, square))

inc_square_pipeline = pipeline(fmap(inc), fmap(square))

def inc_square_transduce_compose(lst):
    transduce(comp(incs, squares), arrayOf, [], lst)


hundredK = range(100000)

def performance_case(name, *args, **kwargs):
    return (name, args, kwargs)

def test_traditional():
    performance_compare(inc_square_comprehension,
                        inc_square_loop,
                        compose(list, inc_square_iter),
                        case_args=[hundredK],
                        timeit_kwargs={'number': 1000})

def test_maps():
    performance_compare(inc_square_map,
                        inc_square_map_lambda,
                        compose(list, inc_square_fmap),
                        case_args=[hundredK],
                        timeit_kwargs={'number': 1000})

def test_compose():
    performance_compare(compose(consume, inc_square_compose),
                        compose(consume, inc_square_composeFunctor),
                        case_args=[hundredK],
                        timeit_kwargs={'number': 1000})

def test_transducers():
    performance_compare(compose(consume, inc_square_pipeline),
                        inc_square_transduce_compose,
                        case_args=[hundredK],
                        timeit_kwargs={'number': 1000})

def test_reverser():
    sample = "/tmp/perftest/.farmfs/userdata/d41/d8c/d98/f00b204e9800998ecf8427e"
    performance_compare(compose(consume, fmap(old_reverser())),
                        compose(consume, fmap(fast_reverser())),
                        timeit_kwargs={'number': 1000},
                        case_args=[[sample] * 10000])

def test_parallelism_short():
    performance_compare(compose(consume, fmap(inc)),
                        compose(consume, pfmap(inc)),
                        case_args=[hundredK],
                        timeit_kwargs={'number': 10})

def test_parallelism_cpu_bound():
    performance_compare(compose(consume, fmap(sum)),
                        compose(consume, pfmap(sum)),
                        case_args=[[range(1000000) for _ in range(10)]],
                        timeit_kwargs={'number': 10})

def test_parallelism_io():
    performance_compare(compose(consume, fmap(sleep)),
                        compose(consume, pfmap(sleep)),
                        case_args=[[.1] * 40],
                        timeit_kwargs={'number': 10})
