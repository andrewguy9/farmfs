# Worked out from https://raganwald.com/2017/04/30/transducers.html
# JS -> Python conversion.

def reduceWith(reducer, seed, iterable):
    """
    reduceWith takes reducer as first argument, computes a reduction over iterable.
    Think foldl from Haskell.
    reducer is (b -> a -> b)
    Seed is b
    iterable is [a]
    reduceWith is (b -> a -> b) -> b -> [a] -> b
    """
    accumulation = seed
    for value in iterable:
        accumulation = reducer(accumulation, value)
    return accumulation

arrayOf = lambda acc, val: acc.append(val) or acc
arrayOf.__doc__ = \
"""
Optimized version of array accumulator which doesn't reallocate on every loop
iteration.
"""

sumOf = lambda acc, val: acc + val
sumOf.__doc__ = """Reducer which computes a sum"""

def joinedWith(seperator):
    def joint(acc, val):
        if acc == '':
            return val
        else:
            return "%s%s%s" % (acc, seperator, val)
    return joint

map = lambda fn: lambda reducer: lambda acc, val: reducer(acc, fn(val))
map.__doc__ = """map is decorator which parameterizes the (+1) as a parameter."""

filter = lambda pred: \
        lambda reducer: \
        lambda acc, val: reducer(acc, val) if pred(val) else acc
filter.__doc__ =  \
"""
pred is (a->Bool)
reducer is (b -> a -> b)
"""

"""
How can we perform an arbitrary series of compositions?
Yes, with a reduction!
"""
compositionOf = lambda acc, val: lambda *args, **kwargs: val(acc(*args, **kwargs))
compose = lambda *fns: reduceWith(compositionOf, lambda x: x, fns);

def transduce(transformer, reducer, seed, iterable):
    """
    transformer is (a -> b)
    reducer is (b -> a -> b)
    seed is b
    iterable is [a]
    """
    transformedReducer = transformer(reducer)
    accumulation = seed
    for value in iterable:
        accumulation = transformedReducer(accumulation, value)
    return accumulation

