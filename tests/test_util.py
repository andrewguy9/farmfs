import farmfs.util
import functools

def add(x,y): return x+y
assert add(1,2) == 3

def mult(x,y): return x*y
assert mult(3,4) == 12

def inc(x): return x+1
assert inc(1) == 2

def double(x): return x*2
assert double(5) == 10

def even(x): return x%2==0
assert even(2) == True
assert even(1) == False

inc_add = farmfs.util.compose(inc, add)
assert inc_add(1,2) == 4

inc_list = farmfs.util.fmap(inc)
assert inc_list([1,2,3,4]) == [2, 3, 4, 5]

even_list = functools.partial(filter, even)
assert even_list([1,2,3,4]) == [2, 4]

double_list = farmfs.util.fmap(double)
assert double_list([1,2,3]) == [2, 4, 6]

t = farmfs.util.transduce(even_list, double_list, inc_list)
assert t([0,1,2,3,4,5,6]) == [1,5,9,13]

assert list(farmfs.util.take(3)([1,2,3,4,5])) == [1,2,3]
assert list(farmfs.util.take(3)([1,2])) == [1,2]
