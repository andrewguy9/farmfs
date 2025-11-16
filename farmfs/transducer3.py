from typing import TypeVar, Callable, Generic

T = TypeVar("T")
U = TypeVar("U")
Reducer = Callable[[T, U], T]

class Transducer(Generic[T, U]):
    def __init__(self, rf: Reducer[T, U]):
        self.rf = rf

    def init(self):
        pass

    def step(self, result: T, input: U):
        return self.rf(result, input)

    def completion(self, result: T):
        return self.rf(result)


A = TypeVar("A")
B = TypeVar("B")

class Mapping(Transducer[T, B], Generic[T, A, B]):

    def __init__(self, f: Callable[[A], B], rf: Reducer[T, B]):
        super().__init__(rf)
        self.f = f

    def step(self, result: T, input: A):
        return self.rf(result, self.f(input))


def mapping(f: Callable[[A], B]):
    def mapped(rf: Reducer[T, B]):
        return Mapping(f, rf)
    return mapped

class Filtering(Transducer):

    def __init__(self, pred, rf):
        super().__init__(rf)
        self.pred = pred

    def step(self, result, input):
        if self.pred(input):
            return self.rf(result, input)
        return result


def filtering(pred: Callable[[U], bool]):
    def filtered(rf: Reducer[T, U]):
        return Filtering(pred, rf)
    return filtered


class Taking(Transducer):

    def __init__(self, n, rf):
        super().__init__(rf)
        self.n = n
        self.seen = 0

    def step(self, result, input):
        self.seen += 1
        if self.seen <= self.n:
            return self.rf(result, input)
        return result  # TODO We want to signal that we are done.


def taking(n: int):
    def taker(rf: Reducer[T, U]):
        return Taking(n, rf)
    return taker


T1 = TypeVar("T1")
T2 = TypeVar("T2")
U1 = TypeVar("U1")

XForm = Callable[[Reducer[T1, U1], T1], Transducer[T2, T1]]


Acc1 = TypeVar("Acc1")
Unit1 = TypeVar("Unit1")
Acc2 = TypeVar("Acc2")
Acc3 = TypeVar("Acc3")
Acc4 = TypeVar("Acc4")

def comp2(xform1: XForm[Acc1, Unit1, Acc2], xform2: XForm[Acc3, Acc2, Acc4]) -> XForm[Acc1, Unit1, Acc4]:
    def combied(rf: Reducer[Acc1, Unit1]) -> Transducer[Acc4, Acc1]:
        return xform1(xform2(rf))
    return combied


def transduce(xform, reducer, coll, init):
    transducer: Transducer = xform(reducer)
    result = init
    transducer.init()
    for input in coll:
        result = transducer.step(result, input)
    return transducer.completion(result)


def plus(x, y):
    return x + y


def inc(x):
    return x + 1


def odd(x):
    return x % 2 == 1


def buildStr(acc: str, val: str):
    return acc + val


assert transduce(mapping(inc), plus, [1, 2, 3], 0) == 9
