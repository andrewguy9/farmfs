from farmfs.transduce import arrayOf, sumOf, joinedWith, map, filter, compose, reduceWith, transduce

def test_accumulators():
    acc = [1, 2, 3]
    val = 4
    reducer = lambda acc, x: acc + [x] or acc
    assert reducer(acc, val) == [1, 2, 3, 4]
    acc = set([1, 2, 3])
    val = 4
    reducer = lambda acc, x: acc.add(x) or acc
    assert reducer(acc, val) == set([1, 2, 3, 4])

def test_reduceWithArrayPlusOperator():
    assert reduceWith(lambda acc, val: acc + [val], [], [1, 2, 3]) == [1, 2, 3]


isOdd = lambda x: x % 2 == 1
def test_reduceWithAsAggregator():
    assert reduceWith(filter(lambda x: x > 5)(squares(arrayOf)), [], one2ten) == [36, 49, 64, 81, 100]
    assert reduceWith(filter(isOdd)(arrayOf), [], one2ten) == [1, 3, 5, 7, 9]
    assert reduceWith(filter(isOdd)(squares(sumOf)), 0, one2ten) == 165

def test_efficientAccumulator():
    assert reduceWith(arrayOf, [], [1, 2, 3]) == [1, 2, 3]

def test_joinedWithReducer():
    assert reduceWith(joinedWith(', '), '', [1, 2, 3]) == "1, 2, 3"

def test_decorators():
    """
    Demonstrate decorators: Functions which consume a function and return a function.
    """
    incrementSecond = lambda binaryFn: lambda x, y: binaryFn(x, y + 1)
    power = lambda base, exp: base ** exp
    higherPower = incrementSecond(power)
    assert power(2, 3) == 8
    assert higherPower(2, 3) == 16


one2ten = list(range(1, 10 + 1))
squares = map(lambda x: x * x)
def test_reduceWithMap():
    incrementValue = map(lambda x: x + 1)
    assert reduceWith(incrementValue(arrayOf), [], [1, 2, 3]) == [2, 3, 4]
    assert reduceWith(map(lambda x: x + 1)(arrayOf), [], [1, 2, 3]) == [2, 3, 4]
    assert reduceWith(map(lambda x: x + 1)(joinedWith('.')), '', [1, 2, 3]) == "2.3.4"
    assert reduceWith(map(lambda x: x + 1)(sumOf), 0, [1, 2, 3]) == 9
    assert reduceWith(squares(sumOf), 0, one2ten) == 385

def test_reduceWith():
    assert reduceWith(arrayOf, [], one2ten) == [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    def bigUns(acc, val):
        if val > 5:
            acc.append(val)
        return acc
    assert reduceWith(bigUns, [], one2ten) == [6, 7, 8, 9, 10]
    assert reduceWith(squares(bigUns), [], one2ten) == [9, 16, 25, 36, 49, 64, 81, 100]


"""
This is the squares which are bigger than 5. How do we get the squares of the numbers bigger than 5?
"""

def test_reduceWithAsFilter():
    bigUnsOf = lambda reducer: lambda acc, val: reducer(acc, val) if val > 5 else acc
    assert reduceWith(squares(arrayOf), [], one2ten) == [1, 4, 9, 16, 25, 36, 49, 64, 81, 100]
    assert reduceWith(bigUnsOf(squares(arrayOf)), [], one2ten) == [36, 49, 64, 81, 100]


"""
bigUnsOf is specific. Lets move predicate to function parameter.
"""
plusFive = lambda x: x + 5
divideByTwo = lambda x: x / 2
plusFiveDividedByTwo = compose(plusFive, divideByTwo)
def test_helpers():
    assert plusFive(3) == 8
    assert divideByTwo(8) == 4

def test_compose():
    assert plusFiveDividedByTwo(3) == 4


squaresOfTheOddNumbers = compose(squares, filter(isOdd))
def test_reduceWithComposition():
    assert reduceWith(squaresOfTheOddNumbers(arrayOf), [], one2ten) == [1, 9, 25, 49, 81]
    assert reduceWith(squaresOfTheOddNumbers(sumOf), 0, one2ten) == 165

def test_transduce():
    assert transduce(squaresOfTheOddNumbers, sumOf, 0, one2ten) == 165
    assert transduce(squaresOfTheOddNumbers, arrayOf, [], one2ten) == [1, 9, 25, 49, 81]
