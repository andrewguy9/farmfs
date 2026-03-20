from __future__ import annotations

from collections.abc import Callable
from typing import Any, ParamSpec, TypeVar, Union, overload

P = ParamSpec("P")
A = TypeVar("A")
B = TypeVar("B")
C = TypeVar("C")
D = TypeVar("D")
E = TypeVar("E")
F = TypeVar("F")
G = TypeVar("G")


@overload
def pipeline(f1: Callable[P, A], /) -> Callable[P, A]: ...
@overload
def pipeline(f1: Callable[P, A], f2: Callable[[A], B], /) -> Callable[P, B]: ...
@overload
def pipeline(f1: Callable[P, A], f2: Callable[[A], B], f3: Callable[[B], C], /) -> Callable[P, C]: ...
@overload
def pipeline(
    f1: Callable[P, A],
    f2: Callable[[A], B],
    f3: Callable[[B], C],
    f4: Callable[[C], D],
    /,
) -> Callable[P, D]: ...
@overload
def pipeline(
    f1: Callable[P, A],
    f2: Callable[[A], B],
    f3: Callable[[B], C],
    f4: Callable[[C], D],
    f5: Callable[[D], E],
    /,
) -> Callable[P, E]: ...
@overload
def pipeline(
    f1: Callable[P, A],
    f2: Callable[[A], B],
    f3: Callable[[B], C],
    f4: Callable[[C], D],
    f5: Callable[[D], E],
    f6: Callable[[E], F],
    /,
) -> Callable[P, F]: ...


def pipeline(*fns: Callable[..., Any]) -> Callable[..., Any]:
    if not fns:
        raise TypeError("pipeline() requires at least one function")

    first = fns[0]
    rest_t = tuple(fns[1:])

    def combined(*args: Any, **kwargs: Any) -> Any:
        x = first(*args, **kwargs)
        for f in rest_t:
            x = f(x)
        return x

    return combined


def then(
    f: Callable[[A], Union[B, Exception]]
) -> Callable[[Union[A, Exception]], Union[B, Exception]]:
    """Railway adapter: propagate Exception unchanged, otherwise feed value to f.

    Composes checks of the form  X -> Y | Exception  into a chain where a
    failure at any step short-circuits the rest:

        result = then(check_b)(check_a(x))
        result = then(check_c)(then(check_b)(check_a(x)))
    """
    def adapter(x: Union[A, Exception]) -> Union[B, Exception]:
        if isinstance(x, Exception):
            return x
        return f(x)
    return adapter
