def _comp_0():
    raise NotImplementedError("Composition of 0 functions is not supported.")


def _comp_1(a):
    return a


def _comp_2(a, b):
    def _combined2(*args, **kwargs):
        return b(a(*args, **kwargs))

    return _combined2


def _comp_3(a, b, c):
    def _combined3(*args, **kwargs):
        return c(b(a(*args, **kwargs)))

    return _combined3


def _comp_4(a, b, c, d):
    def _combined4(*args, **kwargs):
        return d(c(b(a(*args, **kwargs))))

    return _combined4


def _comp_5(a, b, c, d, e):
    def _combined5(*args, **kwargs):
        return e(d(c(b(a(*args, **kwargs)))))

    return _combined5


def _comp_6(a, b, c, d, e, f):
    def _combined6(*args, **kwargs):
        return f(e(d(c(b(a(*args, **kwargs))))))

    return _combined6


def _comp_7(a, b, c, d, e, f, g):
    def _combined7(*args, **kwargs):
        return g(f(e(d(c(b(a(*args, **kwargs)))))))

    return _combined7


def _comp_8(a, b, c, d, e, f, g, h):
    def _combined8(*args, **kwargs):
        return h(g(f(e(d(c(b(a(*args, **kwargs))))))))

    return _combined8


_comp_fns = [
    _comp_0,
    _comp_1,
    _comp_2,
    _comp_3,
    _comp_4,
    _comp_5,
    _comp_6,
    _comp_7,
    _comp_8,
]


def compose(*fns):
    n = len(fns)
    if n < len(_comp_fns):
        return _comp_fns[n](*fns)
    raise NotImplementedError("Composition of more than 8 functions is not supported.")


def _lcomp_0():
    raise NotImplementedError("Composition of 0 functions is not supported.")


def _lcomp_1(a):
    return a


def _lcomp_2(a, b):
    return lambda *args, **kwargs: b(a(*args, **kwargs))


def _lcomp_3(a, b, c):
    return lambda *args, **kwargs: c(b(a(*args, **kwargs)))


def _lcomp_4(a, b, c, d):
    return lambda *args, **kwargs: d(c(b(a(*args, **kwargs))))


def _lcomp_5(a, b, c, d, e):
    return lambda *args, **kwargs: e(d(c(b(a(*args, **kwargs)))))


def _lcomp_6(a, b, c, d, e, f):
    return lambda *args, **kwargs: f(e(d(c(b(a(*args, **kwargs))))))


def _lcomp_7(a, b, c, d, e, f, g):
    return lambda *args, **kwargs: g(f(e(d(c(b(a(*args, **kwargs)))))))


def _lcomp_8(a, b, c, d, e, f, g, h):
    return lambda *args, **kwargs: h(g(f(e(d(c(b(a(*args, **kwargs))))))))


_lcomp_fns = [
    _lcomp_0,
    _lcomp_1,
    _lcomp_2,
    _lcomp_3,
    _lcomp_4,
    _lcomp_5,
    _lcomp_6,
    _lcomp_7,
    _lcomp_8,
]


# TODO lcompose has the same performance as compose, remove it.
def lcompose(*fns):
    n = len(fns)
    if n < len(_lcomp_fns):
        return _lcomp_fns[n](*fns)
    raise NotImplementedError("Composition of more than 8 functions is not supported.")


def _fcomp_0():
    raise NotImplementedError("Composition of 0 functions is not supported.")


def _fcomp_1(a):
    return a


def _fcomp_2(a, b):
    def _combined2(x):
        return b(a(x))

    return _combined2


def _fcomp_3(a, b, c):
    def _combined3(x):
        return c(b(a(x)))

    return _combined3


def _fcomp_4(a, b, c, d):
    def _combined4(x):
        return d(c(b(a(x))))

    return _combined4


def _fcomp_5(a, b, c, d, e):
    def _combined5(x):
        return e(d(c(b(a(x)))))

    return _combined5


def _fcomp_6(a, b, c, d, e, f):
    def _combined6(x):
        return f(e(d(c(b(a(x))))))

    return _combined6


def _fcomp_7(a, b, c, d, e, f, g):
    def _combined7(x):
        return g(f(e(d(c(b(a(x)))))))

    return _combined7


def _fcomp_8(a, b, c, d, e, f, g, h):
    def _combined8(x):
        return h(g(f(e(d(c(b(a(x))))))))

    return _combined8


_fcomp_fns = [
    _fcomp_0,
    _fcomp_1,
    _fcomp_2,
    _fcomp_3,
    _fcomp_4,
    _fcomp_5,
    _fcomp_6,
    _fcomp_7,
    _fcomp_8,
]


# TODO fcompose has the same performance as compose, remove it.
def fcompose(*fns):
    n = len(fns)
    if n < len(_fcomp_fns):
        return _fcomp_fns[n](*fns)
    raise NotImplementedError("Composition of more than 8 functions is not supported.")


# TODO ecompose has the same performance as compose, remove it.
def ecompose(*fns):
    n = len(fns)
    if n == 0:
        raise NotImplementedError("Composition of 0 functions is not supported.")

    # Build "return fN(fN-1(...f1(x)))"
    expr = "x"
    for i in range(n):
        expr = f"_f{i}({expr})"

    # Build function source
    params = ", ".join([f"_f{i}=fns[{i}]" for i in range(n)])
    src = f"def _c(x, {params}):\n    return {expr}\n"

    ns = {"fns": fns}
    exec(src, ns)
    return ns["_c"]
