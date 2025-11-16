from collections import OrderedDict 
from collections import deque
from bisect import bisect


def pairwise(iterable):
    a, b = iter(iterable), iter(iterable)
    next(b, None)
    return zip(a, b)


assert list(pairwise([1, 2, 3, 4])) == [(1, 2), (2, 3), (3, 4)]

def merge_iter(top, bottom):
    top_items = iter(top)
    bottom_items = iter(bottom)
    finished = object()
    top_key = bottom_key = None  # None means pull from iter. finished means done.
    while top_key is not finished and bottom_key is not finished:
        if top_key is None:
            top_key, top_value = next(top_items, (finished, None))
        if bottom_key is not None:
            bottom_key, bottom_value = next(bottom_items, (finished, None))

        if top_key is finished:
            yield bottom_key, bottom_value
            bottom_key = None
        elif bottom_key is finished:
            yield top_key, top_value
            top_key = None
        else:
            if top_key == bottom_key:
                yield top_key, top_value
                top_key = None
                bottom_key = None
            elif top_key < bottom_key:
                yield top_key, top_value
                top_key = None
            elif top_key > bottom_key:
                yield bottom_key, bottom_value
                bottom_key = None


def merge(top, bottom):
    return Layer(merge_iter(top, bottom))


class Pyramid():
    def __init__(self, buffer_limit=1024):
        self.buffer = OrderedDict()
        self.buffer_limit = buffer_limit
        self.layers = deque()

    def add(self, id, data):
        if data is None:
            raise ValueError("data cannot be None")
        self.buffer[id] = data
        if len(self.buffer) > self.buffer_limit:
            self.flush()

    def remove(self, id):
        self.buffer[id] = None

    def flush(self):
        new_layer = Layer(self.buffer)
        self.layers.appendleft(new_layer)
        self.buffer = OrderedDict()
        while True:
            for top, bottom in pairwise(self.layers):
                if len(top) > len(bottom):
                    merged = merge(top, bottom)
                    self.layers.popleft()
                    self.layers.popleft()
                    self.layers.appendleft(merged)
                    break
            else:
                break

    def get(self, id):
        for layer in self.layers:
            try:
                return layer.get(id)
            except KeyError:
                pass
        raise KeyError(id)


class Layer():
    def __init__(self, ts):
        self.ts = list(ts)

    def get(self, id):
        i = bisect(self.ts, (id,))
        if i != len(self.ts) and self.ts[i][0] == id:
            return self.ts[i][1]
        else:
            raise KeyError(id)

    def __iter__(self):
        return iter(self.ts)
