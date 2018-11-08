# -*- coding: utf-8 -*-
class ExponentialBackoff:
    """
    Exponential backoff with an upper limit.

    >>> delay = ExponentialBackoff(1, 2, 100)
    >>> [delay.next() for _ in range(10)]
    [1, 2, 4, 8, 16, 32, 64, 100, 100, 100]

    """

    def __init__(self, start, factor, limit):
        self.current = start
        self.factor = factor
        self.limit = limit

    def __iter__(self):
        return self

    def __next__(self):
        return self.next()

    def next(self) -> int:
        try:
            return round(self.current)
        finally:
            self.current = min(self.current * self.factor, self.limit)


class FibonaccianBackoff(ExponentialBackoff):
    """
    Exponential backoff with parameters that produce a Fibonacci-like sequence:

    1, 1, 2, 3, 5, 8, 13, 21, ...
    """

    def __init__(self, limit):
        super().__init__(start=0.724, factor=1.618, limit=limit)
