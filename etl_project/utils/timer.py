from __future__ import annotations

from collections.abc import Callable
from functools import wraps
from time import perf_counter
from typing import ParamSpec, TypeVar


P = ParamSpec("P")
T = TypeVar("T")


def timed(function: Callable[P, T]) -> Callable[P, tuple[T, float]]:
    @wraps(function)
    def wrapper(*args: P.args, **kwargs: P.kwargs) -> tuple[T, float]:
        start_time = perf_counter()
        result = function(*args, **kwargs)
        elapsed_seconds = perf_counter() - start_time
        return result, round(elapsed_seconds, 4)

    return wrapper
