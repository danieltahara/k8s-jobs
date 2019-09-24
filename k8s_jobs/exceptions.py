import functools
from typing import Callable, Dict, List, Optional, Tuple, Type


class NotFoundException(Exception):
    """
    Exception indicating job definition or job cannot be found
    """

    pass


def remaps_exception(
    exc_map: Optional[Dict[Type[Exception], Type[Exception]]] = None,
    matchers: Optional[
        List[Tuple[Callable[[Exception], bool], Type[Exception]]]
    ] = None,
):
    """
    Returns a decorator that will re-raise an exception according to the arguments.

    In determining a remap, the exc_map preceeds matchers, and matchers are tested in
    order.

    Arguments:
        exc_map:
            A type -> type mapping. If the raised exception is a key of this map, it
            will be re-raised under its corresponding value.
        matchers:
           A list of tuples of match conditions and an exception to wrap in.
    """
    exc_map = exc_map or {}
    matchers = matchers or []

    def decorator(fn):
        @functools.wraps(fn)
        def inner(*args, **kwargs):
            try:
                return fn(*args, **kwargs)
            except Exception as e:
                if type(e) in exc_map:
                    raise exc_map[type(e)](e)
                for matches, exc_type in matchers:
                    if matches(e):
                        raise exc_type(e)
                raise

        return inner

    return decorator
