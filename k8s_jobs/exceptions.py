import functools
from typing import Dict, Type

# TODO: Find the circumstances under which to raise this. Plus add tests.
class NotFoundException(Exception):
    """
    Exception indicating job definition or job cannot be found
    """

    pass


def remaps_exception(exc_map: Dict[Type[Exception], Type[Exception]]):
    """
    Returns a decorator that will take all exceptions of the types indicated by the keys
    of `exc_map` and wrap them in exceptions of the corresponding value
    """

    def decorator(fn):
        @functools.wraps(fn)
        def inner(*args, **kwargs):
            try:
                return fn(*args, **kwargs)
            except Exception as e:
                if type(e) in exc_map:
                    raise exc_map[type(e)](e)
                raise

        return inner

    return decorator
