import functools
import logging

import time


def synchronized(func):
    @functools.wraps(func)
    def wrapper(self, *args, **kwargs):
        with self._lock:
            logging.info('Synchronized calling {func_name} method'.format(
                func_name=func.__name__))
            results = func(self, *args, **kwargs)
        return results

    return wrapper


def timeit(func):
    @functools.wraps(func)
    def new_func(*args, **kwargs):
        start_time = time.time()
        results = func(*args, **kwargs)
        elapsed_time = time.time() - start_time
        skip_param = 'visualize'
        if skip_param not in kwargs or not kwargs['visualize']:
            logging.info('function [{}] finished in {} ms'.format(
                func.__name__, int(elapsed_time * 1000)))
        return results

    return new_func