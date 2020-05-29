import time

from functools import  wraps

def time_query(my_func):
    @wraps(my_func)
    def timed(*args, **kwargs):
        start = time.time()
        output = my_func(*args, **kwargs)
        end = time.time()
        diff = round(end - start, 8)
        print(f'Execution took {diff}s')

        return output
    return timed