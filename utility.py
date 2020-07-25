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

# Print iterations progress
def print_progress_bar (iteration, total, prefix = '', suffix = '', decimals = 1,
                        length = 100, fill = '█', print_end = "\r"):
    """
    Call in a loop to create terminal progress bar
    @params:
        iteration   - Required  : current iteration (Int)
        total       - Required  : total iterations (Int)
        prefix      - Optional  : prefix string (Str)
        suffix      - Optional  : suffix string (Str)
        decimals    - Optional  : positive number of decimals in percent complete (Int)
        length      - Optional  : character length of bar (Int)
        fill        - Optional  : bar fill character (Str)
        printEnd    - Optional  : end character (e.g. "\r", "\r\n") (Str)
    """
    percent = ("{0:." + str(decimals) + "f}").format(100 * (iteration / float(total)))
    filled_length = int(length * iteration // total)
    bar = fill * filled_length + '-' * (length - filled_length)
    print('\r%s |%s| %s%% %s' % (prefix, bar, percent, suffix), end = print_end)
    # Print New Line on Complete
    if iteration == total:
        print()