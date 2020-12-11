import time

def _check_running_time(func):
    def new_func(*args, **kwargs):
        start_time = time.perf_counter()
        RESULT = func(*args, **kwargs)
        end_time = time.perf_counter()

        FUNCTION_NAME = func.__name__
        ELAPSED_TIME = end_time-start_time


        runningtime = '%s | %0.1f' % (FUNCTION_NAME, ELAPSED_TIME)
        print(runningtime)

        return {'result':RESULT,
                 'elapesed_time':ELAPSED_TIME}
    return new_func