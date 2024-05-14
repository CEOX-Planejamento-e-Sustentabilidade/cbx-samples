import time
from functools import wraps

def timing_decorator(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        elapsed_time = end_time - start_time
        print(f"{func.__name__} took {elapsed_time:.2f} seconds to execute.")
        return result
    return wrapper

@timing_decorator
def slow_function():
    """A function that simulates a time-consuming task."""
    print("Executing slow_function.")
    time.sleep(2)
    print("Slow function complete.")

# Now, let's use the decorated function and observe its behavior:

slow_function()
print("Function name:", slow_function.__name__)
print("Docstring:", slow_function.__doc__)