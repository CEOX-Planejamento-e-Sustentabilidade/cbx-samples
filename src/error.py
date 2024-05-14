import sys
import traceback

def divide(x, y):
    try:
        result = x / y
    except Exception as e:
        # Get exception information using sys.exc_info()
        exc_type, exc_value, exc_traceback = sys.exc_info()

        # Print the exception type, value (error message), and traceback
        print(f"Exception Type: {exc_type}")
        print(f"Exception Value: {exc_value}")
        print("Exception Traceback:")
        for tb in traceback.extract_tb(exc_traceback):
            print(tb)

# Example usage
divide(10, 0)