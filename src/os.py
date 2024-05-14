import os

# Get the current working directory
current_directory = os.getcwd()

print("Current Working Directory:", current_directory)

script_directory = os.path.dirname(os.path.abspath(__file__))

print("Script Directory:", script_directory)