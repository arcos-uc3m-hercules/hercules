import os
import sys

def get_file_size(file_path):
    try:
        size = os.stat(file_path).st_size
        return size
    except FileNotFoundError:
        print("File not found.")
    except:
        print("An error occurred.")

# Example usage
file_path = sys.argv[1]  
file_size = get_file_size(file_path)
if file_size is not None:
    print(f"The size of the file is: {file_size} bytes.")
