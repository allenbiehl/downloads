from pathlib import Path
import shutil
import time

def initialize_directory(dir_path: str) -> None:
    """
    Create the directory if it does not exist, or empty the directory if it does exist.
    """
    print(f"Initializing directory {dir_path}")
    path = Path(dir_path)

    if path.exists():
        empty_directory(dir_path)
        return

    try:
        path.mkdir(exist_ok=True, parents=True)
        print(f"Successfully created directory {dir_path}")
    except Exception as err:
        print(f"Failed to create directory {dir_path}, {err}")
        raise

    for wait in range(1, 10):
        while not path.exists():
            time.sleep(wait)

def empty_directory(dir_path: str) -> None:  
    """
    Empty the directory of all files and subdirectories.
    """
    print(f"Emptying directory {dir_path}")
    path = Path(dir_path)

    if not path.exists():
        return

    try:
        for item in path.iterdir():
            if item.is_dir():
                shutil.rmtree(item)
            else:
                item.unlink()

        print(f"Successfully emptied directory {dir_path}")
    except Exception as err:
        print(f"Failed to empty directory {dir_path}, {err}")
        raise
