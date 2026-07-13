import os
import time

def execute_with_retry(action_name: str, func, *args, max_attempts: int = 3, **kwargs):
    """
    Global Infrastructure Utility.
    Executes any target function with a configurable exponential backoff ceiling.
    """
    for attempt in range(1, max_attempts + 1):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            print(f"[Attempt {attempt}/{max_attempts}] Failed {action_name}: {str(e)}")
            if attempt == max_attempts:
                raise e
            time.sleep(2 ** attempt)
