import functools
import time
import types

def profile_etl_step(step_name):
    """
    A robust time profiler decorator compatible with standard functions 
    and Python generator streams (yield). Tracks throughput metrics automatically.
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            print(f"\n[PROFILER] >>> Starting ETL Step: {step_name}...")
            start_time = time.perf_counter()
            
            # Invoke the inner function
            result = func(*args, **kwargs)
            
            # SCENARIO A: The function returns a Generator Stream (yield)
            if isinstance(result, types.GeneratorType):
                def generator_wrapper(gen):
                    item_count = 0
                    try:
                        for chunk in gen:
                            item_count += 1
                            yield chunk
                    finally:
                        # This executes only when the downstream consumer completely drains the generator
                        end_time = time.perf_counter()
                        duration = end_time - start_time
                        print(f"\n" + "="*45)
                        print(f"      STREAM METRICS: {step_name.upper()}")
                        print("="*45)
                        print(f"Status              : Fully Drained")
                        print(f"Total Stream Batches: {item_count:,}")
                        print(f"Total Processing Time: {duration:.4f} seconds")
                        if duration > 0:
                            print(f"Stream Velocity     : {item_count / duration:.2f} batches/sec")
                        print("="*45 + "\n")
                
                return generator_wrapper(result)
                
            # SCENARIO B: The function is a standard execution block
            else:
                end_time = time.perf_counter()
                duration = end_time - start_time
                print(f"\n" + "="*45)
                print(f"    FUNCTION METRICS: {step_name.upper()}")
                print("="*45)
                print(f"Status              : Execution Complete")
                print(f"Total Execution Time: {duration:.4f} seconds")
                print("="*45 + "\n")
                return result
                
        return wrapper
    return decorator
