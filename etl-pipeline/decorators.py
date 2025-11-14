# decorators.py
import time
import functools
import logging
from typing import Callable, Any
from functools import wraps

log = logging.getLogger(__name__)

# ==============================
# 1. @log_time – Đo thời gian chạy
# ==============================
def log_time(func: Callable) -> Callable:
    @wraps(func)
    def wrapper(*args, **kwargs):
        start = time.perf_counter()
        result = func(*args, **kwargs)
        elapsed = time.perf_counter() - start
        log.info(f"{func.__name__} executed in {elapsed:.4f}s")
        return result
    return wrapper

# ==============================
# 2. @retry – Tự động retry (I/O, DB)
# ==============================
def retry(max_attempts: int = 3, delay: float = 1.0, backoff: float = 2.0):
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            attempt = 0
            current_delay = delay
            while attempt < max_attempts:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    attempt += 1
                    if attempt == max_attempts:
                        log.error(f"{func.__name__} failed after {max_attempts} attempts: {e}")
                        raise
                    log.warning(f"{func.__name__} failed (attempt {attempt}/{max_attempts}): {e}. Retrying in {current_delay}s...")
                    time.sleep(current_delay)
                    current_delay *= backoff
            return None
        return wrapper
    return decorator

# ==============================
# 3. @safe_run – Bọc toàn bộ job, log lỗi riêng
# ==============================
class ETLJobError(Exception):
    """Custom exception cho ETL"""
    pass

def safe_run(func: Callable) -> Callable:
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            error_msg = f"ETL JOB FAILED: {func.__name__} | Error: {type(e).__name__}: {e}"
            log.error(error_msg, exc_info=True)
            raise ETLJobError(error_msg) from e
    return wrapper