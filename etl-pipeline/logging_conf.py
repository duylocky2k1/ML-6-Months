# logging_conf.py
import logging
import sys
from logging.handlers import RotatingFileHandler
from pathlib import Path

def setup_logging(
    debug_log: str = "data/etl_debug.log",
    error_log: str = "data/etl_errors.log",
    level: int = logging.INFO
):
    # Tạo thư mục
    Path("data").mkdir(exist_ok=True)

    # Root logger
    logger = logging.getLogger()
    logger.setLevel(level)

    # Format
    formatter = logging.Formatter(
        '%(asctime)s | %(name)s | %(levelname)s | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(level)
    console_handler.setFormatter(formatter)

    # Debug file handler (rotating)
    debug_handler = RotatingFileHandler(debug_log, maxBytes=10_000_000, backupCount=5)
    debug_handler.setLevel(level)
    debug_handler.setFormatter(formatter)

    # Error file handler (chỉ lỗi)
    error_handler = RotatingFileHandler(error_log, maxBytes=5_000_000, backupCount=3)
    error_handler.setLevel(logging.ERROR)
    error_handler.setFormatter(formatter)

    # Clear cũ + add mới
    logger.handlers.clear()
    logger.addHandler(console_handler)
    logger.addHandler(debug_handler)
    logger.addHandler(error_handler)

    logger.info("Logging initialized")