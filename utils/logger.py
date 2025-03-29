import os
import sys

from loguru import logger

# Import LOG_LEVEL from config or use a default value
try:
    from config import DEBUG, LOG_LEVEL
except ImportError:
    LOG_LEVEL = "INFO"
    DEBUG = False

# Remove default logger
logger.remove()

# Setup colorful formatter
format_string = (
    "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
    "<level>{level: <8}</level> | "
    "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - "
    "<level>{message}</level>"
)

# Add a handler for stdout with colors
logger.add(
    sys.stdout,
    format=format_string,
    level=LOG_LEVEL,
    colorize=True,
    backtrace=True,
    diagnose=True,
)

# Add a file handler if needed
log_dir = "logs"
os.makedirs(log_dir, exist_ok=True)
logger.add(
    os.path.join(log_dir, "bot.log"),
    rotation="1 day",
    retention="7 days",
    compression="zip",
    level="DEBUG" if DEBUG else "INFO",
    format=format_string,
)


# Function to get logger for a specific module
def get_logger(name):
    """Get a logger for a specific module."""
    return logger.bind(name=name)
