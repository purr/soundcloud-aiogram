import time
import asyncio

from config import CACHE_CLEANUP_INTERVAL
from utils.logger import logger

from .cache import file_id_cache  # noqa
from .spotify import *  # noqa
from .workers import *  # noqa
from .soundcloud import *  # noqa

# Global variable to track the last cleanup time
_last_cache_cleanup = time.time()


async def periodic_cache_cleanup(interval_seconds: int = CACHE_CLEANUP_INTERVAL):
    """Periodically clean up expired entries in the file_id cache.

    Args:
        interval_seconds: How often to check for expired entries
                         (default from config.CACHE_CLEANUP_INTERVAL)
    """
    global _last_cache_cleanup
    current_time = time.time()

    # Only clean if the interval has passed
    if current_time - _last_cache_cleanup > interval_seconds:
        removed_count = file_id_cache.clear_expired()
        if removed_count > 0:
            logger.info(
                f"Cache cleanup: removed {removed_count} expired file_id entries"
            )
        else:
            logger.debug("Cache cleanup: no expired entries found")

        # Update the last cleanup time
        _last_cache_cleanup = current_time

    # Log current cache size periodically
    logger.debug(f"Current file_id cache size: {file_id_cache.size()} entries")
