import time
import threading
from typing import Dict, Tuple, Optional

from config import FILE_ID_CACHE_EXPIRY


class FileIdCache:
    """Cache for storing Telegram file_ids mapped to SoundCloud track_ids.

    This class provides a thread-safe implementation of a cache that maps
    SoundCloud track IDs to Telegram file IDs. Each entry has an expiration
    time (default 24 hours) after which it will be considered invalid.
    """

    def __init__(self, expiration_seconds: int = FILE_ID_CACHE_EXPIRY):
        """Initialize the cache.

        Args:
            expiration_seconds: Time in seconds after which cache entries expire
                                (default from config.FILE_ID_CACHE_EXPIRY)
        """
        self._cache: Dict[str, Tuple[str, float]] = (
            {}
        )  # {track_id: (file_id, timestamp)}
        self._lock = threading.Lock()
        self.expiration_seconds = expiration_seconds

    def get(self, track_id: str) -> Optional[str]:
        """Get a file_id from the cache if it exists and is not expired.

        Args:
            track_id: SoundCloud track ID

        Returns:
            The Telegram file_id if found and valid, None otherwise
        """
        with self._lock:
            if track_id not in self._cache:
                return None

            file_id, timestamp = self._cache[track_id]
            current_time = time.time()

            # Check if the entry has expired
            if current_time - timestamp > self.expiration_seconds:
                # Remove expired entry
                del self._cache[track_id]
                return None

            return file_id

    def set(self, track_id: str, file_id: str) -> None:
        """Store a file_id in the cache with the current timestamp.

        Args:
            track_id: SoundCloud track ID
            file_id: Telegram file_id to cache
        """
        with self._lock:
            self._cache[track_id] = (file_id, time.time())

    def clear_expired(self) -> int:
        """Remove all expired entries from the cache.

        Returns:
            Number of entries removed
        """
        current_time = time.time()
        removed_count = 0

        with self._lock:
            expired_keys = [
                key
                for key, (_, timestamp) in self._cache.items()
                if current_time - timestamp > self.expiration_seconds
            ]

            for key in expired_keys:
                del self._cache[key]
                removed_count += 1

        return removed_count

    def size(self) -> int:
        """Get the current size of the cache.

        Returns:
            Number of entries in the cache
        """
        with self._lock:
            return len(self._cache)


# Create a global instance of the cache
file_id_cache = FileIdCache()
