import os
import json
import time
import threading
from typing import Dict, Tuple, Optional

from config import CACHE_FILE_PATH, FILE_ID_CACHE_EXPIRY


class FileIdCache:
    """Cache for storing Telegram file_ids mapped to SoundCloud track_ids.

    This class provides a thread-safe implementation of a cache that maps
    SoundCloud track IDs to Telegram file IDs. Each entry has an expiration
    time (default 7 days) after which it will be considered invalid.

    The cache is persisted to a file on disk to survive application restarts.
    """

    def __init__(
        self,
        expiration_seconds: int = FILE_ID_CACHE_EXPIRY,
        cache_file: str = CACHE_FILE_PATH,
    ):
        """Initialize the cache.

        Args:
            expiration_seconds: Time in seconds after which cache entries expire
                                (default from config.FILE_ID_CACHE_EXPIRY)
            cache_file: Path to the file where the cache will be stored
                        (default from config.CACHE_FILE_PATH)
        """
        self._cache: Dict[str, Tuple[str, float]] = (
            {}
        )  # {track_id: (file_id, timestamp)}
        self._lock = threading.Lock()
        self.expiration_seconds = expiration_seconds
        self.cache_file = cache_file

        # Load existing cache from file if available
        self.load_from_file()

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
                self.save_to_file()  # Save changes to file
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
            self.save_to_file()  # Save changes to file

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

            if removed_count > 0:
                self.save_to_file()  # Save changes to file only if entries were removed

        return removed_count

    def size(self) -> int:
        """Get the current size of the cache.

        Returns:
            Number of entries in the cache
        """
        with self._lock:
            return len(self._cache)

    def save_to_file(self) -> None:
        """Save the cache to a file.

        The cache is serialized as JSON and written to the file specified in self.cache_file.
        If the directory doesn't exist, it will be created.
        """
        import logging

        logger = logging.getLogger(__name__)

        try:
            # Create cache directory if it doesn't exist
            cache_dir = os.path.dirname(self.cache_file)
            if cache_dir and not os.path.exists(cache_dir):
                os.makedirs(cache_dir, exist_ok=True)
                logger.info(f"Created cache directory: {cache_dir}")

            # Prepare data for serialization
            serializable_cache = {}
            for track_id, (file_id, timestamp) in self._cache.items():
                serializable_cache[track_id] = {
                    "file_id": file_id,
                    "timestamp": timestamp,
                }

            # Write to file
            with open(self.cache_file, "w", encoding="utf-8") as f:
                json.dump(serializable_cache, f, indent=2)

            logger.debug(
                f"Saved {len(serializable_cache)} items to cache file: {self.cache_file}"
            )

        except Exception as e:
            logger.error(f"Error saving cache to file: {e}")

    def load_from_file(self) -> None:
        """Load the cache from a file.

        The cache is deserialized from JSON and loaded into memory.
        If the file doesn't exist or is invalid, an empty cache is used.
        """
        import logging

        logger = logging.getLogger(__name__)

        if not os.path.exists(self.cache_file):
            logger.info(
                f"Cache file not found at {self.cache_file}, starting with empty cache"
            )
            return

        try:
            with open(self.cache_file, "r", encoding="utf-8") as f:
                serialized_cache = json.load(f)

            # Convert the serialized format back to our internal format
            with self._lock:
                self._cache.clear()
                for track_id, data in serialized_cache.items():
                    file_id = data.get("file_id")
                    timestamp = data.get("timestamp")
                    if file_id and timestamp:
                        self._cache[track_id] = (file_id, timestamp)

            # Log cache loading statistics
            loaded_items_count = len(self._cache)
            logger.info(
                f"Loaded {loaded_items_count} items from cache file: {self.cache_file}"
            )

            # Clean up expired entries right after loading
            expired_count = self.clear_expired()
            if expired_count > 0:
                logger.info(f"Removed {expired_count} expired items from loaded cache")

            valid_items_count = len(self._cache)
            logger.info(f"Cache initialized with {valid_items_count} valid items")

        except Exception as e:
            logger.error(f"Error loading cache from file: {e}")
            # Continue with an empty cache if loading fails
            self._cache = {}


# Create a global instance of the cache
file_id_cache = FileIdCache()
