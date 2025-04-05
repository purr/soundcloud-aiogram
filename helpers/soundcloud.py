import os
import re
import time
import asyncio
import pathlib
import tempfile
from typing import Any, Dict, List, Tuple, Union, Optional
from urllib.parse import urlparse

import aiohttp
import mutagen
import aiofiles
from mutagen.id3 import ID3

from utils import get_low_quality_artwork_url, get_high_quality_artwork_url  # noqa
from config import (
    DEBUG_SEARCH,
    DOWNLOAD_PATH,
    DEBUG_DOWNLOAD,
    DEBUG_EXTRACTIONS,
    SOUNDCLOUD_TRACK_API,
    SOUNDCLOUD_SEARCH_API,
    SOUNDCLOUD_RESOLVE_API,
)
from utils.logger import get_logger
from utils.client_id import get_client_id

# Version 1.3.0 - Never add artist to display titles or filenames, respecting user preferences


# Configure logging
logger = get_logger(__name__)

# Create downloads directory if it doesn't exist
pathlib.Path(DOWNLOAD_PATH).mkdir(parents=True, exist_ok=True)

# Client ID cache
_client_id: Optional[str] = None


async def get_cached_client_id() -> str:
    """
    Get a cached client ID or generate a new one

    Returns:
        str: A valid SoundCloud client ID
    """
    global _client_id

    if _client_id is None:
        _client_id = await get_client_id()

    return _client_id


async def refresh_client_id() -> str:
    """
    Force refresh the client ID

    Returns:
        str: A new valid SoundCloud client ID
    """
    global _client_id
    _client_id = await get_client_id()
    return _client_id


async def search_soundcloud(query: str, limit: int = 50) -> dict:
    """
    Search tracks on SoundCloud

    Args:
        query: Search query
        limit: Maximum number of results (default: 50)

    Returns:
        dict: Search results
    """
    if DEBUG_SEARCH:
        logger.info(f"Searching SoundCloud for: '{query}' with limit {limit}")
    else:
        logger.info(f"Searching SoundCloud for: '{query}'")

    # Handle "skip to" queries by removing that part before searching
    skip_to_pattern = re.compile(
        r"(?:skip(?:\s+to)?\s+\d+(?::\d+|[mM]\d*|\s+min(?:ute)?s?)?)",
        re.IGNORECASE,
    )

    original_query = query
    cleaned_query = skip_to_pattern.sub("", query).strip()

    # If the cleaned query is empty or too short, use the original query
    if not cleaned_query or len(cleaned_query) < 3:
        search_query = original_query
    else:
        logger.info(
            f"Modified search query from '{original_query}' to '{cleaned_query}'"
        )
        search_query = cleaned_query

    try:
        # Get a valid client ID
        client_id = await get_cached_client_id()

        # No cache or expired, perform the search
        async with aiohttp.ClientSession() as session:
            # Ensure API URL has the correct format and parameters
            params = {
                "q": search_query,
                "limit": limit,
                "client_id": client_id,
            }

            # Log the request URL with parameters
            if DEBUG_SEARCH:
                url = f"{SOUNDCLOUD_SEARCH_API}?q={search_query}&limit={limit}&client_id={client_id}"
                logger.info(f"Request URL: {url}")

            async with session.get(SOUNDCLOUD_SEARCH_API, params=params) as response:
                status = response.status

                if DEBUG_SEARCH:
                    logger.info(f"SoundCloud API response status: {status}")

                    # Log response headers for debugging
                    headers = dict(response.headers)
                    logger.debug(f"Response headers: {headers}")

                if status == 200:
                    data = await response.json()

                    if DEBUG_SEARCH:
                        # Log the structure of the response
                        top_level_keys = list(data.keys())
                        logger.debug(f"Response top-level keys: {top_level_keys}")

                    collection_length = len(data.get("collection", []))
                    total_results = data.get("total_results", 0)

                    if DEBUG_SEARCH:
                        logger.info(
                            f"SoundCloud API returned {collection_length} items in collection, total_results: {total_results}"
                        )

                        # Debug first few items to see what's being returned
                        if collection_length > 0:
                            first_item = data.get("collection", [])[0]
                            first_item_keys = list(first_item.keys())
                            logger.debug(f"First item keys: {first_item_keys}")
                            logger.info(
                                f"First item kind: {first_item.get('kind', 'unknown')}"
                            )
                            if "title" in first_item:
                                logger.info(
                                    f"First item title: {first_item.get('title')}"
                                )
                    else:
                        logger.info(f"Found {total_results} tracks")

                    return data
                # Client ID might be expired, try refreshing once
                elif status == 401 or status == 403:
                    logger.warning("Client ID might be expired, refreshing...")
                    client_id = await refresh_client_id()

                    # Retry with new client ID
                    params["client_id"] = client_id
                    async with session.get(
                        SOUNDCLOUD_SEARCH_API, params=params
                    ) as retry_response:
                        if retry_response.status == 200:
                            data = await retry_response.json()
                            collection_length = len(data.get("collection", []))
                            total_results = data.get("total_results", 0)
                            logger.info(
                                f"Found {total_results} tracks after refreshing client ID"
                            )
                            return data
                        else:
                            error_text = await retry_response.text()
                            logger.error(
                                f"SoundCloud API error after refresh: Status {retry_response.status}, Response: {error_text[:200]}"
                            )
                            return {"collection": [], "total_results": 0}
                else:
                    error_text = await response.text()
                    logger.error(
                        f"SoundCloud API error: Status {status}, Response: {error_text[:200]}"
                    )
                    return {"collection": [], "total_results": 0}
    except Exception as e:
        logger.error(
            f"Exception during SoundCloud API request: {type(e).__name__}: {e}"
        )
        return {"collection": [], "total_results": 0}


async def get_track(track_id: Union[str, int]) -> dict:
    """
    Get track details from SoundCloud API

    Args:
        track_id: Track ID

    Returns:
        dict: Track details
    """
    logger.info(f"Getting track details for track ID: {track_id}")
    try:
        # Get a valid client ID
        client_id = await get_cached_client_id()

        async with aiohttp.ClientSession() as session:
            params = {
                "client_id": client_id,
            }

            url = f"{SOUNDCLOUD_TRACK_API}/{track_id}"
            async with session.get(url, params=params) as response:
                status = response.status
                logger.info(f"SoundCloud API response status: {status}")

                if status == 200:
                    data = await response.json()
                    return data
                # Client ID might be expired, try refreshing once
                elif status == 401 or status == 403:
                    logger.warning("Client ID might be expired, refreshing...")
                    client_id = await refresh_client_id()

                    # Retry with new client ID
                    params["client_id"] = client_id
                    async with session.get(url, params=params) as retry_response:
                        if retry_response.status == 200:
                            data = await retry_response.json()
                            return data
                        else:
                            error_text = await retry_response.text()
                            logger.error(
                                f"SoundCloud API error after refresh: Status {retry_response.status}, Response: {error_text[:200]}"
                            )
                            return {}
                else:
                    error_text = await response.text()
                    logger.error(
                        f"SoundCloud API error: Status {status}, Response: {error_text[:200]}"
                    )
                    return {}
    except Exception as e:
        logger.error(f"Exception during track retrieval: {type(e).__name__}: {e}")
        return {}


async def get_playlist(playlist_id: Union[str, int]) -> dict:
    """
    Get playlist details from SoundCloud API

    Args:
        playlist_id: Playlist ID

    Returns:
        dict: Playlist details including tracks
    """
    logger.info(f"Getting playlist details for playlist ID: {playlist_id}")
    try:
        # Get a valid client ID
        client_id = await get_cached_client_id()

        async with aiohttp.ClientSession() as session:
            params = {
                "client_id": client_id,
            }

            url = f"https://api-v2.soundcloud.com/playlists/{playlist_id}"
            async with session.get(url, params=params) as response:
                status = response.status
                logger.info(f"SoundCloud playlist API response status: {status}")

                if status == 200:
                    data = await response.json()
                    return data
                # Client ID might be expired, try refreshing once
                elif status == 401 or status == 403:
                    logger.warning("Client ID might be expired, refreshing...")
                    client_id = await refresh_client_id()

                    # Retry with new client ID
                    params["client_id"] = client_id
                    async with session.get(url, params=params) as retry_response:
                        if retry_response.status == 200:
                            data = await retry_response.json()
                            return data
                        else:
                            error_text = await retry_response.text()
                            logger.error(
                                f"SoundCloud API error after refresh: Status {retry_response.status}, Response: {error_text[:200]}"
                            )
                            return {}
                else:
                    error_text = await response.text()
                    logger.error(
                        f"SoundCloud API error: Status {status}, Response: {error_text[:200]}"
                    )
                    return {}
    except Exception as e:
        logger.error(f"Exception during playlist retrieval: {type(e).__name__}: {e}")
        return {}


async def get_tracks_batch(track_ids: List[str]) -> List[dict]:
    """
    Get multiple tracks in a single batch request from SoundCloud API

    Args:
        track_ids: List of track IDs to fetch

    Returns:
        list: List of track details
    """
    if not track_ids:
        return []

    logger.info(f"Fetching batch information for {len(track_ids)} tracks")

    try:
        # SoundCloud API has a limit on the number of IDs per request
        # Split into chunks of 50 if needed
        batch_size = 50
        results = []

        # Process track IDs in batches
        for i in range(0, len(track_ids), batch_size):
            batch = track_ids[i : i + batch_size]

            # Convert list of IDs to comma-separated string
            ids_param = ",".join(str(id) for id in batch)

            # Get a valid client ID
            client_id = await get_cached_client_id()

            async with aiohttp.ClientSession() as session:
                # Construct the URL with proper parameters
                url = "https://api-v2.soundcloud.com/tracks"
                params = {
                    "ids": ids_param,
                    "client_id": client_id,
                    "app_version": "1743158692",  # Required by the API
                    "app_locale": "en",  # Required by the API
                }

                logger.info(
                    f"Requesting batch track info from: {url} for {len(batch)} tracks"
                )

                async with session.get(url, params=params) as response:
                    status = response.status
                    logger.info(
                        f"SoundCloud tracks batch API response status: {status}"
                    )

                    if status == 200:
                        data = await response.json()
                        if isinstance(data, list):
                            logger.info(
                                f"Successfully fetched {len(data)} tracks in batch {i // batch_size + 1}"
                            )
                            results.extend(data)
                        else:
                            logger.error(
                                f"Unexpected response format: {type(data)}, expected list"
                            )
                    else:
                        response_text = await response.text()
                        logger.error(
                            f"SoundCloud API error: Status {status}, Response: {response_text[:200]}"
                        )

                # Add a small delay between batches to avoid rate limiting
                if i + batch_size < len(track_ids):
                    await asyncio.sleep(0.5)

        logger.info(f"Total tracks retrieved across all batches: {len(results)}")
        return results
    except Exception as e:
        logger.error(f"Exception during batch track retrieval: {type(e).__name__}: {e}")
        return []


async def resolve_url(url: str) -> dict:
    """
    Resolve a SoundCloud URL to get its metadata

    Args:
        url: SoundCloud URL

    Returns:
        dict: Track details
    """

    try:
        # Get a valid client ID
        client_id = await get_cached_client_id()

        async with aiohttp.ClientSession() as session:
            params = {
                "url": url,
                "client_id": client_id,
            }

            async with session.get(SOUNDCLOUD_RESOLVE_API, params=params) as response:
                status = response.status
                logger.info(f"SoundCloud resolve API response status: {status}")

                if status == 200:
                    data = await response.json()
                    return data
                # Client ID might be expired, try refreshing once
                elif status == 401 or status == 403:
                    logger.warning("Client ID might be expired, refreshing...")
                    client_id = await refresh_client_id()

                    # Retry with new client ID
                    params["client_id"] = client_id
                    async with session.get(
                        SOUNDCLOUD_RESOLVE_API, params=params
                    ) as retry_response:
                        if retry_response.status == 200:
                            data = await retry_response.json()
                            return data
                        else:
                            error_text = await retry_response.text()
                            logger.error(
                                f"SoundCloud API error after refresh: Status {retry_response.status}, Response: {error_text[:200]}"
                            )
                            return {}
                else:
                    error_text = await response.text()
                    logger.error(
                        f"SoundCloud API error: Status {status}, Response: {error_text[:200]}"
                    )
                    return {}
    except Exception as e:
        logger.error(f"Exception during URL resolution: {type(e).__name__}: {e}")
        return {}


async def download_audio(url: str, filepath: str) -> bool:
    """
    Download audio file from URL with retries and fallbacks

    Args:
        url: Download URL
        filepath: Path to save the file

    Returns:
        bool: True if download was successful
    """
    logger.info(f"Downloading audio from URL to {filepath}")

    # Check if URL appears to be an HLS stream (contains m3u8)
    is_hls = "m3u8" in url.lower()
    if is_hls:
        logger.info("Detected HLS stream (m3u8), will use special handling")
        return await download_hls_audio(url, filepath)

    # Maximum number of retries
    max_retries = 3
    retry_count = 0
    last_error = None

    # Optimize for large files - use multiple chunks for faster download
    # Define optimal number of chunks for parallel downloading
    optimal_chunks = 4  # Split into 4 parts for parallel download

    # Optimized TCP settings for better throughput
    tcp_connector = aiohttp.TCPConnector(
        limit=10,  # Allow more parallel connections
        ttl_dns_cache=300,  # Cache DNS results for 5 minutes
        ssl=False,  # Disable SSL for better performance when downloading
        use_dns_cache=True,  # Use DNS cache
        family=0,  # Support both IPv4 and IPv6
    )

    while retry_count < max_retries:
        try:
            # Create a session with optimized parameters
            timeout = aiohttp.ClientTimeout(
                total=None,  # No overall timeout
                sock_connect=15,  # 15 seconds to establish connection
                sock_read=30,  # 30 seconds to read data chunks
            )

            # Use a single session for the entire download process
            async with aiohttp.ClientSession(
                connector=tcp_connector,
                timeout=timeout,
                headers={
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
                    "Accept": "*/*",
                    "Accept-Encoding": "gzip, deflate, br",
                    "Connection": "keep-alive",
                },
            ) as session:
                if DEBUG_DOWNLOAD:
                    logger.debug(f"Starting download request to URL: {url}")

                # Add client ID to URL if not present
                if "client_id=" not in url:
                    separator = "&" if "?" in url else "?"
                    url = f"{url}{separator}client_id={await get_cached_client_id()}"

                # First make a HEAD request to check content length for range support
                try:
                    async with session.head(url) as head_response:
                        if head_response.status == 200:
                            content_length = int(
                                head_response.headers.get("Content-Length", 0)
                            )
                            accept_ranges = head_response.headers.get(
                                "Accept-Ranges", ""
                            )
                            supports_ranges = (
                                content_length > 0 and accept_ranges == "bytes"
                            )

                            # Only use range requests for larger files
                            use_parallel_download = (
                                supports_ranges and content_length > 8 * 1024 * 1024
                            )  # > 8MB
                        else:
                            use_parallel_download = False
                            content_length = 0
                except Exception as head_err:
                    logger.warning(
                        f"HEAD request failed, falling back to regular download: {head_err}"
                    )
                    use_parallel_download = False
                    content_length = 0

                # For larger files, use parallel downloads with range requests if supported
                if use_parallel_download:
                    logger.info(
                        f"Using parallel download for {content_length / (1024 * 1024):.2f} MB file"
                    )

                    # Create directory if it doesn't exist
                    os.makedirs(os.path.dirname(filepath), exist_ok=True)

                    # Calculate chunk sizes
                    chunk_size = content_length // optimal_chunks
                    chunks = []

                    for i in range(optimal_chunks):
                        start_byte = i * chunk_size
                        end_byte = (
                            None
                            if i == optimal_chunks - 1
                            else (i + 1) * chunk_size - 1
                        )
                        chunks.append((start_byte, end_byte))

                    # Create temporary files for each chunk
                    temp_files = []
                    for i in range(optimal_chunks):
                        fd, temp_path = tempfile.mkstemp(suffix=f".part{i}")
                        os.close(fd)
                        temp_files.append(temp_path)

                    # Download chunks in parallel
                    download_start = time.time()

                    async def download_chunk(
                        session, chunk_index, start_byte, end_byte, temp_path
                    ):
                        headers = {
                            "Range": f"bytes={start_byte}-{end_byte if end_byte else ''}"
                        }
                        try:
                            async with session.get(url, headers=headers) as response:
                                if response.status in (
                                    200,
                                    206,
                                ):  # 200 OK or 206 Partial Content
                                    downloaded = 0

                                    async with aiofiles.open(temp_path, "wb") as f:
                                        # Use much larger chunks for reading (4MB)
                                        read_chunk_size = 4 * 1024 * 1024
                                        async for data in response.content.iter_chunked(
                                            read_chunk_size
                                        ):
                                            await f.write(data)
                                            downloaded += len(data)

                                    return True
                                else:
                                    logger.error(
                                        f"Failed to download chunk {chunk_index}, status: {response.status}"
                                    )
                                    return False
                        except Exception as e:
                            logger.error(f"Error downloading chunk {chunk_index}: {e}")
                            return False

                    # Start all chunk downloads concurrently
                    tasks = []
                    for i, (start_byte, end_byte) in enumerate(chunks):
                        task = asyncio.create_task(
                            download_chunk(
                                session, i, start_byte, end_byte, temp_files[i]
                            )
                        )
                        tasks.append(task)

                    # Wait for all chunks to download and log progress
                    progress_task = asyncio.create_task(
                        log_download_progress(tasks, content_length, download_start)
                    )

                    results = await asyncio.gather(*tasks)
                    await progress_task

                    # Check if all chunks downloaded successfully
                    if all(results):
                        # Combine chunks into final file
                        async with aiofiles.open(filepath, "wb") as outfile:
                            for temp_file in temp_files:
                                async with aiofiles.open(temp_file, "rb") as infile:
                                    while True:
                                        chunk = await infile.read(
                                            8 * 1024 * 1024
                                        )  # Read 8MB at a time
                                        if not chunk:
                                            break
                                        await outfile.write(chunk)

                        # Clean up temporary files
                        for temp_file in temp_files:
                            try:
                                os.unlink(temp_file)
                            except Exception as e:
                                logger.warning(
                                    f"Error removing temporary file {temp_file}: {e}"
                                )

                        download_time = time.time() - download_start
                        logger.info(
                            f"Parallel download completed: {filepath} in {download_time:.2f} seconds, "
                            f"average speed: {content_length / download_time / 1024:.1f} KB/s"
                        )

                        # Validate the downloaded file
                        file_size = os.path.getsize(filepath)
                        if file_size < 1000:  # Less than 1 KB
                            logger.error(
                                f"Downloaded file is too small: {file_size} bytes"
                            )
                            return False

                        if (
                            abs(file_size - content_length) > 1024
                        ):  # More than 1KB difference
                            logger.warning(
                                f"File size mismatch: expected {content_length} bytes, got {file_size} bytes"
                            )
                            # Continue anyway as this might be due to overhead or compression

                        return True
                    else:
                        logger.error(
                            "Some chunks failed to download, falling back to regular download"
                        )
                        # Clean up temporary files
                        for temp_file in temp_files:
                            try:
                                os.unlink(temp_file)
                            except Exception:
                                pass
                else:
                    # Regular sequential download for smaller files or when range requests not supported
                    async with session.get(url) as response:
                        status = response.status
                        logger.info(f"Download response status: {status}")

                        if status == 200:
                            content_length = int(
                                response.headers.get("Content-Length", 0)
                            )
                            content_type = response.headers.get(
                                "Content-Type", "unknown"
                            )
                            logger.info(
                                f"File size: {content_length / 1024 / 1024:.2f} MB, Content-Type: {content_type}"
                            )

                            if DEBUG_DOWNLOAD:
                                headers = dict(response.headers)
                                logger.debug(f"Full response headers: {headers}")

                            # Check if we might have received an m3u8 playlist despite not detecting it in the URL
                            if content_length < 1000 and (
                                content_type.startswith("application/")
                                or content_type == "text/plain"
                            ):
                                content = await response.text()
                                if "#EXTM3U" in content or ".m3u8" in content:
                                    logger.info(
                                        "Detected HLS stream from response content, will use special handling"
                                    )
                                    return await download_hls_audio(url, filepath)

                            # Create directory if it doesn't exist
                            os.makedirs(os.path.dirname(filepath), exist_ok=True)

                            async with aiofiles.open(filepath, "wb") as f:
                                # Use larger chunks for better throughput (4MB instead of 1MB)
                                chunk_size = 4 * 1024 * 1024
                                downloaded = 0
                                start_time = time.time()
                                last_log_time = start_time

                                async for chunk in response.content.iter_chunked(
                                    chunk_size
                                ):
                                    await f.write(chunk)
                                    downloaded += len(chunk)

                                    # Log progress for all files if DEBUG_DOWNLOAD is True
                                    # or for large files only if DEBUG_DOWNLOAD is False
                                    current_time = time.time()
                                    if DEBUG_DOWNLOAD or (
                                        content_length > 1 * 1024 * 1024
                                    ):
                                        # Log every second at most
                                        if current_time - last_log_time >= 1.0:
                                            progress = (
                                                downloaded / content_length * 100
                                                if content_length
                                                else 0
                                            )
                                            speed = (
                                                downloaded
                                                / (current_time - start_time)
                                                / 1024
                                            )  # KB/s
                                            logger.info(
                                                f"Download progress: {progress:.1f}% ({downloaded / (1024 * 1024):.2f} MB / {content_length / (1024 * 1024):.2f} MB) - {speed:.1f} KB/s"
                                            )
                                            last_log_time = current_time

                            download_time = time.time() - start_time
                            logger.info(
                                f"Download completed: {filepath} in {download_time:.2f} seconds, "
                                f"average speed: {downloaded / download_time / 1024:.1f} KB/s"
                            )

                            # Validate the downloaded file
                            if os.path.getsize(filepath) < 1000:  # Less than 1 KB
                                logger.error(
                                    f"Downloaded file is too small: {os.path.getsize(filepath)} bytes"
                                )
                                return False

                            return True
                        elif status in (301, 302, 303, 307, 308):
                            # Handle redirects
                            redirect_url = response.headers.get("Location")
                            if redirect_url:
                                logger.info(f"Following redirect to: {redirect_url}")
                                return await download_audio(redirect_url, filepath)
                            else:
                                logger.error("Redirect without Location header")
                                return False
                        elif status == 401 or status == 403:
                            # Authentication error - likely client ID issue
                            logger.warning(
                                f"Authentication error (status {status}), refreshing client ID"
                            )
                            await refresh_client_id()
                            retry_count += 1
                        else:
                            logger.error(f"HTTP error: {status}")
                            retry_count += 1
        except asyncio.TimeoutError:
            logger.warning(
                f"Timeout during download (attempt {retry_count + 1}/{max_retries})"
            )
            retry_count += 1
        except aiohttp.ClientError as e:
            logger.warning(
                f"HTTP client error during download (attempt {retry_count + 1}/{max_retries}): {e}"
            )
            last_error = e
            retry_count += 1
        except Exception as e:
            logger.error(f"Unexpected error during download: {type(e).__name__}: {e}")
            last_error = e
            break  # Don't retry for unexpected errors

    logger.error(f"Download failed after {retry_count} retries")
    if last_error:
        logger.error(f"Last error: {last_error}")

    return False


async def log_download_progress(chunk_tasks, total_size, start_time):
    """Log the progress of a parallel download operation"""
    last_log_time = time.time()
    while not all(task.done() for task in chunk_tasks):
        await asyncio.sleep(1.0)  # Check progress every second

        current_time = time.time()
        elapsed = current_time - start_time

        # Only log if at least a second has passed since last log
        if current_time - last_log_time >= 1.0:
            # We don't know exact progress of each chunk, so estimate based on task status
            completed_tasks = sum(1 for task in chunk_tasks if task.done())
            in_progress_tasks = len(chunk_tasks) - completed_tasks

            # Estimate progress as (completed tasks + half of in-progress tasks) / total tasks
            estimated_progress = (completed_tasks + (in_progress_tasks * 0.5)) / len(
                chunk_tasks
            )
            estimated_downloaded = total_size * estimated_progress

            if elapsed > 0:
                speed = estimated_downloaded / elapsed / 1024  # KB/s
                logger.info(
                    f"Parallel download progress: {estimated_progress * 100:.1f}% - "
                    f"Speed: {speed:.1f} KB/s - {completed_tasks}/{len(chunk_tasks)} chunks completed"
                )

            last_log_time = current_time


async def download_hls_audio(url: str, filepath: str) -> bool:
    """
    Download audio from HLS stream (m3u8)

    Args:
        url: HLS stream URL
        filepath: Path to save the file

    Returns:
        bool: True if download was successful
    """
    logger.info(f"Starting HLS download from {url}")
    try:
        # Create temporary directory for segments
        import shutil

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_output = os.path.join(temp_dir, "output.mp3")

            # We'll use FFmpeg to download and process the HLS stream
            # FFmpeg can handle HLS streams and convert them to MP3
            ffmpeg_path = shutil.which("ffmpeg")
            if not ffmpeg_path:
                logger.error("FFmpeg not found, required for HLS downloads")
                # Fallback to regular download method
                logger.info("Falling back to regular download method")
                return await download_audio_fallback(url, filepath)

            # Build the FFmpeg command
            cmd = [
                ffmpeg_path,
                "-y",  # Overwrite output files
                "-loglevel",
                "warning",  # Reduce log output
                "-i",
                url,  # Input URL
                "-c:a",
                "libmp3lame",  # MP3 codec
                "-q:a",
                "0",  # Highest quality
                "-vn",  # No video
                temp_output,
            ]

            logger.info(f"Running FFmpeg command: {' '.join(cmd)}")
            process = await asyncio.create_subprocess_exec(
                *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
            )

            stdout, stderr = await process.communicate()

            if process.returncode != 0:
                logger.error(f"FFmpeg error: {stderr.decode()}")
                return await download_audio_fallback(url, filepath)

            # Ensure output directory exists
            os.makedirs(os.path.dirname(filepath), exist_ok=True)

            # Move the file to the final destination
            shutil.copy2(temp_output, filepath)

            logger.info(f"HLS download completed: {filepath}")
            return True
    except Exception as e:
        logger.error(
            f"Exception during HLS download: {type(e).__name__}: {e}", exc_info=True
        )
        # Try fallback method
        return await download_audio_fallback(url, filepath)


async def download_audio_fallback(url: str, filepath: str) -> bool:
    """
    Fallback method for downloading audio when other methods fail

    Args:
        url: Audio URL
        filepath: Path to save the file

    Returns:
        bool: True if download was successful
    """
    logger.info(f"Using fallback download method for {url}")
    try:
        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(filepath), exist_ok=True)

        # Use optimized TCP settings
        tcp_connector = aiohttp.TCPConnector(
            limit=10,
            ttl_dns_cache=300,
            ssl=False,
            use_dns_cache=True,
            family=0,
        )

        # Create optimized session with appropriate timeouts
        timeout = aiohttp.ClientTimeout(total=None, sock_connect=15, sock_read=30)

        # Use aiohttp for asynchronous HTTP requests with optimized settings
        async with aiohttp.ClientSession(
            connector=tcp_connector,
            timeout=timeout,
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
                "Accept": "*/*",
                "Accept-Encoding": "gzip, deflate, br",
                "Connection": "keep-alive",
            },
        ) as session:
            async with session.get(url) as response:
                if response.status != 200:
                    logger.error(f"Fallback download HTTP error: {response.status}")
                    return False

                content_length = int(response.headers.get("Content-Length", 0))
                logger.info(
                    f"Fallback download content length: {content_length / 1024 / 1024:.2f} MB"
                )

                # Use async file operations for better performance
                async with aiofiles.open(filepath, "wb") as f:
                    # Use larger chunk size (4MB) for better throughput
                    chunk_size = 4 * 1024 * 1024
                    downloaded = 0
                    start_time = time.time()
                    last_log_time = start_time

                    async for chunk in response.content.iter_chunked(chunk_size):
                        await f.write(chunk)
                        downloaded += len(chunk)

                        # Log progress
                        current_time = time.time()
                        if current_time - last_log_time >= 1.0:
                            progress = (
                                (downloaded / content_length * 100)
                                if content_length
                                else 0
                            )
                            speed = (
                                downloaded / (current_time - start_time) / 1024
                            )  # KB/s
                            logger.info(
                                f"Fallback download progress: {progress:.1f}% "
                                f"({downloaded / (1024 * 1024):.2f} MB / {content_length / (1024 * 1024):.2f} MB) - "
                                f"{speed:.1f} KB/s"
                            )
                            last_log_time = current_time

                download_time = time.time() - start_time
                if download_time > 0:
                    speed = downloaded / download_time / 1024  # KB/s
                    logger.info(
                        f"Fallback download completed in {download_time:.2f}s, avg speed: {speed:.1f} KB/s"
                    )

        # Validate the downloaded file
        file_size = os.path.getsize(filepath)
        if file_size < 1000:  # Less than 1 KB
            logger.error(f"Downloaded file is too small: {file_size} bytes")
            return False

        return True
    except Exception as e:
        logger.error(f"Fallback download failed: {type(e).__name__}: {e}")
        return False


def extract_artist_title(title: str) -> Tuple[str, str]:
    """
    Extract artist and title from the track title
    Based on SCDL's approach for robust extraction

    Args:
        title: Track title (possibly containing artist name)

    Returns:
        tuple: (artist, title) or (None, title) if artist couldn't be extracted
    """
    if DEBUG_EXTRACTIONS:
        logger.info(f"EXTRACT: Beginning extraction for title: '{title}'")

    # Define dash characters to use
    dash_chars = ["-", "−", "–", "—", "―"]

    # Create the three types of separators
    dash_separators_both_spaces = [f" {dash} " for dash in dash_chars]
    dash_separators_after_space = [f"{dash} " for dash in dash_chars]
    dash_separators_before_space = [f" {dash}" for dash in dash_chars]

    if DEBUG_EXTRACTIONS:
        logger.debug(
            f"EXTRACT: Using separators - both spaces: {dash_separators_both_spaces}"
        )

    # Check for dashes with spaces on both sides
    dash_counts = {sep: title.count(sep) for sep in dash_separators_both_spaces}
    total_dashes_both_spaces = sum(dash_counts.values())

    if DEBUG_EXTRACTIONS:
        logger.debug(
            f"EXTRACT: Found {total_dashes_both_spaces} separators with spaces on both sides"
        )

    if total_dashes_both_spaces == 1:
        # Found exactly one dash with spaces on both sides, use it
        for dash in dash_separators_both_spaces:
            if dash in title:
                artist_title = title.split(dash, maxsplit=1)
                artist = artist_title[0].strip()
                new_title = artist_title[1].strip()
                if DEBUG_EXTRACTIONS:
                    logger.info(
                        f"EXTRACT: SUCCESS with spaces both sides - Artist: '{artist}', Title: '{new_title}'"
                    )
                return artist, new_title
    elif total_dashes_both_spaces > 1:
        # More than one dash with spaces on both sides, abort
        if DEBUG_EXTRACTIONS:
            logger.info(
                "EXTRACT: ABORTED - Multiple separators with spaces on both sides"
            )
        return None, title

    # Step 2: Check for dashes with space after only (e.g. "- ")
    if DEBUG_EXTRACTIONS:
        logger.debug("EXTRACT: Checking separators with space after only")

    dash_counts = {sep: title.count(sep) for sep in dash_separators_after_space}
    total_dashes_after_space = sum(dash_counts.values())

    if DEBUG_EXTRACTIONS:
        logger.debug(
            f"EXTRACT: Found {total_dashes_after_space} separators with space after only"
        )

    if total_dashes_after_space == 1:
        # Found exactly one dash with space after, use it
        for dash in dash_separators_after_space:
            if dash in title:
                artist_title = title.split(dash, maxsplit=1)
                artist = artist_title[0].strip()
                new_title = artist_title[1].strip()
                if DEBUG_EXTRACTIONS:
                    logger.info(
                        f"EXTRACT: SUCCESS with space after - Artist: '{artist}', Title: '{new_title}'"
                    )
                return artist, new_title
    elif total_dashes_after_space > 1:
        # More than one dash with space after, abort
        if DEBUG_EXTRACTIONS:
            logger.info("EXTRACT: ABORTED - Multiple separators with space after only")
        return None, title

    # Step 3: Check for dashes with space before only (e.g. " -")
    if DEBUG_EXTRACTIONS:
        logger.debug("EXTRACT: Checking separators with space before only")

    dash_counts = {sep: title.count(sep) for sep in dash_separators_before_space}
    total_dashes_before_space = sum(dash_counts.values())

    if DEBUG_EXTRACTIONS:
        logger.debug(
            f"EXTRACT: Found {total_dashes_before_space} separators with space before only"
        )

    if total_dashes_before_space == 1:
        # Found exactly one dash with space before, use it
        for dash in dash_separators_before_space:
            if dash in title:
                artist_title = title.split(dash, maxsplit=1)
                artist = artist_title[0].strip()
                new_title = artist_title[1].strip()
                if DEBUG_EXTRACTIONS:
                    logger.info(
                        f"EXTRACT: SUCCESS with space before - Artist: '{artist}', Title: '{new_title}'"
                    )
                return artist, new_title
    elif total_dashes_before_space > 1:
        # More than one dash with space before, abort
        if DEBUG_EXTRACTIONS:
            logger.info("EXTRACT: ABORTED - Multiple separators with space before only")
        return None, title

    # Fallback: If no structured separators were found, try generic regex pattern
    if DEBUG_EXTRACTIONS:
        logger.debug("EXTRACT: Trying regex fallback")

    match = re.match(r"^(.+?)\s*[-–—]\s*(.+)$", title)
    if match:
        artist = match.group(1).strip()
        new_title = match.group(2).strip()
        if DEBUG_EXTRACTIONS:
            logger.info(
                f"EXTRACT: SUCCESS with regex - Artist: '{artist}', Title: '{new_title}'"
            )
        return artist, new_title

    # No artist found in title
    if DEBUG_EXTRACTIONS:
        logger.info(f"EXTRACT: NO ARTIST FOUND in title: '{title}'")
    return None, title


def clean_title_if_contains_artist(title: str, artist: str) -> str:
    """
    Remove artist name from the title if it appears there

    Args:
        title: Track title
        artist: Artist name to check for

    Returns:
        str: Cleaned title without artist name if found, original title otherwise
    """
    if not artist or not title or len(artist) < 2:
        if DEBUG_EXTRACTIONS:
            logger.debug("CLEAN: Skipping - missing artist or title")
        return title

    if DEBUG_EXTRACTIONS:
        logger.info(f"CLEAN: Checking if title '{title}' contains artist '{artist}'")

    # Define dash characters to use (same as in extract_artist_title)
    dash_chars = ["-", "−", "–", "—", "―"]

    # 1. Check if title starts with artist name
    if title.lower().startswith(artist.lower()):
        if DEBUG_EXTRACTIONS:
            logger.debug("CLEAN: Title starts with artist name")
        # Check if there's a dash or similar separator after the artist name
        remainder = title[len(artist) :].strip()
        if any(remainder.startswith(dash) for dash in dash_chars):
            # Remove the dash and return cleaned title
            for dash in dash_chars:
                if remainder.startswith(dash):
                    cleaned_title = remainder[len(dash) :].strip()
                    if DEBUG_EXTRACTIONS:
                        logger.info(
                            f"CLEAN: SUCCESS - Removed artist from start: '{title}' -> '{cleaned_title}'"
                        )
                    return cleaned_title

    # 2. Check for "artist - " pattern at the beginning of the title
    for dash in dash_chars:
        separator = f" {dash} "
        prefix = f"{artist}{separator}"
        if prefix.lower() in title.lower():
            # Find the actual case-sensitive position
            pos = title.lower().find(prefix.lower())
            if pos == 0:  # Only remove if it's at the beginning
                cleaned_title = title[len(prefix) :].strip()
                if DEBUG_EXTRACTIONS:
                    logger.info(
                        f"CLEAN: SUCCESS - Removed artist with separator: '{title}' -> '{cleaned_title}'"
                    )
                return cleaned_title

    return title


async def download_artwork(artwork_url: str) -> Optional[bytes]:
    """
    Download artwork image from URL

    Args:
        artwork_url: URL of the artwork image

    Returns:
        bytes: Image data if successful, None otherwise
    """
    if not artwork_url:
        logger.warning("No artwork URL provided")
        return None

    logger.info(f"Downloading artwork from: {artwork_url}")

    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(artwork_url) as response:
                if response.status != 200:
                    logger.error(f"Failed to download artwork: HTTP {response.status}")
                    return None

                image_data = await response.read()

                # Validate that we got an image
                if len(image_data) < 100:
                    logger.error(
                        f"Downloaded artwork is too small: {len(image_data)} bytes"
                    )
                    return None

                logger.info(f"Artwork downloaded successfully: {len(image_data)} bytes")
                return image_data
    except Exception as e:
        logger.error(f"Error downloading artwork: {e}")
        return None


async def add_id3_tags(filepath: str, track_data: dict) -> None:
    """
    Add ID3 tags to the downloaded audio file

    Args:
        filepath: Path to the audio file
        track_data: Track data from SoundCloud API
    """
    try:
        # Check if we have extraction info from download_track
        if "_extracted_info" in track_data:
            # Use the pre-extracted and processed information
            extracted_info = track_data["_extracted_info"]
            artist = extracted_info["final_artist"]
            title = extracted_info["final_title"]

            if DEBUG_EXTRACTIONS:
                logger.info("ID3: Using pre-extracted info from download_track")
                logger.info(f"ID3: Artist: '{artist}', Title: '{title}'")

            logger.info(
                f"Final values for ID3 tags - Artist: '{artist}', Title: '{title}'"
            )
        else:
            # Get title
            title = track_data.get("title", "Unknown")
            logger.info(f"Original title: {title}")

            # Get artist from different sources, prioritizing publisher_metadata
            username = track_data.get("user", {}).get("username", "Unknown Artist")
            artist = username  # Default to username
            logger.debug(f"Initial artist value (from username): '{artist}'")

            # First attempt to extract artist and title from the original title
            if DEBUG_EXTRACTIONS:
                logger.info(f"ID3: Attempting extraction from title: '{title}'")

            extracted_artist, extracted_title = extract_artist_title(title)

            if DEBUG_EXTRACTIONS:
                if extracted_artist:
                    logger.info(
                        f"ID3: Extraction successful - Artist: '{extracted_artist}', Title: '{extracted_title}'"
                    )
                else:
                    logger.info("ID3: Extraction failed - No artist found")

            # Try to get artist from publisher_metadata
            has_metadata_artist = False
            if track_data.get("publisher_metadata") and track_data[
                "publisher_metadata"
            ].get("artist"):
                metadata_artist = track_data["publisher_metadata"]["artist"]
                has_metadata_artist = True
                logger.info(
                    f"ID3: Found artist '{metadata_artist}' in publisher metadata"
                )

            # Now apply the results based on extraction success and metadata
            # PRIORITIZE the extracted info
            if extracted_artist:
                # Always use extracted artist if available
                if DEBUG_EXTRACTIONS:
                    logger.info(
                        f"ID3: Using extracted artist '{extracted_artist}' and title '{extracted_title}'"
                    )
                artist = extracted_artist
                title = extracted_title
            elif has_metadata_artist:
                # If no extraction but we have metadata artist
                artist = metadata_artist
                if DEBUG_EXTRACTIONS:
                    logger.info(
                        f"ID3: Using metadata artist '{artist}', checking if title contains it"
                    )

                # Try to clean the title if it contains the artist
                cleaned_title = clean_title_if_contains_artist(title, artist)
                if cleaned_title != title:
                    if DEBUG_EXTRACTIONS:
                        logger.info(
                            f"ID3: Title cleaned: '{title}' -> '{cleaned_title}'"
                        )
                    title = cleaned_title
                else:
                    if DEBUG_EXTRACTIONS:
                        logger.info("ID3: Title unchanged after cleaning")
            else:
                # Fallback to username
                if DEBUG_EXTRACTIONS:
                    logger.info(
                        "ID3: No extraction or metadata, keeping username as artist"
                    )

            if DEBUG_EXTRACTIONS:
                logger.info(f"ID3: Final artist: '{artist}', Final title: '{title}'")

        # Define synchronous functions to run in a separate thread
        def apply_basic_tags():
            # Create or clear existing tags
            audio = mutagen.File(filepath, easy=True)
            if audio is None:
                logger.warning(f"Could not add tags to {filepath} - unsupported format")
                return False

            # Clear existing tags
            audio.delete()

            # Set new tags
            audio["title"] = title
            audio["artist"] = artist

            # Set album to artist name if not available
            album = track_data.get("album", artist)
            audio["album"] = album

            # Add date if available
            if "created_at" in track_data:
                try:
                    release_date = track_data["created_at"].split("T")[
                        0
                    ]  # Get YYYY-MM-DD
                    audio["date"] = release_date
                except (ValueError, IndexError, AttributeError):
                    pass

            # Set genre if available
            if track_data.get("genre"):
                audio["genre"] = track_data["genre"]

            # Save basic tags
            audio.save()
            return True

        def apply_id3_specific_tags():
            try:
                id3 = ID3(filepath)

                # Add description as comment if available
                if track_data.get("description"):
                    from mutagen.id3 import COMM

                    id3.add(
                        COMM(
                            encoding=3,  # UTF-8
                            lang="eng",
                            desc="Description",
                            text=track_data["description"],
                        )
                    )

                # Save ID3 tags
                id3.save(v2_version=3)
                return True
            except Exception as e:
                logger.warning(f"Error adding ID3 specific tags: {e}")
                return False

        # Run the mutagen operations in separate threads to avoid blocking the event loop
        basic_tags_success = await asyncio.to_thread(apply_basic_tags)

        if basic_tags_success:
            await asyncio.to_thread(apply_id3_specific_tags)
            logger.info(f"ID3 tags successfully added to {filepath}")
        else:
            logger.warning(f"Failed to apply basic tags to {filepath}")

    except Exception as e:
        logger.error(f"Error adding ID3 tags to {filepath}: {e}")


async def download_track(track_id: str, bot_user: Dict[str, Any]) -> Dict[str, Any]:
    """Download a track from SoundCloud by ID."""
    logger.info(f"Starting download for track ID: {track_id}")
    download_start = time.time()

    if DEBUG_DOWNLOAD:
        logger.debug(f"Bot user info: {bot_user}")

    # Get track info
    try:
        logger.info(f"Fetching track data for ID: {track_id}")
        track_data = await get_track(track_id)
        if not track_data:
            logger.error(f"Failed to get track data for ID: {track_id}")
            return {"success": False, "error": "Failed to get track data"}

        logger.info(
            f"Successfully retrieved track data, title: {track_data.get('title', 'Unknown')}"
        )

        if DEBUG_DOWNLOAD:
            logger.debug(f"Got track data: {len(str(track_data))} bytes")
    except Exception as e:
        logger.error(f"Exception during track data retrieval: {e}")
        return {"success": False, "error": f"Error retrieving track data: {str(e)}"}

    # Check for waveform URL and analyze for silence if available
    waveform_url = track_data.get("waveform_url")
    silence_analysis = await analyze_waveform_for_silence(waveform_url)

    if silence_analysis["has_silence"]:
        logger.info(
            f"Silence detected in track: {silence_analysis['silence_percentage']:.1f}% is silent"
        )
        if len(silence_analysis["silence_sections"]) > 0:
            logger.info(
                f"Found {len(silence_analysis['silence_sections'])} significant silence sections"
            )
            for i, section in enumerate(silence_analysis["silence_sections"]):
                logger.info(
                    f"  Section {i + 1}: {section['start']:.1f}% - {section['end']:.1f}% ({section['percentage']:.1f}% of track)"
                )

    # Original title
    original_title = track_data.get("title", "Unknown")
    logger.info(f"Original track title: '{original_title}'")

    # Get artist and title with possible extraction
    title = original_title
    username = track_data.get("user", {}).get("username", "Unknown Artist")
    artist = username  # Default to username
    logger.debug(f"Initial artist value (from username): '{artist}'")

    # First, attempt to extract artist and title from the original title
    if DEBUG_EXTRACTIONS:
        logger.info(f"DOWNLOAD: Attempting extraction from title: '{original_title}'")

    extracted_artist, extracted_title = extract_artist_title(original_title)

    if DEBUG_EXTRACTIONS:
        if extracted_artist:
            logger.info(
                f"DOWNLOAD: Extraction successful - Artist: '{extracted_artist}', Title: '{extracted_title}'"
            )
        else:
            logger.info("DOWNLOAD: Extraction failed - No artist found")

    # Try to get artist from metadata
    has_metadata_artist = False
    if track_data.get("publisher_metadata") and track_data["publisher_metadata"].get(
        "artist"
    ):
        metadata_artist = track_data["publisher_metadata"]["artist"]
        has_metadata_artist = True
        logger.info(f"Found artist '{metadata_artist}' in publisher metadata")

    # Now apply the results based on extraction success and metadata
    # PRIORITIZE the extracted info
    if extracted_artist:
        # Always use extracted artist if available
        if DEBUG_EXTRACTIONS:
            logger.info(
                f"DOWNLOAD: Using extracted artist '{extracted_artist}' and title '{extracted_title}'"
            )
        artist = extracted_artist
        title = extracted_title
    elif has_metadata_artist:
        # If no extraction but we have metadata artist
        artist = metadata_artist
        if DEBUG_EXTRACTIONS:
            logger.info(
                f"DOWNLOAD: Using metadata artist '{artist}', checking if title contains it"
            )

        # Try to clean the title if it contains the artist
        cleaned_title = clean_title_if_contains_artist(title, artist)
        if cleaned_title != title:
            if DEBUG_EXTRACTIONS:
                logger.info(f"DOWNLOAD: Title cleaned: '{title}' -> '{cleaned_title}'")
            title = cleaned_title
            # Update the extraction info with cleaned title
            track_data["_extracted_info"]["final_title"] = title
        else:
            if DEBUG_EXTRACTIONS:
                logger.info("DOWNLOAD: Title unchanged after cleaning")
    else:
        # Fallback to username
        if DEBUG_EXTRACTIONS:
            logger.info(
                "DOWNLOAD: No extraction or metadata, keeping username as artist"
            )

    if DEBUG_EXTRACTIONS:
        logger.info(f"DOWNLOAD: Final artist: '{artist}', Final title: '{title}'")

    logger.info(f"Final values for filename - Artist: '{artist}', Title: '{title}'")

    if DEBUG_DOWNLOAD:
        logger.debug(f"Artist: {artist}, Title: {title}")

    # Store the extraction info to ensure consistency
    track_data["_extracted_info"] = {
        "artist": extracted_artist,
        "title": extracted_title,
        "final_artist": artist,
        "final_title": title,
    }

    # Remove "skip to X" time markers from title using regex
    skip_to_pattern = re.compile(
        r"[\(\[\*!\s]*(?:skip(?:\s+to)?\s+\d+(?::\d+|[mM]\d*|\s+min(?:ute)?s?)?)[\)\]\*!\s]*",
        re.IGNORECASE,
    )
    cleaned_title = skip_to_pattern.sub("", title).strip()
    if cleaned_title != title:
        logger.info(f"Removed 'skip' marker from title: '{title}' -> '{cleaned_title}'")
        title = cleaned_title
        # Update the extraction info with cleaned title
        track_data["_extracted_info"]["final_title"] = title

    # Create a temporary file with .mp3 extension
    temp_file = tempfile.NamedTemporaryFile(suffix=".mp3", delete=False)
    filepath = temp_file.name
    temp_file.close()

    logger.info(f"Created temporary file: {filepath}")

    # Get download URL
    download_url = await get_download_url(track_data)
    if not download_url:
        logger.error(f"Could not find download URL for track ID: {track_id}")
        return {
            "success": False,
            "message": "Could not find download URL for this track",
        }

    if DEBUG_DOWNLOAD:
        logger.debug(
            f"Got download URL type: {'direct download' if 'download_url' in download_url else 'stream URL'}"
        )

    # Download the track
    download_success = await download_audio(download_url, filepath)
    if not download_success:
        logger.error(f"Failed to download audio file for track ID: {track_id}")
        # Clean up the temp file
        try:
            os.remove(filepath)
        except Exception as e:
            logger.error(f"Error removing temp file after failed download: {e}")
        return {
            "success": False,
            "message": "Failed to download audio file",
        }

    # Get artwork URL for Telegram
    artwork_url = track_data.get("artwork_url", "")
    if artwork_url:
        artwork_url = get_high_quality_artwork_url(artwork_url)

    # Add metadata
    metadata_start = time.time()
    # Store the extracted information in track_data to ensure it's used in tagging
    track_data["_extracted_info"] = {
        "artist": extracted_artist,
        "title": extracted_title,
        "final_artist": artist,
        "final_title": title,
    }
    await add_id3_tags(filepath, track_data)
    if DEBUG_DOWNLOAD:
        metadata_time = time.time() - metadata_start
        logger.debug(f"Metadata tagging completed in {metadata_time:.2f} seconds")

    # Calculate total download time
    total_time = time.time() - download_start
    logger.info(f"Total download process completed in {total_time:.2f} seconds")

    return {
        "success": True,
        "filepath": filepath,
        "message": "Track downloaded successfully",
        "track_data": track_data,
        "artwork_url": artwork_url,
        "cached": False,  # Always mark as non-cached since we're using temp files
        "silence_analysis": silence_analysis,  # Include silence analysis results
    }


def filter_tracks(data: dict) -> list:
    """
    Filter track items from SoundCloud search results
    Excludes Go+ songs (identified by policy="SNIP")

    Args:
        data: SoundCloud API response

    Returns:
        list: Filtered track items
    """
    if not data or "collection" not in data:
        logger.warning("No 'collection' field in SoundCloud response")
        return []

    tracks = []
    excluded_go_plus = 0

    for item in data.get("collection", []):
        kind = item.get("kind")
        # Skip if not a track
        if kind != "track":
            logger.debug(f"Skipping non-track item of kind: {kind}")
            continue

        # Check if it's a Go+ track (SoundCloud premium song)
        if item.get("policy") == "SNIP":
            logger.debug(f"Skipping Go+ track: {item.get('title', 'Unknown Title')}")
            excluded_go_plus += 1
            continue

        tracks.append(item)

    logger.info(
        f"Filtered {len(tracks)} tracks from {len(data.get('collection', []))} collection items (excluded {excluded_go_plus} Go+ tracks)"
    )
    return tracks


def format_duration(milliseconds: int) -> str:
    """
    Format duration from milliseconds to MM:SS format

    Args:
        milliseconds: Duration in milliseconds

    Returns:
        str: Formatted duration string
    """
    seconds = milliseconds // 1000
    minutes = seconds // 60
    seconds = seconds % 60
    return f"{minutes}:{seconds:02d}"


def get_track_info(track: dict) -> dict:
    """
    Extract relevant track information

    Args:
        track: Track data from SoundCloud

    Returns:
        dict: Formatted track information
    """
    track_id = track.get("id", "unknown")

    if DEBUG_EXTRACTIONS:
        logger.info(f"TRACK {track_id}: Processing track")

    # Extract user data, handling different response formats
    user = track.get("user", {})
    user_id = None
    user_urn = None

    if isinstance(user, dict):
        username = user.get("username", "Unknown Artist")
        user_url = user.get("permalink_url", "")
        user_id = user.get("id")

        # Extract or construct the user URN
        user_urn = user.get("urn")
        if not user_urn and user_id:
            user_urn = f"soundcloud:users:{user_id}"
    else:
        logger.warning(f"Unexpected user data type: {type(user)}")
        username = "Unknown Artist"
        user_url = ""

    # Original track title
    original_title = track.get("title", "Unknown Title")
    if DEBUG_EXTRACTIONS:
        logger.info(f"TRACK {track_id}: Original title: '{original_title}'")

    # Extract publisher_metadata, handling missing data
    publisher_metadata = track.get("publisher_metadata", {})
    if not isinstance(publisher_metadata, dict):
        publisher_metadata = {}

    # Get artist from metadata if available
    has_metadata_artist = False
    metadata_artist = publisher_metadata.get("artist")
    if metadata_artist:
        has_metadata_artist = True
        if DEBUG_EXTRACTIONS:
            logger.info(f"TRACK {track_id}: Found metadata artist: '{metadata_artist}'")

    # Starting title is the original title
    title = original_title

    # First, ALWAYS attempt to extract artist and title from the original title
    if DEBUG_EXTRACTIONS:
        logger.info(f"TRACK {track_id}: Attempting extraction from: '{original_title}'")

    extracted_artist, extracted_title = extract_artist_title(original_title)

    if DEBUG_EXTRACTIONS:
        if extracted_artist:
            logger.info(
                f"TRACK {track_id}: Extraction successful - Artist: '{extracted_artist}', Title: '{extracted_title}'"
            )
        else:
            logger.info(f"TRACK {track_id}: Extraction failed - No artist found")

    # Now apply the results based on extraction success and metadata
    # PRIORITIZE extracted info over metadata
    if extracted_artist:
        # Always use extracted artist if available
        if DEBUG_EXTRACTIONS:
            logger.info(
                f"TRACK {track_id}: Using extracted artist '{extracted_artist}' and title '{extracted_title}'"
            )
        artist = extracted_artist
        title = extracted_title
    elif has_metadata_artist:
        # If no extraction but we have metadata artist
        artist = metadata_artist
        if DEBUG_EXTRACTIONS:
            logger.info(
                f"TRACK {track_id}: Using metadata artist '{artist}', checking if title contains it"
            )

        # Try to clean the title if it contains the artist
        cleaned_title = clean_title_if_contains_artist(title, artist)
        if cleaned_title != title:
            if DEBUG_EXTRACTIONS:
                logger.info(
                    f"TRACK {track_id}: Title cleaned with metadata artist: '{title}' -> '{cleaned_title}'"
                )
            title = cleaned_title
        else:
            if DEBUG_EXTRACTIONS:
                logger.info(
                    f"TRACK {track_id}: Title unchanged after cleaning with metadata artist"
                )
    else:
        # Fallback to username
        artist = username
        if DEBUG_EXTRACTIONS:
            logger.info(
                f"TRACK {track_id}: No artist extracted or in metadata, using username: '{username}'"
            )

    if DEBUG_EXTRACTIONS:
        logger.info(
            f"TRACK {track_id}: Final artist: '{artist}', Final title: '{title}'"
        )

    # Function to check if artist is already in the title
    def artist_in_title(artist_name, title_text):
        if not artist_name or len(artist_name) < 2:
            return False

        # Check if title starts with artist name
        if title_text.lower().startswith(artist_name.lower()):
            return True

        # Check if title contains "artist - " pattern
        dash_chars = ["-", "−", "–", "—", "―"]
        for dash in dash_chars:
            if (
                f"{artist_name.lower()} {dash}" in title_text.lower()
                or f"{artist_name.lower()}{dash}" in title_text.lower()
            ):
                return True

        return False

    # For display_title generation - NEVER add artist to display title
    # Set display_title to just the title, regardless of whether artist is in it or not
    display_title = title

    if DEBUG_EXTRACTIONS:
        logger.info(
            f"TRACK {track_id}: Using title as display title: '{display_title}'"
        )

    # Remove "skip to X" time markers from title and display_title
    skip_to_pattern = re.compile(
        r"[\(\[\*!\s]*(?:skip(?:\s+to)?\s+\d+(?::\d+|[mM]\d*|\s+min(?:ute)?s?)?)[\)\]\*!\s]*",
        re.IGNORECASE,
    )

    cleaned_title = skip_to_pattern.sub("", title).strip()
    # Only use cleaned title if it's not empty and different from the original
    if cleaned_title and cleaned_title != title:
        if DEBUG_EXTRACTIONS:
            logger.info(
                f"TRACK {track_id}: Removed 'skip' marker from title: '{title}' -> '{cleaned_title}'"
            )
        title = cleaned_title
        display_title = cleaned_title

    # Ensure display_title is never empty
    if not display_title or display_title.strip() == "":
        display_title = "Untitled Track"
        if DEBUG_EXTRACTIONS:
            logger.info(
                f"TRACK {track_id}: Empty display title, setting to 'Untitled Track'"
            )

    # Get artwork URL
    artwork_url = track.get("artwork_url", "")

    if artwork_url == "" or not artwork_url:
        avatar_url = track.get("user", {}).get("avatar_url", "")
        artwork_url = avatar_url

    if artwork_url:
        artwork_url = get_high_quality_artwork_url(artwork_url)

    # Check if it's a Go+ song
    is_go_plus = track.get("policy") == "SNIP"

    # Check if duration is snipped
    full_duration = track.get("full_duration", 0)
    current_duration = track.get("duration", 0)
    is_snipped = full_duration > current_duration

    # Format the full duration if available
    full_duration_formatted = format_duration(full_duration) if full_duration else None

    # Get track URN
    track_urn = track.get("urn")
    if not track_urn and track.get("id"):
        track_urn = f"soundcloud:tracks:{track.get('id')}"

    return {
        "id": track.get("id"),
        "title": title,
        "display_title": display_title,  # Added for UI display purposes
        "original_title": original_title,  # Keep original title for reference
        "extracted_artist": extracted_artist,  # Add this for debugging
        "extracted_title": extracted_title,  # Add this for debugging
        "artwork_url": artwork_url,
        "permalink_url": track.get("permalink_url", ""),
        "duration": format_duration(current_duration),
        "full_duration": full_duration_formatted,
        "artist": artist,
        "description": track.get("description", ""),
        "genre": track.get("genre", ""),
        "plays": track.get("playback_count", 0),
        "likes": track.get("likes_count", 0),
        "user": {"name": username, "url": user_url, "id": user_id, "urn": user_urn},
        "urn": track_urn,
        "is_go_plus": is_go_plus,
        "is_snipped": is_snipped,
        "policy": track.get("policy"),
        "monetization_model": track.get("monetization_model"),
        "waveform_url": track.get(
            "waveform_url"
        ),  # Add waveform URL for silence detection
    }


async def extract_track_id_from_url(url: str) -> Union[str, Dict[str, Any]]:
    """
    Extract the track ID or playlist data from a SoundCloud URL.

    Improved to handle various URL formats and be more resilient to formatting issues.

    Args:
        url: SoundCloud URL (can be various formats)

    Returns:
        Union[str, Dict[str, Any]]:
            - For tracks: just the track_id as string
            - For playlists: a dict with {'type': 'playlist', 'id': playlist_id}
            - None if the URL is invalid or not supported
    """
    try:
        # Clean up the URL - remove any trailing whitespace, periods, etc.
        url = url.strip("., \t\n\r")

        # Normalize URL format
        if not url.startswith(("http://", "https://")):
            url = "https://" + url

        # Try to fix malformed URLs where someone might have added space or other characters
        url = re.sub(r"\s+(?=soundcloud\.com)", "", url)

        # Parse URL to handle different formats
        parsed_url = urlparse(url)

        # Check if it's a shortened URL (like on.soundcloud.com/XXXXX)
        is_shortened = "on.soundcloud.com" in parsed_url.netloc and re.match(
            r"/[A-Za-z0-9]+", parsed_url.path
        )

        # Make sure it's a SoundCloud domain
        if not any(
            domain in parsed_url.netloc
            for domain in ["soundcloud.com", "m.soundcloud.com", "on.soundcloud.com"]
        ):
            logger.warning(f"Not a SoundCloud domain: {parsed_url.netloc}")
            return None

        # If it's a shortened URL, follow redirects to get the actual URL
        if is_shortened:
            logger.info(f"Detected shortened SoundCloud URL: {url}")
            try:
                async with aiohttp.ClientSession() as session:
                    # Disable redirects to manually follow them and get the final URL
                    async with session.get(url, allow_redirects=False) as response:
                        if response.status in (301, 302, 303, 307, 308):
                            redirect_url = response.headers.get("Location")
                            if redirect_url:
                                logger.info(
                                    f"Following redirect from {url} to {redirect_url}"
                                )
                                # Update the URL to the redirected URL for further processing
                                url = redirect_url
                                parsed_url = urlparse(url)
                        elif response.status != 200:
                            logger.error(
                                f"Failed to follow redirect for {url}: Status {response.status}"
                            )
                            return None
            except Exception as e:
                logger.error(f"Error following redirect for shortened URL: {e}")
                return None

        # Build API request
        params = {
            "url": url,
            "client_id": await get_cached_client_id(),
        }

        async with aiohttp.ClientSession() as session:
            async with session.get(SOUNDCLOUD_RESOLVE_API, params=params) as response:
                if response.status != 200:
                    logger.error(f"Failed to resolve URL: Status {response.status}")
                    try:
                        error_text = await response.text()
                        logger.error(f"Error response: {error_text[:200]}")
                    except Exception as text_err:
                        logger.error(f"Couldn't read error response: {text_err}")
                    return None

                data = await response.json()

                # Log the structure of the response
                top_keys = list(data.keys())
                logger.info(f"Resolver response keys: {top_keys}")

                # Check if it's a track
                kind = data.get("kind")
                if kind == "track":
                    # Return the track ID
                    track_id = str(data.get("id"))
                    logger.info(
                        f"Resolved track ID: {track_id} with title: {data.get('title', 'Unknown')}"
                    )
                    return track_id
                elif kind == "playlist":
                    # Return playlist information
                    playlist_id = str(data.get("id"))
                    playlist_title = data.get("title", "Unknown Playlist")
                    track_count = data.get("track_count", 0)

                    logger.info(
                        f"Resolved playlist ID: {playlist_id} with title: {playlist_title} containing {track_count} tracks"
                    )

                    # Return a dictionary with the playlist information
                    return {
                        "type": "playlist",
                        "id": playlist_id,
                        "title": playlist_title,
                        "track_count": track_count,
                        "user": data.get("user", {}).get("username", "Unknown Artist"),
                        "artwork_url": data.get("artwork_url"),
                    }
                else:
                    logger.warning(f"URL does not point to a track or playlist: {kind}")
                    return None

    except Exception as e:
        logger.error(f"Error extracting track ID from URL: {e}", exc_info=True)
        return None


async def cleanup_files(filepath: str) -> None:
    """
    Delete downloaded files after they've been sent to Telegram

    Args:
        filepath: Path to the audio file
    """

    # Delete audio file
    if filepath and os.path.exists(filepath):
        try:
            os.remove(filepath)
            logger.info(f"Deleted audio file: {filepath}")
        except Exception as e:
            logger.error(f"Error deleting audio file {filepath}: {e}")


async def get_download_url(track_data: dict) -> Optional[str]:
    """
    Get best quality download URL for a track with retries

    Args:
        track_data: Track data from SoundCloud API

    Returns:
        str or None: Download URL
    """
    logger.info(f"Getting download URL for track: {track_data.get('title', 'Unknown')}")

    max_retries = 3
    retry_count = 0
    last_error = None

    while retry_count < max_retries:
        try:
            # Check if track is downloadable directly (usually free downloads provided by creators)
            if track_data.get("downloadable", False):
                logger.info("Track is marked as downloadable, checking download URL")

                # Some tracks are marked as downloadable but don't have a download_url
                # In this case, we need to construct it manually
                if "download_url" in track_data and track_data["download_url"]:
                    download_url = track_data["download_url"]
                    logger.info("Using provided download_url from track data")
                else:
                    # Construct download URL manually using the track ID
                    track_id = track_data.get("id")
                    download_url = (
                        f"https://api-v2.soundcloud.com/tracks/{track_id}/download"
                    )
                    logger.info("Constructed download URL manually using track ID")

                # Add client_id to download_url
                if "?" in download_url:
                    download_url += f"&client_id={await get_cached_client_id()}"
                else:
                    download_url += f"?client_id={await get_cached_client_id()}"

                # Try to validate the download URL
                async with aiohttp.ClientSession() as session:
                    async with session.head(download_url) as response:
                        if response.status == 200:
                            return download_url
                        elif response.status in (401, 403):
                            logger.warning(
                                "Download URL validation failed, refreshing client ID..."
                            )
                            await refresh_client_id()
                            retry_count += 1
                            continue
                        else:
                            logger.warning(
                                f"Download URL validation failed with status {response.status}"
                            )
                            # Fall through to streaming URL logic

            # If not directly downloadable or direct download failed, get the streaming URL
            # Find the best quality stream
            logger.info("Track not directly downloadable, finding best quality stream")

            transcodings = []
            if "media" in track_data and "transcodings" in track_data["media"]:
                transcodings = track_data["media"]["transcodings"]
                logger.info(f"Found {len(transcodings)} transcodings")
            else:
                logger.error("No media/transcodings found in track data")
                if DEBUG_DOWNLOAD:
                    logger.debug(f"Track data keys: {list(track_data.keys())}")
                    if "media" in track_data:
                        logger.debug(f"Media keys: {list(track_data['media'].keys())}")

            if not transcodings:
                logger.error("No transcodings found for track")
                retry_count += 1
                continue

            # Create a quality score mapping for different formats and protocols
            # Higher score means better quality
            quality_scores = {
                # Progressive streams (usually better for downloading)
                "progressive": {
                    "mp3_0": 60,  # MP3 standard quality
                    "mp3_1": 70,  # MP3 high quality
                    "mp3_2": 80,  # MP3 highest quality
                    "opus_0": 75,  # Opus standard quality
                    "opus_1": 85,  # Opus high quality
                    "aac_0": 65,  # AAC standard quality
                    "aac_1": 75,  # AAC high quality
                },
                # HLS streams
                "hls": {
                    "mp3_0": 40,  # MP3 standard quality
                    "mp3_1": 50,  # MP3 high quality
                    "opus_0": 55,  # Opus standard quality
                    "opus_1": 65,  # Opus high quality
                    "aac_0": 45,  # AAC standard quality
                    "aac_1": 55,  # AAC high quality
                },
            }

            # Score all available transcodings
            scored_transcodings = []
            for encoding in transcodings:
                protocol = encoding.get("format", {}).get("protocol", "")
                preset = encoding.get("preset", "")

                # Extract format and quality level
                format_match = None
                if "_" in preset:
                    format_parts = preset.split("_")
                    if len(format_parts) >= 2:
                        format_type = format_parts[0]  # e.g., "mp3", "opus", "aac"
                        quality_level = "_".join(
                            format_parts[1:]
                        )  # e.g., "0", "1", "0_0"

                        # Simplify multi-part quality levels (e.g., "0_1" to "1")
                        # We prefer the highest number in multi-part quality designations
                        if "_" in quality_level:
                            quality_parts = [
                                int(q) for q in quality_level.split("_") if q.isdigit()
                            ]
                            simplified_quality = (
                                str(max(quality_parts)) if quality_parts else "0"
                            )
                            format_match = f"{format_type}_{simplified_quality}"
                        else:
                            format_match = f"{format_type}_{quality_level}"

                # Assign score based on protocol and format
                score = 0
                if (
                    protocol in quality_scores
                    and format_match in quality_scores[protocol]
                ):
                    score = quality_scores[protocol][format_match]
                elif protocol == "progressive":
                    # Default score for progressive formats we don't explicitly know
                    score = 40
                elif protocol == "hls":
                    # Default score for HLS formats we don't explicitly know
                    score = 30

                # Add small bonus to higher numbers in preset (assuming higher = better quality)
                # This helps differentiate between similar formats
                if preset:
                    digits = [int(d) for d in preset if d.isdigit()]
                    if digits:
                        score += min(sum(digits), 10)  # Max 10 point bonus

                logger.info(f"Scored transcoding: {preset} ({protocol}) = {score}")
                scored_transcodings.append(
                    {
                        "encoding": encoding,
                        "score": score,
                        "protocol": protocol,
                        "preset": preset,
                    }
                )

            # Sort by score in descending order (highest first)
            scored_transcodings.sort(key=lambda x: x["score"], reverse=True)

            # Try to get stream URL from each transcoding in order of score
            for idx, item in enumerate(scored_transcodings):
                encoding = item["encoding"]
                score = item["score"]
                protocol = item["protocol"]
                preset = item["preset"]

                logger.info(
                    f"Trying {idx + 1}/{len(scored_transcodings)}: {preset} ({protocol}) with score {score}"
                )
                stream_url = await get_stream_url(encoding.get("url"))

                if stream_url:
                    # Validate the stream URL
                    async with aiohttp.ClientSession() as session:
                        async with session.head(stream_url) as response:
                            if response.status == 200:
                                logger.info(
                                    f"Successfully got stream URL for {preset} ({protocol})"
                                )
                                return stream_url
                            elif response.status in (401, 403):
                                logger.warning(
                                    "Stream URL validation failed, refreshing client ID..."
                                )
                                await refresh_client_id()
                                break  # Break inner loop to retry with new client ID
                            else:
                                logger.warning(
                                    f"Stream URL validation failed with status {response.status}"
                                )
                                continue

                logger.warning(f"Failed to get stream URL for {preset} ({protocol})")

            retry_count += 1

        except Exception as e:
            last_error = f"Error getting download URL: {str(e)}"
            logger.error(last_error, exc_info=True)
            retry_count += 1
            continue

    if retry_count == max_retries:
        logger.error(
            f"All {max_retries} attempts to get download URL failed. Last error: {last_error}"
        )
    return None


async def get_stream_url(api_url: str) -> Optional[str]:
    """
    Get the actual stream URL from a SoundCloud API URL with retries.

    Args:
        api_url: The SoundCloud API URL

    Returns:
        str or None: The actual stream URL or None if it couldn't be retrieved
    """
    if not api_url:
        logger.error("Empty API URL provided to get_stream_url")
        return None

    max_retries = 3
    for attempt in range(max_retries):
        try:
            logger.info(
                f"Getting stream URL from: {api_url} (attempt {attempt + 1}/{max_retries})"
            )
            async with aiohttp.ClientSession() as session:
                params = {"client_id": await get_cached_client_id()}
                async with session.get(api_url, params=params) as response:
                    status = response.status
                    logger.info(f"Stream URL API response status: {status}")

                    if status == 200:
                        data = await response.json()
                        if "url" in data:
                            stream_url = data["url"]
                            # Validate the stream URL
                            async with session.head(stream_url) as validate_response:
                                if validate_response.status == 200:
                                    logger.info("Stream URL found and validated")
                                    return stream_url
                                elif validate_response.status in (401, 403):
                                    logger.warning(
                                        "Stream URL validation failed, refreshing client ID..."
                                    )
                                    await refresh_client_id()
                                    continue
                        else:
                            logger.error("No 'url' field in response data")
                            if DEBUG_DOWNLOAD:
                                logger.debug(f"Response data keys: {list(data.keys())}")
                    elif status in (401, 403):
                        logger.warning(f"Got {status} error, refreshing client ID...")
                        await refresh_client_id()
                        continue
                    else:
                        error_text = await response.text()
                        logger.error(
                            f"Failed to get stream URL. Status: {status}, Response: {error_text[:200]}"
                        )

        except Exception as e:
            logger.error(f"Error getting stream URL: {e}")

    logger.error(f"All {max_retries} attempts to get stream URL failed")
    return None


async def analyze_waveform_for_silence(waveform_url: str) -> Dict[str, Any]:
    """
    Fetch and analyze waveform data from SoundCloud to detect silence.

    Args:
        waveform_url: URL to the waveform JSON data

    Returns:
        Dict containing silence analysis:
            - has_silence: Whether significant silence was detected
            - silence_percentage: Percentage of the track that is silent
            - silence_sections: List of sections with silence (start and end percentages)
    """
    if not waveform_url:
        logger.info("No waveform URL available for silence detection")
        return {"has_silence": False, "silence_percentage": 0, "silence_sections": []}

    try:
        logger.info(f"Fetching waveform data from: {waveform_url}")
        async with aiohttp.ClientSession() as session:
            async with session.get(waveform_url) as response:
                if response.status != 200:
                    logger.error(
                        f"Failed to fetch waveform data: HTTP {response.status}"
                    )
                    return {
                        "has_silence": False,
                        "silence_percentage": 0,
                        "silence_sections": [],
                    }

                waveform_data = await response.json()

                # Check if we have samples data
                if "samples" not in waveform_data:
                    logger.error("No samples found in waveform data")
                    return {
                        "has_silence": False,
                        "silence_percentage": 0,
                        "silence_sections": [],
                    }

                samples = waveform_data["samples"]
                logger.info(f"Analyzing {len(samples)} waveform samples")

                # Define silence threshold (typically 0 is complete silence)
                silence_threshold = (
                    1  # Anything below or equal to this is considered silence
                )

                # Count silence samples
                silence_count = sum(
                    1 for sample in samples if sample <= silence_threshold
                )
                silence_percentage = (silence_count / len(samples)) * 100

                # Find continuous silence sections (at least 3% of the track)
                min_section_size = max(
                    1, int(len(samples) * 0.03)
                )  # At least 3% of track
                silence_sections = []
                current_section = None

                for i, sample in enumerate(samples):
                    position_percentage = (i / len(samples)) * 100

                    if sample <= silence_threshold:
                        # Start or continue silence section
                        if current_section is None:
                            current_section = {
                                "start": position_percentage,
                                "samples": 1,
                            }
                        else:
                            current_section["samples"] += 1
                    elif current_section is not None:
                        # End of silence section
                        if current_section["samples"] >= min_section_size:
                            # Only record significant silence sections
                            current_section["end"] = position_percentage
                            current_section["percentage"] = (
                                current_section["samples"] / len(samples)
                            ) * 100
                            silence_sections.append(current_section)
                        current_section = None

                # Check if last section is silence and needs to be closed
                if (
                    current_section is not None
                    and current_section["samples"] >= min_section_size
                ):
                    current_section["end"] = 100.0
                    current_section["percentage"] = (
                        current_section["samples"] / len(samples)
                    ) * 100
                    silence_sections.append(current_section)

                # Only consider significant silence
                has_silence = silence_percentage >= 5.0 or len(silence_sections) > 0

                # Clean up silence sections format for return
                formatted_sections = []
                for section in silence_sections:
                    formatted_sections.append(
                        {
                            "start": section["start"],
                            "end": section["end"],
                            "percentage": section["percentage"],
                        }
                    )

                logger.info(
                    f"Silence analysis complete: {silence_percentage:.1f}% silent, {len(formatted_sections)} sections"
                )

                return {
                    "has_silence": has_silence,
                    "silence_percentage": silence_percentage,
                    "silence_sections": formatted_sections,
                }

    except Exception as e:
        logger.error(f"Error analyzing waveform data: {e}")
        return {"has_silence": False, "silence_percentage": 0, "silence_sections": []}
