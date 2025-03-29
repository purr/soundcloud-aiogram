import os
import re
import time
import asyncio
import pathlib
import unicodedata
from typing import Any, Dict, List, Tuple, Union, Optional
from urllib.parse import urlparse

import aiohttp
import mutagen
import aiofiles
from mutagen.id3 import ID3

from utils import get_high_quality_artwork_url
from config import (
    CLIENT_ID,
    NAME_FORMAT,
    DEBUG_SEARCH,
    DOWNLOAD_PATH,
    DEBUG_DOWNLOAD,
    SOUNDCLOUD_TRACK_API,
    SOUNDCLOUD_SEARCH_API,
    SOUNDCLOUD_RESOLVE_API,
    EXTRACT_ARTIST_FROM_TITLE,
)
from utils.logger import get_logger

# Configure logging
logger = get_logger(__name__)

# Create downloads directory if it doesn't exist
pathlib.Path(DOWNLOAD_PATH).mkdir(parents=True, exist_ok=True)


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

    try:

        # No cache or expired, perform the search
        async with aiohttp.ClientSession() as session:
            # Ensure API URL has the correct format and parameters
            params = {
                "q": query,
                "limit": limit,
                "client_id": CLIENT_ID,
            }

            # Log the request URL with parameters
            if DEBUG_SEARCH:
                url = f"{SOUNDCLOUD_SEARCH_API}?q={query}&limit={limit}&client_id={CLIENT_ID}"
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
        async with aiohttp.ClientSession() as session:
            params = {
                "client_id": CLIENT_ID,
            }

            url = f"{SOUNDCLOUD_TRACK_API}/{track_id}"
            async with session.get(url, params=params) as response:
                status = response.status
                logger.info(f"SoundCloud API response status: {status}")

                if status == 200:
                    data = await response.json()
                    return data
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
        async with aiohttp.ClientSession() as session:
            params = {
                "client_id": CLIENT_ID,
            }

            url = f"https://api-v2.soundcloud.com/playlists/{playlist_id}"
            async with session.get(url, params=params) as response:
                status = response.status
                logger.info(f"SoundCloud playlist API response status: {status}")

                if status == 200:
                    data = await response.json()
                    return data
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

            async with aiohttp.ClientSession() as session:
                # Construct the URL with proper parameters
                url = "https://api-v2.soundcloud.com/tracks"
                params = {
                    "ids": ids_param,
                    "client_id": CLIENT_ID,
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
    logger.info(f"Resolving SoundCloud URL: {url}")
    try:
        async with aiohttp.ClientSession() as session:
            params = {
                "url": url,
                "client_id": CLIENT_ID,
            }

            async with session.get(SOUNDCLOUD_RESOLVE_API, params=params) as response:
                status = response.status
                logger.info(f"SoundCloud resolve API response status: {status}")

                if status == 200:
                    data = await response.json()
                    return data
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
    Download audio file from URL

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

    # Standard download for progressive streams and direct downloads
    try:
        async with aiohttp.ClientSession() as session:
            if DEBUG_DOWNLOAD:
                logger.debug(f"Starting download request to URL: {url}")

            async with session.get(url) as response:
                status = response.status
                logger.info(f"Download response status: {status}")

                if status == 200:
                    content_length = int(response.headers.get("Content-Length", 0))
                    content_type = response.headers.get("Content-Type", "unknown")
                    logger.info(
                        f"File size: {content_length / 1024 / 1024:.2f} MB, Content-Type: {content_type}"
                    )

                    if DEBUG_DOWNLOAD:
                        headers = dict(response.headers)
                        logger.debug(f"Full response headers: {headers}")

                    # Check if we might have received an m3u8 playlist despite not detecting it in the URL
                    if content_length < 1000 and (
                        content_type.startswith("application/")
                        or content_type.startswith("text/")
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
                        chunk_size = 1024 * 1024  # 1MB chunks
                        downloaded = 0
                        start_time = time.time()
                        last_log_time = start_time

                        async for chunk in response.content.iter_chunked(chunk_size):
                            await f.write(chunk)
                            downloaded += len(chunk)

                            # Log progress for all files if DEBUG_DOWNLOAD is True
                            # or for large files only if DEBUG_DOWNLOAD is False
                            current_time = time.time()
                            if DEBUG_DOWNLOAD or (content_length > 5 * 1024 * 1024):
                                # Log every second at most
                                if current_time - last_log_time >= 1.0:
                                    progress = (
                                        downloaded / content_length * 100
                                        if content_length
                                        else 0
                                    )
                                    speed = (
                                        downloaded / (current_time - start_time) / 1024
                                    )  # KB/s
                                    logger.info(
                                        f"Download progress: {progress:.1f}% ({downloaded / (1024 * 1024):.2f} MB / {content_length / (1024 * 1024):.2f} MB) - {speed:.1f} KB/s"
                                    )
                                    last_log_time = current_time

                    download_time = time.time() - start_time
                    logger.info(
                        f"Download completed: {filepath} in {download_time:.2f} seconds"
                    )

                    if DEBUG_DOWNLOAD:
                        avg_speed = (
                            content_length / download_time / 1024
                            if download_time > 0
                            else 0
                        )
                        logger.debug(f"Average download speed: {avg_speed:.1f} KB/s")

                    # Validate the downloaded file
                    if os.path.getsize(filepath) < 1000:  # Less than 1 KB
                        logger.error(
                            f"Downloaded file is too small: {os.path.getsize(filepath)} bytes"
                        )
                        return False

                    return True
                else:
                    error_text = await response.text()
                    logger.error(
                        f"Download failed: Status {status}, Response: {error_text[:200]}"
                    )
                    return False
    except Exception as e:
        logger.error(
            f"Exception during download: {type(e).__name__}: {e}", exc_info=True
        )
        return False


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
        import tempfile

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
        import requests

        # Try to use requests library for direct download
        # This is synchronous but might work better for some URLs
        r = requests.get(url, stream=True, timeout=30)
        r.raise_for_status()

        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(filepath), exist_ok=True)

        # Write file in chunks
        with open(filepath, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)

        logger.info(f"Fallback download completed: {filepath}")

        # Validate the downloaded file
        if os.path.getsize(filepath) < 1000:  # Less than 1 KB
            logger.error(
                f"Downloaded file is too small: {os.path.getsize(filepath)} bytes"
            )
            return False

        return True
    except Exception as e:
        logger.error(f"Fallback download failed: {type(e).__name__}: {e}")
        return False


def sanitize_filename(filename: str) -> str:
    """
    Create a safe filename from a string.

    Args:
        filename: The original filename

    Returns:
        str: A sanitized filename
    """
    # First, normalize unicode characters
    filename = (
        unicodedata.normalize("NFKD", filename)
        .encode("ASCII", "ignore")
        .decode("ASCII")
    )

    # Replace invalid characters with underscores
    filename = re.sub(r'[\\/*?:"<>|]', "_", filename)

    # Remove any leading/trailing spaces and dots
    filename = filename.strip(". ")

    # Limit length
    if len(filename) > 200:
        filename = filename[:200]

    return filename


def extract_artist_title(title: str) -> Tuple[str, str]:
    """
    Extract artist and title from the track title
    Based on SCDL's approach for robust extraction

    Args:
        title: Track title (possibly containing artist name)

    Returns:
        tuple: (artist, title) or (None, title) if artist couldn't be extracted
    """
    # List of dash characters used to separate artist and title
    dash_separators = [" - ", " − ", " – ", " — ", " ― "]

    # Count occurrences of each dash separator
    dash_counts = {sep: title.count(sep) for sep in dash_separators}
    total_dashes = sum(dash_counts.values())

    # If there's more than one dash (of any type), don't attempt extraction
    # to avoid incorrect splits in complex titles
    if total_dashes > 1:
        logger.info(
            f"Multiple dash separators found in title, skipping extraction: {title}"
        )
        return None, title

    # Try to split by each dash separator
    for dash in dash_separators:
        if dash not in title:
            continue

        artist_title = title.split(dash, maxsplit=1)
        artist = artist_title[0].strip()
        new_title = artist_title[1].strip()

        return artist, new_title

    # Fallback: try generic regex pattern for dash-like characters
    match = re.match(r"^(.+?)\s*[-–—]\s*(.+)$", title)
    if match:
        artist = match.group(1).strip()
        new_title = match.group(2).strip()
        return artist, new_title

    # No artist found in title
    return None, title


async def add_id3_tags(filepath: str, track_data: dict) -> None:
    """
    Add ID3 tags to the downloaded audio file

    Args:
        filepath: Path to the audio file
        track_data: Track data from SoundCloud API
    """
    logger.info(f"Adding ID3 tags to {filepath}")
    try:
        # Get title
        title = track_data.get("title", "Unknown")
        logger.info(f"Original title: {title}")

        # Get artist from different sources, prioritizing publisher_metadata
        username = track_data.get("user", {}).get("username", "Unknown Artist")
        artist = username  # Default to username

        # Try to get artist from publisher_metadata
        if track_data.get("publisher_metadata") and track_data[
            "publisher_metadata"
        ].get("artist"):
            artist = track_data["publisher_metadata"]["artist"]
            logger.info(f"Using artist from publisher_metadata: {artist}")

        # Extract artist from title if configured and not found in metadata
        if EXTRACT_ARTIST_FROM_TITLE:
            extracted_artist, extracted_title = extract_artist_title(title)

            # Only use extracted artist if it was found and we don't already have a better source
            if extracted_artist:
                # If we already have a publisher-provided artist, compare them
                if artist != username and artist != extracted_artist:
                    pass  # Not using extracted artist '{extracted_artist}' because metadata artist '{artist}' is available

                else:
                    # Use extracted artist and title
                    artist = extracted_artist
                    title = extracted_title
                    # "Using extracted artist: {artist}, new title: {title}

        # Create or clear existing tags
        audio = mutagen.File(filepath, easy=True)
        if audio is None:
            logger.warning(f"Could not add tags to {filepath} - unsupported format")
            return

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
                release_date = track_data["created_at"].split("T")[0]  # Get YYYY-MM-DD
                audio["date"] = release_date
            except (ValueError, IndexError, AttributeError):
                pass

        # Set genre if available
        if track_data.get("genre"):
            audio["genre"] = track_data["genre"]

        # Save basic tags first
        audio.save()

        # Add description as comment using ID3 specific tags
        if track_data.get("description"):
            try:
                id3 = ID3(filepath)
                from mutagen.id3 import COMM

                id3.add(
                    COMM(
                        encoding=3,  # UTF-8
                        lang="eng",
                        desc="Description",
                        text=track_data["description"],
                    )
                )
                id3.save()
            except Exception as e:
                logger.warning(f"Could not add description as comment: {e}")

        logger.info(f"ID3 tags added to {filepath}: Title='{title}', Artist='{artist}'")
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

    # Original title
    original_title = track_data.get("title", "Unknown")

    # Get artist and title with possible extraction
    title = original_title
    username = track_data.get("user", {}).get("username", "Unknown Artist")
    artist = username  # Default to username

    # Try to get artist from metadata
    if track_data.get("publisher_metadata") and track_data["publisher_metadata"].get(
        "artist"
    ):
        artist = track_data["publisher_metadata"]["artist"]
        logger.info(f"Using artist '{artist}' from publisher metadata")

    # Extract artist from title if configured
    if EXTRACT_ARTIST_FROM_TITLE:
        extracted_artist, extracted_title = extract_artist_title(original_title)
        if extracted_artist:
            # If we already have a publisher-provided artist, compare them
            if artist != username and artist != extracted_artist:
                pass  # Not using extracted artist '{extracted_artist}' because metadata artist '{artist}' is available
            else:
                # Use extracted artist and title
                artist = extracted_artist
                title = extracted_title
                # Using extracted artist: {artist}, new title: {title}

    if DEBUG_DOWNLOAD:
        logger.debug(f"Artist: {artist}, Title: {title}")

    # Generate safe filename (no special characters)
    filename = NAME_FORMAT.format(artist=artist, title=title)
    safe_filename = sanitize_filename(filename)
    if DEBUG_DOWNLOAD:
        logger.debug(f"Generated filename: {safe_filename}")

    # Define file path
    filepath = os.path.join(DOWNLOAD_PATH, f"{safe_filename}.mp3")

    # Check if file already exists
    if os.path.exists(filepath):
        file_size = os.path.getsize(filepath) / (1024.0 * 1024.0)  # Convert to MB
        mod_time = os.path.getmtime(filepath)
        logger.info(
            f"File already exists: {filepath} ({file_size:.2f} MB, modified: {time.ctime(mod_time)})"
        )

        # Get high-quality artwork URL for Telegram
        artwork_url = track_data.get("artwork_url", "")
        if artwork_url:
            artwork_url = get_high_quality_artwork_url(artwork_url)
            logger.info(f"Using high-quality artwork URL: {artwork_url}")

        return {
            "success": True,
            "filepath": filepath,
            "artwork_url": artwork_url,
            "track_data": track_data,
            "cached": True,
        }

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
        return {
            "success": False,
            "message": "Failed to download audio file",
        }

    # Get artwork URL for Telegram
    artwork_url = track_data.get("artwork_url", "")
    if artwork_url:
        artwork_url = get_high_quality_artwork_url(artwork_url)
        logger.info(f"Using high-quality artwork URL: {artwork_url}")

    # Add metadata
    metadata_start = time.time()
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
    # Log the keys available in the track object
    track_keys = list(track.keys())
    logger.debug(f"Track keys: {track_keys}")

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

    # Extract publisher_metadata, handling missing data
    publisher_metadata = track.get("publisher_metadata", {})
    if not isinstance(publisher_metadata, dict):
        publisher_metadata = {}

    # Determine artist name from available sources
    artist = publisher_metadata.get("artist") or username
    title = original_title

    # Try to extract artist from title if enabled
    if EXTRACT_ARTIST_FROM_TITLE:
        extracted_artist, extracted_title = extract_artist_title(original_title)
        if extracted_artist:
            # If we have a publisher-provided artist, prefer it
            if publisher_metadata.get("artist"):
                # Keeping publisher artist '{artist}' over extracted '{extracted_artist}'
                pass
            else:
                # Use the extracted artist and title
                artist = extracted_artist
                title = extracted_title
                # Updated artist to '{artist}' and title to '{title}' from original '{original_title}'

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
        "original_title": original_title,  # Keep original title for reference
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

        # Use the SoundCloud resolver API to get track info
        logger.info(f"Resolving SoundCloud URL: {url}")

        # Build API request
        params = {
            "url": url,
            "client_id": CLIENT_ID,
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
    logger.info(f"Cleaning up temporary files: {filepath}")

    # Delete audio file
    if filepath and os.path.exists(filepath):
        try:
            os.remove(filepath)
            logger.info(f"Deleted audio file: {filepath}")
        except Exception as e:
            logger.error(f"Error deleting audio file {filepath}: {e}")


async def get_download_url(track_data: dict) -> Optional[str]:
    """
    Get best quality download URL for a track

    Args:
        track_data: Track data from SoundCloud API

    Returns:
        str or None: Download URL
    """
    logger.info(f"Getting download URL for track: {track_data.get('title', 'Unknown')}")

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
                download_url += f"&client_id={CLIENT_ID}"
            else:
                download_url += f"?client_id={CLIENT_ID}"

            return download_url

        # If not directly downloadable, get the streaming URL
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
            return None

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
                    quality_level = "_".join(format_parts[1:])  # e.g., "0", "1", "0_0"

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
            if protocol in quality_scores and format_match in quality_scores[protocol]:
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
                logger.info(f"Successfully got stream URL for {preset} ({protocol})")
                return stream_url

            logger.warning(f"Failed to get stream URL for {preset} ({protocol})")

        logger.error("No suitable streams found after trying all options")
        return None
    except Exception as e:
        logger.error(f"Error getting download URL: {str(e)}", exc_info=True)
        return None


async def get_stream_url(api_url: str) -> Optional[str]:
    """
    Get the actual stream URL from a SoundCloud API URL.

    Args:
        api_url: The SoundCloud API URL

    Returns:
        str or None: The actual stream URL or None if it couldn't be retrieved
    """
    if not api_url:
        logger.error("Empty API URL provided to get_stream_url")
        return None

    try:
        logger.info(f"Getting stream URL from: {api_url}")
        async with aiohttp.ClientSession() as session:
            params = {
                "client_id": CLIENT_ID,
            }

            logger.debug(f"Request params: {params}")
            async with session.get(api_url, params=params) as response:
                status = response.status
                logger.info(f"Stream URL API response status: {status}")

                if status == 200:
                    try:
                        data = await response.json()
                        if "url" in data:
                            logger.info("Stream URL found in response")
                            return data["url"]
                        else:
                            logger.error("No 'url' field in response data")
                            if DEBUG_DOWNLOAD:
                                logger.debug(f"Response data keys: {list(data.keys())}")
                            return None
                    except Exception as json_error:
                        logger.error(f"Error parsing JSON response: {json_error}")
                        text = await response.text()
                        logger.debug(f"Response text: {text[:200]}")
                        return None
                else:
                    error_text = await response.text()
                    logger.error(
                        f"Failed to get stream URL. Status: {status}, Response: {error_text[:200]}"
                    )
                    return None
    except Exception as e:
        logger.error(f"Error getting stream URL: {e}")
        return None
