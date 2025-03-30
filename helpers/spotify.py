"""
Spotify link processing functions for SoundCloud Search Bot.
Extracts song title and artist from Spotify links and prepares search queries for SoundCloud.
"""

import re

import httpx

from utils.logger import get_logger

# Configure logging
logger = get_logger(__name__)


async def extract_metadata_from_spotify_url(url: str) -> dict:
    """
    Extract song title and artist from a Spotify link.

    Args:
        url: Spotify URL (track)

    Returns:
        dict: Dictionary with 'title' and 'artist' keys, or None if extraction failed
    """
    try:
        logger.info(f"Extracting metadata from Spotify URL: {url}")

        # Remove URL parameters by splitting on "?" and taking the first part
        url = url.split("?")[0]
        logger.info(f"Cleaned Spotify URL (removed parameters): {url}")

        # Validate Spotify URL format first
        if not is_spotify_track_url(url):
            logger.warning(f"Invalid Spotify track URL format: {url}")
            return None

        # Normalize URL format by adding https:// if not present
        if not url.startswith(("http://", "https://")) and "open.spotify.com" in url:
            url = "https://" + url
            logger.info(f"Normalized Spotify URL: {url}")

        # Request the Spotify page
        async with httpx.AsyncClient() as client:
            response = await client.get(url, follow_redirects=True)

        if response.status_code != 200:
            logger.error(
                f"Failed to fetch Spotify page. Status code: {response.status_code}"
            )
            return None

        # Extract metadata from og:title and og:description meta tags using regex
        html_content = response.text

        # Extract title from meta tag
        # Fix regex to match: <meta property="og:title" content="Roi"/>
        title_match = re.search(
            r'<meta property="og:title" content="([^"]+)"', html_content
        )
        if not title_match:
            logger.warning("Could not find title in Spotify page")
            return None

        title = title_match.group(1).strip()

        # Extract artist from description
        # Fix regex to match: <meta property="og:description" content="Videoclub, Adèle Castillon, Mattyeux · Euphories · Song · 2021"/>
        description_match = re.search(
            r'<meta property="og:description" content="([^"]+)"', html_content
        )
        if not description_match:
            logger.warning("Could not find description in Spotify page")
            return None

        description = description_match.group(1).strip()

        # Extract first artist from description
        # Check if there are multiple artists (separated by comma)
        if ", " in description:
            # Take only the first artist before the comma
            artist = description.split(", ")[0].strip()
            logger.info(f"Found multiple artists, using first one: {artist}")
        else:
            # If no comma, take everything before the first bullet point
            artist_parts = description.split(" · ")[0].strip()
            artist = artist_parts

        logger.info(
            f"Successfully extracted metadata: Title='{title}', Artist='{artist}'"
        )
        # Include the original Spotify URL in the returned metadata
        return {"title": title, "artist": artist, "spotify_url": url}

    except Exception as e:
        logger.error(f"Error extracting metadata from Spotify URL: {e}", exc_info=True)
        return None


def is_spotify_track_url(url: str) -> bool:
    """
    Validate if the URL is a Spotify track link.

    Args:
        url: URL to check

    Returns:
        bool: True if it's a valid Spotify track URL
    """
    # Normalize URL format by adding https:// if not present
    if (
        url
        and not url.startswith(("http://", "https://"))
        and "open.spotify.com" in url
    ):
        url = "https://" + url

    # Remove any query parameters if they still exist
    url = url.split("?")[0]

    # Patterns to match:
    # - https://open.spotify.com/track/[ID]
    # - spotify:track:[ID]
    # - open.spotify.com/track/[ID] (without protocol)
    patterns = [
        r"https?://open\.spotify\.com/track/[a-zA-Z0-9]+$",
        r"spotify:track:[a-zA-Z0-9]+",
        r"(?:https?://)?open\.spotify\.com/track/[a-zA-Z0-9]+$",
    ]

    for pattern in patterns:
        if re.match(pattern, url):
            return True

    return False


def create_soundcloud_search_query(title: str, artist: str) -> str:
    """
    Create an optimized SoundCloud search query from Spotify metadata.

    Args:
        title: Song title
        artist: Artist name

    Returns:
        str: Search query for SoundCloud
    """
    # Clean up the title and artist
    # Remove things like "(feat. Artist)" or "- Single", etc.
    cleaned_title = re.sub(r"\s*\(feat\.[^)]*\)", "", title)
    cleaned_title = re.sub(r"\s*\[.*?\]", "", cleaned_title)
    cleaned_title = re.sub(
        r"\s*\-\s*\w+\s*$", "", cleaned_title
    )  # Remove "- Single", "- Remix", etc.

    # Remove unnecessary spaces
    cleaned_title = cleaned_title.strip()
    cleaned_artist = artist.strip()

    # Create the search query
    query = f"{cleaned_artist} {cleaned_title}"
    logger.info(f"Created SoundCloud search query: '{query}'")

    return query
