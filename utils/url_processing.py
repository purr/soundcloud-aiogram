import re
from typing import Tuple, Optional

from helpers import get_track, resolve_url, get_playlist, extract_track_id_from_url
from utils.logger import get_logger

# Configure logging
logger = get_logger(__name__)

# Comprehensive regex pattern for matching SoundCloud URLs - handles all formats including shortened URLs
SOUNDCLOUD_URL_PATTERN = (
    r"(?:https?://)?(?:www\.|m\.|on\.)?soundcloud\.com/(?:[\w\-\+_~:/%]+)(?:\?[^\s]*)?"
)


def extract_soundcloud_url(text: str) -> Optional[str]:
    """
    Extract a SoundCloud URL from a text string.

    Args:
        text: Text that may contain a SoundCloud URL

    Returns:
        str or None: Extracted SoundCloud URL or None if not found
    """
    soundcloud_url_match = re.search(SOUNDCLOUD_URL_PATTERN, text)

    if soundcloud_url_match:
        url = soundcloud_url_match.group(0)

        # Normalize URL format by adding https:// if not present
        if not url.startswith(("http://", "https://")):
            url = "https://" + url

        logger.info(f"Extracted SoundCloud URL: {url}")
        return url

    return None


async def process_soundcloud_url(
    url: str,
) -> Tuple[Optional[str], Optional[dict], Optional[str], Optional[dict]]:
    """
    Process a SoundCloud URL to extract track ID or playlist info and check for Go+ tracks.

    Args:
        url: SoundCloud URL

    Returns:
        tuple: (track_id, track_data, error_message, playlist_data)
            - track_id: The extracted track ID or None if extraction failed or it's a playlist
            - track_data: The track data from SoundCloud API or None if not retrieved or it's a playlist
            - error_message: Error message if any step failed, otherwise None
            - playlist_data: Data about the playlist if the URL is a playlist, otherwise None
    """
    # Extract track ID or playlist info from URL
    logger.info(f"Processing SoundCloud URL: {url}")

    # First try to directly resolve the URL
    try:
        resolved_data = await resolve_url(url)
        if resolved_data:
            kind = resolved_data.get("kind")
            logger.info(f"Resolved URL kind: {kind}")

            # Handle playlist resolved directly
            if kind == "playlist":
                playlist_id = str(resolved_data.get("id"))
                logger.info(f"Resolved playlist ID directly: {playlist_id}")

                # Get full playlist details
                playlist_data = await get_playlist(playlist_id)

                if not playlist_data or "id" not in playlist_data:
                    logger.error(
                        f"Failed to get playlist details for ID: {playlist_id}"
                    )
                    return (
                        None,
                        None,
                        "Couldn't load playlist details. The playlist may be private or unavailable.",
                        None,
                    )

                # Check if the playlist has tracks
                tracks = playlist_data.get("tracks", [])
                if not tracks:
                    logger.warning(f"Playlist has no tracks: {playlist_id}")
                    return (
                        None,
                        None,
                        "This playlist is empty or all tracks are private.",
                        playlist_data,
                    )

                logger.info(f"Successfully loaded playlist with {len(tracks)} tracks")
                return None, None, None, playlist_data

            # Handle track resolved directly
            elif kind == "track":
                track_id = str(resolved_data.get("id"))
                logger.info(f"Resolved track ID directly: {track_id}")

                # We already have the track data from the resolve call
                track_data = resolved_data

                # Check if it's a Go+ (premium) track
                if track_data.get("policy") == "SNIP":
                    return (
                        track_id,
                        track_data,
                        "SoundCloud Go+ Song Detected. This is a premium track that can only be previewed (30 seconds).",
                        None,
                    )

                # All good - it's a track
                return track_id, track_data, None, None
    except Exception as e:
        logger.error(f"Error during direct URL resolution: {e}")
        # Continue with the fallback approach

    # Fallback to our extract_track_id_from_url approach
    result = await extract_track_id_from_url(url)

    # Handle the case where nothing was extracted
    if result is None:
        logger.error(f"Failed to extract track/playlist ID from URL: {url}")
        return (
            None,
            None,
            "Invalid SoundCloud link. Please make sure it's a valid link to a track or playlist.",
            None,
        )

    # Check if result is a playlist
    if isinstance(result, dict) and result.get("type") == "playlist":
        # This is a playlist
        playlist_id = result.get("id")
        logger.info(f"Extracted playlist ID: {playlist_id}")

        try:
            playlist_data = await get_playlist(playlist_id)

            if not playlist_data or "id" not in playlist_data:
                logger.error(f"Failed to get playlist details for ID: {playlist_id}")
                return (
                    None,
                    None,
                    "Couldn't load playlist details. The playlist may be private or unavailable.",
                    None,
                )

            # Check if the playlist has tracks
            tracks = playlist_data.get("tracks", [])
            if not tracks:
                logger.warning(f"Playlist has no tracks: {playlist_id}")
                return (
                    None,
                    None,
                    "This playlist is empty or all tracks are private.",
                    playlist_data,
                )

            logger.info(f"Successfully loaded playlist with {len(tracks)} tracks")
            return None, None, None, playlist_data
        except Exception as e:
            logger.error(f"Error getting playlist data: {e}")
            return (
                None,
                None,
                f"Error processing playlist: {str(e)}",
                None,
            )

    # If we reached here, it's a track ID
    track_id = result
    logger.info(f"Extracted track ID: {track_id}")

    # Get track data
    try:
        track_data = await get_track(track_id)

        if not track_data or "id" not in track_data:
            logger.error(f"Failed to get track data for ID: {track_id}")
            return (
                track_id,
                None,
                "Couldn't load track details. The track was found but details couldn't be loaded.",
                None,
            )

        # Check if it's a Go+ (premium) track
        if track_data.get("policy") == "SNIP":
            logger.info(f"Track {track_id} is a Go+ premium track")
            return (
                track_id,
                track_data,
                "SoundCloud Go+ Song Detected. This is a premium track that can only be previewed (30 seconds).",
                None,
            )

        # All good - it's a track
        return track_id, track_data, None, None
    except Exception as e:
        logger.error(f"Error getting track data: {e}")
        return (
            track_id,
            None,
            f"Error processing track: {str(e)}",
            None,
        )
