import html
from typing import Dict

from utils.logger import get_logger

# Configure logging
logger = get_logger(__name__)


def get_high_quality_artwork_url(artwork_url: str) -> str:
    """Convert a SoundCloud artwork URL to its highest quality version.

    Args:
        artwork_url: Original SoundCloud artwork URL

    Returns:
        str: High quality artwork URL (1080x1080 resolution)
    """
    if not artwork_url or artwork_url == "":
        return artwork_url

    # Handle two different URL formats:
    # 1. URLs ending with -large.jpg (older format)
    # 2. URLs with -large in the middle (newer format)
    if artwork_url.endswith("large.jpg"):
        return artwork_url.replace("large.jpg", "t1080x1080.jpg")
    else:
        return artwork_url.replace("-large", "-t1080x1080")


def get_low_quality_artwork_url(artwork_url: str) -> str:
    """Convert a SoundCloud artwork URL to low quality version.

    Args:
        artwork_url: Original SoundCloud artwork URL

    Returns:
        str: Low quality artwork URL (500x500 resolution)
    """
    if not artwork_url or artwork_url == "":
        return artwork_url

    # Handle two different URL formats and convert to t500x500
    if "t1080x1080" in artwork_url:
        return artwork_url.replace("t1080x1080", "t500x500")
    else:
        if "large" in artwork_url:
            return artwork_url.replace("large", "t500x500")
        else:
            return artwork_url.replace("-t1080x1080", "-t500x500")


def format_track_info_caption(track_info: Dict, bot_username: str) -> str:
    """Format a caption for a track with all necessary info.

    Args:
        track_info: Dictionary with track information
        bot_username: Username of the bot

    Returns:
        Formatted caption HTML string
    """
    # Ensure track_info has all required keys

    permalink_url = track_info.get("permalink_url", "https://soundcloud.com")
    artwork_url = track_info.get("artwork_url")
    spotify_url = track_info.get("spotify_url", "")

    display_title = track_info.get("display_title", "")

    # Ensure display_title is not empty
    if not display_title or display_title.strip() == "":
        display_title = "Untitled Track"
        track_info["display_title"] = display_title

    # Create initial caption with SoundCloud track link
    # Using the proper HTML tag structure to prevent embedding
    caption = f"𝄞 <a href='{permalink_url}'>Link</a>"

    # Add Spotify link if available (positioned after the SoundCloud link)
    if "spotify_url" in track_info:
        caption += f" ❀ <a href='{spotify_url}'>Spotify</a>"

    # Add artwork link if available
    if artwork_url:
        # Convert to high resolution
        artwork_url = get_high_quality_artwork_url(artwork_url)
        caption += f" ꕤ <a href='{artwork_url}'>Cover</a>"

    # Add bot username
    caption += f" ♬ @{bot_username}"

    return caption


def format_error_caption(
    error_message: str, track_info: Dict, bot_username: str
) -> str:
    """Format an error caption with track info.

    Args:
        error_message: The error message to display
        track_info: Dictionary with track information
        bot_username: Username of the bot

    Returns:
        Formatted error caption HTML string
    """
    # Format the error message
    caption = f"❌ <b>{error_message}</b>\n\n"

    # Properly format the link without modifying the URL
    permalink_url = track_info["permalink_url"]
    caption += f"♫ <a href='{permalink_url}'><b>{html.escape(track_info['display_title'])}</b></a>"

    return caption


def format_success_caption(message: str, track_info: Dict, bot_username: str) -> str:
    """Format a success caption with track info.

    Args:
        message: The success message to display
        track_info: Dictionary with track information
        bot_username: Username of the bot

    Returns:
        Formatted success caption HTML string
    """
    # Format the success message
    caption = f"✅ <b>{message}</b>\n\n"

    # Properly format the link without modifying the URL
    permalink_url = track_info["permalink_url"]
    caption += f"♫ <a href='{permalink_url}'><b>{html.escape(track_info['display_title'])}</b></a>"

    return caption
