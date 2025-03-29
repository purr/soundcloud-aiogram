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
    """Convert a SoundCloud artwork URL to its highest quality version.

    Args:
        artwork_url: Original SoundCloud artwork URL

    Returns:
        str: Low quality artwork URL
    """
    if not artwork_url or artwork_url == "":
        return artwork_url

    # Handle two different URL formats:
    # 1. URLs ending with -large.jpg (older format)
    # 2. URLs with -large in the middle (newer format)
    if "t1080x1080" in artwork_url:
        return artwork_url.replace("t1080x1080", "large")
    else:
        if "t500x500" in artwork_url:
            return artwork_url.replace("t500x500", "large")
        else:
            return artwork_url.replace("-t1080x1080", "-large")


def format_track_info_caption(track_info: Dict, bot_username: str) -> str:
    """Format a caption for a track with all necessary info.

    Args:
        track_info: Dictionary with track information
        bot_username: Username of the bot

    Returns:
        Formatted caption HTML string
    """
    permalink_url = track_info["permalink_url"]
    artwork_url = track_info.get("artwork_url")

    # Create initial caption with SoundCloud track link
    # Using zero-width space (\u200c) inside the URL to prevent embedding while keeping it clickable
    permalink_url_no_embed = permalink_url.replace("://", "://\u200c")
    caption = f"♫ <a href='{permalink_url_no_embed}'>Link</a>"

    # Add Spotify link if available
    if "spotify_url" in track_info:
        spotify_url_no_embed = track_info["spotify_url"].replace("://", "://\u200c")
        caption += f" | ✷ <a href='{spotify_url_no_embed}'>Spotify</a>"

    # Add artwork link if available
    if artwork_url:
        # Convert to high resolution
        artwork_url = get_high_quality_artwork_url(artwork_url)
        artwork_url_no_embed = artwork_url.replace("://", "://\u200c")
        caption += f" | ꕤ <a href='{artwork_url_no_embed}'>Artwork</a>"

    # Add bot username
    caption += f" | ✿ @{bot_username}"

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

    # Using zero-width space (\u200c) inside the URL to prevent embedding
    permalink_url_no_embed = track_info["permalink_url"].replace("://", "://\u200c")
    caption += f"♫ <a href='{permalink_url_no_embed}'><b>{html.escape(track_info['title'])}</b> - <b>{html.escape(track_info['artist'])}</b></a>"

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

    # Using zero-width space (\u200c) inside the URL to prevent embedding
    permalink_url_no_embed = track_info["permalink_url"].replace("://", "://\u200c")
    caption += f"♫ <a href='{permalink_url_no_embed}'><b>{html.escape(track_info['title'])}</b> - <b>{html.escape(track_info['artist'])}</b></a>"

    return caption
