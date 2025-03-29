import re
import time
import random
from typing import Dict, Optional

import aiohttp

# Configure logging
from utils.logger import get_logger

logger = get_logger(__name__)

# Cache for client IDs
client_id_cache: Dict[str, float] = {}
CLIENT_ID_EXPIRY = 60 * 60 * 12  # 12 hours expiry

# Fallback client IDs in case dynamic generation fails
FALLBACK_CLIENT_IDS = [
    # Add a few fallback client IDs here - these are examples and will eventually expire
    "a3e059563d7fd3372b49b37f00a00bcf",
    "2t9loNQH90kzJcsFCODdigxfp325aq4z",
    "iZIs9mchVcX5lhVRyQGGAYlNPVldzAoX",
    "0t6waxGNs0ofxFhTgTsKkJfIsItcGIDp",
    "mXiDMmvLfAR7NxQI6l9dZ6X2yYiS71F9",
]

# Regular expressions for client ID extraction
ASSETS_SCRIPTS_REGEX = re.compile(r'src="(https://a-v2\.sndcdn\.com/assets/[^"]+\.js)"')
CLIENT_ID_REGEX = re.compile(r'"?client_id"?\s*:\s*"([a-zA-Z0-9]+)"')


async def extract_client_id_from_page() -> Optional[str]:
    """
    Extract client ID from SoundCloud website by fetching their main page
    and looking for client ID in the JavaScript sources.

    Uses a focused approach targeting SoundCloud asset scripts.

    Returns:
        Optional[str]: The client ID if found, None otherwise
    """
    logger.info("Extracting client ID from SoundCloud website...")

    try:
        # Fetch SoundCloud main page
        async with aiohttp.ClientSession() as session:
            async with session.get("https://soundcloud.com/") as response:
                if response.status != 200:
                    logger.error(
                        f"Failed to fetch SoundCloud homepage: {response.status}"
                    )
                    return None

                html = await response.text()

                # Find asset script URLs
                script_matches = ASSETS_SCRIPTS_REGEX.findall(html)
                if not script_matches:
                    logger.error("No asset scripts found on SoundCloud homepage")
                    return None

                # Try each script until we find a client ID
                for script_url in script_matches:
                    logger.debug(f"Checking script: {script_url}")

                    async with session.get(script_url) as script_response:
                        if script_response.status != 200:
                            logger.warning(
                                f"Failed to fetch script {script_url}: {script_response.status}"
                            )
                            continue

                        script_content = await script_response.text()
                        client_id_match = CLIENT_ID_REGEX.search(script_content)

                        if client_id_match:
                            client_id = client_id_match.group(1)
                            logger.info(f"Found client ID: {client_id}")
                            return client_id

                # Try alternative method with a broader search if no client ID found
                logger.info(
                    "No client ID found in asset scripts, trying broader search..."
                )

                # Extract all JavaScript URLs
                js_urls = re.findall(r'<script[^>]+src="([^"]+)"', html)
                js_urls = [
                    url
                    for url in js_urls
                    if "sndcdn.com" in url or "soundcloud.com" in url
                ]

                for js_url in js_urls:
                    if not js_url.startswith("http"):
                        js_url = f"https://soundcloud.com{js_url}"

                    logger.debug(f"Examining additional script: {js_url}")

                    async with session.get(js_url) as js_response:
                        if js_response.status != 200:
                            continue

                        js_content = await js_response.text()

                        # Alternative patterns to try
                        alt_patterns = [
                            r'client_id:"([a-zA-Z0-9]+)"',
                            r"client_id=([a-zA-Z0-9]+)",
                            r'clientId:"([a-zA-Z0-9]+)"',
                            r'clientID:"([a-zA-Z0-9]+)"',
                            r'client_id="([a-zA-Z0-9]+)"',
                        ]

                        for pattern in alt_patterns:
                            match = re.search(pattern, js_content)
                            if match:
                                client_id = match.group(1)
                                logger.info(
                                    f"Found client ID using alternative pattern: {client_id}"
                                )
                                return client_id

                logger.error("Couldn't find client ID in any JavaScript files")
                return None

    except Exception as e:
        logger.error(f"Error extracting client ID: {e}")
        return None


async def verify_client_id(client_id: str) -> bool:
    """
    Verify if a client ID is valid by making a test API request

    Args:
        client_id: The client ID to verify

    Returns:
        bool: True if the client ID is valid, False otherwise
    """
    try:
        logger.info(f"Verifying client ID: {client_id}")

        # Use a simple API call to verify the client ID
        track_ids = [294091744, 1180823458, 2047164164]
        for track_id in track_ids:
            test_url = (
                f"https://api-v2.soundcloud.com/tracks/{track_id}?client_id={client_id}"
            )

            async with aiohttp.ClientSession() as session:
                async with session.get(test_url) as response:
                    if response.status == 200:
                        logger.info("Client ID verified successfully")
                        return True
                    else:
                        logger.warning(
                            f"Client ID verification failed with status: {response.status}"
                        )
        return False

    except Exception as e:
        logger.error(f"Error verifying client ID: {e}")
        return False


async def get_client_id() -> str:
    """
    Get a valid SoundCloud client ID.
    First tries from cache, then tries to extract from the website,
    and finally falls back to predefined client IDs.

    Returns:
        str: A valid SoundCloud client ID
    """
    # Check if we have a recent valid client ID in cache
    current_time = time.time()
    for client_id, timestamp in list(client_id_cache.items()):
        if current_time - timestamp < CLIENT_ID_EXPIRY:
            logger.info(f"Using cached client ID: {client_id}")
            return client_id

    # Try to extract a client ID from the website
    client_id = await extract_client_id_from_page()

    if client_id and await verify_client_id(client_id):
        # Store in cache
        client_id_cache[client_id] = current_time
        return client_id

    # Fall back to predefined client IDs
    logger.warning("Falling back to predefined client IDs")

    # Try each fallback client ID
    random.shuffle(FALLBACK_CLIENT_IDS)  # Try in random order to distribute load

    for fallback_id in FALLBACK_CLIENT_IDS:
        if await verify_client_id(fallback_id):
            logger.info(f"Using fallback client ID: {fallback_id}")
            client_id_cache[fallback_id] = current_time
            return fallback_id

    # If all else fails, raise an exception
    error_msg = "Could not obtain a valid SoundCloud client ID"
    logger.error(error_msg)
    raise Exception(error_msg)


async def refresh_client_id() -> str:
    """
    Force refresh the client ID by clearing the cache and getting a new one.

    Returns:
        str: A new valid SoundCloud client ID
    """
    logger.info("Forcing client ID refresh")

    # Clear the cache
    client_id_cache.clear()

    # Get a new client ID
    return await get_client_id()
