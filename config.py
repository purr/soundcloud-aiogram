import os

from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Bot token - retrieved from environment variables
BOT_TOKEN = os.getenv("BOT_TOKEN", "")

# SoundCloud client ID - retrieved from environment variables
CLIENT_ID = os.getenv("CLIENT_ID", "")

# SoundCloud API URL - Changed to the working URL format for the API v2
SOUNDCLOUD_SEARCH_API = "https://api-v2.soundcloud.com/search/tracks"
SOUNDCLOUD_TRACK_API = "https://api-v2.soundcloud.com/tracks"
SOUNDCLOUD_RESOLVE_API = "https://api-v2.soundcloud.com/resolve"

# Debug mode
DEBUG = True

# Logging level
LOG_LEVEL = "INFO" if DEBUG else "WARNING"

# Debug options
DEBUG_SEARCH = False  # Set to False to disable verbose logging for search
DEBUG_DOWNLOAD = True  # Set to True to enable verbose logging for downloads

# Search timeout in seconds
SEARCH_TIMEOUT = 0.5

# Download settings
DOWNLOAD_PATH = os.getenv("DOWNLOAD_PATH", "downloads")
EXTRACT_ARTIST_FROM_TITLE = True
DOWNLOAD_ORIGINAL_QUALITY = True
MAX_DOWNLOAD_SIZE = 100 * 1024 * 1024  # 100MB
NAME_FORMAT = "{artist} - {title}"

# Playlist settings
MAX_PLAYLIST_TRACKS_TO_SHOW = 50  # Maximum number of tracks to display from a playlist

# SoundCloud logo URL
SOUNDCLOUD_LOGO_URL = "https://d21buns5ku92am.cloudfront.net/26628/images/419679-1x1_SoundCloudLogo_cloudmark-f5912b-original-1645807040.jpg"

# Version
VERSION = "0.7.93"
