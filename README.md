# SoundCloud Search Bot

A Telegram bot that allows users to search for and download tracks from SoundCloud directly within Telegram.

## Features

- 🔍 Inline search for SoundCloud tracks
- 🎵 One-click track downloading
- 🖼️ High quality audio with artwork
- 🔗 Direct links to SoundCloud and artist pages
- 💬 Works in private chats and groups
- 🎨 Clean, minimalist interface
- 💾 Caching for faster repeated downloads
- 📎 Direct download from SoundCloud links
- 📁 SoundCloud playlist support with track selection
- 🎵 Spotify track link conversion and search
- ⚠️ Detailed error handling with clear messages
- ⏳ Clear "Processing..." status indication when downloading

## Usage

1. Add the bot to your Telegram: [@your_bot_username]
2. Start a chat with the bot and send `/start` (required for inline downloads)
3. In any chat, type `@your_bot_username` followed by your search query
4. Select a track from the search results
5. The bot will automatically download and send the audio file

### Direct Link Downloads

You can download tracks directly from SoundCloud or Spotify links in two ways:

1. **Send the link directly to the bot** - Simply paste any SoundCloud or Spotify track link in a chat with the bot, and it will automatically search or download the audio file.

2. **Use inline mode with a link** - Type `@your_bot_username` followed by a SoundCloud or Spotify link in any chat, and it will immediately show a download option.
   - For SoundCloud links, this will directly provide the track
   - For Spotify links, this will find the best match on SoundCloud
   - Both methods require that you have messaged the bot directly first with `/start`

### Playlist Support

The bot supports SoundCloud playlists in both direct messages and inline mode:

1. **Send a playlist link to the bot** - When you send a SoundCloud playlist link to the bot directly, it will display the playlist information and list of tracks (up to 10) as buttons. Click any track button to download it.

2. **Use inline mode with a playlist link** - Type `@your_bot_username` followed by a SoundCloud playlist link in any chat. The results will show the playlist and individual tracks that you can select.

Supported link formats:

- https://soundcloud.com/artist/track
- https://on.soundcloud.com/xyz
- https://m.soundcloud.com/artist/track
- https://open.spotify.com/track/ID
- Other SoundCloud and Spotify track link formats

## Important Notes

- You must first message the bot directly with `/start` before you can use inline downloads
- This is a Telegram API limitation - bots cannot initiate conversations with users
- High quality audio downloads are provided as MP3 files
- Album artwork is included when available
- Placeholder audio uses direct URL reference (no downloading or caching)

## Technical Details

- Built with aiogram v3 for Telegram Bot API
- Caches file IDs and downloaded content for efficiency
- Implements fallback mechanisms for inline message updates
- Validates downloaded audio files to ensure they're playable
- Handles formatted duration strings (e.g., "3:25") in track metadata
- Distinguishes between permission errors and system errors
- Includes search query/URL in error messages for easy retry

## Version History

- v0.7.93: Enhanced download system with improved HLS stream handling and advanced quality scoring for all audio streams
- v0.7.92: Improved track download support for HLS-only tracks and direct downloadable tracks, removed local artwork download
- v0.7.91: Fixed artwork download functionality to properly save and attach artwork to audio files
- v0.7.83: Fixed critical bug where bot would incorrectly report DM access issues after successfully sending a file
- v0.7.82: Simplified DM check logic to only attempt sending files directly
- v0.7.81: Further improved DM detection by checking recent sent files history to prevent false "Chat Access Needed" messages
- v0.7.80: Fixed critical DM detection logic to prevent false "Chat Access Needed" messages when files were actually sent successfully
- v0.7.79: Improved DM access detection with more reliable checks and fixed error logging for non-accessible chats
- v0.7.69: Fixed InputFile usage for thumbnails in audio files
- v0.7.68: Fixed caption format in inline results and added thumbnails back to audio files
- v0.7.67: Removed "Downloading..." messages from inline search results
- v0.7.66: Fixed temp_message access error and Unicode character issues
- v0.7.65: Keep original caption text, link, cover, and @ during download process
- v0.7.64: Improved playlist support with batch track retrieval
- v0.7.61: Added SoundCloud playlist support with inline track selection
- v0.7.60: Fixed "Try Download Again" button and added duplicate file detection to avoid sending multiple copies
- v0.7.59: Added silent notifications for audio files sent via DM
- v0.7.58: Fixed shortened SoundCloud URLs by following redirects before resolving
- v0.7.57: Improved SoundCloud URL detection with a single comprehensive regex pattern
- v0.7.56: Added support for shortened SoundCloud URLs (on.soundcloud.com format)
- v0.7.55: Added Loguru for colorful and better formatted logging
- v0.7.50: Improved code organization with worker functions
- v0.7.49: Improved SoundCloud URL extraction
- v0.7.48: Standardized use of article results for better user experience
- v0.7.47: Fixed detection of corrupted audio files to prevent sending broken media
- v0.7.46: Added robust audio file validation to detect zero-length audio files and fixed artwork handling
- v0.7.45: Added better validation for empty audio files with clear error messages
- v0.7.44: Prevented hyperlink embedding in captions for cleaner display
- v0.7.43: Improved article results formatting for consistent user experience
- v0.7.42: Fixed article results for direct SoundCloud URLs
- v0.7.41: Added support for article result mode with better inline result formatting
- v0.7.40: Added Spotify URLs to audio captions when using Spotify links
- v0.7.39: Fixed Spotify metadata extraction for proper artist and title parsing
- v0.7.38: Added Spotify link support
- v0.7.37: Added "Processing..." title for audio files when a track is selected
- v0.7.36: Uses placeholder URL directly without any downloading
- v0.7.35: Always fetch placeholder file from URL without local caching
- v0.7.34: Using remote placeholder file from URL instead of requiring a local file
- v0.7.33: Simplified placeholder audio handling to always use local file
- v0.7.32: Fixed download task storage in chosen_inline_result_handler to ensure downloads complete
- v0.7.31: Updated search API to use tracks-specific endpoint for better filtering
- v0.7.30: Fixed placeholder file_id caching to use first user instead of bot itself
- v0.7.29: Added artwork thumbnails to inline search results for better visual display
- v0.7.28: Removed all placeholder audio URL references, exclusively using local placeholder and file_id
- v0.7.27: Stopped remote placeholder fetching and added search query to error messages
- v0.7.26: Improved error handling with separate system error and permission messages
- v0.7.25: Removed kaomojis for a cleaner, more professional interface
- v0.7.24: Improved placeholder audio handling with local file and caching
- v0.7.23: Enhanced kaomoji integration throughout bot interactions
- v0.7.22: Added random kaomojis to messages and refactored with worker functions
- v0.7.21: Updated caption formatting with minimalist, monochrome UI
- v0.7.20: Fixed direct URL handling to use direct API method instead of search
- v0.7.19: Improved URL handling with proper placeholder audio integration
- v0.7.18: Fixed placeholder audio handling in inline URL processing
- v0.7.17: Enhanced direct link support with inline URL processing
- v0.7.16: Added direct SoundCloud link download feature
- v0.7.14: Fixed syntax error in DM permission handling
- v0.7.13: Improved DM permission handling with better user instructions
- v0.7.12: Standardized placeholder file terminology throughout codebase
- v0.7.11: Renamed placeholder file
- v0.7.10: Fixed duration validation to handle formatted time strings
- v0.7.9: Fixed track validation error
- v0.7.8: Added validation checks for downloaded tracks
- v0.7.7 and earlier: Various improvements and bugfixes

## Environment Setup

The bot uses environment variables for sensitive information. To set up:

1. Create a `.env` file in the root directory of the project
2. Add the following variables:

```
# Telegram Bot Token (required)
BOT_TOKEN=your_telegram_bot_token

# SoundCloud API Client ID (required)
CLIENT_ID=your_soundcloud_client_id
```

Obtain your Telegram bot token from [BotFather](https://t.me/botfather).

For the SoundCloud client ID, you'll need to use a valid client ID from the SoundCloud API.

The bot will automatically load these variables from the `.env` file using `python-dotenv`.

## BotFather Configuration

After creating your bot with [BotFather](https://t.me/botfather), you need to configure these additional settings:

1. **Enable Inline Mode**: Send `/mybots` to BotFather, select your bot, then select "Bot Settings" > "Inline Mode" > "Turn on"

2. **Configure Inline Feedback**: Send `/mybots` to BotFather, select your bot, then select "Bot Settings" > "Inline Feedback" and set it to "100%"

   - This is **crucial** for the bot to properly update audio results when selected
   - Without 100% inline feedback, the bot won't be able to update the audio file after a user selects a track

3. **Set Commands**: Consider setting up commands via BotFather's `/setcommands` to provide users with a helpful command menu

## Troubleshooting

- **Inline downloads don't work**: Make sure you've messaged the bot directly with /start first
- **No audio plays in inline results**: Ensure your Telegram app is up to date - some older versions may not properly handle audio placeholder updates
- **Downloads get stuck**: If a download takes too long, cancel and try again later when SoundCloud servers may be less busy
- **System error messages**: If you see a system error message, this indicates a technical issue with the bot or server - not a permissions problem. Try again later.

## Privacy & Legal

This bot uses the SoundCloud API to search for and download tracks. Users are responsible for ensuring they have the right to download and use any content obtained through this bot.

## Credits

Built with ❤️ using Python and aiogram

## Logging

The bot uses the [Loguru](https://github.com/Delgan/loguru) library for logging, which provides:

- Colorful console output with intuitive log levels
- Automatic file rotation and compression
- Contextual information (filename, function name, line number)
- Easy configuration and customization

Logs are stored in the `logs` directory with daily rotation and 7-day retention period.
