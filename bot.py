import html
import time
import asyncio
import traceback
from typing import Dict
from datetime import datetime

from aiogram import F, Bot, Router, Dispatcher
from aiogram.enums import ParseMode
from aiogram.types import (
    Message,
    InlineQuery,
    CallbackQuery,
    ChosenInlineResult,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    InputTextMessageContent,
    InlineQueryResultArticle,
)
from aiogram.filters import CommandStart
from aiogram.exceptions import TelegramBadRequest
from aiogram.client.default import DefaultBotProperties

from utils import (
    SOUNDCLOUD_URL_PATTERN,
    format_error_caption,
    extract_soundcloud_url,
    format_success_caption,
    process_soundcloud_url,
    format_track_info_caption,
    get_high_quality_artwork_url,
)
from config import (
    VERSION,
    BOT_TOKEN,
    SEARCH_TIMEOUT,
    SOUNDCLOUD_LOGO_URL,
    MAX_PLAYLIST_TRACKS_TO_SHOW,
)
from helpers import (
    get_track,
    cleanup_files,
    filter_tracks,
    download_track,
    get_track_info,
    get_tracks_batch,
    search_soundcloud,
    create_soundcloud_search_query,
    extract_metadata_from_spotify_url,
)
from predefined import (
    artist_button,
    try_again_button,
    soundcloud_button,
    start_chat_button,
    download_status_button,
    example_inline_search_button,
)
from utils.logger import get_logger
from helpers.workers import (
    send_audio_file,
    handle_download_failure,
    validate_downloaded_track,
    update_inline_message_with_audio,
)


# Define a function to classify errors
def is_permission_error(error):
    """
    Determine if an error is related to permissions (bot was blocked, not started, etc.)
    or is a system/network error.

    Args:
        error: The exception object or error message

    Returns:
        bool: True if it's a permission error, False if it's a system error
    """
    error_text = str(error).lower()

    # Check for common permission error messages
    permission_error_phrases = [
        "forbidden",
        "bot was blocked",
        "bot was not found",
        "chat not found",
        "user is deactivated",
        "not enough rights",
        "timed out",
        "waiting for an ack",
        "bot can't initiate conversation",
        "user not found",
        "access denied",
        "message not found",
        "chat access required",
    ]

    return any(phrase in error_text for phrase in permission_error_phrases)


# Configure logging
logger = get_logger(__name__)

# Initialize bot and dispatcher
bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()
router = Router()

# Cached queries to avoid running search for every keystroke
search_cache: Dict[str, asyncio.Task] = {}

# Dictionary to store download tasks for each message
download_tasks: Dict[str, asyncio.Task] = {}

# Store user_id for each inline_message_id
inline_message_users: Dict[str, int] = {}

# Store Spotify URLs for inline queries
inline_spotify_urls: Dict[str, str] = {}

# Create a download queue
download_queue = asyncio.Queue()


@router.message(CommandStart())
async def cmd_start(message: Message):
    """Handler for /start command"""
    # Get bot info for proper username display
    bot_info = await bot.get_me()
    bot_username = bot_info.username

    await message.answer(
        f"𝄞⨾𓍢ִ໋ ♫⋆｡♪₊˚♬ <b>SoundCloud Search Bot v{VERSION}</b>\n\n"
        f"⇩ <b>Download tracks from SoundCloud</b>\n"
        f"𝄞 <b>Supports tracks, playlists and Spotify links</b>\n"
        f"♫ <b>Use the bot inline to search/download anywhere</b>\n\n"
        f"ⓘ <b>How to use:</b>\n"
        f"• Inline search: <code>@{bot_username}</code> [search query]\n"
        f"• Direct links: Send any SoundCloud or Spotify URL\n"
        f"• Examples:\n"
        f"  <code>@{bot_username} drain gang</code>\n"
        f"  <code>@{bot_username} https://soundcloud.com/21olxa01gdby/somewhere</code>\n\n"
        f"𖤍 <b>Features:</b>\n"
        f"• One-click track downloading from SoundCloud\n"
        f"• Highest quality audio with proper ID3 tags\n"
        f"• Artist name extraction from track titles\n"
        f"• Direct links to the track and cover art\n"
        f"• Clean, minimalist interface\n\n"
        f"❀ <b>Supported content:</b>\n"
        f"• Search with words or links\n"
        f"• SoundCloud tracks, playlists and albums\n"
        f"• Spotify track links (converted to SoundCloud)\n\n"
        f"❥ By @pinkiepie",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[
                [example_inline_search_button],
                [
                    InlineKeyboardButton(
                        text="🏷️ Edit ID3 Tags with @id3_robot",
                        url="https://t.me/id3_robot?start=dlmus",
                    ),
                ],
            ]
        ),
    )


@router.inline_query()
async def inline_search(query: InlineQuery):
    """Handler for inline search queries"""
    # Check if query is empty
    bot_info = await bot.get_me()
    if not query.query:
        # Return default examples
        await query.answer(
            results=[
                InlineQueryResultArticle(
                    id="example1",
                    title="Search for SoundCloud tracks",
                    description="Search for Track/Artist or use a SoundCloud/Spotify link",
                    input_message_content=InputTextMessageContent(
                        message_text=(
                            f"How to use this bot:\n\n"
                            f"1. Dm the bot @{bot_info.username}\n"
                            f"2. Type your search query inline anywhere\n"
                            f"3. Select a track from the results\n"
                            f"4. The track will be sent automatically"
                        )
                    ),
                    thumbnail_url=SOUNDCLOUD_LOGO_URL,
                    reply_markup=InlineKeyboardMarkup(
                        inline_keyboard=[[example_inline_search_button]]
                    ),
                )
            ],
            cache_time=60 * 60 * 24,
        )
        return

    # Extract query text
    search_text = query.query.strip()
    username = (
        f"{query.from_user.first_name} {query.from_user.last_name if query.from_user.last_name else ''}"
        f"@{query.from_user.username if query.from_user.username else ''} (id:{query.from_user.id})"
    )

    logger.info(f"Inline search query: {search_text} by {username}")

    # Check if this is a SoundCloud URL
    soundcloud_url = extract_soundcloud_url(search_text)
    if soundcloud_url:
        logger.info(f"Found SoundCloud URL in inline query: {soundcloud_url}")

        # Process the URL to get track ID or playlist info
        track_id, track_data, error_message, playlist_data = (
            await process_soundcloud_url(soundcloud_url)
        )

        # Handle playlist case
        if playlist_data:
            playlist_title = playlist_data.get("title", "Unknown Playlist")
            user = playlist_data.get("user", {}).get("username", "Unknown Artist")
            track_count = playlist_data.get("track_count", 0)
            tracks = playlist_data.get("tracks", [])

            logger.info(
                f"Processing playlist: {playlist_title} with {track_count} tracks"
            )

            # Extract track IDs for batch fetching
            track_ids = []
            for track in tracks:
                if track and isinstance(track, dict) and "id" in track:
                    track_ids.append(str(track["id"]))

            # If we have track IDs, fetch their complete information
            full_tracks = []
            if track_ids:
                logger.info(
                    f"Fetching complete information for {len(track_ids)} tracks"
                )
                try:
                    full_tracks = await get_tracks_batch(track_ids)
                    logger.info(
                        f"Retrieved {len(full_tracks)} tracks with complete information"
                    )

                    # Sort full_tracks to match the original playlist order
                    if full_tracks:
                        # Create a mapping of track IDs to track data
                        track_map = {
                            str(track.get("id")): track for track in full_tracks
                        }

                        # Reorder full_tracks to match the original playlist order
                        ordered_tracks = []
                        for track_id in track_ids:
                            if track_id in track_map:
                                ordered_tracks.append(track_map[track_id])

                        # Replace full_tracks with the ordered version
                        if ordered_tracks:
                            full_tracks = ordered_tracks
                            logger.info("Reordered tracks to match playlist sequence")
                except Exception as e:
                    logger.error(f"Error fetching track details: {e}")
                    # Continue with limited info if batch fetch fails
                    full_tracks = []

            # Create inline results for each track
            inline_results = []

            # Get artwork URL for thumbnail
            artwork_url = playlist_data.get("artwork_url", "")
            if artwork_url:
                artwork_url = get_high_quality_artwork_url(artwork_url)

            # Determine max tracks to show
            max_tracks_to_show = min(
                MAX_PLAYLIST_TRACKS_TO_SHOW,
                len(full_tracks) if full_tracks else len(tracks),
            )  # Show up to MAX_PLAYLIST_TRACKS_TO_SHOW tracks in the inline results

            # If we have full tracks info, use that
            if full_tracks:
                for i, track in enumerate(full_tracks[:max_tracks_to_show]):
                    # Get processed track info with correct artist/title
                    track_info = get_track_info(track)
                    track_id = track_info.get("id")

                    # Calculate duration of the track
                    duration_ms = track.get("duration", 0)
                    minutes, seconds = divmod(duration_ms // 1000, 60)
                    duration_str = f"{minutes}:{seconds:02d}"

                    # Create result for this track
                    track_result = InlineQueryResultArticle(
                        id=f"{track_id}_{i}",
                        title=f"{i + 1}. {track_info['display_title']}",
                        description=f"By {track_info['artist']} • {duration_str}",
                        input_message_content=InputTextMessageContent(
                            message_text=format_track_info_caption(
                                track_info, bot_info.username
                            ),
                            parse_mode=ParseMode.HTML,
                        ),
                        thumbnail_url=track_info.get("artwork_url")
                        or artwork_url
                        or SOUNDCLOUD_LOGO_URL,
                        reply_markup=InlineKeyboardMarkup(
                            inline_keyboard=[[download_status_button]]
                        ),
                    )
                    inline_results.append(track_result)
            # Fall back to partial info if full info not available
            else:
                # Filter out invalid tracks from the list
                valid_tracks = []
                for track in tracks:
                    if track and isinstance(track, dict) and "id" in track:
                        valid_tracks.append(track)

                for i, track in enumerate(valid_tracks[:max_tracks_to_show]):
                    # Get processed track info with correct artist/title
                    track_info = get_track_info(track)
                    track_id = track_info.get("id")

                    # Calculate duration of the track
                    duration_str = track_info.get("duration", "0:00")

                    # Create result for this track
                    track_result = InlineQueryResultArticle(
                        id=f"{track_id}_{i}",
                        title=f"{i + 1}. {track_info['display_title']}",
                        description=f"By {track_info['artist']} • {duration_str}",
                        input_message_content=InputTextMessageContent(
                            message_text=format_track_info_caption(
                                track_info, bot_info.username
                            ),
                            parse_mode=ParseMode.HTML,
                        ),
                        thumbnail_url=track_info.get("artwork_url")
                        or artwork_url
                        or SOUNDCLOUD_LOGO_URL,
                        reply_markup=InlineKeyboardMarkup(
                            inline_keyboard=[[download_status_button]]
                        ),
                    )
                    inline_results.append(track_result)

            # If no valid results, show a message
            if not inline_results:
                error_result = InlineQueryResultArticle(
                    id=f"playlist_error_{int(time.time())}",
                    title=f"Playlist: {playlist_title}",
                    description="No available tracks in this playlist",
                    input_message_content=InputTextMessageContent(
                        message_text=f"🎵 <b>SoundCloud Playlist:</b> {html.escape(playlist_title)}\n"
                        f"<b>By:</b> {html.escape(user)}\n"
                        f"<b>Tracks:</b> {track_count}\n\n"
                        f"<i>This playlist either has no tracks or all tracks are private/unavailable.</i>"
                    ),
                    thumbnail_url=artwork_url or SOUNDCLOUD_LOGO_URL,
                    reply_markup=InlineKeyboardMarkup(
                        inline_keyboard=[
                            [
                                InlineKeyboardButton(
                                    text="❌ No tracks available",
                                    callback_data="no_tracks",
                                ),
                            ]
                        ]
                    ),
                )
                inline_results.append(error_result)

            # Create the switch_pm_text with playlist information
            switch_pm_text = f"🎵 Playlist: {playlist_title} • {track_count} tracks"
            # Limit the switch_pm_text to a reasonable length
            if len(switch_pm_text) > 60:
                switch_pm_text = f"🎵 {playlist_title[:40]}... • {track_count} tracks"

            # Answer with playlist track results
            await query.answer(
                results=inline_results,
                cache_time=300,
                switch_pm_text=switch_pm_text,
                switch_pm_parameter="playlist_info",
            )
            return

        # Handle error case in URL processing
        if error_message:
            await query.answer(
                results=[
                    InlineQueryResultArticle(
                        id="error",
                        title=f"Error: {error_message.split('.')[0]}",
                        description=error_message,
                        input_message_content=InputTextMessageContent(
                            message_text=f"❌ <b>Error:</b> {error_message}\n\n<i>URL: {soundcloud_url}</i>"
                        ),
                        thumbnail_url=SOUNDCLOUD_LOGO_URL,
                    )
                ],
                cache_time=10,
            )
            return

        # If we have a track ID, this is a direct URL to a track
        if track_id and track_data:
            # Get track info
            track_info = get_track_info(track_data)
            track_title = track_info["title"]
            track_artist = track_info["artist"]
            track_duration = track_info["duration"]

            # Create keyboard with download button
            keyboard = InlineKeyboardMarkup(inline_keyboard=[[download_status_button]])

            # Get artwork URL for thumbnail
            artwork_url = track_info.get("artwork_url", "")

            # Create a result with this track
            result = InlineQueryResultArticle(
                id=f"{track_id}_0",
                title=f"🎵 {track_title}",
                description=f"By {track_artist} • {track_duration}",
                input_message_content=InputTextMessageContent(
                    message_text=format_track_info_caption(
                        track_info, bot_info.username
                    ),
                    parse_mode=ParseMode.HTML,
                ),
                reply_markup=keyboard,
                thumbnail_url=artwork_url or SOUNDCLOUD_LOGO_URL,
            )

            await query.answer(results=[result], cache_time=300)
            return

    # Check if the query is a Spotify URL
    if search_text.startswith(("https://open.spotify.com/track/", "spotify:track:")):
        logger.info(f"Detected Spotify URL in inline query: {search_text}")

        # Extract metadata from Spotify URL
        metadata = await extract_metadata_from_spotify_url(search_text)

        if not metadata:
            # Failed to extract metadata from Spotify
            error_result = InlineQueryResultArticle(
                id=f"spotify_error_{int(time.time())}",
                title="Invalid Spotify track",
                description="Could not extract track information from this Spotify link",
                input_message_content=InputTextMessageContent(
                    message_text="❌ <b>Could not process this Spotify link.</b>\n\nPlease try with a direct SoundCloud link or a search query instead."
                ),
                thumbnail_url=SOUNDCLOUD_LOGO_URL,
            )

            await query.answer(
                results=[error_result],
                cache_time=30,
            )
            return

        # Store the Spotify URL for this search query so we can retrieve it later when chosen
        inline_spotify_urls[search_text] = metadata["spotify_url"]

        # Create a SoundCloud search query from the Spotify metadata
        soundcloud_query = create_soundcloud_search_query(
            metadata["title"], metadata["artist"]
        )

        # Add some context in the logs
        logger.info(
            f"Converted Spotify track '{metadata['title']}' by '{metadata['artist']}' to SoundCloud query: '{soundcloud_query}'"
        )

        # Replace the search_text with our constructed query for SoundCloud search
        search_text = soundcloud_query

    # For regular text searches (not URLs), use the debounced search
    logger.info(f"Performing text search for: {search_text}")

    # Cancel previous search task if it exists
    if (
        query.from_user.id in search_cache
        and not search_cache[query.from_user.id].done()
    ):
        search_cache[query.from_user.id].cancel()

    # Create new search task with debounce
    search_task = asyncio.create_task(debounced_search(search_text, query))
    search_cache[query.from_user.id] = search_task


async def debounced_search(search_text: str, query: InlineQuery):
    """Perform search after a delay to avoid too many requests"""
    try:
        # Wait for debounce timeout
        await asyncio.sleep(SEARCH_TIMEOUT)

        logger.info(f"Searching for: '{search_text}'")

        # Perform search
        results = await search_soundcloud(search_text)
        tracks = filter_tracks(results)
        total_results = results.get("total_results", 0)

        logger.info(f"Found {len(tracks)} tracks")

        # Get bot info for username in captions
        bot_info = await bot.get_me()

        # Process search results
        inline_results = []

        for i, track in enumerate(tracks[:50]):  # Limit to 50 results
            track_info = get_track_info(track)

            # Create result item
            result_id = f"{track_info['id']}_{i}"

            # Get artwork URL for thumbnail
            artwork_url = track_info.get("artwork_url") or ""
            if artwork_url:
                # Convert to high resolution
                artwork_url = get_high_quality_artwork_url(artwork_url)
            else:
                # Default artwork if none available
                artwork_url = (
                    "https://i1.sndcdn.com/artworks-000000000000-000000-large.jpg"
                )

            # Create keyboard with download button
            keyboard = InlineKeyboardMarkup(inline_keyboard=[[download_status_button]])

            # Format duration for display
            duration_str = track_info.get("duration", "")

            # Prepare detailed description
            description = f"By: {track_info['artist']}"
            if duration_str:
                description += f" • {duration_str}"
            if track_info.get("genre"):
                description += f" • {track_info['genre']}"

            # Create article result
            article_result = InlineQueryResultArticle(
                id=result_id,
                title=track_info["display_title"],
                description=description,
                thumbnail_url=artwork_url,
                input_message_content=InputTextMessageContent(
                    message_text=format_track_info_caption(
                        track_info, bot_info.username
                    ),
                    parse_mode=ParseMode.HTML,
                ),
                reply_markup=keyboard,  # Use the same keyboard as audio results
            )
            inline_results.append(article_result)

        # Answer inline query
        logger.info(f"Answering with {len(inline_results)} results")
        await query.answer(
            results=inline_results,
            cache_time=60,  # Cache for 1 minute
            is_personal=True,
            switch_pm_text=f"Found {total_results} tracks",
            switch_pm_parameter="from_inline",
        )

    except asyncio.CancelledError:
        # Task was cancelled, do nothing
        logger.debug("Search task was cancelled")
        pass
    except Exception as e:
        logger.error(f"Error in debounced search: {e}")
        # Try to answer with an error message
        try:
            await query.answer(
                results=[],
                cache_time=5,
                is_personal=True,
                switch_pm_text="Error occurred, try again",
                switch_pm_parameter="error",
            )
        except Exception as answer_err:
            logger.error(f"Failed to answer inline query with error: {answer_err}")
            pass


@router.chosen_inline_result()
async def chosen_inline_result_handler(chosen_result: ChosenInlineResult):
    """Handler for chosen inline results"""
    if not chosen_result.inline_message_id:
        logger.warning("Chosen result has no inline_message_id")
        return

    # Extract track ID and result index from the result ID
    result_parts = chosen_result.result_id.split("_")
    if len(result_parts) >= 2:
        track_id = result_parts[0]
        result_index = result_parts[1]
    else:
        logger.error(f"Invalid result ID format: {chosen_result.result_id}")
        return

    # Store the user ID for this inline message ID
    inline_message_users[chosen_result.inline_message_id] = chosen_result.from_user.id

    # Add this message to the download queue
    # This effectively starts the download
    await download_queue.put(
        {
            "track_id": track_id,
            "inline_message_id": chosen_result.inline_message_id,
            "user_id": chosen_result.from_user.id,
            "search_query": chosen_result.query,
            "result_index": result_index,
        }
    )


async def download_and_update_inline_message(
    inline_message_id: str, track_id: str, search_query: str = None
):
    """Download track and update the inline message with audio file instead of creating a new message."""
    message_identifier = inline_message_id  # For logging clarity
    filepath = None
    should_cleanup = False

    try:
        logger.debug(
            f"🚀 Starting inline audio update for message {message_identifier}"
        )

        # Get bot info for metadata
        bot_info = await bot.get_me()
        bot_user = {
            "username": bot_info.username,
            "id": bot_info.id,
            "name": bot_info.full_name,
        }

        # Download the track in the background
        logger.info(
            f"Downloading track ID: {track_id} for inline message: {message_identifier}"
        )
        download_start = time.time()
        download_result = await download_track(track_id, bot_user)
        download_time = time.time() - download_start

        # Store file paths for potential cleanup
        filepath = download_result.get("filepath")

        # Flag to track if we need to clean up files
        should_cleanup = not download_result.get("cached", False)

        # Track info for either success or failure cases
        track_info = get_track_info(download_result.get("track_data", {}))

        # Check if the search query was a Spotify URL and add it to track_info
        if search_query and search_query in inline_spotify_urls:
            track_info["spotify_url"] = inline_spotify_urls[search_query]
            logger.info(f"Added Spotify URL to track_info: {track_info['spotify_url']}")

            # Make sure we have a valid artwork URL
            if not track_info.get("artwork_url") and download_result.get(
                "track_data", {}
            ).get("artwork_url"):
                track_info["artwork_url"] = get_high_quality_artwork_url(
                    download_result["track_data"]["artwork_url"]
                )
                logger.info(
                    f"Added missing artwork URL from track_data: {track_info['artwork_url']}"
                )

        if download_result["success"]:
            # Track downloaded successfully - now validate it
            filepath = download_result["filepath"]

            # Validate the downloaded track
            is_valid, error_message = await validate_downloaded_track(
                filepath, track_info
            )

            if not is_valid:
                logger.warning(f"Downloaded track validation failed: {error_message}")
                await handle_download_failure(
                    bot=bot,
                    message_id=inline_message_id,
                    track_info=track_info,
                    error_message=error_message,
                    search_query=search_query,
                )
                return

            # Track is valid, proceed with normal processing
            logger.info(
                f"Download successful in {download_time:.2f} seconds: {download_result.get('filepath')}"
            )

            user_id = inline_message_users.get(inline_message_id)

            # Only proceed with file_id approach if the user has open DMs
            if user_id:
                logger.info(f"Sending audio to user {user_id} to get file_id")

                try:
                    # Send audio to get file_id
                    success = await send_audio_file(
                        bot=bot,
                        chat_id=user_id,
                        filepath=filepath,
                        track_info=track_info,
                        reply_to_message_id=None,
                    )

                    if not success:
                        # Show the permission required message
                        await fallback_to_direct_message(
                            inline_message_id,
                            track_id,
                            track_info,
                            bot_user,
                            search_query,
                        )
                        return

                    if hasattr(success, "audio") and success.audio:
                        file_id = success.audio.file_id
                        logger.info(f"Obtained file_id for inline update: {file_id}")

                        # Update the inline message with the file_id
                        success = await update_inline_message_with_audio(
                            bot=bot,
                            inline_message_id=inline_message_id,
                            file_id=file_id,
                            track_info=track_info,
                        )

                        if not success:
                            await handle_download_failure(
                                bot=bot,
                                message_id=inline_message_id,
                                track_info=track_info,
                                error_message="Failed to update inline message with audio",
                                search_query=search_query,
                            )
                    else:
                        logger.error(
                            "Failed to get file_id: temp_message has no audio attribute"
                        )
                        await fallback_to_direct_message(
                            inline_message_id,
                            track_id,
                            track_info,
                            bot_user,
                            search_query,
                        )

                except Exception as dm_err:
                    if is_permission_error(dm_err):
                        logger.error(
                            f"Permission error when sending DM to user: {dm_err}"
                        )
                        # Show the permission required message
                        await fallback_to_direct_message(
                            inline_message_id,
                            track_id,
                            track_info,
                            bot_user,
                            search_query,
                        )
                    else:
                        logger.error(f"System error when sending DM to user: {dm_err}")
                        await handle_system_error(
                            bot=bot,
                            message_id=inline_message_id,
                            track_info=track_info,
                            error_message=f"System error: {str(dm_err)[:100]}",
                            search_query=search_query,
                            filepath=filepath,
                        )
            else:
                # User doesn't have DMs open, show chat access needed message
                await fallback_to_direct_message(
                    inline_message_id,
                    track_id,
                    track_info,
                    bot_user,
                    search_query,
                )
        else:
            # Download failed
            error_message = download_result.get("message", "Unknown error")
            logger.error(f"Download failed: {error_message}")
            await handle_download_failure(
                bot=bot,
                message_id=inline_message_id,
                track_info=track_info,
                error_message=error_message,
                search_query=search_query,
            )

    except Exception as e:
        traceback.print_exc()
        logger.error(f"Error in download_and_update_inline_message: {e}", exc_info=True)
        try:
            error_caption = (
                "❌ <b>Error:</b> Something went wrong while processing your download."
            )
            if search_query:
                error_caption += (
                    f"\n\n<b>Query:</b> <code>{html.escape(search_query)}</code>"
                )

            await bot.edit_message_caption(
                inline_message_id=inline_message_id,
                caption=error_caption,
                reply_markup=InlineKeyboardMarkup(
                    inline_keyboard=[[try_again_button(track_id)]]
                ),
            )
        except Exception:
            pass
    finally:
        # Clean up files if needed (not cached or not needed anymore)
        if should_cleanup and filepath:
            try:
                await cleanup_files(filepath)
                logger.info(f"Cleaned up files after processing: {filepath}")
            except Exception as cleanup_err:
                logger.error(f"Error cleaning up files: {cleanup_err}")

        # Clean up the download task
        if inline_message_id in download_tasks:
            del download_tasks[inline_message_id]
            logger.debug(
                f"🧹 Download task for {message_identifier} removed from active tasks"
            )


async def handle_system_error(
    inline_message_id,
    track_id,
    track_info,
    bot_user,
    filepath=None,
    error_message="System error occurred",
    search_query=None,
):
    """Handle system errors during download or processing.

    This differs from permission errors - these are actual system/network issues that the user can't fix
    by simply starting the bot.
    """
    try:
        # Create a caption that explains this is a system error, not a permissions issue
        final_caption = "⚠️ <b>System Error</b>\n\n"

        # Use zero-width space to prevent URL embedding
        permalink_url_no_embed = track_info["permalink_url"].replace("://", "://\u200c")
        final_caption += f"♫ <a href='{permalink_url_no_embed}'><b>{html.escape(track_info['display_title'])}</b> - <b>{html.escape(track_info['artist'])}</b></a>\n\n"

        final_caption += f"<b>Error details:</b> {error_message}\n\n"

        # Include the search query or URL if provided
        if search_query:
            final_caption += (
                f"<b>Query:</b> <code>{html.escape(search_query)}</code>\n\n"
            )

        final_caption += "This appears to be a technical error with the bot or server, not a permissions issue. "
        final_caption += "You can try again later or download directly from SoundCloud."

        # Create keyboard with SoundCloud link
        markup = InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    soundcloud_button(track_info["permalink_url"]),
                ],
                [try_again_button(track_id)],
            ]
        )

        await bot.edit_message_caption(
            inline_message_id=inline_message_id,
            caption=final_caption,
            reply_markup=markup,
        )
    except Exception as e:
        logger.error(f"Error updating system error caption: {e}")
        try:
            # Simpler fallback if the first attempt fails
            simple_caption = "⚠️ <b>System Error:</b> An error occurred while processing your request. Please try again later."
            if search_query:
                simple_caption += (
                    f"\n\n<b>Query:</b> <code>{html.escape(search_query)}</code>"
                )

            await bot.edit_message_caption(
                inline_message_id=inline_message_id,
                caption=simple_caption,
                reply_markup=InlineKeyboardMarkup(
                    inline_keyboard=[[try_again_button(track_id)]]
                ),
            )
        except Exception as e:
            logger.error(f"Final system error fallback failed: {e}")
    finally:
        # Clean up files if they exist
        if filepath:
            await cleanup_files(
                filepath,
            )


async def fallback_to_direct_message(
    inline_message_id, track_id, track_info, bot_user, search_query=None
):
    """Fallback to direct message approach if inline update fails due to permission issues"""
    filepath = None
    try:
        # Get the artwork URL for a hyperlink
        artwork_url = track_info.get("artwork_url") or ""
        if artwork_url:
            # Convert to high resolution
            artwork_url = get_high_quality_artwork_url(artwork_url)

        # Prepare caption with minimalistic format and clearer instructions
        final_caption = "⚠️ <b>Permission Required</b>\n\n"

        # Use zero-width space to prevent URL embedding
        permalink_url_no_embed = track_info["permalink_url"].replace("://", "://\u200c")
        final_caption += f"♫ <a href='{permalink_url_no_embed}'>{html.escape(track_info['display_title'])} - {html.escape(track_info['artist'])}</a>\n\n"

        # Include the search query if provided
        if search_query:
            final_caption += (
                f"<b>Query:</b> <code>{html.escape(search_query)}</code>\n\n"
            )

        # Add a timestamp to ensure message is always different when "Try Again" is clicked
        final_caption += "To download this track:\n"
        final_caption += "1. ➥ Click the button below to send <code>/start</code>\n"
        final_caption += "2. ↺ Return here and try downloading again\n\n"
        final_caption += f"<i>This is necessary because Telegram requires you to message the bot first before it can send you files. (Last attempted: {int(time.time())})</i>"

        # Create keyboard with button to open chat with bot and try again button
        markup = InlineKeyboardMarkup(
            inline_keyboard=[
                [start_chat_button(bot_user["username"])],
                [try_again_button(track_id)],
            ]
        )

        try:
            await bot.edit_message_caption(
                inline_message_id=inline_message_id,
                caption=final_caption,
                reply_markup=markup,
            )
        except TelegramBadRequest as bad_req:
            # Handle "message is not modified" error gracefully
            if "message is not modified" in str(bad_req).lower():
                logger.info(
                    "Message content unchanged, user likely tried again without changes"
                )
                # No need to update the message, just ignore this error
                pass
            else:
                # Some other bad request error, re-raise it to be caught by outer exception handler
                raise
    except Exception as caption_err:
        logger.error(f"Error updating fallback caption: {caption_err}")
        try:
            # Simpler fallback if the first attempt fails
            simple_caption = f"🔒 <b>Permission Required:</b> Please message @{bot_user['username']} first (/start) before downloading."
            if search_query:
                simple_caption += (
                    f"\n\n<b>Query:</b> <code>{html.escape(search_query)}</code>"
                )

            # Add a timestamp to ensure message is always different
            simple_caption += f"\n\n<i>Last attempt: {int(time.time())}</i>"

            try:
                await bot.edit_message_caption(
                    inline_message_id=inline_message_id,
                    caption=simple_caption,
                    reply_markup=InlineKeyboardMarkup(
                        inline_keyboard=[
                            [start_chat_button(bot_user["username"])],
                            [try_again_button(track_id)],
                        ]
                    ),
                )
            except TelegramBadRequest as bad_req:
                # Handle "message is not modified" error gracefully
                if "message is not modified" in str(bad_req).lower():
                    logger.info("Simple fallback message content unchanged")
                    # No need to update the message, just ignore this error
                    pass
                else:
                    # Some other bad request error, re-raise it
                    raise
        except Exception as final_err:
            logger.error(f"Final fallback caption update failed: {final_err}")
    finally:
        # Clean up any downloaded files
        if filepath:
            await cleanup_files(filepath)


@router.callback_query(F.data == "download_status")
async def download_status_callback(callback: CallbackQuery):
    """Handle status check for ongoing downloads"""
    await callback.answer(
        "Your track is still downloading. Please wait...",
        show_alert=False,
        cache_time=60 * 60 * 24,
    )


@router.message()
async def handle_selected_track(message: Message):
    """Handle when a user selects a track from inline results"""
    # We're no longer using the hidden command approach,
    # so this handler is just a placeholder for now.
    # All downloads are now handled through the callback_query handler
    pass


async def download_and_send(message: Message, track_id: str, spotify_url=None):
    chat_id = message.chat.id
    original_message_id = message.message_id

    # Get bot info for metadata
    bot_info = await bot.get_me()
    bot_user = {
        "username": bot_info.username,
        "id": bot_info.id,
        "name": bot_info.full_name,
    }

    try:
        # First, get track data to verify it's not a Go+ track
        track_data = await get_track(track_id)

        # Check if it's a Go+ (premium) track
        if track_data.get("policy") == "SNIP":
            await message.reply(
                "⚠️ <b>SoundCloud Go+ Song Detected</b>\n\n"
                "This is a SoundCloud premium track that can only be previewed (30 seconds).\n"
                "Full track downloads are not available for Go+ songs."
            )
            return

        # Download the track in the background
        logger.info(f"Downloading track ID: {track_id}")
        download_start = time.time()
        download_result = await download_track(track_id, bot_user)
        download_time = time.time() - download_start

        # Track info for either success or failure cases
        track_info = get_track_info(download_result.get("track_data", {}))

        # Add Spotify URL if provided
        if spotify_url:
            track_info["spotify_url"] = spotify_url

        if download_result["success"]:
            # Track downloaded successfully
            logger.info(
                f"Download successful in {download_time:.2f} seconds: {download_result.get('filepath')}"
            )
            filepath = download_result["filepath"]

            # Flag to track if we need to clean up files
            should_cleanup = not download_result.get("cached", False)

            # Use the worker function to send the audio
            success = await send_audio_file(
                bot=bot,
                chat_id=chat_id,
                filepath=filepath,
                track_info=track_info,
                reply_to_message_id=original_message_id,
            )

            if not success:
                # Handle failure
                await handle_download_failure(
                    bot=bot,
                    message_id=message.message_id,
                    track_info=track_info,
                    error_message="Failed to send audio file",
                )

            # Clean up files if needed (not cached)
            if should_cleanup:
                await cleanup_files(
                    filepath,
                )

        else:
            # Download failed
            error_message = download_result.get("message", "Unknown error")
            logger.error(f"Download failed: {error_message}")

            # Send error message
            await message.reply(
                format_error_caption(
                    "Download failed: " + error_message, track_info, bot_info.username
                ),
                reply_markup=InlineKeyboardMarkup(
                    inline_keyboard=[
                        [
                            soundcloud_button(track_info["permalink_url"]),
                        ]
                    ]
                ),
                disable_notification=True,
            )

    except Exception as e:
        logger.error(f"Error processing track download: {e}")

        # Send error message
        await message.reply(
            "❌ <b>Error:</b> Something went wrong while downloading the track.\n"
            "Please try again later.",
            disable_notification=True,
        )


@router.callback_query(F.data.startswith("details:"))
async def track_details(callback: CallbackQuery):
    """Handler for track details callback"""
    track_id = callback.data.split(":", 1)[1]

    try:
        # Check if message exists - this is crucial
        if callback.message is None:
            logger.warning(f"Callback message is None for track_id: {track_id}")
            await callback.answer(
                "Cannot display details in this message. Try selecting the track again.",
                show_alert=True,
            )
            return

        # Get track details from SoundCloud
        results = await search_soundcloud(f"tracks:{track_id}")
        tracks = filter_tracks(results)

        if not tracks:
            await callback.answer("Track information not found", show_alert=True)
            return

        track = tracks[0]
        track_info = get_track_info(track)

        # Format created date
        created_at = track.get("created_at", "")
        if created_at:
            try:
                dt = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
                created_date = dt.strftime("%B %d, %Y")
            except ValueError:
                created_date = "Unknown date"
        else:
            created_date = "Unknown date"

        # Build detailed information
        details = (
            f"♫ <b>{html.escape(track_info['display_title'])}</b>\n\n"
            f"👤 Artist: <b>{html.escape(track_info['artist'])}</b>\n"
            f"⏱ Duration: {track_info['duration']}\n"
            f"📅 Released: {created_date}\n"
            f"▶️ Plays: {track_info['plays']:,}\n"
            f"❤️ Likes: {track_info['likes']:,}\n"
        )

        if track_info["genre"]:
            details += f"🎭 Genre: {html.escape(track_info['genre'])}\n"

        if track_info["description"]:
            details += (
                f"\n📝 Description: {html.escape(track_info['description'][:200])}"
            )
            if len(track_info["description"]) > 200:
                details += "..."

        # Get bot username for attribution
        bot_info = await bot.get_me()

        # Use zero-width space to prevent URL embedding
        permalink_url_no_embed = track_info["permalink_url"].replace("://", "://\u200c")
        details += (
            f"\n\n<a href='{permalink_url_no_embed}'>Listen on SoundCloud</a>\n"
            f"Download via @{bot_info.username}"
        )

        # Answer with track details
        try:
            await callback.message.edit_text(
                details,
                reply_markup=InlineKeyboardMarkup(
                    inline_keyboard=[
                        [
                            soundcloud_button(track_info["permalink_url"]),
                            artist_button(
                                track_info["user"]["url"]
                                + f"?urn={track_info['user']['urn']}"
                            ),
                        ],
                        [
                            InlineKeyboardButton(
                                text="⬇️ Download Track",
                                callback_data=f"download:{track_id}",
                            ),
                        ],
                    ]
                ),
            )
            await callback.answer()
        except Exception as edit_error:
            logger.error(f"Error editing message: {edit_error}")
            await callback.answer(
                "Cannot update message. The message may be too old.", show_alert=True
            )

    except Exception as e:
        logger.error(f"Error in track details: {e}")
        await callback.answer("Error fetching track details", show_alert=True)


@router.callback_query(F.data.startswith("download:"))
async def download_callback(callback: CallbackQuery):
    """Handle download button click"""
    track_id = callback.data.split(":", 1)[1]
    search_query = None  # Initialize search_query to avoid undefined variable errors

    try:
        logger.info(f"Download callback initiated for track ID: {track_id}")
        # Answer the callback query to stop loading animation
        await callback.answer("Processing...", show_alert=False)

        # Check message availability - handle both regular and inline messages
        has_access = (
            callback.message is not None or callback.inline_message_id is not None
        )
        if not has_access:
            logger.warning(f"Cannot access message for track_id: {track_id}")
            await callback.answer(
                "Cannot process download: message not accessible.",
                show_alert=True,
            )
            return

        # For inline messages, update to processing status
        if callback.inline_message_id:
            # Store the user ID for this inline message ID if not already stored
            if (
                callback.inline_message_id not in inline_message_users
                and callback.from_user
            ):
                inline_message_users[callback.inline_message_id] = callback.from_user.id
                logger.info(
                    f"Stored user ID {callback.from_user.id} for inline message {callback.inline_message_id}"
                )

            # Check if the user has DMs open with the bot
            user_id = callback.from_user.id if callback.from_user else None

            if user_id:
                # Get track info to show in the message
                track_data = await get_track(track_id)
                if not track_data:
                    await callback.answer(
                        "Error: Could not get track info. Please try again.",
                        show_alert=True,
                    )
                    return

                track_info = get_track_info(track_data)

                # Get bot info for the permission message
                bot_info = await bot.get_me()
                bot_user = {
                    "username": bot_info.username,
                    "id": bot_info.id,
                    "name": bot_info.full_name,
                }

                # Check if the user has started the bot after the last try
                has_started_bot = False
                try:
                    # Try to send a service message to check if user has DM access
                    test_message = await bot.send_message(
                        user_id, "Checking access...", disable_notification=True
                    )
                    has_started_bot = True
                    # Delete the test message immediately
                    await bot.delete_message(user_id, test_message.message_id)
                except Exception as e:
                    # If we get an error, user hasn't started the bot yet
                    logger.info(f"User {user_id} has not started the bot yet: {e}")
                    has_started_bot = False

                if not has_started_bot:
                    # Show chat access needed message again but update timestamp to make it clear it's a new attempt
                    await fallback_to_direct_message(
                        callback.inline_message_id,
                        track_id,
                        track_info,
                        bot_user,
                        search_query,
                    )

                    # Show more detailed instructions in an alert
                    await callback.answer(
                        "Please open a chat with the bot first and send /start, then try again.",
                        show_alert=True,
                    )
                    return

                # User has DMs open, we need to update the inline message instead of just sending a DM
                logger.info(
                    f"User {user_id} has DMs open, updating inline message directly"
                )

                # Start a task to download and update the inline message
                # This will handle both the DM (to get file_id) and the inline message update
                update_task = asyncio.create_task(
                    download_and_update_inline_message(
                        callback.inline_message_id, track_id, search_query
                    )
                )
                download_tasks[callback.inline_message_id] = update_task

                # Show confirmation to the user that download has started
                await callback.answer(
                    "Download started! The track will appear shortly.",
                    show_alert=False,
                )

                # We've started the inline message update task, so return early
                # This prevents the function from proceeding to the regular DM flow below
                return
            # For regular messages, don't update text to keep original caption
            elif callback.message:
                # Skip updating text message to keep original caption
                logger.info(
                    "Skipping text update to keep original caption as per user request"
                )
                pass

            # Get chat ID for sending messages
            chat_id = None
            original_message_id = None
            if callback.message:
                chat_id = callback.message.chat.id
                original_message_id = callback.message.message_id
            # For inline messages, we need to get the chat ID from the from_user
            elif callback.from_user:
                chat_id = callback.from_user.id
                original_message_id = None

            if not chat_id:
                logger.warning(f"Cannot determine chat ID for track_id: {track_id}")
                await callback.answer(
                    "Cannot determine where to send the audio. Please try in a private chat.",
                    show_alert=True,
                )
                return

            # Get bot info for metadata
            bot_info = await bot.get_me()
            bot_user = {
                "username": bot_info.username,
                "id": bot_info.id,
                "name": bot_info.full_name,
            }
            # Download the actual track
            logger.info(f"Calling download_track function for track ID: {track_id}")
            download_start = time.time()
            download_result = await download_track(track_id, bot_user)
            download_time = time.time() - download_start

            # Track info for either success or failure cases
            track_info = get_track_info(download_result.get("track_data", {}))

            # Add Spotify URL if provided
            spotify_url = None
            if hasattr(download_result, "spotify_url"):
                spotify_url = download_result.spotify_url
                track_info["spotify_url"] = spotify_url

            if download_result["success"]:
                # Track downloaded successfully - now validate it
                filepath = download_result["filepath"]

                # Validate the downloaded track
                is_valid, error_message = await validate_downloaded_track(
                    filepath, track_info
                )

                if not is_valid:
                    logger.warning(
                        f"Downloaded track validation failed: {error_message}"
                    )

                    # Format the error message for inline message
                    validation_failure_text = format_error_caption(
                        "Invalid track: " + error_message, track_info, bot_info.username
                    )

                    # Include the search query if provided
                    if search_query:
                        validation_failure_text += f"\n\n<b>Query:</b> <code>{html.escape(search_query)}</code>"

                    # Try to update message if possible
                    try:
                        if callback.message:
                            await callback.message.edit_text(
                                validation_failure_text,
                                reply_markup=None,  # No buttons
                            )
                        elif callback.inline_message_id:
                            await bot.edit_message_text(
                                inline_message_id=callback.inline_message_id,
                                text=validation_failure_text,
                                reply_markup=None,  # No buttons
                            )
                    except Exception as e:
                        logger.error(
                            f"Error updating message with validation failure: {e}"
                        )

                    # Show an alert for validation failures
                    await callback.answer(
                        f"Invalid track: {error_message}", show_alert=True
                    )
                    return

                # Track is valid, proceed with normal processing
                logger.info(
                    f"Download successful in {download_time:.2f} seconds: {download_result.get('filepath')}"
                )

                # Get artwork URL for a hyperlink
                artwork_url = track_info.get("artwork_url") or ""
                if artwork_url:
                    # Convert to high resolution
                    artwork_url = get_high_quality_artwork_url(artwork_url)

                try:

                    await send_audio_file(
                        bot=bot,
                        chat_id=chat_id,
                        filepath=filepath,
                        track_info=track_info,
                        reply_to_message_id=original_message_id,
                    )
                except Exception as e:
                    logger.error(f"Error sending audio: {e}")

                    # Try to update messages with error information
                    error_text = format_error_caption(
                        f"Error: {str(e)[:200]}", track_info, bot_info.username
                    )

                    try:
                        if callback.message:
                            await callback.message.edit_text(
                                error_text,
                                reply_markup=None,  # No buttons
                            )
                        elif callback.inline_message_id:
                            await bot.edit_message_text(
                                inline_message_id=callback.inline_message_id,
                                text=error_text,
                                reply_markup=None,  # No buttons
                            )
                    except Exception as msg_update_err:
                        logger.error(
                            f"Error updating message with error: {msg_update_err}"
                        )
            else:
                # Download failed
                error_message = download_result.get("message", "Unknown error")
                logger.error(f"Download failed: {error_message}")

                # Format the error message
                failure_text = format_error_caption(
                    "Download failed: " + error_message, track_info, bot_info.username
                )

                # Include the search query if provided
                if search_query:
                    failure_text += (
                        f"\n\n<b>Query:</b> <code>{html.escape(search_query)}</code>"
                    )

                # Try to update message if possible
                try:
                    if callback.message:
                        await callback.message.edit_text(
                            failure_text,
                            reply_markup=None,  # No buttons
                        )
                    elif callback.inline_message_id:
                        await bot.edit_message_text(
                            inline_message_id=callback.inline_message_id,
                            text=failure_text,
                            reply_markup=None,  # No buttons
                        )
                except Exception as e:
                    logger.error(f"Error updating message with failure: {e}")

                # Always show an alert for download failures
                await callback.answer(
                    f"Download failed: {error_message}", show_alert=True
                )

    except Exception as e:
        logger.error(f"Error in download callback: {e}")
        await callback.answer(
            "Error processing download. Please try again.", show_alert=True
        )


@router.message(CommandStart(deep_link=True))
async def deep_link_start(message: Message):
    """Handler for /start commands with deep links for downloading tracks"""
    raw_args = message.text.split(maxsplit=1)
    if len(raw_args) > 1:
        args = raw_args[1]
        if args.startswith("download_"):
            track_id = args.replace("download_", "")
            logger.info(f"Deep link download request for track ID: {track_id}")

            # Send a loading message
            status_msg = await message.answer("⏳ <b>Preparing your audio...</b>")

            # Get bot info for metadata
            bot_info = await bot.get_me()
            bot_user = {
                "username": bot_info.username,
                "id": bot_info.id,
                "name": bot_info.full_name,
            }

            # Download or get from cache
            download_result = await download_track(track_id, bot_user)

            # Delete status message
            await status_msg.delete()

            if download_result["success"]:
                # Get track info
                track_info = get_track_info(download_result.get("track_data", {}))
                filepath = download_result["filepath"]

                # Validate the downloaded track
                is_valid, error_message = await validate_downloaded_track(
                    filepath, track_info
                )

                if not is_valid:
                    logger.warning(
                        f"Downloaded track validation failed: {error_message}"
                    )

                    # Format the validation error message
                    validation_failure_text = format_error_caption(
                        "Invalid track: " + error_message, track_info, bot_info.username
                    )

                    # Send validation error message
                    await message.answer(
                        validation_failure_text,
                        reply_markup=InlineKeyboardMarkup(
                            inline_keyboard=[
                                [
                                    soundcloud_button(track_info["permalink_url"]),
                                ]
                            ]
                        ),
                    )
                    return

                # Use the worker function to send the audio
                success = await send_audio_file(
                    bot=bot,
                    chat_id=message.chat.id,
                    filepath=filepath,
                    track_info=track_info,
                )

                if not success:
                    # Handle failure
                    await handle_download_failure(
                        bot=bot,
                        message_id=message.message_id,
                        track_info=track_info,
                        error_message="Failed to send audio file",
                    )

            else:
                # Handle download failure
                error_message = download_result.get("message", "Unknown error")
                track_info = get_track_info(download_result.get("track_data", {}))

                await message.answer(
                    format_error_caption(
                        "Download failed: " + error_message,
                        track_info,
                        bot_info.username,
                    ),
                    reply_markup=InlineKeyboardMarkup(
                        inline_keyboard=[
                            [
                                soundcloud_button(track_info["permalink_url"]),
                            ]
                        ]
                    ),
                    disable_notification=True,  # Send silently
                )
            return  # End processing here
        elif args == "open_dms":
            # This parameter is used to open DMs with the bot
            # Create a simple fake track info dict for the formatter
            success_message = format_success_caption(
                "Chat successfully opened",
                {
                    "permalink_url": f"https://t.me/{(await bot.get_me()).username}",
                    "title": "SoundCloud Search Bot",
                    "artist": "Telegram",
                },
                (await bot.get_me()).username,
            )

            await message.answer(
                f"{success_message}\n\n"
                "You can now download tracks directly in inline mode.\n"
                "Please go back to the chat where you selected the track and try again.",
                disable_notification=True,  # Send silently
            )
            return  # End processing here

    # If not a deep link download, show the regular start message
    await cmd_start(message)


@router.message(F.text.regexp(r".*" + SOUNDCLOUD_URL_PATTERN + r".*"))
async def handle_soundcloud_link(message: Message):
    """Handle SoundCloud link sent directly to the bot"""
    # Extract URL from the message
    soundcloud_url = extract_soundcloud_url(message.text)

    if not soundcloud_url:
        return

    logger.info(f"Handling direct SoundCloud link: {soundcloud_url}")

    # Send loading message
    loading_msg = await message.reply("⏳ <b>Processing link...</b>")

    # Process the URL
    track_id, track_data, error_message, playlist_data = await process_soundcloud_url(
        soundcloud_url
    )

    # Handle playlist case
    if playlist_data:
        playlist_title = playlist_data.get("title", "Unknown Playlist")
        user = playlist_data.get("user", {}).get("username", "Unknown Artist")
        track_count = playlist_data.get("track_count", 0)
        tracks = playlist_data.get("tracks", [])

        # Extract track IDs for batch fetching
        track_ids = []
        for track in tracks:
            if track and isinstance(track, dict) and "id" in track:
                track_ids.append(str(track["id"]))

        # If we have track IDs, fetch their complete information
        full_tracks = []
        if track_ids:
            logger.info(
                f"Fetching complete information for {len(track_ids)} tracks in playlist"
            )
            try:
                full_tracks = await get_tracks_batch(track_ids)
                logger.info(
                    f"Retrieved {len(full_tracks)} tracks with complete information"
                )

                # Sort full_tracks to match the original playlist order
                if full_tracks:
                    # Create a mapping of track IDs to track data
                    track_map = {str(track.get("id")): track for track in full_tracks}

                    # Reorder full_tracks to match the original playlist order
                    ordered_tracks = []
                    for track_id in track_ids:
                        if track_id in track_map:
                            ordered_tracks.append(track_map[track_id])

                    # Replace full_tracks with the ordered version
                    if ordered_tracks:
                        full_tracks = ordered_tracks
                        logger.info("Reordered tracks to match playlist sequence")

                # Use full_tracks if available, otherwise fall back to the original tracks
                playlist_tracks = full_tracks if full_tracks else tracks
            except Exception as e:
                logger.error(f"Error fetching track details for playlist: {e}")
                playlist_tracks = tracks  # Fall back to original track data
        else:
            playlist_tracks = tracks

        # Get artwork URL
        artwork_url = playlist_data.get("artwork_url", "")
        if artwork_url:
            artwork_url = get_high_quality_artwork_url(artwork_url)

        # Create a message with playlist info
        playlist_info = "🎵 <b>SoundCloud Playlist</b>\n\n"
        playlist_info += f"<b>Title:</b> {html.escape(playlist_title)}\n"
        playlist_info += f"<b>By:</b> {html.escape(user)}\n"
        playlist_info += f"<b>Tracks:</b> {track_count}\n\n"

        # Create inline keyboard with tracks
        keyboard = []

        # Create a button for each track in the playlist
        max_tracks_to_show = min(
            MAX_PLAYLIST_TRACKS_TO_SHOW, len(playlist_tracks)
        )  # Limit to MAX_PLAYLIST_TRACKS_TO_SHOW tracks for UI reasons

        for i, track in enumerate(playlist_tracks[:max_tracks_to_show]):
            # Some playlists may have null tracks or private tracks
            if not track or not isinstance(track, dict):
                continue

            track_title = track.get("title", "Unknown Track")
            track_id = track.get("id")

            # Truncate title if too long
            display_title = track_title
            if len(display_title) > 30:
                display_title = display_title[:27] + "..."

            # Add track to keyboard
            keyboard.append(
                [
                    InlineKeyboardButton(
                        text=f"{i + 1}. {display_title}",
                        callback_data=f"download:{track_id}",
                    )
                ]
            )

        # Add "View on SoundCloud" button
        permalink_url = playlist_data.get("permalink_url", soundcloud_url)
        keyboard.append([soundcloud_button(permalink_url)])

        # If there are more tracks than we're showing, add note
        if len(playlist_tracks) > max_tracks_to_show:
            playlist_info += f"<i>Showing {max_tracks_to_show} of {len(playlist_tracks)} tracks. View all tracks on SoundCloud.</i>\n\n"

        # Edit the message with playlist info
        await loading_msg.edit_text(
            playlist_info, reply_markup=InlineKeyboardMarkup(inline_keyboard=keyboard)
        )
        return

    # Handle error case
    if error_message:
        await loading_msg.edit_text(f"❌ <b>Error:</b> {error_message}")
        return

    # If not a playlist and no error, it's a track
    if track_id and track_data:
        # Use the existing function to handle tracks
        await loading_msg.delete()
        await download_and_send(message, track_id)


@router.message(F.text.regexp(r"(https?://)?open\.spotify\.com/track/[a-zA-Z0-9]+"))
async def handle_spotify_link(message: Message):
    """Handler for Spotify track links"""
    try:
        link = message.text.strip()
        logger.info(f"Received Spotify link: {link}")

        # Send status message
        status_msg = await message.reply("🔎 <b>Processing Spotify link...</b>")

        # Extract metadata from Spotify URL
        metadata = await extract_metadata_from_spotify_url(link)

        if not metadata:
            await status_msg.edit_text(
                "❌ <b>Invalid Spotify link or unable to extract track info.</b>\n\n"
                "Please try with a direct SoundCloud link or a search query instead.",
                disable_notification=True,  # Send silently
            )
            return

        # Create a SoundCloud search query from the Spotify metadata
        soundcloud_query = create_soundcloud_search_query(
            metadata["title"], metadata["artist"]
        )

        # Log the conversion
        logger.info(
            f"Converted Spotify track '{metadata['title']}' by '{metadata['artist']}' to SoundCloud query: '{soundcloud_query}'"
        )

        # Show what we're searching for
        await status_msg.edit_text(
            f"🔍 <b>Searching SoundCloud for:</b>\n"
            f"<b>Title:</b> {html.escape(metadata['title'])}\n"
            f"<b>Artist:</b> {html.escape(metadata['artist'])}",
            disable_notification=True,  # Send silently
        )

        # Perform search
        results = await search_soundcloud(soundcloud_query)
        tracks = filter_tracks(results)

        if not tracks:
            await status_msg.edit_text(
                f"❌ <b>No tracks found on SoundCloud.</b>\n\n"
                f"<b>Title:</b> {html.escape(metadata['title'])}\n"
                f"<b>Artist:</b> {html.escape(metadata['artist'])}\n\n"
                f"Try searching with different keywords or a direct SoundCloud link.",
                disable_notification=True,  # Send silently
            )
            return

        # Get the first (most relevant) track
        track_data = tracks[0]

        # Check if it's a Go+ (premium) track
        if track_data.get("policy") == "SNIP":
            await status_msg.edit_text(
                f"⚠️ <b>SoundCloud Go+ Song Detected</b>\n\n"
                f"<b>Title:</b> {html.escape(track_data.get('title', 'Unknown'))}\n"
                f"<b>Artist:</b> {html.escape(track_data.get('user', {}).get('username', 'Unknown'))}\n\n"
                f"This is a SoundCloud premium track that can only be previewed (30 seconds).\n"
                f"Full track downloads are not available for Go+ songs.",
                disable_notification=True,  # Send silently
            )
            return

        # Get track info and add the Spotify URL
        track_info = get_track_info(track_data)

        # Add Spotify URL to track_info
        track_info["spotify_url"] = metadata["spotify_url"]

        track_id = track_info["id"]

        # Show that we found a match
        await status_msg.edit_text(
            f"✅ <b>Found on SoundCloud:</b>\n"
            f"<b>{html.escape(track_info['display_title'])}</b> by <b>{html.escape(track_info['artist'])}</b>\n\n"
            f"⏳ Starting download...",
            disable_notification=True,  # Send silently
        )

        # Delete the status message
        await status_msg.delete()

        await download_and_send(
            message, track_id, spotify_url=metadata.get("spotify_url")
        )

    except Exception as e:
        logger.error(f"Error handling Spotify link: {e}", exc_info=True)
        await message.reply(
            "❌ <b>Error processing this Spotify link.</b>\n\nPlease try with a direct SoundCloud link or a search query instead.",
            disable_notification=True,  # Send silently
        )


async def process_download_queue():
    """Process download queue items"""
    while True:
        try:
            # Get an item from the queue
            item = await download_queue.get()

            # Extract data from the item
            track_id = item["track_id"]
            inline_message_id = item["inline_message_id"]
            search_query = item.get("search_query")

            # Start the download task
            task = asyncio.create_task(
                download_and_update_inline_message(
                    inline_message_id, track_id, search_query
                )
            )
            download_tasks[inline_message_id] = task

            # Mark the task as done
            download_queue.task_done()
        except Exception as e:
            logger.error(f"Error in download queue worker: {e}", exc_info=True)
            # Sleep briefly to avoid CPU spinning in case of continuous errors
            await asyncio.sleep(1)
