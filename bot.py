import os
import html
import time
import asyncio
import tempfile
from typing import Any, Dict

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
    format_error_caption,
    extract_soundcloud_url,
    process_soundcloud_url,
    format_track_info_caption,
    get_low_quality_artwork_url,
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
    add_id3_tags,
    cleanup_files,
    filter_tracks,
    download_track,
    get_track_info,
    get_download_url,
    get_tracks_batch,
    search_soundcloud,
    analyze_waveform_for_silence,
    download_track_and_thumbnail,
    get_high_quality_artwork_url,
)
from predefined import (
    artist_button,
    try_again_button,
    soundcloud_button,
    start_chat_button,
    download_status_button,
    download_progress_button,
    example_inline_search_button,
)
from utils.logger import get_logger
from helpers.spotify import (
    create_soundcloud_search_query,
    extract_metadata_from_spotify_url,
)
from helpers.workers import (
    send_audio_file,
    handle_system_error,
    is_permission_error,
    edit_message_with_audio,
    handle_download_failure,
    validate_downloaded_track,
    forward_to_channel_if_enabled,
    update_inline_message_with_audio,
)

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

# Store search queries for track IDs
track_search_queries: Dict[str, str] = {}

# Store inline message to DM message mapping
inline_message_to_dm_message: Dict[str, Dict[str, int]] = {}

# File ID cache
file_id_cache: Dict[str, str] = {}


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
        f"• Silence detection and removal from audio\n"
        f'• "Skip to" timestamps removal from titles\n'
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
        f" @{query.from_user.username if query.from_user.username else ''} (id:{query.from_user.id})"
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
                artwork_url = get_low_quality_artwork_url(artwork_url)

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

                    # Ensure display_title is never empty
                    display_title = track_info.get("display_title", "")
                    if not display_title or display_title.strip() == "":
                        display_title = "Untitled Track"
                        logger.warning(
                            f"Empty display_title for track ID {track_id}, setting to 'Untitled Track'"
                        )
                        # Update the track_info to ensure future references have the title too
                        track_info["display_title"] = display_title

                    # Create result for this track
                    track_result = InlineQueryResultArticle(
                        id=f"{track_id}_{i}",
                        title=f"{i + 1}. {display_title}",
                        description=f"By {track_info['artist']} • {duration_str}",
                        input_message_content=InputTextMessageContent(
                            message_text=format_track_info_caption(
                                track_info, bot_info.username
                            ),
                            parse_mode=ParseMode.HTML,
                            disable_web_page_preview=True,
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

                    # Ensure display_title is never empty
                    display_title = track_info.get("display_title", "")
                    if not display_title or display_title.strip() == "":
                        display_title = "Untitled Track"
                        logger.warning(
                            f"Empty display_title for track ID {track_id}, setting to 'Untitled Track'"
                        )
                        # Update the track_info to ensure future references have the title too
                        track_info["display_title"] = display_title

                    # Create result for this track
                    track_result = InlineQueryResultArticle(
                        id=f"{track_id}_{i}",
                        title=f"{i + 1}. {display_title}",
                        description=f"By {track_info['artist']} • {duration_str}",
                        input_message_content=InputTextMessageContent(
                            message_text=format_track_info_caption(
                                track_info, bot_info.username
                            ),
                            parse_mode=ParseMode.HTML,
                            disable_web_page_preview=True,
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
                url = playlist_data.get("permalink_url", "")
                error_result = InlineQueryResultArticle(
                    id=f"playlist_error_{int(time.time())}",
                    title=f"Playlist: {playlist_title}",
                    description="No available tracks in this playlist",
                    input_message_content=InputTextMessageContent(
                        message_text=f"🎵 <a href='{url}'><b>SoundCloud Playlist:</b></a> {html.escape(playlist_title)}\n"
                        f"<b>By</b> {html.escape(user)}\n"
                        f"<b>Tracks:</b> {track_count}\n\n"
                        f"<i>This playlist either has no tracks or all tracks are private/unavailable.</i>",
                        disable_web_page_preview=True,
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
                            message_text=f"❌ <b>Error:</b> {error_message}\n\n<i>URL: {soundcloud_url}</i>",
                            disable_web_page_preview=True,
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

            # Ensure track title is never empty
            track_title = track_info.get("title", "")
            if not track_title or track_title.strip() == "":
                track_title = "Untitled Track"
                logger.warning(
                    f"Empty title for track ID {track_id}, setting to 'Untitled Track'"
                )
                track_info["title"] = track_title

            # Get other track info
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
                    disable_web_page_preview=True,
                ),
                reply_markup=keyboard,
                thumbnail_url=artwork_url or SOUNDCLOUD_LOGO_URL,
            )

            await query.answer(results=[result], cache_time=300)
            return

    # Check if the query is a Spotify URL
    if search_text.startswith(("https://open.spotify.com/track/", "spotify:track:")):
        search_text = search_text.split("?")[0]
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
                    message_text=f"❌ <b>Could not process this <a href='{search_text}'>Spotify link</a>.</b>\n\nPlease search with the song title and artist instead.",
                    disable_web_page_preview=True,
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
                artwork_url = get_low_quality_artwork_url(artwork_url)
            else:
                # Default artwork if none available
                artwork_url = SOUNDCLOUD_LOGO_URL

            # Create keyboard with download button
            keyboard = InlineKeyboardMarkup(inline_keyboard=[[download_status_button]])

            # Format duration for display
            duration_str = track_info.get("duration", "")

            # Prepare detailed description
            description = f"By {track_info['artist']}"
            if duration_str:
                description += f" • {duration_str}"
            if track_info.get("genre"):
                description += f" • {track_info['genre']}"

            # Ensure we have a valid title for the article result
            display_title = track_info.get("display_title", "")
            if not display_title or display_title.strip() == "":
                display_title = "Untitled Track"
                logger.warning(
                    f"Empty display_title for track ID {track_info['id']}, setting to 'Untitled Track'"
                )
                # Update the track_info to ensure future references have the title too
                track_info["display_title"] = display_title

            # Create article result
            article_result = InlineQueryResultArticle(
                id=result_id,
                title=display_title,
                description=description,
                thumbnail_url=artwork_url,
                input_message_content=InputTextMessageContent(
                    message_text=format_track_info_caption(
                        track_info, bot_info.username
                    ),
                    parse_mode=ParseMode.HTML,
                    disable_web_page_preview=True,
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
        logger.error(f"Error in debounced search: {e}", exc_info=True)
        # Try to answer with an error message
        try:
            # Create a more specific error message based on error type
            error_message = "Error occurred, try again"
            error_details = str(e)

            if "ARTICLE_TITLE_EMPTY" in error_details:
                logger.error("Empty article title detected in search results")
                error_message = "Search terms produced invalid results"
            elif "Bad Request" in error_details:
                error_message = "Invalid search request"

            await query.answer(
                results=[],
                cache_time=5,
                is_personal=True,
                switch_pm_text=error_message,
                switch_pm_parameter="error",
            )
        except Exception as answer_err:
            logger.error(
                f"Failed to answer inline query with error: {answer_err}", exc_info=True
            )
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

    # Store the search query for this track ID
    if chosen_result.query:
        track_search_queries[track_id] = chosen_result.query
        logger.info(
            f"Stored search query for track ID {track_id}: {chosen_result.query}"
        )

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
    # Get bot info for metadata
    bot_info = await bot.get_me()
    bot_user = {
        "username": bot_info.username,
        "id": bot_info.id,
        "name": bot_info.full_name,
    }

    # Get user_id from the map if available
    user_id = inline_message_users.get(inline_message_id)

    # Prepare user info for channel forwarding
    user_info = None
    if user_id:
        try:
            # Get user information
            user = await bot.get_chat(user_id)
            if user:
                user_info = {
                    "id": user.id,
                    "username": getattr(user, "username", None),
                    "first_name": getattr(user, "first_name", ""),
                    "last_name": getattr(user, "last_name", ""),
                }
        except Exception as e:
            logger.warning(f"Could not get user information for user ID {user_id}: {e}")

    try:
        # STEP 1: Get track data first (lightweight operation) to have track info for any error messages
        logger.info(f"Fetching track data for ID: {track_id}")
        track_data = await get_track(track_id)
        if not track_data:
            logger.error(f"Failed to get track data for ID: {track_id}")
            await bot.edit_message_reply_markup(
                inline_message_id=inline_message_id,
                reply_markup=InlineKeyboardMarkup(
                    inline_keyboard=[
                        [
                            InlineKeyboardButton(
                                text="❌ Error: Track not found",
                                callback_data="error_info",
                            )
                        ]
                    ]
                ),
            )
            return

        # Format the track info for display
        track_info = get_track_info(track_data)

        # Check if the search query was a Spotify URL and add it to track_info
        if search_query and search_query in inline_spotify_urls:
            track_info["spotify_url"] = inline_spotify_urls[search_query]

        # CHECK CACHE: Check if we already have a cached file_id for this track
        cached_file_id = file_id_cache.get(track_id)
        if cached_file_id:
            logger.info(
                f"Found cached file_id for track ID {track_id}, skipping download"
            )

            # Update inline message with the cached audio file_id
            success = await update_inline_message_with_audio(
                bot=bot,
                inline_message_id=inline_message_id,
                file_id=cached_file_id,
                track_info=track_info,
                user_info=user_info,
            )

            if success:
                logger.info(f"Successfully used cached file_id for track ID {track_id}")
                return
            else:
                logger.warning(
                    f"Failed to use cached file_id for track ID {track_id}, will download file"
                )

        # STEP 2: Verify we can send messages to the user BEFORE downloading
        # This should be done before any resource-intensive operations
        if user_id:
            logger.info(f"Testing if we can send a message to user {user_id}")
            try:
                # Format a complete track info message with all the information
                track_caption = format_track_info_caption(
                    track_info, bot_user["username"]
                )

                # Send the actual track info message that we'll update with audio later
                test_message = await bot.send_message(
                    chat_id=user_id,
                    text=track_caption,
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
                                    text="⏳ Downloading...",
                                    callback_data="download_status",
                                ),
                            ],
                        ]
                    ),
                    disable_notification=True,
                    disable_web_page_preview=True,
                )

                # Save the message ID so we can update it later with the audio
                inline_message_to_dm_message[inline_message_id] = {
                    "user_id": user_id,
                    "message_id": test_message.message_id,
                }

            except Exception as e:
                # If we can't send a message to the user, mark as not able to send
                logger.error(f"Cannot send direct message to user {user_id}: {e}")

                # Update the inline message with a permission required button
                await fallback_to_direct_message(
                    inline_message_id, track_id, track_info, bot_user, search_query
                )
                return

        # STEP 3: Now begin the actual download process
        logger.info(f"Starting download process for track ID: {track_id}")
        download_start = time.time()

        # Analyze waveform for silence (lightweight operation)
        waveform_url = track_data.get("waveform_url")
        silence_analysis = await analyze_waveform_for_silence(waveform_url)

        # Add silence analysis to track data
        track_data["silence_analysis"] = silence_analysis

        # Update track_info with silence analysis
        track_info["silence_analysis"] = silence_analysis

        if silence_analysis["has_silence"]:
            logger.info(
                f"Silence detected in track: {silence_analysis['silence_percentage']:.1f}% is silent"
            )

            # Update the inline message to indicate silence detection
            try:
                await bot.edit_message_reply_markup(
                    inline_message_id=inline_message_id,
                    reply_markup=InlineKeyboardMarkup(
                        inline_keyboard=[[download_progress_button("checking_silence")]]
                    ),
                )
            except Exception as e:
                logger.warning(f"Error updating silence check status: {e}")

        # Start the actual download
        download_result = {
            "success": False,
            "track_data": track_data,
            "message": "Starting download...",
        }

        # Prepare temp file and perform the actual download
        logger.info(f"Starting file download for track ID: {track_id}")

        # Create a temporary file with .mp3 extension
        temp_file = tempfile.NamedTemporaryFile(suffix=".mp3", delete=False)
        filepath = temp_file.name
        temp_file.close()

        logger.info(f"Created temporary file: {filepath}")

        # Get download URL
        download_url = await get_download_url(track_data)
        if not download_url:
            logger.error(f"Could not find download URL for track ID: {track_id}")
            await handle_download_failure(
                bot=bot,
                message_id=inline_message_id,
                track_info=track_info,
                error_message="Could not find download URL for this track",
                search_query=search_query,
                track_search_queries_dict=track_search_queries,
            )
            return

        # Download track and thumbnail concurrently
        download_success, thumbnail = await download_track_and_thumbnail(
            download_url, filepath, track_data
        )
        if not download_success:
            logger.error(f"Failed to download audio file for track ID: {track_id}")
            # Clean up the temp file
            try:
                os.remove(filepath)
            except Exception as e:
                logger.error(f"Error removing temp file after failed download: {e}")

            await handle_download_failure(
                bot=bot,
                message_id=inline_message_id,
                track_info=track_info,
                error_message="Failed to download audio file",
                search_query=search_query,
                track_search_queries_dict=track_search_queries,
            )
            return

        # Add metadata
        await add_id3_tags(filepath, track_data)

        # Get the high quality artwork URL for the response data
        artwork_url = track_data.get("artwork_url", "")
        if artwork_url:
            artwork_url = get_high_quality_artwork_url(artwork_url)

        # Update download_result
        download_result = {
            "success": True,
            "filepath": filepath,
            "message": "Track downloaded successfully",
            "track_data": track_data,
            "artwork_url": artwork_url,
            "cached": False,
            "silence_analysis": silence_analysis,
        }

        # Calculate download time
        download_time = time.time() - download_start
        logger.info(f"Download completed in {download_time:.2f} seconds")

        # STEP 4: Validate the downloaded track
        is_valid, error_message = await validate_downloaded_track(filepath, track_info)

        if not is_valid:
            logger.warning(f"Downloaded track validation failed: {error_message}")
            await handle_download_failure(
                bot=bot,
                message_id=inline_message_id,
                track_info=track_info,
                error_message=error_message,
                search_query=search_query,
                track_search_queries_dict=track_search_queries,
            )

            # Clean up file
            await cleanup_files(filepath)
            return

        # STEP 5: Track is valid, proceed with messaging
        logger.info(
            f"Download successful in {download_time:.2f} seconds: {download_result.get('filepath')}"
        )

        # Since we've already verified user permissions earlier, just send the audio
        if user_id:
            logger.info(f"Sending audio to user {user_id} to get file_id")

            try:
                # Send audio to get file_id
                if inline_message_id in inline_message_to_dm_message:
                    # If we have a direct message already sent for this inline message,
                    # edit that message with audio instead of sending a new one
                    msg_details = inline_message_to_dm_message[inline_message_id]
                    success, error_type, result = await edit_message_with_audio(
                        bot=bot,
                        chat_id=msg_details["user_id"],
                        message_id=msg_details["message_id"],
                        filepath=filepath,
                        track_info=track_info,
                        inline_message_id=inline_message_id,
                        user_info=user_info,
                        thumbnail=thumbnail,  # Pass the pre-downloaded thumbnail
                    )
                else:
                    # Use the regular send_audio_file if we don't have a message to edit
                    success, error_type, result = await send_audio_file(
                        bot=bot,
                        chat_id=user_id,
                        filepath=filepath,
                        track_info=track_info,
                        reply_to_message_id=None,
                        inline_message_id=inline_message_id,
                        user_info=user_info,
                        thumbnail=thumbnail,  # Pass the pre-downloaded thumbnail
                    )

                # If we got a file_id, update the inline message
                if (
                    success
                    and result
                    and hasattr(result, "audio")
                    and result.audio
                    and result.audio.file_id
                ):
                    file_id = result.audio.file_id

                    # STEP 1: First update the inline message with the audio
                    inline_update_success = await update_inline_message_with_audio(
                        bot=bot,
                        inline_message_id=inline_message_id,
                        file_id=file_id,
                        track_info=track_info,
                        user_info=user_info,
                    )

                    if inline_update_success:
                        logger.info(
                            f"Updated inline message with audio file_id: {file_id}"
                        )

                        # STEP 2: Now forward the message to the channel if needed
                        # Import from channel module to check channel status
                        from utils.channel import channel_manager

                        if channel_manager.is_enabled and result:
                            logger.info(
                                f"Now forwarding message to channel after inline update"
                            )
                            await forward_to_channel_if_enabled(bot, result, user_info)
                    else:
                        logger.error(f"Failed to update inline message with audio")
                else:
                    logger.error(f"Failed to get audio file_id from result: {result}")

            except Exception as e:
                # Check very specifically for permission errors using our helper
                if is_permission_error(e):
                    logger.error(f"Permission error when sending DM to user: {e}")
                    # Show the permission required message - this is the correct use case
                    await fallback_to_direct_message(
                        inline_message_id,
                        track_id,
                        track_info,
                        bot_user,
                        search_query,
                    )
                else:
                    # For any other error, show a system error message
                    logger.error(f"System error when sending DM to user: {e}")
                    await handle_system_error(
                        bot=bot,
                        message_id=inline_message_id,
                        track_info=track_info,
                        error_message=f"System error: {str(e)[:100]}",
                        search_query=search_query,
                        filepath=filepath,
                        track_search_queries_dict=track_search_queries,
                    )

        # Clean up files
        await cleanup_files(filepath)

    except Exception as e:
        logger.error(f"Error in download process: {e}")
        # Update the inline message with an error message
        try:
            await bot.edit_message_text(
                inline_message_id=inline_message_id,
                text=f"❌ <b>Error:</b> {str(e)[:100]}",
                reply_markup=InlineKeyboardMarkup(
                    inline_keyboard=[
                        [
                            InlineKeyboardButton(
                                text="Try Again",
                                callback_data=f"download:{track_id}",
                            )
                        ]
                    ]
                ),
                disable_web_page_preview=True,
            )
        except Exception as edit_error:
            logger.error(f"Error updating inline message with error: {edit_error}")


async def fallback_to_direct_message(
    inline_message_id, track_id, track_info, bot_user, search_query=None
):
    """Fallback to direct message approach if inline update fails due to permission issues"""
    filepath = None
    try:
        # Log that this is specifically for permission issues
        logger.info(f"Showing permission required message for track ID {track_id}")

        # Store the search query for this track ID if available
        if search_query:
            track_search_queries[track_id] = search_query
            logger.info(
                f"Stored search query in fallback for track ID {track_id}: {search_query}"
            )

        # Instead of changing the entire message, just update the buttons
        # Create a button layout specific to permission errors
        markup = InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(
                        text="🔒 Permission Required", callback_data="permission_info"
                    )
                ],
                [start_chat_button(bot_user["username"])],
                [try_again_button(track_id)],
            ]
        )

        # Just update the reply markup without changing the message content
        try:
            await bot.edit_message_reply_markup(
                inline_message_id=inline_message_id,
                reply_markup=markup,
            )
            logger.info("Updated message with permission required buttons")
            return  # Success! No need for fallback
        except Exception as e:
            logger.error(f"Failed to update reply markup for permission: {e}")
            # If updating just the markup fails, try the full message update
            # Continue to the original implementation below

        # Get the artwork URL for a hyperlink
        artwork_url = track_info.get("artwork_url") or ""
        if artwork_url:
            # Convert to high resolution
            artwork_url = get_high_quality_artwork_url(artwork_url)

        # Prepare caption with minimalistic format and clearer instructions
        final_caption = "🔒 <b>Permission Required</b>\n\n"

        # Use the original URL without modification
        permalink_url = track_info["permalink_url"]
        final_caption += f"♫ <a href='{permalink_url}'>{html.escape(track_info['display_title'])} - {html.escape(track_info['artist'])}</a>\n\n"

        # Add Spotify URL if available
        if "spotify_url" in track_info:
            spotify_url = track_info["spotify_url"]
            final_caption += f"🎧 <b>Spotify:</b> <a href='{spotify_url}'>{html.escape(spotify_url)}</a>\n\n"

        # Include the search query if provided
        if search_query:
            final_caption += (
                f"<b>Query:</b> <code>{html.escape(search_query)}</code>\n\n"
            )

        # Add a timestamp to ensure message is always different when "Try Again" is clicked
        final_caption += "<b>Why am I seeing this?</b>\n"
        final_caption += "Your privacy settings don't allow the bot to message you directly. To download this track:\n\n"
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
            simple_caption = (
                f"🔒 <b>Permission Required:</b> Please message @{bot_user['username']} "
                f"first (/start) before downloading.\n\n"
                f"Your privacy settings don't allow the bot to message you directly."
            )

            # Add Spotify URL in simpler fallback too
            if "spotify_url" in track_info:
                spotify_url = track_info["spotify_url"]
                simple_caption += f"\n\n🎧 <b>Spotify:</b> <a href='{spotify_url}'>{html.escape(spotify_url)}</a>"

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

    # Get user info for attribution in channel forwarding
    user_info = None
    if message.from_user:
        user_info = {
            "id": message.from_user.id,
            "username": message.from_user.username,
            "first_name": message.from_user.first_name,
            "last_name": message.from_user.last_name,
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
            success, error_type, result = await send_audio_file(
                bot=bot,
                chat_id=chat_id,
                filepath=filepath,
                track_info=track_info,
                reply_to_message_id=original_message_id,
                user_info=user_info,
            )

            if not success:
                # Handle failure based on error type
                if error_type == "permission":
                    await message.reply(
                        "🔒 <b>Permission Error:</b> I can't send you messages directly. Please check your privacy settings.",
                        reply_markup=InlineKeyboardMarkup(
                            inline_keyboard=[
                                [soundcloud_button(track_info["permalink_url"])]
                            ]
                        ),
                    )
                else:
                    # Handle system error
                    await handle_download_failure(
                        bot=bot,
                        message_id=message.message_id,
                        track_info=track_info,
                        error_message="Failed to send audio file due to a system error",
                        track_search_queries_dict=track_search_queries,
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


@router.callback_query(F.data.startswith("download:"))
async def download_callback(callback: CallbackQuery):
    """Handle download button click"""
    track_id = callback.data.split(":", 1)[1]

    # Try to get the search query from the stored track search queries
    search_query = track_search_queries.get(track_id)
    if search_query:
        logger.info(
            f"Retrieved stored search query for track ID {track_id}: {search_query}"
        )
    else:
        search_query = (
            None  # Initialize search_query to avoid undefined variable errors
        )
        logger.info(f"No stored search query found for track ID: {track_id}")

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

        # For inline messages, update to processing status and delegate to specialized handler
        if callback.inline_message_id:
            # First, only update the buttons to show downloading status
            try:
                await bot.edit_message_reply_markup(
                    inline_message_id=callback.inline_message_id,
                    reply_markup=InlineKeyboardMarkup(
                        inline_keyboard=[[download_progress_button()]]
                    ),
                )
                logger.info("Updated buttons to show downloading status")
            except Exception as e:
                logger.warning(f"Error updating buttons to downloading status: {e}")

            # Store the user ID for this inline message ID if not already stored
            if (
                callback.inline_message_id not in inline_message_users
                and callback.from_user
            ):
                inline_message_users[callback.inline_message_id] = callback.from_user.id
                logger.info(
                    f"Stored user ID {callback.from_user.id} for inline message {callback.inline_message_id}"
                )

            # Get bot info for metadata first - we'll need this for any messaging
            bot_info = await bot.get_me()
            bot_user = {
                "username": bot_info.username,
                "id": bot_info.id,
                "name": bot_info.full_name,
            }

            # Get track info before fully downloading
            try:
                track_data = await get_track(track_id)
                track_info = get_track_info(track_data)
                # Add search query as Spotify URL if it looks like a Spotify URL
                if search_query and "spotify.com" in search_query:
                    track_info["spotify_url"] = search_query
            except Exception as e:
                logger.error(f"Failed to get track info for {track_id}: {e}")
                await callback.answer(
                    "Error retrieving track information. Please try again.",
                    show_alert=True,
                )
                return

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

        # For regular messages in direct chats
        # Get chat ID for sending messages
        chat_id = None
        if callback.message:
            chat_id = callback.message.chat.id
        # For inline messages, we need to get the chat ID from the from_user
        elif callback.from_user:
            chat_id = callback.from_user.id

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

        # STEP 1: Get track data first (lightweight operation) before downloading
        logger.info(f"Fetching track data for ID: {track_id}")
        track_data = await get_track(track_id)
        if not track_data:
            logger.error(f"Failed to get track data for ID: {track_id}")
            await callback.answer(
                "Error: Could not find track data. Please try a different track.",
                show_alert=True,
            )
            return

        # Format the track info for display
        track_info = get_track_info(track_data)

        # Add Spotify URL if provided
        if search_query and "spotify.com" in search_query:
            spotify_url = search_query
            track_info["spotify_url"] = spotify_url

        # STEP 2: Verify we can send messages to the user BEFORE downloading
        if callback.message:
            # For regular message callbacks we're already in a chat so no need to check permissions
            logger.info("Permission check not needed for regular message callbacks")
        else:
            # For inline message callbacks we need to verify permissions
            logger.info(f"Testing if we can send a message to user {chat_id}")
            try:
                # Send a temporary test message to verify permissions
                test_message = await bot.send_message(
                    chat_id=chat_id,
                    text="🔄 Preparing your download...",
                    disable_notification=True,
                )

                # If we get here, we have permission - delete the message
                await bot.delete_message(
                    chat_id=chat_id, message_id=test_message.message_id
                )
                logger.info("Permission check passed, user can receive messages")

            except Exception as e:
                # Permission check failed
                logger.error(f"Cannot send direct message to user {chat_id}: {e}")

                if callback.inline_message_id:
                    # Update the inline message with a permission required button
                    await fallback_to_direct_message(
                        callback.inline_message_id,
                        track_id,
                        track_info,
                        bot_user,
                        search_query,
                    )
                else:
                    # Just show an alert for regular messages
                    await callback.answer(
                        "I can't send you messages directly. Please check your privacy settings.",
                        show_alert=True,
                    )
                return

        # STEP 3: Now actually download the track
        logger.info(f"Starting download for track ID: {track_id}")

        # Analyze waveform for silence (lightweight operation)
        waveform_url = track_data.get("waveform_url")
        silence_analysis = await analyze_waveform_for_silence(waveform_url)

        # Add silence analysis to track data
        track_data["silence_analysis"] = silence_analysis
        track_info["silence_analysis"] = silence_analysis

        # Start the actual download
        download_result = await download_track(track_id, bot_user)

        # Track info for either success or failure cases
        track_info = get_track_info(download_result.get("track_data", {}))

        # Ensure Spotify URL is preserved
        if search_query and "spotify.com" in search_query:
            track_info["spotify_url"] = search_query

        if download_result["success"]:
            # Track downloaded successfully - now validate it
            filepath = download_result["filepath"]

            # Validate the downloaded track
            is_valid, error_message = await validate_downloaded_track(
                filepath, track_info
            )

            if not is_valid:
                logger.warning(f"Downloaded track validation failed: {error_message}")

                # Format the error message for inline message
                validation_failure_text = format_error_caption(
                    "Invalid track: " + error_message, track_info, bot_info.username
                )

                # Try to update message if possible
                try:
                    if callback.message:
                        # For regular messages, we might still need to update the text
                        await callback.message.edit_text(
                            validation_failure_text,
                            reply_markup=None,  # No buttons
                        )
                    elif callback.inline_message_id:
                        # For inline messages, use the helper to update only the buttons
                        await update_buttons_with_error_status(
                            bot=bot,
                            inline_message_id=callback.inline_message_id,
                            track_id=track_id,
                            track_info=track_info,
                            error_type="validation",
                            error_message=error_message,
                        )
                except Exception as e:
                    logger.error(f"Error updating message with validation failure: {e}")

                # Cleanup if needed
                if filepath:
                    await cleanup_files(filepath)

                return

            # If we got here, track is valid - send it to the user
            success, error_type, result = await send_audio_file(
                bot=bot,
                chat_id=chat_id,
                filepath=filepath,
                track_info=track_info,
                reply_to_message_id=(
                    callback.message.message_id if callback.message else None
                ),
            )

            # Cleanup the file
            if filepath:
                await cleanup_files(filepath)

            if not success:
                # Handle failure based on error type
                error_msg = "An error occurred while sending the audio."
                if error_type == "permission":
                    error_msg = "I can't send you messages directly. Please check your privacy settings."
                elif error_type == "system":
                    error_msg = "A system error occurred. Please try again later."

                await callback.answer(error_msg, show_alert=True)
                return

            # Success!
            await callback.answer("Track sent successfully!", show_alert=False)
            return

        else:
            # Download failed
            error_message = download_result.get("message", "Unknown error")
            logger.error(f"Download failed: {error_message}")

            # Format the error message
            failure_text = format_error_caption(
                "Download failed: " + error_message, track_info, bot_info.username
            )

            # Try to update message if possible
            try:
                if callback.message:
                    await callback.message.edit_text(
                        failure_text,
                        reply_markup=None,  # No buttons
                    )
                elif callback.inline_message_id:
                    # For inline messages, use the helper to update only the buttons
                    await update_buttons_with_error_status(
                        bot=bot,
                        inline_message_id=callback.inline_message_id,
                        track_id=track_id,
                        track_info=track_info,
                        error_type="download",
                        error_message=error_message,
                    )
            except Exception as e:
                logger.error(f"Error updating message with failure: {e}")

            # Always show an alert for download failures
            await callback.answer(f"Download failed: {error_message}", show_alert=True)

    except Exception as e:
        logger.error(f"Error in download callback: {e}")
        await callback.answer(
            "Error processing download. Please try again.", show_alert=True
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


@router.callback_query(F.data == "error_info")
async def error_info_callback(callback: CallbackQuery):
    """Handle error info button click"""
    await callback.answer(
        "There was a technical error processing your request. This is an internal system error, not a permissions issue. Please try again later.",
        show_alert=True,
        cache_time=5,
    )


@router.callback_query(F.data == "permission_info")
async def permission_info_callback(callback: CallbackQuery):
    """Handle permission info button click"""
    await callback.answer(
        "You need to message the bot directly first before it can send you files. Click the button below to open a chat with the bot.",
        show_alert=True,
    )


@router.callback_query(F.data == "too_many_errors")
async def too_many_errors_callback(callback: CallbackQuery):
    """Handle too many errors button click"""
    await callback.answer(
        "There have been multiple errors trying to process this track. Please try a different track or try again later.",
        show_alert=True,
    )


# Track consecutive error attempts
consecutive_errors = {}


async def update_buttons_with_error_status(
    bot: Bot,
    inline_message_id: str,
    track_id: str,
    track_info: Dict[str, Any],
    error_type: str,
    error_message: str,
):
    """Update only the buttons when an error occurs, tracking consecutive errors.

    Args:
        bot: Bot instance
        inline_message_id: Inline message ID
        track_id: Track ID
        track_info: Track metadata
        error_type: Type of error (e.g., 'download', 'permission', 'system')
        error_message: Error message to display
    """
    # Track error for this message
    if inline_message_id not in consecutive_errors:
        consecutive_errors[inline_message_id] = 1
    else:
        consecutive_errors[inline_message_id] += 1

    error_count = consecutive_errors[inline_message_id]
    logger.info(f"Error count for message {inline_message_id}: {error_count}")

    # If too many consecutive errors, show a different button
    if error_count >= 3:
        try:
            await bot.edit_message_reply_markup(
                inline_message_id=inline_message_id,
                reply_markup=InlineKeyboardMarkup(
                    inline_keyboard=[
                        [soundcloud_button(track_info["permalink_url"])],
                        [
                            InlineKeyboardButton(
                                text="❌ Too Many Errors - Try Different Track",
                                callback_data="too_many_errors",
                            )
                        ],
                    ]
                ),
            )
            logger.info("Updated buttons to show too many errors status")
        except Exception as e:
            logger.error(f"Error updating buttons for too many errors: {e}")
    else:
        # Regular error handling with try again option
        try:
            error_prefix = "❌ "
            if error_type == "permission":
                error_prefix = "🔒 "

            await bot.edit_message_reply_markup(
                inline_message_id=inline_message_id,
                reply_markup=InlineKeyboardMarkup(
                    inline_keyboard=[
                        [soundcloud_button(track_info["permalink_url"])],
                        [try_again_button(track_id)],
                        [
                            InlineKeyboardButton(
                                text=f"{error_prefix}{error_type.title()} Error: "
                                + error_message[:25]
                                + "...",
                                callback_data=(
                                    "error_info"
                                    if error_type != "permission"
                                    else "permission_info"
                                ),
                            )
                        ],
                    ]
                ),
            )
            logger.info(f"Updated buttons to show {error_type} error status")
        except Exception as e:
            logger.error(f"Error updating buttons for {error_type} error: {e}")
