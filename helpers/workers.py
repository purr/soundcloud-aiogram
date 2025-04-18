import io
import os
import html
import time
import asyncio
import tempfile
from typing import Any, Dict, Tuple, Optional

import aiohttp
import mutagen
from PIL import Image
from pydub import AudioSegment
from aiogram import Bot
from aiogram.enums import ParseMode
from aiogram.types import (
    Message,
    FSInputFile,
    URLInputFile,
    InputMediaAudio,
    BufferedInputFile,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
)
from pydub.silence import detect_nonsilent

from utils import format_error_caption, format_track_info_caption
from predefined import (
    artist_button,
    try_again_button,
    soundcloud_button,
    download_progress_button,
)
from utils.logger import logger

from .cache import file_id_cache
from .soundcloud import (
    cleanup_files,
    download_audio,
    get_low_quality_artwork_url,
    get_high_quality_artwork_url,
)


async def validate_downloaded_track(
    filepath: str, track_info: Dict[str, Any]
) -> Tuple[bool, str]:
    """Validate that the downloaded track is valid and playable.

    Args:
        filepath: Path to the downloaded audio file
        track_info: Track metadata from SoundCloud API

    Returns:
        tuple: (is_valid, error_message)
    """

    # Define a synchronous function to check file exists and size
    def check_file_exists_and_size():
        # Check if file exists
        if not os.path.exists(filepath):
            return (False, "Downloaded file does not exist")

        # Check file size
        try:
            file_size = os.path.getsize(filepath)
            # Check if file is empty (0 bytes)
            if file_size == 0:
                logger.error(f"Downloaded file is empty (0 bytes): {filepath}")
                return (
                    False,
                    "Download failed - The audio file is empty. Please try again later.",
                )

            # Check if file is too small to be a valid audio file
            if file_size < 1024:  # Less than 1KB
                file_size_kb = file_size / 1024
                logger.error(f"Downloaded file is too small: {file_size_kb:.2f} KB")
                return (
                    False,
                    f"Downloaded file is too small ({file_size_kb:.2f} KB) and likely corrupted",
                )

            # Log file size for debugging
            file_size_mb = file_size / (1024 * 1024)
            logger.info(f"File size: {file_size_mb:.2f} MB")

            return (True, "")
        except Exception as e:
            logger.error(f"Error checking file size: {e}")
            return (False, f"Error validating file: {str(e)}")

    # Define a synchronous function to check audio validity with mutagen
    def check_audio_validity():
        try:
            audio = mutagen.File(filepath)

            # Check if mutagen could parse the file at all
            if audio is None:
                logger.error(
                    f"File is not recognized as a valid audio format: {filepath}"
                )
                return (False, "The file is not a valid audio format")

            # Check audio length
            if hasattr(audio, "info") and hasattr(audio.info, "length"):
                audio_length = audio.info.length  # Length in seconds
                logger.info(f"Audio length: {audio_length:.2f} seconds")

                # If audio length is 0 or extremely short, the file is likely corrupted
                if audio_length < 0.1:  # Less than 0.1 seconds
                    logger.error(
                        f"Audio file has zero or extremely short duration: {audio_length:.2f} seconds"
                    )
                    return (
                        False,
                        "The audio file has no playable content (zero duration)",
                    )

            return (True, "")
        except Exception as e:
            error_message = str(e).lower()
            logger.error(f"Error checking audio validity with mutagen: {e}")

            if (
                "can't sync to mpeg frame" in error_message
                or "invalid data" in error_message
            ):
                logger.error(f"Audio file is corrupted: {error_message}")
                return (False, "The audio file is corrupted and cannot be played")
            elif (
                "no tags" in error_message
                or "no appropriate stream found" in error_message
            ):
                logger.error(f"Invalid audio format: {error_message}")
                return (False, "Invalid audio format detected")

            return (False, f"Audio file validation failed: {str(e)[:100]}")

    # Run file existence and size check in a separate thread
    is_valid, error_message = await asyncio.to_thread(check_file_exists_and_size)
    if not is_valid:
        return is_valid, error_message

    # Run audio validity check in a separate thread
    is_valid, error_message = await asyncio.to_thread(check_audio_validity)
    if not is_valid:
        return is_valid, error_message

    # Check duration from track_info
    try:
        duration_ms = track_info.get("duration", 0)
        if isinstance(duration_ms, str):
            if ":" in duration_ms:
                try:
                    parts = duration_ms.split(":")
                    if len(parts) == 2:  # MM:SS format
                        minutes, seconds = parts
                        duration_sec = int(minutes) * 60 + int(seconds)
                        duration_ms = duration_sec * 1000
                    elif len(parts) == 3:  # HH:MM:SS format
                        hours, minutes, seconds = parts
                        duration_sec = (
                            int(hours) * 3600 + int(minutes) * 60 + int(seconds)
                        )
                        duration_ms = duration_sec * 1000
                    else:
                        return True, ""
                except ValueError:
                    return True, ""
            else:
                try:
                    duration_ms = int(duration_ms)
                except ValueError:
                    return True, ""

        if duration_ms <= 1000:  # Less than 1 second
            return False, "Track duration is too short (likely unplayable)"
    except Exception as e:
        logger.error(f"Error validating track duration: {e}")
        return True, ""

    return True, ""


async def handle_download_failure(
    bot: Bot,
    message_id: str,
    track_info: Dict[str, Any],
    error_message: str,
    search_query: Optional[str] = None,
    track_search_queries_dict: Optional[Dict[str, str]] = None,
) -> None:
    """Handle download failure scenarios with appropriate messaging.

    Args:
        bot: Bot instance
        message_id: Message ID to update
        track_info: Track metadata
        error_message: Error message to display
        search_query: Optional search query that led to this download
        track_search_queries_dict: Optional dictionary to store track ID to search query mapping
    """
    try:
        # Store the search query for this track ID if available
        if (
            search_query
            and "id" in track_info
            and track_search_queries_dict is not None
        ):
            track_id = track_info["id"]
            track_search_queries_dict[track_id] = search_query
            logger.info(
                f"Stored search query in handle_download_failure for track ID {track_id}: {search_query}"
            )

        # Instead of changing the entire message, just update the buttons
        # Create a button layout specific to download failures
        markup = InlineKeyboardMarkup(
            inline_keyboard=[
                [soundcloud_button(track_info["permalink_url"])],
                [try_again_button(track_info["id"])],
                [
                    InlineKeyboardButton(
                        text="❌ Download Failed: " + str(error_message)[:30] + "...",
                        callback_data="error_info",
                    )
                ],
            ]
        )

        # Just update the reply markup without changing the message content
        try:
            await bot.edit_message_reply_markup(
                inline_message_id=message_id,
                reply_markup=markup,
            )
            logger.info("Updated message with download failure buttons")
        except Exception as e:
            logger.error(f"Failed to update reply markup for download failure: {e}")
            # If updating just the markup fails, try the fallback approach
            await fallback_download_failure_message(
                bot, message_id, track_info, error_message, search_query
            )
    except Exception as e:
        logger.error(f"Error handling download failure: {e}")
        # Try fallback approach
        await fallback_download_failure_message(
            bot, message_id, track_info, error_message, search_query
        )


async def fallback_download_failure_message(
    bot: Bot,
    message_id: str,
    track_info: Dict[str, Any],
    error_message: str,
    search_query: Optional[str] = None,
) -> None:
    """Fallback method for when updating just the markup fails for download failures."""
    try:
        # Format the error message
        failure_text = format_error_caption(
            "Download failed: " + error_message,
            track_info,
            (await bot.get_me()).username,
        )

        # Add Spotify URL if available
        if "spotify_url" in track_info:
            spotify_url = track_info["spotify_url"]
            failure_text += f"\n\n🎧 <b>Spotify:</b> <a href='{spotify_url}'>{html.escape(spotify_url)}</a>"

        # Include the search query if provided
        if search_query:
            failure_text += (
                f"\n\n<b>Query:</b> <code>{html.escape(search_query)}</code>"
            )

        # Update message with failure
        await bot.edit_message_caption(
            inline_message_id=message_id,
            caption=failure_text,
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[
                    [soundcloud_button(track_info["permalink_url"])],
                    [try_again_button(track_info["id"])],
                ]
            ),
        )
    except Exception as e:
        logger.error(f"Error updating message with failure: {e}")


async def handle_system_error(
    bot: Bot,
    message_id: str,
    track_info: Dict[str, Any],
    error_message: str,
    search_query: Optional[str] = None,
    filepath: Optional[str] = None,
    track_search_queries_dict: Optional[Dict[str, str]] = None,
) -> None:
    """Handle system-level errors during download or processing.

    Args:
        bot: Bot instance
        message_id: Message ID to update
        track_info: Track metadata
        error_message: Error message to display
        search_query: Optional search query
        filepath: Optional filepath to clean up
        track_search_queries_dict: Optional dictionary to store track ID to search query mapping
    """
    try:
        # Store the search query for this track ID if available
        if (
            search_query
            and "id" in track_info
            and track_search_queries_dict is not None
        ):
            track_id = track_info["id"]
            track_search_queries_dict[track_id] = search_query
            logger.info(
                f"Stored search query in handle_system_error for track ID {track_id}: {search_query}"
            )

        # Instead of changing the entire message, just update the buttons
        # Create a button layout specific to system errors
        markup = InlineKeyboardMarkup(
            inline_keyboard=[
                [soundcloud_button(track_info["permalink_url"])],
                [try_again_button(track_info["id"])],
                [
                    InlineKeyboardButton(
                        text="❌ System Error: " + str(error_message)[:30] + "...",
                        callback_data="error_info",
                    )
                ],
            ]
        )

        # Just update the reply markup without changing the message content
        try:
            await bot.edit_message_reply_markup(
                inline_message_id=message_id,
                reply_markup=markup,
            )
            logger.info("Updated message with system error buttons")
        except Exception as e:
            logger.error(f"Failed to update reply markup: {e}")
            # If updating just the markup fails, try to update the whole message
            await fallback_system_error_message(
                bot, message_id, track_info, error_message, search_query
            )
    except Exception as e:
        logger.error(f"Error handling system error: {e}")
        # Try fallback approach
        await fallback_system_error_message(
            bot, message_id, track_info, error_message, search_query
        )
    finally:
        if filepath:
            await cleanup_files(filepath)


async def fallback_system_error_message(
    bot: Bot,
    message_id: str,
    track_info: Dict[str, Any],
    error_message: str,
    search_query: Optional[str] = None,
) -> None:
    """Fallback method for when updating just the markup fails."""
    try:
        final_caption = "❌ <b>System Error</b>\n\n"
        permalink_url = track_info["permalink_url"]
        final_caption += f"♫ <a href='{permalink_url}'><b>{html.escape(track_info['title'])}</b> - <b>{html.escape(track_info['artist'])}</b></a>\n\n"
        final_caption += (
            f"<b>Error:</b> There was a technical issue processing this track.\n"
        )
        final_caption += f"<i>{error_message}</i>\n\n"

        # Add Spotify URL if available
        if "spotify_url" in track_info:
            spotify_url = track_info["spotify_url"]
            final_caption += f"🎧 <b>Spotify:</b> <a href='{spotify_url}'>{html.escape(spotify_url)}</a>\n\n"

        if search_query:
            final_caption += (
                f"<b>Query:</b> <code>{html.escape(search_query)}</code>\n\n"
            )

        final_caption += "This appears to be a technical error with the bot or server, not a permissions issue. "
        final_caption += "You can try again later or download directly from SoundCloud."

        markup = InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    soundcloud_button(track_info["permalink_url"]),
                ],
                [try_again_button(track_info["id"])],
            ]
        )

        await bot.edit_message_caption(
            inline_message_id=message_id,
            caption=final_caption,
            reply_markup=markup,
        )
    except Exception as e:
        logger.error(f"Final system error fallback failed: {e}")
        try:
            simple_caption = "❌ <b>System Error:</b> An error occurred while processing your request. Please try again later."

            # Add Spotify URL in simpler fallback too
            if "spotify_url" in track_info:
                spotify_url = track_info["spotify_url"]
                simple_caption += f"\n\n🎧 <b>Spotify:</b> <a href='{spotify_url}'>{html.escape(spotify_url)}</a>"

            if search_query:
                simple_caption += (
                    f"\n\n<b>Query:</b> <code>{html.escape(search_query)}</code>"
                )

            await bot.edit_message_caption(
                inline_message_id=message_id,
                caption=simple_caption,
                reply_markup=InlineKeyboardMarkup(
                    inline_keyboard=[[try_again_button(track_info["id"])]]
                ),
            )
        except Exception as e:
            logger.error(f"Final system error fallback failed: {e}")


async def download_and_resize_image(
    url: str, size: tuple[int, int] = (320, 320)
) -> bytes:
    """Download image from URL and resize it to specified dimensions.
    Ensures the image meets Telegram's thumbnail requirements:
    - JPEG format
    - Less than 200 kB (with safety margin)
    - Max dimensions 320x320 (with safety margin)
    - Proper compression

    Args:
        url: Image URL to download
        size: Target size as (width, height), defaults to (320, 320)

    Returns:
        bytes: Resized image in bytes that meets Telegram's requirements
    """
    logger.info(f"Starting image download and processing from URL: {url}")

    # Download the image data asynchronously
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            if response.status != 200:
                logger.error(
                    f"Failed to download image. Status code: {response.status}"
                )
                return None
            image_data = await response.read()
            logger.info(f"Downloaded image size: {len(image_data) / 1024:.1f} kB")

    # Define a function to process the image with PIL in a separate thread
    def process_image_with_pil(image_bytes):
        try:
            # Open image
            img = Image.open(io.BytesIO(image_bytes))
            logger.info(f"Original image size: {img.size}, mode: {img.mode}")

            # Convert RGBA to RGB if necessary
            if img.mode == "RGBA":
                img = img.convert("RGB")
                logger.info("Converted RGBA image to RGB")

            # Calculate aspect ratio preserving dimensions
            width, height = img.size
            if width > 320 or height > 320:
                ratio = min(320 / width, 320 / height)
                new_size = (int(width * ratio), int(height * ratio))
                img = img.resize(new_size, Image.Resampling.LANCZOS)
                logger.info(f"Resized image to: {new_size}")

            # Save to bytes with progressive compression
            img_byte_arr = io.BytesIO()
            quality = 85
            while True:
                img_byte_arr.seek(0)
                img_byte_arr.truncate()
                img.save(
                    img_byte_arr,
                    format="JPEG",
                    quality=quality,
                    optimize=True,
                    progressive=True,
                )
                size_kb = img_byte_arr.tell() / 1024
                logger.info(
                    f"Compressed image size at quality {quality}: {size_kb:.1f} kB"
                )

                if size_kb <= 200 or quality <= 5:  # 200 kB limit for safety margin
                    logger.info(
                        f"Final image quality: {quality}, size: {size_kb:.1f} kB"
                    )
                    break
                quality -= 5

            return img_byte_arr.getvalue()
        except Exception as e:
            logger.error(f"Error processing image: {e}")
            return None

    # Process the image in a separate thread to avoid blocking the event loop
    return await asyncio.to_thread(process_image_with_pil, image_data)


async def get_resized_thumbnail(
    track_info: Dict[str, Any],
) -> Optional[BufferedInputFile]:
    """Creates a properly formatted thumbnail for Telegram audio messages from track info.

    This centralized function handles all the steps for thumbnail preparation:
    1. Extracts the artwork URL from track_info
    2. Uses a lower-quality version for bandwidth efficiency
    3. Downloads and properly sizes/compresses the image to meet Telegram requirements
    4. Returns a properly formatted BufferedInputFile ready to use with audio messages

    Args:
        track_info: Track metadata dictionary containing artwork_url

    Returns:
        Optional[BufferedInputFile]: Prepared thumbnail or None if unavailable/error
    """
    # Get artwork URL if available
    artwork_url = track_info.get("artwork_url")

    if not artwork_url:
        logger.warning("No artwork URL found in track_info for thumbnail")
        return None

    # Use lower quality artwork for thumbnails to reduce bandwidth
    artwork_url = get_low_quality_artwork_url(artwork_url)

    try:
        # Download and resize image
        thumbnail_data = await download_and_resize_image(artwork_url)
        if thumbnail_data:
            return BufferedInputFile(thumbnail_data, filename="thumbnail.jpg")
        else:
            logger.warning("Failed to download and resize artwork")
            return None
    except Exception as e:
        logger.error(f"Error processing artwork for thumbnail: {e}")
        return None


async def detect_and_remove_silence(
    filepath: str, threshold_db: float = -55.0, min_silence_duration: int = 5000
) -> str:
    """Detect and remove silence from audio file.

    Args:
        filepath: Path to audio file
        threshold_db: Threshold in dB to consider as silence (default -55.0)
        min_silence_duration: Minimum duration of silence to remove in ms (default 5000)

    Returns:
        str: Path to processed file (might be the same as input if no silence)
    """
    try:
        # Skip processing if file doesn't exist
        if not os.path.exists(filepath):
            logger.warning(f"File not found for silence detection: {filepath}")
            return filepath

        # Define a function to process the audio in a separate thread
        def process_audio_file():
            # Load the audio file
            logger.info(f"Analyzing audio for silence: {filepath}")
            start_time = time.time()
            audio = AudioSegment.from_file(filepath)

            # Check duration
            duration_ms = len(audio)
            logger.info(
                f"Audio duration: {duration_ms}ms, loaded in {time.time() - start_time:.2f}s"
            )

            if duration_ms < 2000:  # Very short audio
                logger.info("Audio too short for silence detection, skipping")
                return filepath

            # Use direct approach - find all non-silent segments
            # This is more aggressive for finding extended silences
            silence_detect_start = time.time()

            # Detect all non-silent parts with aggressive settings
            nonsilent_ranges = detect_nonsilent(
                audio,
                min_silence_len=min_silence_duration,
                silence_thresh=threshold_db,
                seek_step=50,  # Smaller step for more precision
            )

            logger.info(
                f"Found {len(nonsilent_ranges)} non-silent segments in {time.time() - silence_detect_start:.2f}s"
            )

            # If we have multiple segments or only one segment but with trimming needed
            if not nonsilent_ranges:
                logger.warning("No non-silent segments found, returning original audio")
                return filepath

            # Check if significant silence exists
            first_segment_start = nonsilent_ranges[0][0]
            last_segment_end = nonsilent_ranges[-1][1]

            # Calculate total silence duration
            # 1. Silence at beginning and end
            edge_silence = first_segment_start + (duration_ms - last_segment_end)

            # 2. Silence between segments
            middle_silence = 0
            if len(nonsilent_ranges) > 1:
                for i in range(len(nonsilent_ranges) - 1):
                    gap = nonsilent_ranges[i + 1][0] - nonsilent_ranges[i][1]
                    middle_silence += gap
                    if gap > 10000:  # Log large gaps (10+ seconds)
                        logger.info(
                            f"Found large silence gap: {gap}ms between segments {i} and {i + 1}"
                        )

            total_silence = edge_silence + middle_silence

            # If no significant silence found
            if total_silence < 1000:  # Less than 1 second total
                logger.info(
                    f"Only {total_silence}ms of silence found, keeping original file"
                )
                return filepath

            # Build new audio by concatenating non-silent segments
            logger.info(
                f"Building processed audio from {len(nonsilent_ranges)} segments"
            )
            processed_audio = AudioSegment.empty()

            for i, (start_ms, end_ms) in enumerate(nonsilent_ranges):
                # Only add a small buffer for start/end segments
                if i == 0:  # First segment - consider start buffer
                    start_ms = max(0, start_ms - 100)  # Small buffer at start

                if i == len(nonsilent_ranges) - 1:  # Last segment - consider end buffer
                    end_ms = min(duration_ms, end_ms + 100)  # Small buffer at end

                # For gaps between segments, check how big the gap is
                if i > 0:
                    # Calculate gap from previous segment
                    prev_end = nonsilent_ranges[i - 1][1]
                    current_gap = start_ms - prev_end

                    # If gap is large (> 10s), add a short silence (500ms) instead of removing entirely
                    # This makes the transition feel more natural
                    if current_gap > 10000:
                        # Add a small silence transition (500ms) instead of removing entirely
                        processed_audio += AudioSegment.silent(duration=500)
                        logger.info(
                            f"Added 500ms silence transition for {current_gap}ms gap"
                        )

                # Add this segment
                segment = audio[start_ms:end_ms]
                processed_audio += segment

            # Create a temporary file
            _, ext = os.path.splitext(filepath)
            with tempfile.NamedTemporaryFile(suffix=ext, delete=False) as temp_file:
                temp_filepath = temp_file.name

            # Export the processed audio to the temporary file
            logger.info(
                f"Exporting processed audio ({len(processed_audio)}ms) to temporary file..."
            )
            export_start = time.time()
            processed_audio.export(temp_filepath, format=ext.lstrip("."))
            logger.info(f"Audio export completed in {time.time() - export_start:.2f}s")

            # Log stats
            reduction = duration_ms - len(processed_audio)
            reduction_percent = (reduction / duration_ms) * 100
            logger.info(
                f"Removed {reduction}ms ({reduction_percent:.1f}%) of silence from audio"
            )

            return temp_filepath

        # Run the audio processing in a separate thread to avoid blocking the event loop
        return await asyncio.to_thread(process_audio_file)

    except Exception as e:
        logger.error(f"Error processing silence: {e}")
        return filepath


async def send_audio_file(
    bot: Bot,
    chat_id: int,
    filepath: str,
    track_info: Dict[str, Any],
    reply_to_message_id: Optional[int] = None,
    inline_message_id: Optional[str] = None,
    user_info: Optional[Dict[str, Any]] = None,
    thumbnail: Optional[BufferedInputFile] = None,
) -> tuple[bool, Optional[str], Optional[Any]]:
    """Send audio file with proper formatting and metadata.

    Args:
        bot: Bot instance
        chat_id: Chat ID to send to
        filepath: Path to audio file
        track_info: Track metadata
        reply_to_message_id: Optional message ID to reply to
        inline_message_id: Optional inline message ID to update with removal status
        user_info: Optional user information for attribution in channel forwarding
        thumbnail: Optional pre-downloaded thumbnail for the audio

    Returns:
        tuple[bool, Optional[str], Optional[Any]]:
            - bool: True if successful, False otherwise
            - str: Error type if failed ('permission' or 'system' or None)
            - Any: Message object if successful, None otherwise
    """
    original_filepath = filepath
    trimmed_file_created = False

    logger.info(f"Processing audio file for chat_id {chat_id}")

    # Check if we have a cached file_id for this track
    track_id = str(track_info.get("id", ""))
    cached_file_id = None

    if track_id:
        cached_file_id = file_id_cache.get(track_id)
        if cached_file_id:
            logger.info(f"Found cached file_id for track ID {track_id}")
            try:
                # Create caption and buttons
                caption = format_track_info_caption(
                    track_info, (await bot.get_me()).username
                )

                # Get artwork URL if needed
                artwork_url = track_info.get("artwork_url", "")
                if artwork_url:
                    artwork_url = get_high_quality_artwork_url(artwork_url)

                # Use send_audio with the file_id
                result = await bot.send_audio(
                    chat_id=chat_id,
                    audio=cached_file_id,
                    caption=caption,
                    title=track_info["title"],
                    performer=track_info["artist"],
                    reply_to_message_id=reply_to_message_id,
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
                                    text="❓ Wrong Artist/Title? Click here!",
                                    url="https://t.me/id3_robot?start=dlmus",
                                ),
                            ],
                        ]
                    ),
                    disable_notification=True,
                )

                logger.info(f"Successfully sent audio using cached file_id")

                # We'll skip forwarding to the channel here and do it after updating inline
                # This is intentionally commented out as we'll forward after inline update
                # await forward_to_channel_if_enabled(bot, result, user_info)

                return True, None, result

            except Exception as e:
                logger.warning(f"Failed to use cached file_id: {e}")
                # Continue with normal upload if cached file_id fails

    try:
        # Check if we have silence analysis information and if silence was detected
        silence_analysis = track_info.get("silence_analysis", {})
        has_silence = silence_analysis.get("has_silence", False)

        processed_filepath = filepath

        # Only process audio to remove silence if waveform analysis indicates silence
        if has_silence:
            logger.info(
                f"Waveform analysis indicates silence: {silence_analysis.get('silence_percentage', 0):.1f}% silent"
            )

            # Update the inline message if provided
            if inline_message_id:
                try:
                    await bot.edit_message_reply_markup(
                        inline_message_id=inline_message_id,
                        reply_markup=InlineKeyboardMarkup(
                            inline_keyboard=[
                                [download_progress_button("checking_silence")]
                            ]
                        ),
                    )
                except Exception as e:
                    logger.warning(f"Error updating silence check status: {e}")

            # Process audio to remove silence - more aggressive settings
            processed_filepath = await detect_and_remove_silence(
                filepath,
                threshold_db=-55.0,  # Higher threshold to catch more subtle silence
                min_silence_duration=5000,  # 5 seconds minimum for removal
            )

            # Track if we created a new file that needs cleanup
            trimmed_file_created = processed_filepath != filepath

            # If silence was removed, update the button to let the user know
            if trimmed_file_created:
                logger.info(f"Silence was detected and removed from audio file")

                # Update the inline message if provided
                if inline_message_id:
                    try:
                        await bot.edit_message_reply_markup(
                            inline_message_id=inline_message_id,
                            reply_markup=InlineKeyboardMarkup(
                                inline_keyboard=[
                                    [download_progress_button("removing_silence")]
                                ]
                            ),
                        )
                        logger.info("Updated button to show silence removal status")
                    except Exception as e:
                        logger.warning(
                            f"Error updating button to silence removal status: {e}"
                        )

                logger.info(f"Using trimmed audio file: {processed_filepath}")
                filepath = processed_filepath
        else:
            logger.info(
                "No significant silence detected in waveform analysis, skipping silence removal"
            )

        # Format caption and prepare audio file
        caption = format_track_info_caption(track_info, (await bot.get_me()).username)
        audio = FSInputFile(filepath)

        # Get thumbnail using the centralized worker function
        if not thumbnail:
            thumbnail = await get_resized_thumbnail(track_info)

        logger.info("Sending audio with artwork thumbnail")
        try:
            result = await bot.send_audio(
                chat_id=chat_id,
                audio=audio,
                caption=caption,
                title=track_info["title"],
                performer=track_info["artist"],
                thumbnail=thumbnail,
                reply_to_message_id=reply_to_message_id,
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
                                text="❓ Wrong Artist/Title? Click here!",
                                url="https://t.me/id3_robot?start=dlmus",
                            ),
                        ],
                    ]
                ),
                disable_notification=True,
            )

            # Cache the file_id for future use
            if track_id and hasattr(result, "audio") and result.audio:
                file_id = result.audio.file_id
                file_id_cache.set(track_id, file_id)
                logger.info(f"Cached file_id for track ID {track_id}")

            # No channel forwarding here - we'll handle it after this function

        except Exception as send_err:
            # Check if this is a permission error
            error_message = str(send_err).lower()
            is_perm_error = (
                "forbidden" in error_message
                or "blocked" in error_message
                or "bot was blocked" in error_message
                or "not enough rights" in error_message
                or "bot can't initiate" in error_message
                or "user is deactivated" in error_message
                or "chat not found" in error_message
            )

            if is_perm_error:
                logger.error(f"Permission error when sending audio: {send_err}")
                return False, "permission", None
            else:
                logger.error(f"System error when sending audio: {send_err}")
                return False, "system", None

        # Clean up the trimmed file if it was created
        if trimmed_file_created and os.path.exists(filepath):
            try:
                os.remove(filepath)
                logger.info(f"Cleaned up trimmed file: {filepath}")
            except Exception as e:
                logger.error(f"Error cleaning up trimmed file {filepath}: {e}")

        # Note: We don't clean up the original file here as that's handled by the caller
        # based on the should_cleanup flag

        return True, None, result

    except Exception as e:
        logger.error(f"Error sending audio: {e}")

        # Clean up the trimmed file if there was an error
        if (
            trimmed_file_created
            and filepath != original_filepath
            and os.path.exists(filepath)
        ):
            try:
                os.remove(filepath)
                logger.info(f"Cleaned up trimmed file after error: {filepath}")
            except Exception as cleanup_err:
                logger.error(
                    f"Error cleaning up trimmed file after error: {cleanup_err}"
                )

        return False, "system", None


async def forward_to_channel_if_enabled(
    bot: Bot, message: Message, user_info: Optional[Dict[str, Any]] = None
) -> None:
    """
    Forward a message to the channel if channel forwarding is enabled.

    Args:
        bot: Bot instance
        message: Message to forward
        user_info: Optional user information for attribution
    """
    # Import here to avoid circular imports
    from utils.channel import channel_manager

    logger.info(
        f"Attempting to forward message with user_info: {user_info is not None}"
    )

    # Use the channel manager to handle forwarding
    result = await channel_manager.forward_message(bot, message, user_info)

    if not result:
        logger.info("Message was not forwarded to the channel")
    else:
        logger.info("Message was successfully forwarded to the channel")


async def update_inline_message_with_audio(
    bot: Bot,
    inline_message_id: str,
    file_id: str,
    track_info: Dict[str, Any],
    user_info: Optional[Dict[str, Any]] = None,
) -> bool:
    """Update inline message with audio using file_id.

    Args:
        bot: Bot instance
        inline_message_id: Inline message ID to update
        file_id: Telegram file_id for the audio
        track_info: Track metadata
        user_info: Optional user information for channel attribution

    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Get artwork URL for thumbnail
        artwork_url = track_info.get("artwork_url", "")

        if artwork_url:
            # Ensure it's high quality
            artwork_url = get_low_quality_artwork_url(artwork_url)
        else:
            logger.warning("No artwork URL found in track_info")

        # Create caption for the message
        caption = format_track_info_caption(track_info, (await bot.get_me()).username)

        media = InputMediaAudio(
            media=file_id,
            caption=caption,
            parse_mode=ParseMode.HTML,
            title=track_info["title"],
            performer=track_info["artist"],
            thumbnail=(URLInputFile(artwork_url) if artwork_url else None),
        )

        # Update the inline message with audio
        await bot.edit_message_media(inline_message_id=inline_message_id, media=media)

        # Cache the file_id for future use
        track_id = str(track_info.get("id", ""))
        if track_id:
            file_id_cache.set(track_id, file_id)
            logger.info(f"Cached file_id for track ID {track_id}")

        # No channel forwarding here - we'll handle it after this function

        logger.info("Successfully updated inline message with audio")
        return True

    except Exception as e:
        logger.error(f"Error updating inline message with file_id: {e}")
        return False


def is_permission_error(error: Exception) -> bool:
    """Determine if an error is related to permissions.

    Args:
        error: The exception object

    Returns:
        bool: True if it's a permission error
    """
    error_text = str(error).lower()
    permission_error_phrases = [
        "forbidden",
        "bot was blocked",
        "blocked by the user",
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
        "user is restricted",  # When user is restricted by Telegram
        "kicked by the user",  # When bot was kicked
        "not enough rights to send",  # Permission issue
        "chat not accessible",  # Chat not accessible
        "chat write forbidden",  # Can't write to chat
    ]
    return any(phrase in error_text for phrase in permission_error_phrases)


async def edit_message_with_audio(
    bot: Bot,
    chat_id: int,
    message_id: int,
    filepath: str,
    track_info: Dict[str, Any],
    inline_message_id: Optional[str] = None,
    user_info: Optional[Dict[str, Any]] = None,
    thumbnail: Optional[BufferedInputFile] = None,
) -> tuple[bool, Optional[str], Optional[Any]]:
    """Edit message with audio file.

    Args:
        bot: Bot instance
        chat_id: Chat ID to edit message in
        message_id: Message ID to edit
        filepath: Path to audio file
        track_info: Track metadata
        inline_message_id: Optional inline message ID for updates
        user_info: Optional user information for attribution in channel forwarding
        thumbnail: Optional pre-downloaded thumbnail for the audio

    Returns:
        tuple[bool, Optional[str], Optional[Any]]:
            - bool: True if successful, False otherwise
            - str: Error type if failed ('permission' or 'system' or None)
            - Any: Message object if successful, None otherwise
    """
    try:
        # Check if we have silence analysis information and if silence was detected
        silence_analysis = track_info.get("silence_analysis", {})
        has_silence = silence_analysis.get("has_silence", False)

        processed_filepath = filepath
        trimmed_file_created = False

        # Only process audio to remove silence if waveform analysis indicates silence
        if has_silence:
            logger.info(
                f"Waveform analysis indicates silence: {silence_analysis.get('silence_percentage', 0):.1f}% silent"
            )

            # Process audio to remove silence
            processed_filepath = await detect_and_remove_silence(
                filepath, threshold_db=-55.0, min_silence_duration=5000
            )

            # Track if we created a file that needs cleanup
            trimmed_file_created = processed_filepath != filepath

            if trimmed_file_created:
                logger.info(f"Silence was detected and removed from audio file")
                filepath = processed_filepath
        else:
            logger.info(
                "No significant silence detected in waveform analysis, skipping silence removal"
            )

        # First send a new message with the audio file to get the file_id
        success, error_type, result = await send_audio_file(
            bot=bot,
            chat_id=chat_id,
            filepath=filepath,
            track_info=track_info,
            user_info=user_info,
            thumbnail=thumbnail,
        )

        if not success:
            return False, error_type, None

        # Now delete the old message
        try:
            await bot.delete_message(chat_id=chat_id, message_id=message_id)
        except Exception as e:
            logger.warning(f"Failed to delete original message: {e}")
            # Continue since we already sent the new message with audio

        # Clean up the trimmed file if it was created
        if trimmed_file_created and os.path.exists(processed_filepath):
            try:
                os.remove(processed_filepath)
                logger.info(f"Cleaned up trimmed file: {processed_filepath}")
            except Exception as e:
                logger.error(
                    f"Error cleaning up trimmed file {processed_filepath}: {e}"
                )

        return True, None, result

    except Exception as e:
        logger.error(f"Error editing message with audio: {e}")
        return False, "system", None


async def download_track_and_thumbnail(
    download_url: str, filepath: str, track_info: Dict[str, Any]
) -> tuple[bool, Optional[BufferedInputFile]]:
    """
    Download track audio and thumbnail image concurrently to improve performance.

    Args:
        download_url: URL to download the audio from
        filepath: Path to save the audio file
        track_info: Track metadata containing artwork_url

    Returns:
        tuple[bool, Optional[BufferedInputFile]]:
            - bool: True if audio download successful, False otherwise
            - Optional[BufferedInputFile]: Prepared thumbnail or None if unavailable/error
    """
    # Start timing for performance measurement
    start_time = time.time()

    # Extract artwork URL for downloading
    artwork_url = track_info.get("artwork_url")
    thumbnail_task = None

    if artwork_url:
        # Use lower quality artwork for thumbnails to reduce bandwidth
        artwork_url = get_low_quality_artwork_url(artwork_url)

        # Start thumbnail download and processing task
        thumbnail_task = asyncio.create_task(download_and_resize_image(artwork_url))
        logger.info("Started concurrent thumbnail download")

    # Start audio download task
    download_task = asyncio.create_task(download_audio(download_url, filepath))

    # Wait for both tasks to complete
    results = await asyncio.gather(
        download_task,
        thumbnail_task if thumbnail_task else asyncio.sleep(0),
        return_exceptions=True,
    )

    # Process audio download result
    download_success = results[0]
    if isinstance(download_success, Exception):
        logger.error(f"Audio download failed with exception: {download_success}")
        download_success = False

    # Process thumbnail result if it was started
    thumbnail = None
    if thumbnail_task:
        thumbnail_data = results[1]
        if isinstance(thumbnail_data, Exception):
            logger.error(f"Thumbnail download failed with exception: {thumbnail_data}")
        elif thumbnail_data:
            thumbnail = BufferedInputFile(thumbnail_data, filename="thumbnail.jpg")

    # Calculate and log the total time
    total_time = time.time() - start_time
    logger.info(f"Concurrent download completed in {total_time:.2f} seconds")

    return download_success, thumbnail
