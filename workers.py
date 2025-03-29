import io
import os
import html
import logging
from typing import Any, Dict, Tuple, Optional

import aiohttp
import mutagen
from PIL import Image
from aiogram import Bot
from aiogram.enums import ParseMode
from aiogram.types import (
    FSInputFile,
    URLInputFile,
    InputMediaAudio,
    BufferedInputFile,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
)

from utils import (
    format_error_caption,
    format_track_info_caption,
    get_high_quality_artwork_url,
)
from soundcloud import cleanup_files
from utils.formatting import get_low_quality_artwork_url

logger = logging.getLogger(__name__)


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
    # Check if file exists
    if not os.path.exists(filepath):
        return False, "Downloaded file does not exist"

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

    except Exception as e:
        logger.error(f"Error checking file size: {e}")
        return False, f"Error validating file: {str(e)}"

    # Check if the file is a valid audio file using mutagen
    try:
        audio = mutagen.File(filepath)

        # Check if mutagen could parse the file at all
        if audio is None:
            logger.error(f"File is not recognized as a valid audio format: {filepath}")
            return False, "The file is not a valid audio format"

        # Check audio length
        if hasattr(audio, "info") and hasattr(audio.info, "length"):
            audio_length = audio.info.length  # Length in seconds
            logger.info(f"Audio length: {audio_length:.2f} seconds")

            # If audio length is 0 or extremely short, the file is likely corrupted
            if audio_length < 0.1:  # Less than 0.1 seconds
                logger.error(
                    f"Audio file has zero or extremely short duration: {audio_length:.2f} seconds"
                )
                return False, "The audio file has no playable content (zero duration)"

    except Exception as e:
        error_message = str(e).lower()
        logger.error(f"Error checking audio validity with mutagen: {e}")

        if (
            "can't sync to mpeg frame" in error_message
            or "invalid data" in error_message
        ):
            logger.error(f"Audio file is corrupted: {error_message}")
            return False, "The audio file is corrupted and cannot be played"
        elif (
            "no tags" in error_message or "no appropriate stream found" in error_message
        ):
            logger.error(f"Invalid audio format: {error_message}")
            return False, "Invalid audio format detected"

        return False, f"Audio file validation failed: {str(e)[:100]}"

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
) -> None:
    """Handle download failure scenarios with appropriate messaging.

    Args:
        bot: Bot instance
        message_id: Message ID to update
        track_info: Track metadata
        error_message: Error message to display
        search_query: Optional search query that led to this download
    """
    try:
        # Format the error message
        failure_text = format_error_caption(
            "Download failed: " + error_message,
            track_info,
            (await bot.get_me()).username,
        )

        # Include the search query if provided
        if search_query:
            failure_text += (
                f"\n\n<b>Query:</b> <code>{html.escape(search_query)}</code>"
            )

        # Update message with failure
        await bot.edit_message_caption(
            inline_message_id=message_id, caption=failure_text, reply_markup=None
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
) -> None:
    """Handle system-level errors during download or processing.

    Args:
        bot: Bot instance
        message_id: Message ID to update
        track_info: Track metadata
        error_message: Error message to display
        search_query: Optional search query
        filepath: Optional filepath to clean up
    """
    try:
        final_caption = "⚠️ <b>System Error</b>\n\n"
        permalink_url_no_embed = track_info["permalink_url"].replace("://", "://\u200c")
        final_caption += f"♫ <a href='{permalink_url_no_embed}'><b>{html.escape(track_info['title'])}</b> - <b>{html.escape(track_info['artist'])}</b></a>\n\n"
        final_caption += f"<b>Error details:</b> {error_message}\n\n"

        if search_query:
            final_caption += (
                f"<b>Query:</b> <code>{html.escape(search_query)}</code>\n\n"
            )

        final_caption += "This appears to be a technical error with the bot or server, not a permissions issue. "
        final_caption += "You can try again later or download directly from SoundCloud."

        markup = InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(
                        text="▶️ Open in SoundCloud",
                        url=track_info["permalink_url"],
                    ),
                ],
                [
                    InlineKeyboardButton(
                        text="🔄 Try Again",
                        callback_data=f"download:{track_info['id']}",
                    ),
                ],
            ]
        )

        await bot.edit_message_caption(
            inline_message_id=message_id,
            caption=final_caption,
            reply_markup=markup,
        )
    except Exception as e:
        logger.error(f"Error updating system error caption: {e}")
        try:
            simple_caption = "⚠️ <b>System Error:</b> An error occurred while processing your request. Please try again later."
            if search_query:
                simple_caption += (
                    f"\n\n<b>Query:</b> <code>{html.escape(search_query)}</code>"
                )

            await bot.edit_message_caption(
                inline_message_id=message_id,
                caption=simple_caption,
                reply_markup=InlineKeyboardMarkup(
                    inline_keyboard=[
                        [
                            InlineKeyboardButton(
                                text="🔄 Try Again",
                                callback_data=f"download:{track_info['id']}",
                            ),
                        ]
                    ]
                ),
            )
        except Exception as e:
            logger.error(f"Final system error fallback failed: {e}")
    finally:
        if filepath:
            await cleanup_files(filepath)


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

    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            if response.status != 200:
                logger.error(
                    f"Failed to download image. Status code: {response.status}"
                )
                return None
            image_data = await response.read()
            logger.info(f"Downloaded image size: {len(image_data) / 1024:.1f} kB")

    # Open image using PIL
    try:
        img = Image.open(io.BytesIO(image_data))
        logger.info(f"Original image size: {img.size}, mode: {img.mode}")
    except Exception as e:
        logger.error(f"Failed to open image with PIL: {e}")
        return None

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
        logger.info(f"Compressed image size at quality {quality}: {size_kb:.1f} kB")

        if size_kb <= 200 or quality <= 5:  # 200 kB limit for safety margin
            logger.info(f"Final image quality: {quality}, size: {size_kb:.1f} kB")
            break
        quality -= 5

    return img_byte_arr.getvalue()


async def send_audio_file(
    bot: Bot,
    chat_id: int,
    filepath: str,
    track_info: Dict[str, Any],
    reply_to_message_id: Optional[int] = None,
) -> bool:
    """Send audio file with proper formatting and metadata.

    Args:
        bot: Bot instance
        chat_id: Chat ID to send to
        filepath: Path to audio file
        track_info: Track metadata
        reply_to_message_id: Optional message ID to reply to

    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Format caption and prepare audio file
        caption = format_track_info_caption(track_info, (await bot.get_me()).username)
        audio = FSInputFile(filepath)

        # Get artwork URL if available
        artwork_url = track_info.get("artwork_url")

        if artwork_url:
            # Use lower quality artwork for thumbnails to reduce bandwidth
            artwork_url = get_low_quality_artwork_url(artwork_url)
            logger.info(f"Using artwork URL for audio message: {artwork_url}")

            # Download and resize image
            try:
                thumbnail_data = await download_and_resize_image(artwork_url)
                if thumbnail_data:
                    thumbnail = BufferedInputFile(
                        thumbnail_data, filename="thumbnail.jpg"
                    )
                else:
                    thumbnail = None
                    logger.warning("Failed to download and resize artwork")
            except Exception as e:
                logger.error(f"Error processing artwork: {e}")
                thumbnail = None
        else:
            logger.warning("No artwork URL found in track_info for audio message")
            thumbnail = None

        logger.info("Sending audio with artwork thumbnail")
        return await bot.send_audio(
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
                        InlineKeyboardButton(
                            text="▶️ SoundCloud",
                            url=track_info["permalink_url"],
                        ),
                        InlineKeyboardButton(
                            text="👤 Artist",
                            url=(
                                track_info["user"]["url"] or track_info["permalink_url"]
                            )
                            + f"?urn={track_info['user']['urn']}",
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

    except Exception as e:
        logger.error(f"Error sending audio: {e}")
        return False


async def update_inline_message_with_audio(
    bot: Bot, inline_message_id: str, file_id: str, track_info: Dict[str, Any]
) -> bool:
    """Update inline message with audio using file_id.

    Args:
        bot: Bot instance
        inline_message_id: Inline message ID to update
        file_id: Telegram file_id for the audio
        track_info: Track metadata

    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Get artwork URL for thumbnail
        artwork_url = track_info.get("artwork_url", "")
        logger.info(f"Artwork URL from track_info: {artwork_url}")

        if artwork_url:
            # Ensure it's high quality
            artwork_url = get_high_quality_artwork_url(artwork_url)
            logger.info(f"Using high quality artwork URL: {artwork_url}")
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
