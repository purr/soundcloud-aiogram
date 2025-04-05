import os
import shutil
import asyncio

from bot import dp, bot, router, process_download_queue
from utils import refresh_client_id
from config import VERSION, DOWNLOAD_PATH, FORWARD_CHANNEL_ID, CACHE_CLEANUP_INTERVAL
from helpers import periodic_cache_cleanup
from utils.logger import get_logger
from utils.channel import channel_manager

# Configure logging
logger = get_logger(__name__)


async def cache_cleanup_task():
    """Task that runs periodically to clean up expired cache entries"""
    while True:
        try:
            await periodic_cache_cleanup()
            # Run the cleanup using the interval from config
            await asyncio.sleep(CACHE_CLEANUP_INTERVAL)
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"Error in cache cleanup task: {e}")
            # Still sleep before retrying, using the configured interval
            await asyncio.sleep(CACHE_CLEANUP_INTERVAL)


async def main():
    """Main function to start the bot"""
    # Log startup information
    logger.info(f"Starting SoundCloud Search Bot v{VERSION}")

    # Create necessary directories
    os.makedirs(DOWNLOAD_PATH, exist_ok=True)

    # Create data directory for cache file
    data_dir = os.path.dirname(os.path.join(os.getcwd(), "data"))
    os.makedirs(data_dir, exist_ok=True)
    logger.info(f"Ensured data directory exists at: {data_dir}")

    # Get bot info and log it
    bot_info = await bot.get_me()
    logger.info(f"{bot_info.full_name} @{bot_info.username} ({bot_info.id})")

    # Get a fresh client ID at startup
    if await refresh_client_id():
        logger.info("Successfully obtained fresh client ID")

    # Check channel access if configured
    if FORWARD_CHANNEL_ID:
        logger.info(f"Setting up channel forwarding with ID: {FORWARD_CHANNEL_ID}")
        if await channel_manager.verify_and_setup(bot, FORWARD_CHANNEL_ID):
            logger.info(
                f"Channel forwarding enabled to: {channel_manager.channel_name} (ID: {channel_manager.channel_id})"
            )
        else:
            logger.warning(f"Channel forwarding disabled due to access issues")

    # Start the download queue worker
    asyncio.create_task(process_download_queue())

    # Start the cache cleanup task
    asyncio.create_task(cache_cleanup_task())
    # Include the router in the dispatcher
    dp.include_router(router)

    # Start polling
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)


if __name__ == "__main__":
    try:
        if shutil.which("ffmpeg") is None:
            logger.error("FFmpeg not found, required for HLS downloads")
            logger.info("Please install ffmpeg to start the bot")
            exit(1)
        else:
            asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Bot stopped!")
    except Exception as e:
        logger.error(f"Unhandled exception: {e}", exc_info=True)
