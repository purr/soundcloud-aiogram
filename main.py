import os
import asyncio

from bot import dp, bot, router, process_download_queue
from utils import refresh_client_id
from config import VERSION, DOWNLOAD_PATH
from utils.logger import get_logger

# Configure logging
logger = get_logger(__name__)


async def main():
    """Main function to start the bot"""
    # Log startup information
    logger.info(f"Starting SoundCloud Search Bot v{VERSION}")

    # Create necessary directories
    os.makedirs(DOWNLOAD_PATH, exist_ok=True)

    # Get bot info and log it
    bot_info = await bot.get_me()
    logger.info(f"{bot_info.full_name} @{bot_info.username} ({bot_info.id})")

    # Get a fresh client ID at startup
    logger.info("Getting fresh SoundCloud client ID...")
    await refresh_client_id()
    logger.info("Successfully obtained fresh client ID")

    # Start the download queue worker
    asyncio.create_task(process_download_queue())

    # Include the router in the dispatcher
    dp.include_router(router)

    # Start polling
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Bot stopped!")
    except Exception as e:
        logger.error(f"Unhandled exception: {e}", exc_info=True)
