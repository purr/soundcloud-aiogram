"""
Utility functions for channel management and verification.
"""

from typing import Any, Dict, Optional

from aiogram import Bot
from aiogram.types import Message
from aiogram.exceptions import TelegramAPIError

from utils.logger import get_logger

# Configure logging
logger = get_logger(__name__)


class ChannelManager:
    """
    Class to manage channel operations including verification and forwarding.
    """

    def __init__(self):
        self.channel_id = None
        self.channel_name = None
        self.is_enabled = False

    async def verify_and_setup(self, bot: Bot, channel_id_str: str) -> bool:
        """
        Verify access to the channel and set up the manager if successful.

        Args:
            bot: Bot instance
            channel_id_str: Channel ID or username as string

        Returns:
            bool: True if verification and setup successful, False otherwise
        """
        if not channel_id_str:
            logger.info("No channel ID provided for verification")
            self.is_enabled = False
            self.channel_id = None
            return False

        # Process channel ID - convert to int if it's a numeric string and not a username
        channel_id = channel_id_str
        if isinstance(channel_id_str, str) and not channel_id_str.startswith("@"):
            try:
                channel_id = int(channel_id_str)

            except ValueError:
                logger.warning(
                    f"Could not convert channel ID to integer: {channel_id_str}"
                )
                channel_id = channel_id_str

        try:
            # Try to get chat information to verify access
            chat = await bot.get_chat(channel_id)

            # Check if it's a channel
            if chat.type != "channel":
                logger.warning(f"Chat {chat.id} is not a channel (type: {chat.type})")
                self.is_enabled = False
                return False

            # Try to get bot's permissions in the channel
            bot_member = await bot.get_chat_member(chat.id, (await bot.get_me()).id)

            # Check if bot has permission to send messages
            if (
                not hasattr(bot_member, "can_post_messages")
                or not bot_member.can_post_messages
            ):
                logger.warning(
                    f"Bot doesn't have permission to send messages in channel {chat.title}"
                )
                self.is_enabled = False
                return False

            # Store the channel information
            self.channel_id = chat.id
            self.channel_name = chat.title
            self.is_enabled = True

            return True

        except TelegramAPIError as e:
            error_msg = str(e).lower()
            if "chat not found" in error_msg:
                logger.warning(f"Channel not found: {channel_id}")
            elif "bot is not a member" in error_msg:
                logger.warning(f"Bot is not a member of the channel: {channel_id}")
            else:
                logger.warning(f"Error accessing channel: {error_msg}")

            self.is_enabled = False
            self.channel_id = None
            return False

        except Exception as e:
            logger.error(f"Unexpected error verifying channel access: {str(e)}")
            self.is_enabled = False
            self.channel_id = None
            return False

    async def forward_message(
        self, bot: Bot, message: Message, user_info: Optional[Dict[str, Any]] = None
    ) -> bool:
        """
        Forward a message to the channel if the channel is enabled.

        Args:
            bot: Bot instance
            message: Message to forward
            user_info: Optional user information for attribution

        Returns:
            bool: True if forwarding was successful, False otherwise
        """
        if not self.is_enabled or not self.channel_id:
            logger.info("Channel forwarding is disabled or not configured")
            return False

        try:

            # Forward the message to the channel
            forwarded_msg = await bot.forward_message(
                chat_id=self.channel_id,
                from_chat_id=message.chat.id,
                message_id=message.message_id,
                disable_notification=True,
            )

            logger.info(
                f"Successfully forwarded message to channel {self.channel_name}"
            )

            # Send attribution if user info is provided
            if user_info:
                await self.send_attribution(bot, user_info, forwarded_msg.message_id)
            else:
                logger.info("No user info provided for attribution")

            return True

        except TelegramAPIError as e:
            logger.error(f"Failed to forward message to channel: {e}")
            return False

        except Exception as e:
            logger.error(f"Unexpected error forwarding message to channel: {e}")
            return False

    async def send_attribution(
        self,
        bot: Bot,
        user_info: Dict[str, Any],
        reply_to_message_id: Optional[int] = None,
    ) -> bool:
        """
        Send user attribution message to the channel.

        Args:
            bot: Bot instance
            user_info: User information dictionary
            reply_to_message_id: Optional message ID to reply to

        Returns:
            bool: True if successful, False otherwise
        """
        if not self.is_enabled or not self.channel_id:
            logger.info("Cannot send attribution - channel forwarding is disabled")
            return False

        try:
            # Extract initial user information
            user_id = user_info.get("id")
            username = user_info.get("username")
            first_name = user_info.get("first_name", "")
            last_name = user_info.get("last_name", "")

            # Try to get the latest user information from Telegram
            try:
                user = await bot.get_chat(user_id)
                if user:
                    # Update user information with the latest data
                    username = getattr(user, "username", username)
                    first_name = getattr(user, "first_name", first_name)
                    last_name = getattr(user, "last_name", last_name)
                    logger.info(
                        f"Retrieved updated user information for user ID {user_id}"
                    )
            except Exception as e:
                logger.warning(
                    f"Could not get updated user information for user ID {user_id}: {e}"
                )
                # Continue with the original user info

            # Create full name
            display_text = ""
            if first_name:
                display_text += f"{first_name} "

            if last_name:
                display_text += f"{last_name} "

            if username:
                for username in user.active_usernames:
                    display_text += f"@{username} "

            attribution = f'Requested by {display_text}(<a href="tg://user?id={user_id}">{user_id}</a>)'

            # Send the attribution message
            await bot.send_message(
                chat_id=self.channel_id,
                text=attribution,
                reply_to_message_id=reply_to_message_id,
                disable_notification=True,
                parse_mode="HTML",  # Enable HTML parsing for the link
            )

            logger.info(f"Sent user attribution to channel {self.channel_name}")
            return True

        except TelegramAPIError as e:
            logger.error(f"Failed to send user attribution to channel: {e}")
            return False

        except Exception as e:
            logger.error(f"Unexpected error sending user attribution: {e}")
            return False


# Global instance of the channel manager
channel_manager = ChannelManager()
