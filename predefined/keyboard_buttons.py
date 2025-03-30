from aiogram.types import InlineKeyboardButton

example_inline_search_button = InlineKeyboardButton(
    text="🔍 Click here to start searching",
    switch_inline_query_current_chat="drain gang",
)

download_status_button = InlineKeyboardButton(
    text="⬇️ Downloading...",
    callback_data="download_status",
)


def try_again_button(track_id: str):
    return InlineKeyboardButton(
        text="🔄 Try Again",
        callback_data=f"download:{track_id}",
    )


def artist_button(url: str):
    return InlineKeyboardButton(
        text="👤 Artist",
        url=url,
    )


def soundcloud_button(url: str):
    return InlineKeyboardButton(
        text="🔊 SoundCloud",
        url=url,
    )


def start_chat_button(bot_username: str):
    return InlineKeyboardButton(
        text="💬 Send /start",
        url=f"https://t.me/{bot_username}?start=open_dms",
    )
