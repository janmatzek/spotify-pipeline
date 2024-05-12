"""utils.py to contain recurring functions"""

import os

import requests
from dotenv import load_dotenv

load_dotenv()


def send_telegram_message(message: str, is_alert: bool):
    """
    Function to send a simple message to Telegram via API.
    The function takes two arguements:
        1) message (string) - the message that will be sent to Telgram
        2) is_alert (boolean) - denoting whether the message should be sent
            to the regular logging channel or the alerting channel
    The function returns a HTTP response given by the Telegram API
    """
    if is_alert:
        chat_id = os.getenv("TELEGRAM_ALERTING_CHANNEL_ID")
    else:
        chat_id = os.getenv("TELEGRAM_LOGGING_CHANNEL_ID")

    token = os.getenv("TELEGRAM_BOT_TOKEN")

    url = f"https://api.telegram.org/bot{token}/sendMessage?chat_id={chat_id}&text={message}"

    response = requests.get(url=url, timeout=10).json()

    return response
