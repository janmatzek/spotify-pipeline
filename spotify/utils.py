"""utils.py to contain recurring functions"""

import json
import os

import google.cloud.exceptions
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


def send_response(status_code: int, message: str, e=""):
    """
    Sends a HTTP response.
    Args:
        status_code (int): HTTP status
        message (str): message in the body of the response
        e (str) : optional, error description
    """
    if e == "":
        sep = ""
    else:
        sep = "\n"

    message = f"{message}{sep}{e}"

    if status_code in [200, 202]:
        send_as_alert = False
    else:
        send_as_alert = True

    send_telegram_message(message=message, is_alert=send_as_alert)

    response = {"status_code": status_code, "body": json.dumps({"message": f"{message}"})}
    print(response)
    return response


def table_exists(table_id: str, client):
    """
    Check if a table exists in a BigQuery dataset.

    Args:
        table_id (str): The ID of the table to check.
        client (google.cloud.bigquery.Client): The BigQuery client instance.

    Returns:
        bool: True if the table exists, False otherwise.
    """
    table_ref = client.dataset(table_id.dataset_id).table(table_id.table_id)

    try:
        # Check if the table exists
        client.get_table(table_ref)
        return True
    except google.cloud.exceptions.NotFound:
        print(f"The table '{table_id}' does not exist.")
        return False
    except Exception as e:  # pylint: disable=broad-except
        print(f"An error occurred while checking for table '{table_id}': {str(e)}")
        send_telegram_message(
            message=f"An error occurred while checking for table '{table_id}': {str(e)}",
            is_alert=True,
        )
        return False
