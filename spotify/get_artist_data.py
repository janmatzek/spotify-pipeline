""" fetch data for spotify artists missing in BigQuery """

import datetime
import json
import os

import pandas as pd
import pandas_gbq
import requests
from dotenv import load_dotenv
from google.auth import exceptions as auth_exceptions
from google.cloud import bigquery
from google.cloud.bigquery import TableReference
from google.oauth2 import service_account

from .utils import send_response, table_exists

load_dotenv()
credentials_path = os.getenv("SERVICE_ACCOUNT_PATH")


def transform_spotify_artists_data(data: dict, current_datetime: pd.Timestamp) -> pd.DataFrame:
    """flatten and transform json response to tabular format"""

    for index in range(len(data["artists"])):
        artist_images = data["artists"][index]["images"]
        artist_genres = data["artists"][index]["genres"]
        if artist_images:
            data["artists"][index]["images"] = artist_images[0]
        if artist_genres:
            data["artists"][index]["main_genre"] = min(artist_genres, key=len)

    # Flatten the JSON data using pandas
    flattened_data = pd.json_normalize(data, record_path="artists", sep="_")

    columns_to_keep = [
        "external_urls_spotify",
        "followers_total",
        "genres",
        "id",
        "images_height",
        "images_url",
        "name",
        "popularity",
        "type",
        "main_genre",
    ]

    flattened_data = flattened_data[columns_to_keep]

    flattened_data["queried_at"] = current_datetime

    return flattened_data


def retrieve_access_token(client_id: str, client_secret: str):
    """RETRIEVE ACCESS TOKEN FROM SPOTIFY"""
    url = "https://accounts.spotify.com/api/token"

    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
    }

    body = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
    }

    response = requests.post(url, data=body, headers=headers, timeout=10)

    response_data = response.json()

    access_token = response_data["access_token"]

    return access_token


def get_artist_ids(client: str) -> str:
    """Retrieves artists ids from bigquery"""
    unique_artists_table_id = "jan-sandbox2024.spotifyData.unique_artists"
    artits_table_id = "jan-sandbox2024.spotifyData.artists_data"
    bq_table_reference = TableReference.from_string(artits_table_id)
    if table_exists(bq_table_reference, client):
        query = f"""
            SELECT track_artists_id 
            FROM `{unique_artists_table_id}`
            WHERE track_artists_id NOT IN (
                SELECT id FROM `{artits_table_id}`
            ) 
            LIMIT 50
        """
    else:
        query = f"""
            SELECT track_artists_id 
            FROM `{unique_artists_table_id}`
            LIMIT 50
        """

    query_job = client.query(query)

    result = query_job.result()

    artists_ids = [row[0] for row in result]

    artists_ids = ",".join(artists_ids)

    return artists_ids


def set_up_big_query_client():
    """set up BigQuery client"""
    try:
        bigquery_client = bigquery.Client(
            credentials=service_account.Credentials.from_service_account_file(credentials_path)
        )
        return bigquery_client
    except auth_exceptions.DefaultCredentialsError as e:
        return send_response(500, "Failed to load default credentials for BigQuery client", e)
    except FileNotFoundError as e:
        return send_response(500, "Failed to find the credentials file for BigQuery client", e)
    except Exception as e:  # pylint: disable=broad-except
        return send_response(500, "Failed to set up BigQuery client", e)


def fetch_artists_data(event, context):
    """Fetch artists data from Spotify API"""

    event_body = event.get("body", {}) if "body" in event else {}
    print("Received event:", json.dumps(event_body))

    print(f"Running {context.function_name}")

    bigquery_client = set_up_big_query_client()

    artists_ids = get_artist_ids(bigquery_client)

    if len(artists_ids) == 0:
        return send_response(202, "No new data to upload")

    url = f"https://api.spotify.com/v1/artists?ids={artists_ids}"

    client_id = os.getenv("SPOTIFY_CLIENT_ID")

    client_secret = os.getenv("SPOTIFY_CLIENT_SECRET")

    access_token = retrieve_access_token(client_id=client_id, client_secret=client_secret)

    headers = {"Authorization": f"Bearer {access_token}"}

    response = requests.get(url=url, headers=headers, timeout=10)

    data = response.json()

    current_datetime = datetime.datetime.now(datetime.UTC)

    processed_data = transform_spotify_artists_data(data, current_datetime)

    table_schema = [
        {"name": "id", "type": "STRING"},
        {"name": "name", "type": "STRING"},
        {"name": "main_genre", "type": "STRING"},
        {"name": "genres", "type": "STRING", "mode": "REPEATED"},
        {"name": "popularity", "type": "INTEGER"},
        {"name": "followers_total", "type": "INTEGER"},
        {"name": "images_height", "type": "INTEGER"},
        {"name": "type", "type": "STRING"},
        {"name": "images_url", "type": "STRING"},
        {"name": "external_urls_spotify", "type": "STRING"},
        {"name": "queried_at", "type": "TIMESTAMP"},
    ]

    # send the data to BigQuery
    try:
        pandas_gbq.to_gbq(
            dataframe=processed_data,
            project_id="jan-sandbox2024",
            destination_table="jan-sandbox2024.spotifyData.artists_data",
            if_exists="append",
            table_schema=table_schema,
            credentials=service_account.Credentials.from_service_account_file(credentials_path),
        )
    except Exception as e:  # pylint: disable=broad-except
        return send_response(500, "Failed to upload the data to BigQuery.", e)

    return send_response(200, f"Uploaded data about {len(processed_data)} artists")
