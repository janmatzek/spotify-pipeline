"""main spotify data pipeline"""

import base64
import datetime
import json
import os

import pandas as pd
import pandas_gbq
import requests
from dotenv import load_dotenv
from google.cloud import bigquery
from google.cloud.bigquery import SchemaField, TableReference
from google.oauth2 import service_account

from .schema import return_schema
from .utils import send_response, table_exists


def create_big_query_table(table_id, schema, client):
    """
    Creates a table in BigQuery.
    Args:
        table_id (str): The ID of the table to create
        schema (list of dicts): schema of the created table
        client: BigQuery client
    """
    table_ref = client.dataset(table_id.dataset_id).table(table_id.table_id)

    table = bigquery.Table(table_ref, schema=schema)

    client.create_table(table)  # Creates the table
    print(f"Table '{table_id.table_id}' created successfully in dataset '{table_id.dataset_id}'.")


def run_big_query_query(query, client):
    """
    Runs a BigQuery query
    args:
        query (str): SQL query
        client: BigQuery client
    """
    query_job = client.query(query)

    result = query_job.result()

    return result


def refresh_access_token(client_id: str, client_secret: str, refresh_token: str):
    """
    Retrieve a short-lived access token from Spotify API
    """
    # , authorization_code: str
    url = "https://accounts.spotify.com/api/token"

    encoded_credentials = base64.b64encode(
        client_id.encode() + b":" + client_secret.encode()
    ).decode("utf-8")

    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Authorization": f"Basic {encoded_credentials}",
    }

    body = {
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
        "client_id": client_id,
    }

    response = requests.post(url, data=body, headers=headers, timeout=10)

    response_data = response.json()

    token = response_data["access_token"]

    return token


def get_recently_played_tracks(access_token: str, query_from: int):
    """
    Retrieves 'recently played' data from Spotify API
    Args:
        access_token (str): access token to authenticate access to user data
        query_from (int): unix timestamp, start point from which the data will be queried.
            Can be whatever value as it does not seem to influence the response of the API
    Returns:
        A list of dicts containing the tracks data
    """
    url = f"https://api.spotify.com/v1/me/player/recently-played?limit=50&after={query_from}"

    headers = {"Authorization": f"Bearer {access_token}"}

    response = requests.get(url=url, headers=headers, timeout=30)
    data = response.json()

    return data


def transform_spotify_track_data(data, current_datetime):
    """
    Processes the data from Spotify API - selects first artist, first image,
        flattens the json and returns the data as a DataFrame
    Args:
        data: the list of dicts from Spotify API
        current_datetime (timestamp): current timestamp which will be printed to 'queried_at' column
    """

    for index, item in enumerate(data["items"]):
        if data["items"][index]["track"]["artists"]:
            data["items"][index]["track"]["artists"] = data["items"][index]["track"]["artists"][0]
        if data["items"][index]["track"]["album"]["images"]:
            data["items"][index]["track"]["album"]["images"] = data["items"][index]["track"][
                "album"
            ]["images"][0]
        if data["items"][index]["context"] is None:
            data["items"][index]["context"] = {}
            data["items"][index]["context"]["type"] = None
            data["items"][index]["context"]["context_external_urls_spotify"] = None

    # Flatten the JSON data using pandas
    flattened_data = pd.json_normalize(data, record_path="items", sep="_")

    columns_to_keep = [
        "track_name",
        "track_explicit",
        "track_popularity",
        "track_id",
        "track_track_number",
        "track_type",
        "played_at",
        "context_type",
        "context_external_urls_spotify",
        "track_artists_id",
        "track_artists_name",
        "track_artists_type",
        "track_album_album_type",
        "track_album_id",
        "track_album_images_url",
        "track_album_images_height",
        "track_album_name",
        "track_album_release_date",
        "track_album_release_date_precision",
        "track_album_total_tracks",
        "track_duration_ms",
    ]

    flattened_data = flattened_data[columns_to_keep]

    # better_column_names = {col: renameColumns(col) for col in flattened_data.columns}
    # better_column_names = {col: col.replace(".", "_") for col in flattened_data.columns}

    # flattened_data = flattened_data.rename(columns=better_column_names)

    flattened_data["queried_at"] = current_datetime

    return flattened_data


def track_data_handler(event, context):
    """
    Lambda function handler for fetching data form the Spotify API
    """
    event_body = event.get("body", {}) if "body" in event else {}
    print("Received event:", json.dumps(event_body))

    print(f"Running {context.function_name}")

    current_datetime = datetime.datetime.now(datetime.UTC)

    last_queried_from = pd.Timestamp("1970-01-01 00:00:00", tz="UTC")

    # get environmenmt variables
    load_dotenv()

    client_id = os.getenv("SPOTIFY_CLIENT_ID")
    client_secret = os.getenv("SPOTIFY_CLIENT_SECRET")
    refresh_token = os.getenv("REFRESH_TOKEN")
    # authorization_code = os.getenv("AUTHORIZATION_CODE")
    credentials_path = os.getenv("SERVICE_ACCOUNT_PATH")
    destination_table_id = os.getenv("TABLE_ID")

    bq_table_reference = TableReference.from_string(destination_table_id)
    table_schema = return_schema()
    bigquery_schema = [SchemaField(field["name"], field["type"]) for field in table_schema]

    # set up BigQuery client
    try:
        bigquery_client = bigquery.Client(
            credentials=service_account.Credentials.from_service_account_file(credentials_path)
        )
    except Exception as e:  # pylint: disable=broad-except
        return send_response(500, "Failed to set up BigQuery client", e)

    # check if destination table exists in BigQUery
    if table_exists(bq_table_reference, bigquery_client):
        # query max played_at
        str_query = f"SELECT MAX(played_at) FROM `{destination_table_id}`"
        result = run_big_query_query(str_query, bigquery_client)
        for row in result:
            last_queried_from = row[0]
            query_from = int(row[0].timestamp())
    else:
        query_from = "0000000001"
        create_big_query_table(bq_table_reference, bigquery_schema, bigquery_client)

    # get access token using your refresh token
    try:
        token = refresh_access_token(client_id, client_secret, refresh_token)
    except Exception as e:  # pylint: disable=broad-except
        return send_response(500, "Failed to refresh access token.", e)

    # retrieve the data from the API
    try:
        track_data = get_recently_played_tracks(token, query_from)
    except Exception as e:  # pylint: disable=broad-except
        return send_response(500, "Failed to retrieve the data from the API.", e)

    # flatten the json and keep the data you want
    try:
        transformed_data = transform_spotify_track_data(track_data, current_datetime)
    except Exception as e:  # pylint: disable=broad-except
        return send_response(500, "Failed to transform the data.", e)

    #  coerce the data types before pushing stuff to bigQuery
    try:
        for field in table_schema:
            col_name = field["name"]
            data_type = field["type"]

            if data_type == "INTEGER":
                transformed_data[col_name] = pd.to_numeric(
                    transformed_data[col_name], errors="coerce"
                ).astype("Int64")
            elif data_type == "FLOAT":
                transformed_data[col_name] = pd.to_numeric(
                    transformed_data[col_name], errors="coerce"
                )
            elif data_type == "STRING":
                transformed_data[col_name] = transformed_data[col_name].apply(
                    lambda x: str(x) if x is not None else None
                )
            elif data_type == "TIMESTAMP":
                transformed_data[col_name] = pd.to_datetime(
                    transformed_data[col_name], errors="coerce"
                )

        # Replace NaN values with None
        transformed_data = transformed_data.where(pd.notnull(transformed_data), None)

    except Exception as e:  # pylint: disable=broad-except
        return send_response(500, "Failed to apply data validation.", e)

    transformed_data = transformed_data[transformed_data["played_at"] > last_queried_from]

    # send the data to BigQuery
    try:
        pandas_gbq.to_gbq(
            dataframe=transformed_data,
            project_id=destination_table_id.split(".")[0],
            destination_table=destination_table_id,
            if_exists="append",
            table_schema=table_schema,
            credentials=service_account.Credentials.from_service_account_file(credentials_path),
        )
    except Exception as e:  # pylint: disable=broad-except
        return send_response(500, "Failed to upload the data to BigQuery.", e)

    return send_response(
        200, f"you have listened to {len(transformed_data)} songs since {last_queried_from}"
    )
