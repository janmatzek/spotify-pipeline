import requests
import os
import datetime
import base64
import pandas_gbq
import pandas as pd
from .schema import returnSchema
from dotenv import load_dotenv
from google.cloud import bigquery
from google.cloud.bigquery import SchemaField
from google.cloud.bigquery import TableReference
from google.oauth2 import service_account


def tableExists(tableId, client):
    table_ref = client.dataset(tableId.dataset_id).table(tableId.table_id)
    
    try:
        client.get_table(table_ref)  # Raises an exception if the table doesn't exist
        return True
    except Exception as e:
        print(f"The table '{tableId}' does not exist.")
        return False


def createBigQueryTable(table_id, schema, client):    
    table_ref = client.dataset(table_id.dataset_id).table(table_id.table_id)
    
    table = bigquery.Table(table_ref, schema=schema)
    
    client.create_table(table)  # Creates the table
    print(f"Table '{table_id.table_id}' created successfully in dataset '{table_id.dataset_id}'.")


def runBigQueryQuery(query, client):  
    query_job = client.query(query)
    
    result = query_job.result()
    
    return result

def retrierveAccessToken(clientId: str, clientSecret:str):
    # RETRIEVE ACCESS TOKEN FROM SPOTIFY
    url = "https://accounts.spotify.com/api/token"

    headers = {
        'Content-Type': 'application/x-www-form-urlencoded',
    }

    body = {
        'grant_type': 'client_credentials',
        'client_id': clientId,
        'client_secret': clientSecret
    }

    response = requests.post(url, data=body, headers=headers)

    responseData = response.json() 

    accessToken = responseData['access_token']

    return accessToken


def refreshAccessToken(clientId: str, clientSecret:str, refreshToken:str, authorizationCode:str):
    # RETRIEVE ACCESS TOKEN FROM SPOTIFY
    url = "https://accounts.spotify.com/api/token"

    encodedCredentials = base64.b64encode(clientId.encode() + b':' + clientSecret.encode()).decode("utf-8")

    headers = {
        'Content-Type': 'application/x-www-form-urlencoded',
        'Authorization': f'Basic {encodedCredentials}'
    }

    body = {
        'grant_type': 'refresh_token',
        'refresh_token': refreshToken,
        'client_id': clientId,
    }

    response = requests.post(url, data=body, headers=headers)

    responseData = response.json() 
    
    token = responseData['access_token']

    return token


def findArtist(accessToken:str):
    artistId = '19EXmY8ETqXPinMES14I7q'
    url = f'https://api.spotify.com/v1/artists/{artistId}'

    # Define headers
    headers = {
        'Authorization': f'Bearer {accessToken}'
    }

    # Make a GET request
    response = requests.get(url, headers=headers)
    artistData = response.json()

    return artistData


def getRecentlyPlayedTracks(accessToken, queryFrom):

    url = f'https://api.spotify.com/v1/me/player/recently-played?limit=50&after={queryFrom}'

    headers = {
        'Authorization': f'Bearer {accessToken}'
    }
    
    response = requests.get(url=url, headers=headers)
    data = response.json()

    # Save the retrieved data to a JSON file
    # with open('tracks.json', 'w') as f:
    #     json.dump(data, f)

    return data


def renameColumns(col):
    parts = col.split('.')
    parts = [part.split('_') for part in parts]
    flattened = [item for sublist in parts for item in sublist]
    return flattened[0].lower() + ''.join(word.capitalize() for word in flattened[1:])


def transformSpotifyTrackData(data, currentDatetime):

    for index, item in enumerate(data['items']):
        data['items'][index]['track']['artists'] = data['items'][index]['track']['artists'][0]
        data['items'][index]['track']['album']['images'] = data['items'][index]['track']['album']['images'][0]

    # Flatten the JSON data using pandas
    flattenedData = pd.json_normalize(data, record_path='items',)

    columnsToKeep = [
        'track.name',
        'track.explicit',
        'track.popularity',
        'track.id',
        'track.track_number',
        'track.type',
        'played_at',
        'context.type',
        'context.external_urls.spotify',
        'track.artists.id',
        'track.artists.name',
        'track.artists.type',
        'track.album.album_type',
        'track.album.id',
        'track.album.images.url',
        'track.album.images.height',
        'track.album.name',
        'track.album.release_date',
        'track.album.release_date_precision',
        'track.album.total_tracks',
        'track.duration_ms'
    ]

    flattenedData = flattenedData[columnsToKeep]

    betterColumnNames = {col: renameColumns(col) for col in flattenedData.columns}

    flattenedData = flattenedData.rename(columns=betterColumnNames)

    flattenedData['queriedAt'] = currentDatetime

    return(flattenedData)

def sendResponse(statusCode, message, e = ''):
    if e == '':
        sep = ''
    else:
        sep = '\n'

    response = {
        'status': statusCode,
        'message': f'{message}{sep}{e}'
    }
    print(response)
    return response

def trackData_handler(event, context):
    currentDatetime = datetime.datetime.now(datetime.UTC)
    
    lastQueriedFrom = pd.Timestamp('1970-01-01 00:00:00', tz='UTC')

    # get environmenmt variables
    load_dotenv()

    clientId = os.getenv('SPOTIFY_CLIENT_ID')
    clientSecret = os.getenv('SPOTIFY_CLIENT_SECRET')
    refreshToken = os.getenv('REFRESH_TOKEN')
    authorizationCode = os.getenv('AUTHORIZATION_CODE')
    credentialsPath = os.getenv('SERVICE_ACCOUNT_PATH')
    destinationTableId = os.getenv('TABLE_ID')

    bqTableReference = TableReference.from_string(destinationTableId)
    tableSchema = returnSchema()
    bigQuerySchema = [SchemaField(field['name'], field['type']) for field in tableSchema]

    # set up BigQuery client
    try:
        bigQueryclient = bigquery.Client(credentials=service_account.Credentials.from_service_account_file(credentialsPath))
    except Exception as e:
        return sendResponse(500, 'Failed to set up BigQuery client', e)
    
    # check if destination table exists in BigQUery
    if tableExists(bqTableReference, bigQueryclient):
        # query max playedAt
        strQuery = f'SELECT MAX(playedAt) FROM `{destinationTableId}`'
        result = runBigQueryQuery(strQuery, bigQueryclient)
        for row in result:
            lastQueriedFrom = row[0]
            queryFrom = int(row[0].timestamp())
    else:
        queryFrom = '0000000001'
        createBigQueryTable(bqTableReference, bigQuerySchema, bigQueryclient)

    # queryFromTimestamp = pd.to_datetime(queryFrom).tz_localize('UTC')
    
    # if lastQueriedFrom:
    #     print(f'querying from {lastQueriedFrom}')
    # else:
    #     lastQueriedFrom = 'beginning of time'

    # get access token using your refresh token
    try:
        token = refreshAccessToken(clientId, clientSecret, refreshToken, authorizationCode)
    except Exception as e:
        return sendResponse(500, 'Failed to refresh access token.', e)
    
    # retrieve the data from the API
    try:
        trackData = getRecentlyPlayedTracks(token, queryFrom)
    except Exception as e:
        return sendResponse(500, 'Failed to retrieve the data from the API.', e)  
       
    # flatten the json and keep the data you want
    try:
        transformedData = transformSpotifyTrackData(trackData, currentDatetime)
    except Exception as e:
        return sendResponse(500, 'Failed to transform the data.', e)
    
    # coerce the data types before pushing stuff to bigQuery
    try:
        for field in tableSchema:
            colName = field['name']
            dataType = field['type']
            
            if dataType == 'INTEGER':
                transformedData[colName] = pd.to_numeric(transformedData[colName], errors='coerce').astype('Int64')
            elif dataType == 'FLOAT': 
                transformedData[colName] = pd.to_numeric(transformedData[colName], errors='coerce')
            elif dataType == 'STRING':
                transformedData[colName] = transformedData[colName].apply(lambda x: str(x) if x is not None else None)
            elif dataType == 'TIMESTAMP':
                transformedData[colName] = pd.to_datetime(transformedData[colName], errors='coerce')

        # Replace NaN values with None
        transformedData = transformedData.where(pd.notnull(transformedData), None)

    except Exception as e:
        return sendResponse(500, 'Failed to apply data validation.', e)

    transformedData = transformedData[transformedData['playedAt'] > lastQueriedFrom]

    # send the data to BigQuery
    try:
        pandas_gbq.to_gbq(dataframe=transformedData,
                        project_id=destinationTableId.split('.')[0],
                        destination_table=destinationTableId, 
                        if_exists='append', 
                        table_schema=tableSchema,
                        credentials=service_account.Credentials.from_service_account_file(credentialsPath)
                        )
    except Exception as e:
        return sendResponse(500, 'Failed to upload the data to BigQuery.', e)    

    print(f'you have listened to {len(transformedData)} songs since {lastQueriedFrom}')

    return sendResponse(200, f'you have listened to {len(transformedData)} songs since {lastQueriedFrom}')    

    
