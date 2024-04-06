import requests
import os
import json
import datetime
import base64
import pandas as pd
from dotenv import load_dotenv

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


def getRecentlyPlayedTracks(accessToken: str, currentDatetime: pd.Timestamp):

    # currentDatetime = datetime.datetime.now(datetime.UTC)
    someTimeAgo = currentDatetime - datetime.timedelta(hours=2)
    unixTimestamp = int(someTimeAgo.timestamp())
    after = unixTimestamp
    print(f'querying from {someTimeAgo} (unix time code {unixTimestamp})')
    url = f'https://api.spotify.com/v1/me/player/recently-played?limit=50&after={after}'

    headers = {
        'Authorization': f'Bearer {accessToken}'
    }
    
    response = requests.get(url=url, headers=headers)
    data = response.json()

    # Save the retrieved data to a JSON file
    with open('tracks.json', 'w') as f:
        json.dump(data, f)

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

    # flattenedData.to_csv('testOutput.csv', index=False)

    return(flattenedData)

def main():

    currentDatetime = datetime.datetime.now(datetime.UTC)

    # get environmenmt variables
    load_dotenv()

    clientId = os.getenv('SPOTIFY_CLIENT_ID')
    clientSecret = os.getenv('SPOTIFY_CLIENT_SECRET')
    refreshToken = os.getenv('REFRESH_TOKEN')
    authorizationCode = os.getenv('AUTHORIZATION_CODE')

    # get access token using your refresh token
    token = refreshAccessToken(clientId, clientSecret, refreshToken, authorizationCode)

    # retrieve the data from the API
    trackData = getRecentlyPlayedTracks(token, currentDatetime)
    
    # flatten the json and keep the data you want
    transformedData = transformSpotifyTrackData(trackData, currentDatetime)

    # send the data to BigQuery

    return 'Done'

print(main())
    
