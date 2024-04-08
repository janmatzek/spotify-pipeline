def returnSchema():
    schema = [
        {'name': 'trackName', 'type': 'STRING'},
        {'name': 'trackExplicit', 'type': 'BOOLEAN'},
        {'name': 'trackPopularity', 'type': 'INTEGER'},
        {'name': 'trackId', 'type': 'STRING'},
        {'name': 'trackTrackNumber', 'type': 'INTEGER'},
        {'name': 'trackType', 'type': 'STRING'},
        {'name': 'playedAt', 'type': 'TIMESTAMP'},
        {'name': 'contextType', 'type': 'STRING'},
        {'name': 'contextExternalUrlsSpotify', 'type': 'STRING'},
        {'name': 'trackArtistsId', 'type': 'STRING'},
        {'name': 'trackArtistsName', 'type': 'STRING'},
        {'name': 'trackArtistsType', 'type': 'STRING'},
        {'name': 'trackAlbumAlbumType', 'type': 'STRING'},
        {'name': 'trackAlbumId', 'type': 'STRING'},
        {'name': 'trackAlbumImagesUrl', 'type': 'STRING'},
        {'name': 'trackAlbumImagesHeight', 'type': 'INTEGER'},
        {'name': 'trackAlbumName', 'type': 'STRING'},
        {'name': 'trackAlbumReleaseDate', 'type': 'STRING'},
        {'name': 'trackAlbumReleaseDatePrecision', 'type': 'STRING'},
        {'name': 'trackAlbumTotalTracks', 'type': 'INTEGER'},
        {'name': 'trackDurationMs', 'type': 'INTEGER'},
        {'name': 'queriedAt', 'type': 'TIMESTAMP'}
    ]
    return schema
