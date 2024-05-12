"""BigQuery schema for recently_played_tracks"""


def return_schema():
    """
    returns schema for the track data table
    """
    schema = [
        {"name": "track_name", "type": "STRING"},
        {"name": "track_explicit", "type": "BOOLEAN"},
        {"name": "track_popularity", "type": "INTEGER"},
        {"name": "track_id", "type": "STRING"},
        {"name": "track_track_number", "type": "INTEGER"},
        {"name": "track_type", "type": "STRING"},
        {"name": "played_at", "type": "TIMESTAMP"},
        {"name": "context_type", "type": "STRING"},
        {"name": "context_external_urls_spotify", "type": "STRING"},
        {"name": "track_artists_id", "type": "STRING"},
        {"name": "track_artists_name", "type": "STRING"},
        {"name": "track_artists_type", "type": "STRING"},
        {"name": "track_album_album_type", "type": "STRING"},
        {"name": "track_album_id", "type": "STRING"},
        {"name": "track_album_images_url", "type": "STRING"},
        {"name": "track_album_images_height", "type": "INTEGER"},
        {"name": "track_album_name", "type": "STRING"},
        {"name": "track_album_release_date", "type": "STRING"},
        {"name": "track_album_release_date_precision", "type": "STRING"},
        {"name": "track_album_total_tracks", "type": "INTEGER"},
        {"name": "track_duration_ms", "type": "INTEGER"},
        {"name": "queried_at", "type": "TIMESTAMP"},
    ]
    return schema
