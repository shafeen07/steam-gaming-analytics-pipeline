SELECT 
    id,
    app_id,
    game_name,
    player_count,
    timestamp,
    api_response_time_ms,
    DATE(timestamp) as date,
    EXTRACT(hour FROM timestamp) as hour_of_day,
    EXTRACT(dow FROM timestamp) as day_of_week
FROM {{ source('raw', 'steam_player_tracking') }}
WHERE player_count > 0