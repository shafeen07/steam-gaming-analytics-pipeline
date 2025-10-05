WITH player_metrics AS (
    SELECT 
        app_id,
        game_name,
        AVG(player_count) as avg_players,
        MAX(player_count) as peak_players
    FROM {{ ref('stg_player_tracking') }}
    GROUP BY app_id, game_name
),

sentiment_metrics AS (
    SELECT 
        app_id,
        game_name,
        AVG(sentiment_score) as avg_sentiment,
        COUNT(*) FILTER (WHERE sentiment_label = 'positive') as positive_reviews,
        COUNT(*) FILTER (WHERE sentiment_label = 'negative') as negative_reviews,
        COUNT(*) as total_reviews
    FROM {{ source('raw', 'steam_review_tracking') }}
    WHERE sentiment_label IS NOT NULL
    GROUP BY app_id, game_name
)

SELECT 
    p.app_id,
    p.game_name,
    p.avg_players,
    p.peak_players,
    s.avg_sentiment,
    s.positive_reviews,
    s.negative_reviews,
    s.total_reviews,
    ROUND(s.positive_reviews::NUMERIC / NULLIF(s.total_reviews, 0) * 100, 1) as positive_review_pct
FROM player_metrics p
LEFT JOIN sentiment_metrics s ON p.app_id = s.app_id