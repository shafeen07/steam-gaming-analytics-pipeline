from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
import psycopg2
import os
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import logging
import pandas as pd
from pydantic import BaseModel

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Steam Sentiment Analytics API",
    description="Real-time sentiment analysis for Steam game reviews",
    version="1.0.0"
)

# Add CORS middleware for dashboard integration
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure appropriately for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Database connection
def get_db_connection():
    return psycopg2.connect(
        host=os.getenv('POSTGRES_HOST', 'postgres'),
        database=os.getenv('POSTGRES_DB', 'airflow'),
        user=os.getenv('POSTGRES_USER', 'airflow'),
        password=os.getenv('POSTGRES_PASSWORD', 'airflow')
    )

class SentimentSummary(BaseModel):
    app_id: int
    game_name: str
    total_reviews: int
    positive_count: int
    negative_count: int
    neutral_count: int
    avg_sentiment_score: float
    sentiment_trend: str

class GameSentimentDetail(BaseModel):
    app_id: int
    game_name: str
    recent_reviews: List[Dict]
    sentiment_distribution: Dict[str, int]
    hourly_sentiment: List[Dict]

@app.get("/")
async def root():
    """API health check and information."""
    return {
        "message": "Steam Sentiment Analytics API",
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "endpoints": {
            "sentiment_summary": "/sentiment/summary",
            "game_sentiment": "/sentiment/game/{app_id}",
            "trending_games": "/sentiment/trending",
            "sentiment_metrics": "/metrics/sentiment"
        }
    }

@app.get("/sentiment/summary", response_model=List[SentimentSummary])
async def get_sentiment_summary(
    limit: int = Query(10, description="Number of games to return"),
    hours: int = Query(24, description="Hours of data to analyze")
):
    """Get sentiment summary for top games."""
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        query = """
            SELECT 
                app_id,
                game_name,
                COUNT(*) as total_reviews,
                SUM(CASE WHEN sentiment_label = 'positive' THEN 1 ELSE 0 END) as positive_count,
                SUM(CASE WHEN sentiment_label = 'negative' THEN 1 ELSE 0 END) as negative_count,
                SUM(CASE WHEN sentiment_label = 'neutral' THEN 1 ELSE 0 END) as neutral_count,
                AVG(sentiment_score) as avg_sentiment_score
            FROM steam_review_tracking 
            WHERE processed_timestamp > NOW() - INTERVAL '%s hours'
                AND sentiment_label IS NOT NULL
            GROUP BY app_id, game_name
            HAVING COUNT(*) >= 5
            ORDER BY COUNT(*) DESC, AVG(sentiment_score) DESC
            LIMIT %s
        """
        
        cur.execute(query, (hours, limit))
        results = cur.fetchall()
        
        summaries = []
        for row in results:
            app_id, game_name, total, positive, negative, neutral, avg_score = row
            
            # Determine sentiment trend
            positive_ratio = positive / total if total > 0 else 0
            if positive_ratio > 0.6:
                trend = "very positive"
            elif positive_ratio > 0.4:
                trend = "positive"
            elif positive_ratio > 0.3:
                trend = "mixed"
            else:
                trend = "negative"
            
            summaries.append(SentimentSummary(
                app_id=app_id,
                game_name=game_name,
                total_reviews=total,
                positive_count=positive,
                negative_count=negative,
                neutral_count=neutral,
                avg_sentiment_score=round(avg_score or 0, 3),
                sentiment_trend=trend
            ))
        
        cur.close()
        conn.close()
        
        return summaries
        
    except Exception as e:
        logger.error(f"Failed to get sentiment summary: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve sentiment data")

@app.get("/sentiment/game/{app_id}", response_model=GameSentimentDetail)
async def get_game_sentiment(
    app_id: int,
    hours: int = Query(24, description="Hours of data to analyze")
):
    """Get detailed sentiment analysis for a specific game."""
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        # Get game name
        cur.execute("SELECT game_name FROM steam_review_tracking WHERE app_id = %s LIMIT 1", (app_id,))
        game_result = cur.fetchone()
        
        if not game_result:
            raise HTTPException(status_code=404, detail="Game not found")
        
        game_name = game_result[0]
        
        # Get recent reviews
        recent_query = """
            SELECT review_text, sentiment_label, sentiment_score, review_timestamp, review_helpful
            FROM steam_review_tracking 
            WHERE app_id = %s 
                AND processed_timestamp > NOW() - INTERVAL '%s hours'
                AND sentiment_label IS NOT NULL
            ORDER BY processed_timestamp DESC
            LIMIT 20
        """
        
        cur.execute(recent_query, (app_id, hours))
        recent_reviews = []
        
        for row in cur.fetchall():
            review_text, sentiment_label, sentiment_score, review_timestamp, helpful = row
            recent_reviews.append({
                "text": review_text[:200] + "..." if len(review_text) > 200 else review_text,
                "sentiment": sentiment_label,
                "score": round(sentiment_score, 3),
                "timestamp": review_timestamp.isoformat() if review_timestamp else None,
                "helpful_votes": helpful or 0
            })
        
        # Get sentiment distribution
        distribution_query = """
            SELECT sentiment_label, COUNT(*)
            FROM steam_review_tracking 
            WHERE app_id = %s 
                AND processed_timestamp > NOW() - INTERVAL '%s hours'
                AND sentiment_label IS NOT NULL
            GROUP BY sentiment_label
        """
        
        cur.execute(distribution_query, (app_id, hours))
        sentiment_distribution = {}
        for label, count in cur.fetchall():
            sentiment_distribution[label] = count
        
        # Get hourly sentiment trend
        hourly_query = """
            SELECT 
                DATE_TRUNC('hour', processed_timestamp) as hour,
                sentiment_label,
                COUNT(*) as count,
                AVG(sentiment_score) as avg_score
            FROM steam_review_tracking 
            WHERE app_id = %s 
                AND processed_timestamp > NOW() - INTERVAL '%s hours'
                AND sentiment_label IS NOT NULL
            GROUP BY DATE_TRUNC('hour', processed_timestamp), sentiment_label
            ORDER BY hour DESC
        """
        
        cur.execute(hourly_query, (app_id, hours))
        hourly_data = []
        
        for row in cur.fetchall():
            hour, sentiment, count, avg_score = row
            hourly_data.append({
                "hour": hour.isoformat(),
                "sentiment": sentiment,
                "count": count,
                "avg_score": round(avg_score, 3)
            })
        
        cur.close()
        conn.close()
        
        return GameSentimentDetail(
            app_id=app_id,
            game_name=game_name,
            recent_reviews=recent_reviews,
            sentiment_distribution=sentiment_distribution,
            hourly_sentiment=hourly_data
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get game sentiment: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve game sentiment data")

@app.get("/sentiment/trending")
async def get_trending_sentiment():
    """Get trending sentiment changes in the last few hours."""
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        # Compare sentiment in last 4 hours vs previous 4 hours
        query = """
            WITH recent_sentiment AS (
                SELECT 
                    app_id,
                    game_name,
                    AVG(sentiment_score) as recent_avg,
                    COUNT(*) as recent_count
                FROM steam_review_tracking 
                WHERE processed_timestamp > NOW() - INTERVAL '4 hours'
                    AND sentiment_label IS NOT NULL
                GROUP BY app_id, game_name
                HAVING COUNT(*) >= 3
            ),
            previous_sentiment AS (
                SELECT 
                    app_id,
                    game_name,
                    AVG(sentiment_score) as previous_avg,
                    COUNT(*) as previous_count
                FROM steam_review_tracking 
                WHERE processed_timestamp BETWEEN NOW() - INTERVAL '8 hours' AND NOW() - INTERVAL '4 hours'
                    AND sentiment_label IS NOT NULL
                GROUP BY app_id, game_name
                HAVING COUNT(*) >= 3
            )
            SELECT 
                r.app_id,
                r.game_name,
                r.recent_avg,
                p.previous_avg,
                r.recent_avg - p.previous_avg as sentiment_change,
                r.recent_count + p.previous_count as total_reviews
            FROM recent_sentiment r
            JOIN previous_sentiment p ON r.app_id = p.app_id
            WHERE ABS(r.recent_avg - p.previous_avg) > 0.1
            ORDER BY ABS(r.recent_avg - p.previous_avg) DESC
            LIMIT 10
        """
        
        cur.execute(query)
        trending = []
        
        for row in cur.fetchall():
            app_id, game_name, recent_avg, previous_avg, change, total_reviews = row
            
            trend_direction = "improving" if change > 0 else "declining"
            
            trending.append({
                "app_id": app_id,
                "game_name": game_name,
                "recent_sentiment": round(recent_avg, 3),
                "previous_sentiment": round(previous_avg, 3),
                "sentiment_change": round(change, 3),
                "trend_direction": trend_direction,
                "total_reviews": total_reviews
            })
        
        cur.close()
        conn.close()
        
        return {"trending_games": trending}
        
    except Exception as e:
        logger.error(f"Failed to get trending sentiment: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve trending data")

@app.get("/metrics/sentiment")
async def get_sentiment_metrics():
    """Get overall sentiment processing metrics."""
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        # Overall processing stats
        stats_query = """
            SELECT 
                COUNT(*) as total_processed,
                COUNT(DISTINCT app_id) as games_analyzed,
                AVG(processing_time_ms) as avg_processing_time,
                AVG(sentiment_confidence) as avg_confidence,
                COUNT(*) FILTER (WHERE processed_timestamp > NOW() - INTERVAL '1 hour') as processed_last_hour,
                COUNT(*) FILTER (WHERE processed_timestamp > NOW() - INTERVAL '24 hours') as processed_last_24h
            FROM steam_review_tracking 
            WHERE sentiment_label IS NOT NULL
        """
        
        cur.execute(stats_query)
        stats = cur.fetchone()
        
        # Sentiment distribution
        distribution_query = """
            SELECT sentiment_label, COUNT(*) 
            FROM steam_review_tracking 
            WHERE sentiment_label IS NOT NULL
                AND processed_timestamp > NOW() - INTERVAL '24 hours'
            GROUP BY sentiment_label
        """
        
        cur.execute(distribution_query)
        distribution = {}
        for label, count in cur.fetchall():
            distribution[label] = count
        
        # Processing rate over time
        rate_query = """
            SELECT 
                DATE_TRUNC('hour', processed_timestamp) as hour,
                COUNT(*) as reviews_processed,
                AVG(processing_time_ms) as avg_processing_time
            FROM steam_review_tracking 
            WHERE sentiment_label IS NOT NULL
                AND processed_timestamp > NOW() - INTERVAL '24 hours'
            GROUP BY DATE_TRUNC('hour', processed_timestamp)
            ORDER BY hour DESC
        """
        
        cur.execute(rate_query)
        processing_rate = []
        for row in cur.fetchall():
            hour, count, avg_time = row
            processing_rate.append({
                "hour": hour.isoformat(),
                "reviews_processed": count,
                "avg_processing_time_ms": round(avg_time or 0, 2)
            })
        
        cur.close()
        conn.close()
        
        total, games, avg_time, avg_conf, last_hour, last_24h = stats
        
        return {
            "overall_stats": {
                "total_reviews_processed": total or 0,
                "games_analyzed": games or 0,
                "avg_processing_time_ms": round(avg_time or 0, 2),
                "avg_confidence": round(avg_conf or 0, 3),
                "processed_last_hour": last_hour or 0,
                "processed_last_24h": last_24h or 0
            },
            "sentiment_distribution": distribution,
            "processing_rate": processing_rate
        }
        
    except Exception as e:
        logger.error(f"Failed to get sentiment metrics: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve metrics")

# Additional endpoints for Power BI integration
@app.get("/powerbi/sentiment-data")
async def get_powerbi_sentiment_data(
    hours: int = Query(24, description="Hours of data to include"),
    format: str = Query("json", description="Output format: json or csv")
):
    """Get sentiment data formatted for Power BI consumption."""
    try:
        conn = get_db_connection()
        
        query = """
            SELECT 
                app_id,
                game_name,
                sentiment_label,
                sentiment_score,
                sentiment_confidence,
                review_helpful,
                processed_timestamp,
                DATE_TRUNC('hour', processed_timestamp) as hour_bucket
            FROM steam_review_tracking 
            WHERE sentiment_label IS NOT NULL
                AND processed_timestamp > NOW() - INTERVAL '%s hours'
            ORDER BY processed_timestamp DESC
        """
        
        # Use pandas for easy format conversion
        df = pd.read_sql_query(query, conn, params=(hours,))
        conn.close()
        
        if format.lower() == "csv":
            from fastapi.responses import Response
            csv_data = df.to_csv(index=False)
            return Response(
                content=csv_data,
                media_type="text/csv",
                headers={"Content-Disposition": "attachment; filename=sentiment_data.csv"}
            )
        else:
            return df.to_dict(orient='records')
        
    except Exception as e:
        logger.error(f"Failed to get Power BI data: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve Power BI data")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)