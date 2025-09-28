# Power BI Integration Guide

## Connecting Power BI to Sentiment Data

### Option 1: Direct PostgreSQL Connection
1. In Power BI Desktop, select "Get Data" > "PostgreSQL Database"
2. Server: localhost:5432
3. Database: airflow
4. Username: airflow
5. Password: airflow

### Option 2: REST API Connection
1. Use "Get Data" > "Web" 
2. URL: http://localhost:8000/powerbi/sentiment-data
3. For CSV format: http://localhost:8000/powerbi/sentiment-data?format=csv

### Key Tables for Analysis:
- `steam_review_tracking`: Detailed sentiment data
- `sentiment_insights`: Pre-calculated insights
- `steam_player_tracking`: Player count data for correlation

### Sample Power BI Queries:

```sql
-- Sentiment over time
SELECT 
    DATE_TRUNC('day', processed_timestamp) as date,
    sentiment_label,
    COUNT(*) as reviews,
    AVG(sentiment_score) as avg_score
FROM steam_review_tracking 
WHERE processed_timestamp > NOW() - INTERVAL '30 days'
GROUP BY DATE_TRUNC('day', processed_timestamp), sentiment_label
ORDER BY date;

-- Game sentiment rankings
SELECT 
    game_name,
    COUNT(*) as total_reviews,
    AVG(sentiment_score) as avg_sentiment,
    COUNT(*) FILTER (WHERE sentiment_label = 'positive') as positive_reviews
FROM steam_review_tracking 
WHERE processed_timestamp > NOW() - INTERVAL '7 days'
GROUP BY game_name
HAVING COUNT(*) >= 10
ORDER BY AVG(sentiment_score) DESC;
```

### Recommended Visualizations:
1. Sentiment trend line chart over time
2. Game sentiment ranking bar chart
3. Sentiment distribution pie chart
4. Correlation between player count and sentiment
5. Real-time sentiment dashboard with auto-refresh
