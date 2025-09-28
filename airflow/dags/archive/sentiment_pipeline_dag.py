from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.bash import BashOperator
import logging
import json
import pandas as pd
from typing import Dict, List

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Default arguments for the DAG
default_args = {
    'owner': 'steam-analytics',
    'depends_on_past': False,
    'start_date': datetime(2025, 9, 25),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'catchup': False
}

# Create the DAG
dag = DAG(
    'steam_sentiment_pipeline',
    default_args=default_args,
    description='Orchestrate LLM sentiment analysis pipeline for Steam reviews - Direct DB Version',
    schedule_interval=timedelta(hours=2),  # Run every 2 hours
    max_active_runs=1,
    tags=['steam', 'sentiment', 'llm', 'pipeline']
)

def check_sentiment_consumer_health(**context):
    """Check if the sentiment analysis consumer is processing reviews properly."""
    
    postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    health_report = {
        'timestamp': datetime.now().isoformat(),
        'consumer_healthy': False,
        'processing_rate': 0,
        'recent_reviews': 0,
        'avg_processing_time': 0,
        'issues': []
    }
    
    try:
        # Check recent sentiment processing activity
        activity_query = """
            SELECT 
                COUNT(*) as recent_reviews,
                AVG(processing_time_ms) as avg_processing_time,
                MIN(processed_timestamp) as oldest_processed,
                MAX(processed_timestamp) as latest_processed
            FROM steam_review_tracking
            WHERE processed_timestamp > NOW() - INTERVAL '2 hours'
                AND sentiment_label IS NOT NULL
        """
        
        result = postgres_hook.get_first(activity_query)
        
        if result:
            recent_reviews, avg_time, oldest, latest = result
            
            health_report['recent_reviews'] = recent_reviews or 0
            health_report['avg_processing_time'] = avg_time or 0
            
            # Calculate processing rate (reviews per minute)
            if recent_reviews and oldest and latest:
                time_diff = (latest - oldest).total_seconds() / 60  # minutes
                if time_diff > 0:
                    health_report['processing_rate'] = recent_reviews / time_diff
            
            # Health checks
            if recent_reviews > 0:
                health_report['consumer_healthy'] = True
                logger.info(f"Sentiment consumer healthy: {recent_reviews} reviews processed")
            else:
                health_report['issues'].append("No recent sentiment processing detected")
            
            # Performance warnings
            if avg_time and avg_time > 10000:  # 10 seconds seems slow
                health_report['issues'].append(f"Slow processing: {avg_time:.0f}ms average")
        else:
            health_report['issues'].append("No sentiment processing data found")
    
    except Exception as e:
        health_report['issues'].append(f"Health check failed: {str(e)}")
        logger.error(f"Sentiment consumer health check failed: {e}")
    
    # Store health report for monitoring
    context['task_instance'].xcom_push(key='health_report', value=health_report)
    
    return health_report

def validate_sentiment_quality(**context):
    """Validate the quality of sentiment analysis results."""
    
    postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    quality_issues = []
    
    try:
        # Check for sentiment score distribution (should be reasonable)
        distribution_query = """
            SELECT 
                sentiment_label,
                COUNT(*) as count,
                AVG(sentiment_score) as avg_score,
                MIN(sentiment_score) as min_score,
                MAX(sentiment_score) as max_score
            FROM steam_review_tracking
            WHERE processed_timestamp > NOW() - INTERVAL '24 hours'
                AND sentiment_label IS NOT NULL
            GROUP BY sentiment_label
        """
        
        results = postgres_hook.get_records(distribution_query)
        
        if results:
            total_reviews = sum(row[1] for row in results)
            
            for label, count, avg_score, min_score, max_score in results:
                percentage = (count / total_reviews) * 100
                
                # Quality checks
                if label == 'positive' and avg_score < 0:
                    quality_issues.append(f"Positive reviews have negative avg score: {avg_score:.3f}")
                
                if label == 'negative' and avg_score > 0:
                    quality_issues.append(f"Negative reviews have positive avg score: {avg_score:.3f}")
                
                # Check for extreme skew (might indicate model issues)
                if percentage > 80:
                    quality_issues.append(f"Extreme sentiment skew: {percentage:.1f}% {label}")
        
        # Check for reviews with low confidence scores
        low_confidence_query = """
            SELECT COUNT(*) 
            FROM steam_review_tracking
            WHERE processed_timestamp > NOW() - INTERVAL '24 hours'
                AND sentiment_confidence < 0.3
                AND sentiment_label IS NOT NULL
        """
        
        low_conf_result = postgres_hook.get_first(low_confidence_query)
        low_confidence_count = low_conf_result[0] if low_conf_result else 0
        
        if low_confidence_count > 0:
            total_query = """
                SELECT COUNT(*) 
                FROM steam_review_tracking
                WHERE processed_timestamp > NOW() - INTERVAL '24 hours'
                    AND sentiment_label IS NOT NULL
            """
            total_result = postgres_hook.get_first(total_query)
            total_count = total_result[0] if total_result else 1
            
            low_conf_percentage = (low_confidence_count / total_count) * 100
            
            if low_conf_percentage > 20:  # More than 20% low confidence
                quality_issues.append(f"High low-confidence predictions: {low_conf_percentage:.1f}%")
        
        # Check for processing errors or timeouts
        timeout_query = """
            SELECT COUNT(*) 
            FROM steam_review_tracking
            WHERE processed_timestamp > NOW() - INTERVAL '24 hours'
                AND processing_time_ms > 30000  -- 30 seconds timeout
        """
        
        timeout_result = postgres_hook.get_first(timeout_query)
        timeout_count = timeout_result[0] if timeout_result else 0
        
        if timeout_count > 0:
            quality_issues.append(f"Processing timeouts detected: {timeout_count} reviews")
    
    except Exception as e:
        quality_issues.append(f"Quality validation failed: {str(e)}")
        logger.error(f"Sentiment quality validation failed: {e}")
    
    context['task_instance'].xcom_push(key='quality_issues', value=quality_issues)
    
    logger.info(f"Sentiment quality check: {len(quality_issues)} issues found")
    return quality_issues

def generate_sentiment_insights(**context):
    """Generate insights and trends from sentiment analysis data."""
    
    postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    insights = {
        'timestamp': datetime.now().isoformat(),
        'top_positive_games': [],
        'top_negative_games': [],
        'sentiment_trends': [],
        'review_volume': 0,
        'overall_sentiment': 'neutral'
    }
    
    try:
        # Get top positive games
        positive_query = """
            SELECT 
                app_id,
                game_name,
                COUNT(*) as review_count,
                AVG(sentiment_score) as avg_sentiment,
                COUNT(*) FILTER (WHERE sentiment_label = 'positive') as positive_reviews
            FROM steam_review_tracking
            WHERE processed_timestamp > NOW() - INTERVAL '24 hours'
                AND sentiment_label IS NOT NULL
            GROUP BY app_id, game_name
            HAVING COUNT(*) >= 5
                AND AVG(sentiment_score) > 0.5
            ORDER BY AVG(sentiment_score) DESC, COUNT(*) DESC
            LIMIT 5
        """
        
        positive_results = postgres_hook.get_records(positive_query)
        
        for app_id, game_name, count, avg_sentiment, positive_count in positive_results:
            insights['top_positive_games'].append({
                'app_id': app_id,
                'game_name': game_name,
                'review_count': count,
                'avg_sentiment': round(avg_sentiment, 3),
                'positive_percentage': round((positive_count / count) * 100, 1)
            })
        
        # Get top negative games
        negative_query = """
            SELECT 
                app_id,
                game_name,
                COUNT(*) as review_count,
                AVG(sentiment_score) as avg_sentiment,
                COUNT(*) FILTER (WHERE sentiment_label = 'negative') as negative_reviews
            FROM steam_review_tracking
            WHERE processed_timestamp > NOW() - INTERVAL '24 hours'
                AND sentiment_label IS NOT NULL
            GROUP BY app_id, game_name
            HAVING COUNT(*) >= 5
                AND AVG(sentiment_score) < -0.2
            ORDER BY AVG(sentiment_score) ASC, COUNT(*) DESC
            LIMIT 5
        """
        
        negative_results = postgres_hook.get_records(negative_query)
        
        for app_id, game_name, count, avg_sentiment, negative_count in negative_results:
            insights['top_negative_games'].append({
                'app_id': app_id,
                'game_name': game_name,
                'review_count': count,
                'avg_sentiment': round(avg_sentiment, 3),
                'negative_percentage': round((negative_count / count) * 100, 1)
            })
        
        # Get sentiment trends over last 24 hours
        trend_query = """
            SELECT 
                DATE_TRUNC('hour', processed_timestamp) as hour,
                COUNT(*) as review_count,
                AVG(sentiment_score) as avg_sentiment,
                COUNT(*) FILTER (WHERE sentiment_label = 'positive') as positive_count,
                COUNT(*) FILTER (WHERE sentiment_label = 'negative') as negative_count
            FROM steam_review_tracking
            WHERE processed_timestamp > NOW() - INTERVAL '24 hours'
                AND sentiment_label IS NOT NULL
            GROUP BY DATE_TRUNC('hour', processed_timestamp)
            ORDER BY hour DESC
            LIMIT 24
        """
        
        trend_results = postgres_hook.get_records(trend_query)
        
        for hour, count, avg_sentiment, positive, negative in trend_results:
            insights['sentiment_trends'].append({
                'hour': hour.isoformat(),
                'review_count': count,
                'avg_sentiment': round(avg_sentiment, 3),
                'positive_percentage': round((positive / count) * 100, 1) if count > 0 else 0,
                'negative_percentage': round((negative / count) * 100, 1) if count > 0 else 0
            })
        
        # Calculate overall metrics
        overall_query = """
            SELECT 
                COUNT(*) as total_reviews,
                AVG(sentiment_score) as overall_sentiment
            FROM steam_review_tracking
            WHERE processed_timestamp > NOW() - INTERVAL '24 hours'
                AND sentiment_label IS NOT NULL
        """
        
        overall_result = postgres_hook.get_first(overall_query)
        
        if overall_result:
            total_reviews, overall_sentiment_score = overall_result
            insights['review_volume'] = total_reviews or 0
            
            # Classify overall sentiment
            if overall_sentiment_score and overall_sentiment_score > 0.2:
                insights['overall_sentiment'] = 'positive'
            elif overall_sentiment_score and overall_sentiment_score < -0.2:
                insights['overall_sentiment'] = 'negative'
            else:
                insights['overall_sentiment'] = 'neutral'
    
    except Exception as e:
        logger.error(f"Failed to generate sentiment insights: {e}")
        insights['error'] = str(e)
    
    # Store insights for dashboard consumption
    try:
        # Create insights table if it doesn't exist
        create_table_sql = """
            CREATE TABLE IF NOT EXISTS sentiment_insights (
                id SERIAL PRIMARY KEY,
                insight_timestamp TIMESTAMP WITH TIME ZONE,
                insights_data JSONB,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            )
        """
        
        postgres_hook.run(create_table_sql)
        
        # Insert insights
        insert_sql = """
            INSERT INTO sentiment_insights (insight_timestamp, insights_data)
            VALUES (%s, %s)
        """
        
        postgres_hook.run(insert_sql, parameters=[
            insights['timestamp'],
            json.dumps(insights)
        ])
        
        logger.info("Sentiment insights generated and stored")
        
    except Exception as e:
        logger.error(f"Failed to store sentiment insights: {e}")
    
    context['task_instance'].xcom_push(key='insights', value=insights)
    
    return insights

def optimize_sentiment_performance(**context):
    """Optimize sentiment analysis performance and clean up old data."""
    
    postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    optimization_results = []
    
    try:
        # Clean up old sentiment data (keep last 7 days of detailed data)
        cleanup_sql = """
            DELETE FROM steam_review_tracking 
            WHERE processed_timestamp < NOW() - INTERVAL '7 days'
                AND sentiment_label IS NOT NULL
        """
        
        result = postgres_hook.run(cleanup_sql)
        optimization_results.append("Cleaned up old sentiment data")
        
        # Update table statistics for better query performance
        analyze_sql = "ANALYZE steam_review_tracking"
        postgres_hook.run(analyze_sql)
        optimization_results.append("Updated table statistics")
        
        # Check for duplicate review processing
        duplicate_check_sql = """
            SELECT review_id, COUNT(*) as duplicate_count
            FROM steam_review_tracking
            WHERE sentiment_label IS NOT NULL
            GROUP BY review_id
            HAVING COUNT(*) > 1
        """
        
        duplicates = postgres_hook.get_records(duplicate_check_sql)
        
        if duplicates:
            # Remove duplicate sentiment entries (keep most recent)
            dedup_sql = """
                DELETE FROM steam_review_tracking 
                WHERE id NOT IN (
                    SELECT DISTINCT ON (review_id) id
                    FROM steam_review_tracking
                    WHERE sentiment_label IS NOT NULL
                    ORDER BY review_id, processed_timestamp DESC
                )
                AND sentiment_label IS NOT NULL
            """
            
            postgres_hook.run(dedup_sql)
            optimization_results.append(f"Removed {len(duplicates)} duplicate sentiment entries")
        
        # Vacuum the table to reclaim space
        vacuum_sql = "VACUUM ANALYZE steam_review_tracking"
        postgres_hook.run(vacuum_sql)
        optimization_results.append("Vacuumed sentiment table")
        
        logger.info(f"Sentiment optimization completed: {len(optimization_results)} operations")
        
    except Exception as e:
        logger.error(f"Sentiment optimization failed: {e}")
        optimization_results.append(f"Optimization failed: {str(e)}")
    
    return optimization_results

def send_sentiment_report(**context):
    """Generate and send a comprehensive sentiment analysis report."""
    
    # Pull results from previous tasks
    health_report = context['task_instance'].xcom_pull(task_ids='check_sentiment_health', key='health_report')
    quality_issues = context['task_instance'].xcom_pull(task_ids='validate_sentiment_quality', key='quality_issues')
    insights = context['task_instance'].xcom_pull(task_ids='generate_sentiment_insights', key='insights')
    
    # Compile comprehensive report
    report = {
        'report_timestamp': datetime.now().isoformat(),
        'pipeline_status': 'HEALTHY',
        'summary': {
            'reviews_processed': health_report.get('recent_reviews', 0) if health_report else 0,
            'processing_rate': round(health_report.get('processing_rate', 0), 2) if health_report else 0,
            'quality_issues': len(quality_issues) if quality_issues else 0,
            'overall_sentiment': insights.get('overall_sentiment', 'unknown') if insights else 'unknown',
            'review_volume': insights.get('review_volume', 0) if insights else 0
        },
        'health_status': health_report or {},
        'quality_issues': quality_issues or [],
        'insights': insights or {},
        'recommendations': []
    }
    
    # Determine overall pipeline status
    total_issues = len(quality_issues) if quality_issues else 0
    
    if not (health_report and health_report.get('consumer_healthy', False)):
        report['pipeline_status'] = 'CRITICAL'
        report['recommendations'].append("Sentiment consumer appears to be down - check service logs")
    elif total_issues > 5:
        report['pipeline_status'] = 'WARNING'
        report['recommendations'].append("Multiple quality issues detected - review model performance")
    elif health_report and health_report.get('processing_rate', 0) < 1:
        report['pipeline_status'] = 'WARNING'
        report['recommendations'].append("Low processing rate - consider scaling up resources")
    
    # Add performance recommendations
    if health_report:
        avg_processing_time = health_report.get('avg_processing_time', 0)
        if avg_processing_time > 5000:  # 5 seconds
            report['recommendations'].append("Consider optimizing LLM backend for faster processing")
    
    if report['summary']['review_volume'] > 1000:
        report['recommendations'].append("High review volume - monitor resource usage")
    
    logger.info(f"Sentiment Pipeline Report: {report['pipeline_status']} - {total_issues} issues")
    
    # Store report for monitoring
    postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    try:
        # Create reports table if it doesn't exist
        create_table_sql = """
            CREATE TABLE IF NOT EXISTS sentiment_pipeline_reports (
                id SERIAL PRIMARY KEY,
                report_timestamp TIMESTAMP WITH TIME ZONE,
                pipeline_status VARCHAR(20),
                total_issues INTEGER,
                report_details JSONB,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            )
        """
        
        postgres_hook.run(create_table_sql)
        
        # Insert the report
        insert_sql = """
            INSERT INTO sentiment_pipeline_reports 
            (report_timestamp, pipeline_status, total_issues, report_details)
            VALUES (%s, %s, %s, %s)
        """
        
        postgres_hook.run(insert_sql, parameters=[
            report['report_timestamp'],
            report['pipeline_status'],
            total_issues,
            json.dumps(report)
        ])
        
        logger.info("Sentiment pipeline report stored successfully")
        
    except Exception as e:
        logger.error(f"Failed to store sentiment report: {e}")
    
    return report

# Define tasks
check_health_task = PythonOperator(
    task_id='check_sentiment_health',
    python_callable=check_sentiment_consumer_health,
    dag=dag,
    doc_md="""
    ### Sentiment Consumer Health Check
    Monitors the sentiment analysis consumer service to ensure it's processing
    reviews and generating sentiment scores properly.
    """
)

validate_quality_task = PythonOperator(
    task_id='validate_sentiment_quality',
    python_callable=validate_sentiment_quality,
    dag=dag,
    doc_md="""
    ### Sentiment Quality Validation
    Validates the quality of sentiment analysis results, checking for
    reasonable score distributions and confidence levels.
    """
)

generate_insights_task = PythonOperator(
    task_id='generate_sentiment_insights',
    python_callable=generate_sentiment_insights,
    dag=dag,
    doc_md="""
    ### Sentiment Insights Generation
    Generates business insights from sentiment analysis data including
    trending games, sentiment patterns, and performance metrics.
    """
)

optimize_performance_task = PythonOperator(
    task_id='optimize_sentiment_performance',
    python_callable=optimize_sentiment_performance,
    dag=dag,
    doc_md="""
    ### Performance Optimization
    Optimizes sentiment analysis performance by cleaning old data,
    updating statistics, and removing duplicates.
    """
)

generate_report_task = PythonOperator(
    task_id='generate_sentiment_report',
    python_callable=send_sentiment_report,
    dag=dag,
    doc_md="""
    ### Sentiment Pipeline Report
    Compiles comprehensive report on sentiment analysis pipeline health,
    quality, and insights for monitoring and alerting.
    """
)

# Additional SQL task to ensure indexes exist for performance
create_sentiment_indexes = PostgresOperator(
    task_id='create_sentiment_indexes',
    postgres_conn_id='postgres_default',
    sql="""
        -- Create indexes for sentiment analysis performance
        CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_sentiment_processed_timestamp 
        ON steam_review_tracking(processed_timestamp) 
        WHERE sentiment_label IS NOT NULL;
        
        CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_sentiment_app_processed 
        ON steam_review_tracking(app_id, processed_timestamp) 
        WHERE sentiment_label IS NOT NULL;
        
        CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_sentiment_label_score 
        ON steam_review_tracking(sentiment_label, sentiment_score) 
        WHERE sentiment_label IS NOT NULL;
        
        -- Create partial index for recent sentiment data
        CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_sentiment_recent 
        ON steam_review_tracking(app_id, sentiment_score, processed_timestamp) 
        WHERE processed_timestamp > NOW() - INTERVAL '7 days' 
        AND sentiment_label IS NOT NULL;
    """,
    dag=dag
)

# Health check for the entire sentiment pipeline
pipeline_health_check = BashOperator(
    task_id='pipeline_health_check',
    bash_command="""
    echo "Steam Sentiment Analysis Pipeline completed successfully at $(date)"
    echo "Next sentiment analysis cycle scheduled for: $(date -d '+2 hours')"
    echo "Check sentiment processor status..."
    docker ps --format "table {{.Names}}\t{{.Status}}" | grep sentiment-processor || echo "Sentiment processor not running"
    """,
    dag=dag
)

# Set task dependencies
create_sentiment_indexes >> [check_health_task, validate_quality_task] 
[check_health_task, validate_quality_task] >> generate_insights_task
generate_insights_task >> optimize_performance_task 
optimize_performance_task >> generate_report_task 
generate_report_task >> pipeline_health_check