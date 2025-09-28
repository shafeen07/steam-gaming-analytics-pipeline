from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.bash import BashOperator
import logging
import json

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Default arguments for the DAG
default_args = {
    'owner': 'steam-analytics',
    'depends_on_past': False,
    'start_date': datetime(2025, 9, 25, 13, 0),  # Start from today at 1 PM
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'catchup': False
}

# Create the DAG
dag = DAG(
    'steam_pipeline_monitoring',
    default_args=default_args,
    description='Monitor Steam analytics data pipeline health and performance',
    schedule_interval=timedelta(hours=2),  # Run every 2 hours
    max_active_runs=1,
    tags=['steam', 'monitoring', 'portfolio']
)

def check_data_pipeline_health(**context):
    """Check overall health of the Steam data pipeline."""
    
    postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    health_report = {
        'timestamp': datetime.now().isoformat(),
        'pipeline_status': 'HEALTHY',
        'data_sources': {},
        'issues': []
    }
    
    try:
        # Check player tracking data
        player_query = """
            SELECT 
                COUNT(*) as total_records,
                COUNT(DISTINCT app_id) as games_tracked,
                MAX(timestamp) as last_update,
                MIN(timestamp) as first_record
            FROM steam_player_tracking
        """
        
        result = postgres_hook.get_first(player_query)
        
        if result:
            total, games, last_update, first_record = result
            health_report['data_sources']['player_tracking'] = {
                'total_records': total,
                'games_tracked': games,
                'last_update': last_update.isoformat() if last_update else None,
                'first_record': first_record.isoformat() if first_record else None,
                'status': 'active' if total > 0 else 'empty'
            }
            
            if total == 0:
                health_report['issues'].append("No player tracking data found")
                health_report['pipeline_status'] = 'WARNING'
        
        # Check review data
        review_query = """
            SELECT 
                COUNT(*) as total_reviews,
                COUNT(*) FILTER (WHERE sentiment_label IS NOT NULL) as processed_reviews,
                COUNT(DISTINCT app_id) as games_with_reviews
            FROM steam_review_tracking
        """
        
        result = postgres_hook.get_first(review_query)
        
        if result:
            total_reviews, processed, games_reviews = result
            health_report['data_sources']['review_tracking'] = {
                'total_reviews': total_reviews,
                'processed_reviews': processed,
                'games_with_reviews': games_reviews,
                'processing_rate': round((processed / total_reviews) * 100, 1) if total_reviews > 0 else 0,
                'status': 'active' if total_reviews > 0 else 'empty'
            }
            
            if total_reviews == 0:
                health_report['issues'].append("No review data found")
        
        # Check game configuration
        config_query = """
            SELECT 
                COUNT(*) as configured_games,
                COUNT(*) FILTER (WHERE tracking_enabled = TRUE) as active_games
            FROM game_tracking_config
        """
        
        result = postgres_hook.get_first(config_query)
        
        if result:
            configured, active = result
            health_report['data_sources']['game_config'] = {
                'configured_games': configured,
                'active_games': active,
                'status': 'configured' if configured > 0 else 'empty'
            }
        
        # Determine overall status
        if len(health_report['issues']) == 0:
            health_report['pipeline_status'] = 'HEALTHY'
        elif len(health_report['issues']) <= 2:
            health_report['pipeline_status'] = 'WARNING'
        else:
            health_report['pipeline_status'] = 'CRITICAL'
        
        logger.info(f"Pipeline Health Check: {health_report['pipeline_status']} - {len(health_report['issues'])} issues")
        
    except Exception as e:
        health_report['pipeline_status'] = 'ERROR'
        health_report['issues'].append(f"Health check failed: {str(e)}")
        logger.error(f"Pipeline health check error: {e}")
    
    # Store results for next task
    context['task_instance'].xcom_push(key='health_report', value=health_report)
    
    return health_report

def generate_summary_report(**context):
    """Generate a summary report of pipeline performance."""
    
    postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
    health_report = context['task_instance'].xcom_pull(task_ids='check_pipeline_health', key='health_report')
    
    summary = {
        'report_timestamp': datetime.now().isoformat(),
        'pipeline_status': health_report.get('pipeline_status', 'UNKNOWN'),
        'summary_stats': {},
        'recommendations': []
    }
    
    try:
        # Get summary statistics
        stats_query = """
            WITH recent_activity AS (
                SELECT 
                    COUNT(*) as recent_player_records
                FROM steam_player_tracking 
                WHERE timestamp > NOW() - INTERVAL '24 hours'
            ),
            total_stats AS (
                SELECT 
                    COUNT(DISTINCT spt.app_id) as active_games,
                    AVG(spt.player_count) as avg_player_count,
                    MAX(spt.player_count) as peak_player_count
                FROM steam_player_tracking spt
                WHERE spt.timestamp > NOW() - INTERVAL '7 days'
            )
            SELECT 
                ra.recent_player_records,
                ts.active_games,
                ts.avg_player_count,
                ts.peak_player_count
            FROM recent_activity ra, total_stats ts
        """
        
        result = postgres_hook.get_first(stats_query)
        
        if result:
            recent_records, active_games, avg_players, peak_players = result
            
            summary['summary_stats'] = {
                'recent_activity_24h': recent_records or 0,
                'active_games': active_games or 0,
                'avg_player_count': round(avg_players, 0) if avg_players else 0,
                'peak_player_count': peak_players or 0
            }
            
            # Generate recommendations
            if recent_records == 0:
                summary['recommendations'].append("No recent data collection - check data ingestion services")
            elif recent_records < 50:  # Less than expected for 24 hours
                summary['recommendations'].append("Low data collection rate - monitor API performance")
            
            if active_games < 3:
                summary['recommendations'].append("Consider adding more games to tracking configuration")
        
        # Store the summary report
        create_table_sql = """
            CREATE TABLE IF NOT EXISTS pipeline_monitoring_reports (
                id SERIAL PRIMARY KEY,
                report_timestamp TIMESTAMP WITH TIME ZONE,
                pipeline_status VARCHAR(20),
                report_data JSONB,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            )
        """
        
        postgres_hook.run(create_table_sql)
        
        insert_sql = """
            INSERT INTO pipeline_monitoring_reports (report_timestamp, pipeline_status, report_data)
            VALUES (%s, %s, %s)
        """
        
        postgres_hook.run(insert_sql, parameters=[
            summary['report_timestamp'],
            summary['pipeline_status'],
            json.dumps(summary)
        ])
        
        logger.info("Pipeline monitoring report generated and stored")
        
    except Exception as e:
        logger.error(f"Failed to generate summary report: {e}")
        summary['error'] = str(e)
    
    return summary

def cleanup_old_reports(**context):
    """Clean up old monitoring reports to prevent database bloat."""
    
    postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    try:
        # Keep only last 72 reports (about 1 week with 2-hour intervals)
        cleanup_sql = """
            DELETE FROM pipeline_monitoring_reports 
            WHERE id NOT IN (
                SELECT id FROM pipeline_monitoring_reports 
                ORDER BY created_at DESC 
                LIMIT 72
            )
        """
        
        postgres_hook.run(cleanup_sql)
        logger.info("Old monitoring reports cleaned up")
        
        return "Cleanup completed successfully"
        
    except Exception as e:
        logger.error(f"Cleanup failed: {e}")
        return f"Cleanup failed: {str(e)}"

# Define tasks
health_check_task = PythonOperator(
    task_id='check_pipeline_health',
    python_callable=check_data_pipeline_health,
    dag=dag,
    doc_md="""
    ### Pipeline Health Check
    Monitors the overall health of the Steam analytics data pipeline by checking
    data availability, processing status, and configuration across all components.
    """
)

generate_report_task = PythonOperator(
    task_id='generate_summary_report',
    python_callable=generate_summary_report,
    dag=dag,
    doc_md="""
    ### Summary Report Generation
    Creates comprehensive summary reports of pipeline performance including
    data volume metrics, processing statistics, and operational recommendations.
    """
)

cleanup_task = PythonOperator(
    task_id='cleanup_old_reports',
    python_callable=cleanup_old_reports,
    dag=dag,
    doc_md="""
    ### Report Cleanup
    Maintains monitoring report database by removing old entries while preserving
    recent performance history for trend analysis.
    """
)

# Simple completion notification
completion_task = BashOperator(
    task_id='monitoring_complete',
    bash_command='echo "Steam Pipeline Monitoring completed successfully at $(date)"',
    dag=dag
)

# Set task dependencies
health_check_task >> generate_report_task >> cleanup_task >> completion_task