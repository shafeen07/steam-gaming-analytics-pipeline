from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
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
    'start_date': datetime(2025, 9, 25),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'catchup': False
}

# Create the DAG
dag = DAG(
    'steam_data_quality_monitoring',
    default_args=default_args,
    description='Monitor data quality for Steam analytics pipeline - Direct DB Version',
    schedule_interval=timedelta(hours=1),
    max_active_runs=1,
    tags=['steam', 'data-quality', 'monitoring']
)

def check_data_freshness(**context):
    """Check if data is being updated regularly across tracking tables."""
    
    # Use Airflow's connection management system
    postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    # Tables that should have recent data based on your actual implementation
    tables_to_check = [
        ('steam_player_tracking', 'timestamp', 15),  # Should update every ~15 minutes
        ('steam_review_tracking', 'processed_timestamp', 60),  # Sentiment processing
        ('game_tracking_config', 'last_updated', 1440)  # Config changes (24 hours)
    ]
    
    quality_report = {
        'timestamp': datetime.now().isoformat(),
        'checks_passed': 0,
        'checks_failed': 0,
        'issues': [],
        'table_status': {}
    }
    
    for table, timestamp_col, max_minutes_old in tables_to_check:
        try:
            # Check when data was last inserted/updated
            query = f"""
                SELECT 
                    COUNT(*) as total_records,
                    MAX({timestamp_col}) as last_update,
                    EXTRACT(EPOCH FROM (NOW() - MAX({timestamp_col})))/60 as minutes_since_update
                FROM {table}
                WHERE {timestamp_col} IS NOT NULL
            """
            
            result = postgres_hook.get_first(query)
            
            if result:
                total_records, last_update, minutes_since = result
                
                table_status = {
                    'total_records': total_records,
                    'last_update': last_update.isoformat() if last_update else None,
                    'minutes_since_update': round(minutes_since, 1) if minutes_since else None,
                    'status': 'unknown'
                }
                
                logger.info(f"{table}: {total_records} records, last update {minutes_since:.1f} minutes ago")
                
                # Check if data is stale
                if minutes_since and minutes_since > max_minutes_old:
                    quality_report['checks_failed'] += 1
                    quality_report['issues'].append(f"{table}: Data is {minutes_since:.1f} minutes old (max: {max_minutes_old})")
                    table_status['status'] = 'stale'
                elif total_records > 0:
                    quality_report['checks_passed'] += 1
                    table_status['status'] = 'healthy'
                else:
                    quality_report['checks_failed'] += 1
                    quality_report['issues'].append(f"{table}: No records found")
                    table_status['status'] = 'empty'
                    
                quality_report['table_status'][table] = table_status
            else:
                quality_report['checks_failed'] += 1
                quality_report['issues'].append(f"{table}: No data found or table doesn't exist")
                quality_report['table_status'][table] = {'status': 'missing'}
                
        except Exception as e:
            quality_report['checks_failed'] += 1
            quality_report['issues'].append(f"{table}: Query failed - {str(e)}")
            quality_report['table_status'][table] = {'status': 'error', 'error': str(e)}
    
    # Store results for downstream tasks
    context['task_instance'].xcom_push(key='quality_report', value=quality_report)
    
    logger.info(f"Data freshness check complete: {quality_report['checks_passed']} passed, {quality_report['checks_failed']} failed")
    
    return quality_report

def validate_data_completeness(**context):
    """Check for data completeness issues in your actual data."""
    
    postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    completeness_issues = []
    
    try:
        # Check if all configured games have recent player data
        missing_data_query = """
            SELECT 
                gtc.app_id,
                gtc.game_name,
                COUNT(spt.id) as recent_records,
                MAX(spt.timestamp) as last_player_update
            FROM game_tracking_config gtc
            LEFT JOIN steam_player_tracking spt ON gtc.app_id = spt.app_id 
                AND spt.timestamp > NOW() - INTERVAL '2 hours'
            WHERE gtc.tracking_enabled = TRUE AND gtc.track_players = TRUE
            GROUP BY gtc.app_id, gtc.game_name
            ORDER BY recent_records ASC
        """
        
        missing_data = postgres_hook.get_records(missing_data_query)
        
        if missing_data:
            for app_id, game_name, count, last_update in missing_data:
                if count == 0:
                    completeness_issues.append(f"No recent player data for {game_name} (ID: {app_id})")
                elif count < 3:  # Less than expected for 2-hour window
                    completeness_issues.append(f"Low data frequency for {game_name}: {count} records in 2 hours")
        
        # Check for suspicious player count patterns
        anomaly_detection_query = """
            WITH player_stats AS (
                SELECT 
                    app_id,
                    game_name,
                    player_count,
                    LAG(player_count) OVER (PARTITION BY app_id ORDER BY timestamp) as prev_count,
                    timestamp
                FROM steam_player_tracking
                WHERE timestamp > NOW() - INTERVAL '4 hours'
                ORDER BY app_id, timestamp
            )
            SELECT 
                app_id,
                game_name,
                player_count,
                prev_count,
                timestamp
            FROM player_stats
            WHERE prev_count IS NOT NULL 
                AND prev_count > 100  -- Only check games with substantial player counts
                AND ABS(player_count - prev_count)::float / prev_count > 0.5  -- 50% change
                AND player_count != 0  -- Exclude API failures
            ORDER BY timestamp DESC
            LIMIT 10
        """
        
        anomalies = postgres_hook.get_records(anomaly_detection_query)
        
        if anomalies:
            for app_id, game_name, current, previous, timestamp in anomalies:
                change_pct = ((current - previous) / previous) * 100
                completeness_issues.append(f"{game_name}: {change_pct:+.0f}% change detected at {timestamp.strftime('%H:%M')}")
        
        # Check API response health
        api_health_query = """
            SELECT 
                COUNT(*) as total_requests,
                AVG(api_response_time_ms) as avg_response_time,
                COUNT(*) FILTER (WHERE api_response_time_ms > 10000) as slow_requests
            FROM steam_player_tracking
            WHERE timestamp > NOW() - INTERVAL '2 hours'
                AND api_response_time_ms IS NOT NULL
        """
        
        api_result = postgres_hook.get_first(api_health_query)
        
        if api_result:
            total, avg_time, slow_count = api_result
            if total > 0:
                slow_percentage = (slow_count / total) * 100
                if avg_time > 3000:  # 3 seconds average
                    completeness_issues.append(f"High API response times: {avg_time:.0f}ms average")
                if slow_percentage > 10:  # More than 10% slow requests
                    completeness_issues.append(f"API performance degraded: {slow_percentage:.1f}% requests >10s")
        
    except Exception as e:
        completeness_issues.append(f"Completeness check failed: {str(e)}")
        logger.error(f"Data completeness validation error: {e}")
    
    context['task_instance'].xcom_push(key='completeness_issues', value=completeness_issues)
    
    logger.info(f"Found {len(completeness_issues)} completeness issues")
    return completeness_issues

def check_sentiment_processing(**context):
    """Check sentiment analysis processing health."""
    
    postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    sentiment_health = {
        'processing_healthy': False,
        'recent_processed': 0,
        'pending_reviews': 0,
        'avg_processing_time': 0,
        'issues': []
    }
    
    try:
        # Check recent sentiment processing activity
        processing_query = """
            SELECT 
                COUNT(*) FILTER (WHERE sentiment_label IS NOT NULL) as processed_reviews,
                COUNT(*) FILTER (WHERE sentiment_label IS NULL AND review_text IS NOT NULL) as pending_reviews,
                AVG(processing_time_ms) FILTER (WHERE processing_time_ms > 0) as avg_processing_time
            FROM steam_review_tracking
            WHERE review_timestamp > NOW() - INTERVAL '24 hours' OR processed_timestamp > NOW() - INTERVAL '2 hours'
        """
        
        result = postgres_hook.get_first(processing_query)
        
        if result:
            processed, pending, avg_time = result
            
            sentiment_health['recent_processed'] = processed or 0
            sentiment_health['pending_reviews'] = pending or 0
            sentiment_health['avg_processing_time'] = round(avg_time, 2) if avg_time else 0
            
            # Health checks
            if processed > 0:
                sentiment_health['processing_healthy'] = True
                logger.info(f"Sentiment processing healthy: {processed} reviews processed recently")
            else:
                sentiment_health['issues'].append("No recent sentiment processing activity")
            
            if pending > 50:  # Backlog building up
                sentiment_health['issues'].append(f"Large processing backlog: {pending} reviews pending")
                
            if avg_time and avg_time > 5000:  # 5 seconds seems slow for sentiment
                sentiment_health['issues'].append(f"Slow sentiment processing: {avg_time:.0f}ms average")
        
        # Check for processing errors or stuck reviews
        stuck_reviews_query = """
            SELECT COUNT(*)
            FROM steam_review_tracking
            WHERE review_text IS NOT NULL 
                AND sentiment_label IS NULL
                AND review_timestamp < NOW() - INTERVAL '1 day'
        """
        
        stuck_result = postgres_hook.get_first(stuck_reviews_query)
        stuck_count = stuck_result[0] if stuck_result else 0
        
        if stuck_count > 0:
            sentiment_health['issues'].append(f"{stuck_count} reviews stuck in processing queue")
    
    except Exception as e:
        sentiment_health['issues'].append(f"Sentiment health check failed: {str(e)}")
        logger.error(f"Sentiment processing check error: {e}")
    
    context['task_instance'].xcom_push(key='sentiment_health', value=sentiment_health)
    
    return sentiment_health

def generate_quality_report(**context):
    """Compile comprehensive quality report."""
    
    # Pull results from previous tasks
    quality_report = context['task_instance'].xcom_pull(task_ids='check_data_freshness', key='quality_report')
    completeness_issues = context['task_instance'].xcom_pull(task_ids='validate_data_completeness', key='completeness_issues')
    sentiment_health = context['task_instance'].xcom_pull(task_ids='check_sentiment_processing', key='sentiment_health')
    
    # Generate comprehensive report
    report = {
        'report_timestamp': datetime.now().isoformat(),
        'overall_status': 'HEALTHY',
        'summary': {
            'freshness_checks_passed': quality_report.get('checks_passed', 0) if quality_report else 0,
            'freshness_checks_failed': quality_report.get('checks_failed', 0) if quality_report else 0,
            'completeness_issues': len(completeness_issues) if completeness_issues else 0,
            'sentiment_processing_healthy': sentiment_health.get('processing_healthy', False) if sentiment_health else False,
            'pending_sentiment_reviews': sentiment_health.get('pending_reviews', 0) if sentiment_health else 0
        },
        'detailed_issues': [],
        'table_status': quality_report.get('table_status', {}) if quality_report else {},
        'processing_stats': sentiment_health if sentiment_health else {}
    }
    
    # Compile all issues
    all_issues = []
    
    if quality_report and quality_report.get('issues'):
        all_issues.extend(quality_report['issues'])
    
    if completeness_issues:
        all_issues.extend(completeness_issues)
    
    if sentiment_health and sentiment_health.get('issues'):
        all_issues.extend(sentiment_health['issues'])
    
    report['detailed_issues'] = all_issues
    
    # Determine overall status
    total_issues = len(all_issues)
    critical_issues = sum(1 for issue in all_issues if any(word in issue.lower() for word in ['failed', 'error', 'missing', 'stuck']))
    
    if total_issues == 0:
        report['overall_status'] = 'HEALTHY'
    elif critical_issues > 0 or total_issues > 5:
        report['overall_status'] = 'CRITICAL'
    else:
        report['overall_status'] = 'WARNING'
    
    logger.info(f"Data Quality Report: {report['overall_status']} - {total_issues} issues ({critical_issues} critical)")
    
    # Store report in database for trend analysis
    postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    try:
        # Create quality reports table if it doesn't exist
        create_table_sql = """
            CREATE TABLE IF NOT EXISTS data_quality_reports (
                id SERIAL PRIMARY KEY,
                report_timestamp TIMESTAMP WITH TIME ZONE,
                overall_status VARCHAR(20),
                total_issues INTEGER,
                critical_issues INTEGER,
                report_details JSONB,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            )
        """
        
        postgres_hook.run(create_table_sql)
        
        # Insert the report
        insert_sql = """
            INSERT INTO data_quality_reports 
            (report_timestamp, overall_status, total_issues, critical_issues, report_details)
            VALUES (%s, %s, %s, %s, %s)
        """
        
        postgres_hook.run(insert_sql, parameters=[
            report['report_timestamp'],
            report['overall_status'],
            total_issues,
            critical_issues,
            json.dumps(report)
        ])
        
        logger.info("Quality report stored in database")
        
    except Exception as e:
        logger.error(f"Failed to store quality report: {str(e)}")
    
    return report

def cleanup_old_data(**context):
    """Clean up old tracking data to prevent database bloat."""
    
    postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    cleanup_results = []
    
    try:
        # Remove player tracking data older than 14 days (keep 2 weeks for trend analysis)
        cleanup_player_sql = """
            DELETE FROM steam_player_tracking 
            WHERE timestamp < NOW() - INTERVAL '14 days'
        """
        
        result = postgres_hook.run(cleanup_player_sql)
        cleanup_results.append("Cleaned old player tracking data (>14 days)")
        
        # Remove old sentiment processing data (keep processed sentiment, remove duplicates)
        cleanup_sentiment_sql = """
            DELETE FROM steam_review_tracking 
            WHERE sentiment_label IS NULL 
                AND review_timestamp < NOW() - INTERVAL '7 days'
        """
        
        postgres_hook.run(cleanup_sentiment_sql)
        cleanup_results.append("Cleaned unprocessed old reviews (>7 days)")
        
        # Remove old quality reports (keep last 168 = 1 week of hourly reports)
        cleanup_reports_sql = """
            DELETE FROM data_quality_reports 
            WHERE id NOT IN (
                SELECT id FROM data_quality_reports 
                ORDER BY created_at DESC 
                LIMIT 168
            )
        """
        
        postgres_hook.run(cleanup_reports_sql)
        cleanup_results.append("Cleaned old quality reports")
        
        # Update table statistics for performance
        analyze_sql = """
            ANALYZE steam_player_tracking;
            ANALYZE steam_review_tracking;
            ANALYZE game_tracking_config;
        """
        
        postgres_hook.run(analyze_sql)
        cleanup_results.append("Updated table statistics")
        
        logger.info(f"Data cleanup completed: {len(cleanup_results)} operations")
        
    except Exception as e:
        logger.error(f"Data cleanup failed: {str(e)}")
        cleanup_results.append(f"Cleanup failed: {str(e)}")
    
    return cleanup_results

# Define tasks
check_freshness_task = PythonOperator(
    task_id='check_data_freshness',
    python_callable=check_data_freshness,
    dag=dag
)

validate_completeness_task = PythonOperator(
    task_id='validate_data_completeness',
    python_callable=validate_data_completeness,
    dag=dag
)

check_sentiment_task = PythonOperator(
    task_id='check_sentiment_processing',
    python_callable=check_sentiment_processing,
    dag=dag
)

generate_report_task = PythonOperator(
    task_id='generate_quality_report',
    python_callable=generate_quality_report,
    dag=dag
)

cleanup_task = PythonOperator(
    task_id='cleanup_old_data',
    python_callable=cleanup_old_data,
    dag=dag
)

# System health check
system_health_task = BashOperator(
    task_id='system_health_check',
    bash_command="""
    echo "Steam Analytics Data Quality Check completed at $(date)"
    echo "Checking Docker container status..."
    docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}" | grep -E "(kafka-producer|sentiment-processor|postgres)" || echo "Some containers not running"
    """,
    dag=dag
)

# Set task dependencies
[check_freshness_task, validate_completeness_task, check_sentiment_task] >> generate_report_task
generate_report_task >> cleanup_task >> system_health_task