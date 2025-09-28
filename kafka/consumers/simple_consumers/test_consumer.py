import json
import psycopg2
from kafka import KafkaConsumer
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    logger.info("Starting simple test consumer...")
    
    # Connect with fresh group ID to read from beginning
    consumer = KafkaConsumer(
        'steam-player-counts',
        bootstrap_servers=['kafka:9093'],
        auto_offset_reset='earliest',
        group_id='test-fresh-group',  # New group reads from beginning
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        consumer_timeout_ms=30000  # Exit after 30 seconds of no messages
    )

    # Connect to database
    db_conn = psycopg2.connect(
        host='postgres',
        database='airflow',
        user='airflow',
        password='airflow'
    )
    logger.info("Database connection established")

    message_count = 0

    try:
        logger.info("Processing all available messages...")
        
        for message in consumer:
            message_count += 1
            data = message.value
            
            logger.info(f"Received message {message_count}: {data}")
            
            # Insert into database
            cur = db_conn.cursor()
            cur.execute('''
                INSERT INTO steam_realtime_players 
                (app_id, game_name, player_count, timestamp, api_response_time_ms, message_id)
                VALUES (%s, %s, %s, %s, %s, %s)
            ''', (
                data.get('app_id'),
                data.get('game_name', 'Unknown'),
                data.get('player_count'),
                data.get('timestamp'),
                data.get('api_response_time_ms'),
                data.get('message_id')
            ))
            db_conn.commit()
            cur.close()
            
            logger.info(f"Processed: {data.get('game_name')} - {data.get('player_count'):,} players")
            
    except Exception as e:
        logger.error(f"Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        consumer.close()
        db_conn.close()
        logger.info(f"Total messages processed: {message_count}")

if __name__ == "__main__":
    main()
