from kafka import KafkaConsumer
import json
import psycopg2
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    # Consumer that we know works
    consumer = KafkaConsumer(
        'steam-player-counts',
        bootstrap_servers=['kafka:9093'],
        auto_offset_reset='earliest',
        group_id=None,  # No group - read everything
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )

    # Database connection
    db_conn = psycopg2.connect(
        host='postgres',
        database='airflow',
        user='airflow',
        password='airflow'
    )
    
    # Create table
    cur = db_conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS steam_realtime_players (
            id SERIAL PRIMARY KEY,
            app_id INTEGER,
            game_name VARCHAR(100),
            player_count INTEGER,
            timestamp VARCHAR(100),
            api_response_time_ms FLOAT,
            message_id VARCHAR(100),
            processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    db_conn.commit()
    cur.close()

    logger.info('Processing messages and storing in database...')
    message_count = 0
    
    for message in consumer:
        message_count += 1
        data = message.value
        
        logger.info(f'Processing message {message_count}: {data}')
        
        # Insert into database
        cur = db_conn.cursor()
        cur.execute("""
            INSERT INTO steam_realtime_players 
            (app_id, game_name, player_count, timestamp, api_response_time_ms, message_id)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (
            data['app_id'],
            data['game_name'],
            data['player_count'],
            data['timestamp'],
            data['api_response_time_ms'],
            data['message_id']
        ))
        db_conn.commit()
        cur.close()
        
        logger.info(f'Stored in database: {data["game_name"]} - {data["player_count"]} players')
        
        if message_count >= 2:  # Process your 2 messages
            break
    
    consumer.close()
    db_conn.close()
    logger.info(f'Complete! Processed {message_count} messages')

if __name__ == "__main__":
    main()