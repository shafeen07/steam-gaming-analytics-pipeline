import json
import psycopg2
from kafka import KafkaConsumer
from datetime import datetime
import logging
import os

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class PlayerCountConsumer:
    """
    Kafka consumer that processes Steam player count messages.
    This completes your streaming pipeline: API → Kafka → Consumer → Database
    """
    
    def __init__(self):
        # Get configuration from environment variables (Docker setup)
        kafka_servers = [os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9093')]
        self.postgres_config = {
            'host': os.getenv('POSTGRES_HOST', 'postgres'),
            'database': os.getenv('POSTGRES_DB', 'airflow'),
            'user': os.getenv('POSTGRES_USER', 'airflow'),
            'password': os.getenv('POSTGRES_PASSWORD', 'airflow')
        }
        
        # Initialize Kafka consumer
        try:
            self.consumer = KafkaConsumer(
                'steam-player-counts',  # Subscribe to your topic
                bootstrap_servers=kafka_servers,
                auto_offset_reset='earliest',  # Read from beginning
                enable_auto_commit=True,
                auto_commit_interval_ms=1000,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                key_deserializer=lambda k: k.decode('utf-8') if k else None,
                group_id='player-count-processors',  # Consumer group
                consumer_timeout_ms=10000  # Exit after 10s of no messages (for testing)
            )
            logger.info("Kafka consumer initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize Kafka consumer: {e}")
            raise
        
        # Initialize database connection
        self.db_connection = None
        self.connect_to_database()
    
    def connect_to_database(self):
        """Connect to PostgreSQL database (same as your Airflow setup)."""
        try:
            self.db_connection = psycopg2.connect(**self.postgres_config)
            logger.info("Database connection established")
        except Exception as e:
            logger.error(f"Database connection failed: {e}")
            raise
    
    def create_realtime_table(self):
        """
        Create enhanced table for streaming data.
        Similar to your steam_test_results but optimized for real-time processing.
        """
        try:
            cur = self.db_connection.cursor()
            
            cur.execute("""
                CREATE TABLE IF NOT EXISTS steam_realtime_players (
                    id SERIAL PRIMARY KEY,
                    app_id INTEGER NOT NULL,
                    game_name VARCHAR(100),
                    player_count INTEGER NOT NULL,
                    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
                    api_response_time_ms FLOAT,
                    message_id VARCHAR(100),
                    source VARCHAR(50) DEFAULT 'kafka_stream',
                    processed_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Add indexes for better query performance
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_app_timestamp 
                ON steam_realtime_players (app_id, timestamp)
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_game_name 
                ON steam_realtime_players (game_name)
            """)
            
            self.db_connection.commit()
            cur.close()
            logger.info("Enhanced realtime table created/verified")
            
        except Exception as e:
            logger.error(f"Table creation failed: {e}")
            self.db_connection.rollback()
            raise
    
    def process_message(self, message_data):
        """
        Process a single player count message from Kafka.
        This extends your original PostgreSQL insert logic.
        """
        try:
            # Extract data from the Kafka message
            app_id = message_data.get('app_id')
            game_name = message_data.get('game_name', 'Unknown')
            player_count = message_data.get('player_count')
            timestamp = message_data.get('timestamp')
            api_response_time = message_data.get('api_response_time_ms')
            message_id = message_data.get('message_id')
            
            # Validate required fields
            if not all([app_id, player_count, timestamp]):
                logger.warning(f"Invalid message - missing required fields: {message_data}")
                return False
            
            # Insert into database (similar to your original DAG logic)
            cur = self.db_connection.cursor()
            cur.execute("""
                INSERT INTO steam_realtime_players 
                (app_id, game_name, player_count, timestamp, api_response_time_ms, message_id)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (app_id, game_name, player_count, timestamp, api_response_time, message_id))
            
            self.db_connection.commit()
            cur.close()
            
            logger.info(f"Processed: {game_name} - {player_count:,} players at {timestamp}")
            return True
            
        except Exception as e:
            logger.error(f"Processing failed: {e}")
            if self.db_connection:
                self.db_connection.rollback()
            return False
    
    def detect_significant_changes(self, current_data):
        """
        Example of real-time analytics: detect significant player count changes.
        This demonstrates the power of streaming - immediate insights!
        """
        try:
            app_id = current_data.get('app_id')
            current_count = current_data.get('player_count')
            game_name = current_data.get('game_name', f'App_{app_id}')
            
            # Get the previous count for this game
            cur = self.db_connection.cursor()
            cur.execute("""
                SELECT player_count, timestamp 
                FROM steam_realtime_players 
                WHERE app_id = %s 
                ORDER BY timestamp DESC 
                LIMIT 1 OFFSET 1
            """, (app_id,))
            
            result = cur.fetchone()
            cur.close()
            
            if result:
                previous_count, previous_time = result
                
                if previous_count > 0:
                    change_percent = ((current_count - previous_count) / previous_count) * 100
                    
                    # Alert on significant changes (20% threshold)
                    if abs(change_percent) > 20:
                        direction = "UP" if change_percent > 0 else "DOWN"
                        logger.info(f"ALERT: {game_name} is {direction} {abs(change_percent):.1f}%! "
                                   f"{previous_count:,} -> {current_count:,}")
                        
                        # This is where you could trigger notifications, alerts, etc.
                        return True
            
            return False
            
        except Exception as e:
            logger.error(f"Change detection failed: {e}")
            return False
    
    def start_consuming(self):
        """
        Main consumer loop - processes messages as they arrive from Kafka.
        """
        logger.info("Starting real-time consumer...")
        logger.info("Listening for messages on topic: steam-player-counts")
        
        # Create table if it doesn't exist
        self.create_realtime_table()
        
        message_count = 0
        
        try:
            # This loop processes messages as they arrive
            for message in self.consumer:
                message_count += 1
                
                logger.info(f"Received message #{message_count}: "
                           f"partition={message.partition}, offset={message.offset}")
                
                # Process the message data
                success = self.process_message(message.value)
                
                if success:
                    # Optional: Run real-time analytics
                    self.detect_significant_changes(message.value)
                else:
                    logger.warning(f"Failed to process message: {message.value}")
                
                # Log progress
                if message_count % 10 == 0:
                    logger.info(f"Processed {message_count} messages so far...")
                    
        except KeyboardInterrupt:
            logger.info("Consumer stopped by user")
        except Exception as e:
            logger.error(f"Consumer error: {e}")
            raise
        finally:
            logger.info(f"Total messages processed: {message_count}")
            self.close()
    
    def close(self):
        """Clean shutdown."""
        try:
            if self.consumer:
                self.consumer.close()
            if self.db_connection:
                self.db_connection.close()
            logger.info("Consumer closed cleanly")
        except Exception as e:
            logger.error(f"Error during cleanup: {e}")

def main():
    """
    Main execution function.
    """
    logger.info("Steam Player Count Consumer Starting...")
    
    try:
        consumer = PlayerCountConsumer()
        consumer.start_consuming()
    except Exception as e:
        logger.error(f"Consumer failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
