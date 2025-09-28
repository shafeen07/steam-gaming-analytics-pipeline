import json
import time
import requests
from kafka import KafkaProducer
from datetime import datetime, timezone
import logging
import os

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class SteamPlayerProducer:
    """
    Real-time Kafka producer for Steam player counts.
    Extends your existing Steam API logic with streaming capabilities.
    """
    
    def __init__(self):
        # Get Kafka servers from environment (Docker networking)
        kafka_servers = [os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9093')]
        
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=kafka_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: str(k).encode('utf-8') if k else None,
                # Reliability settings for production
                acks='all',
                retries=3,
                batch_size=16384,
                # Add some performance tuning
                linger_ms=10,  # Wait 10ms to batch messages
                compression_type='gzip'
            )
            logger.info("✅ Kafka producer initialized successfully")
        except Exception as e:
            logger.error(f"❌ Failed to initialize Kafka producer: {e}")
            raise
    
    def fetch_steam_player_count(self, app_id):
        """
        Same Steam API call from your test_steam_dag.py
        Enhanced with better error handling and metadata.
        """
        url = f"https://api.steampowered.com/ISteamUserStats/GetNumberOfCurrentPlayers/v1/?appid={app_id}"
        
        try:
            start_time = time.time()
            response = requests.get(url, timeout=10)
            api_response_time = (time.time() - start_time) * 1000
            
            response.raise_for_status()
            data = response.json()
            player_count = data['response']['player_count']
            
            # Create enriched message with metadata
            message = {
                'app_id': app_id,
                'player_count': player_count,
                'timestamp': datetime.now(timezone.utc).isoformat(),
                'source': 'steam_api',
                'api_response_time_ms': round(api_response_time, 2),
                'message_id': f"{app_id}_{int(time.time())}"
            }
            
            logger.info(f"📊 Fetched: App {app_id} has {player_count:,} players")
            return message
            
        except requests.exceptions.Timeout:
            logger.warning(f"⏰ Steam API timeout for app_id {app_id}")
            return None
        except requests.exceptions.RequestException as e:
            logger.error(f"🌐 Steam API error for app_id {app_id}: {e}")
            return None
        except (KeyError, ValueError) as e:
            logger.error(f"📝 Data parsing error for app_id {app_id}: {e}")
            return None
    
    def publish_to_kafka(self, topic, key, message):
        """
        Generic method to publish messages to any Kafka topic.
        """
        try:
            future = self.producer.send(topic, key=key, value=message)
            record_metadata = future.get(timeout=10)
            
            logger.info(f"✅ Published to {topic}: partition={record_metadata.partition}, "
                       f"offset={record_metadata.offset}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to publish to {topic}: {e}")
            return False
    
    def stream_single_game(self, app_id, game_name=None):
        """
        Stream player count for a single game (testing method).
        """
        player_data = self.fetch_steam_player_count(app_id)
        
        if not player_data:
            return False
        
        # Add game name if provided
        if game_name:
            player_data['game_name'] = game_name
        
        return self.publish_to_kafka('steam-player-counts', str(app_id), player_data)
    
    def stream_multiple_games(self, game_list, interval_seconds=60):
        """
        Continuously stream player counts for multiple games.
        This creates your real-time data pipeline!
        """
        logger.info(f"🚀 Starting real-time streaming for {len(game_list)} games")
        logger.info(f"📡 Publishing every {interval_seconds} seconds to Kafka topic: steam-player-counts")
        
        cycle_count = 0
        
        try:
            while True:
                cycle_start = time.time()
                cycle_count += 1
                successful_publishes = 0
                
                logger.info(f"🔄 Starting cycle #{cycle_count}")
                
                for app_id, game_name in game_list:
                    try:
                        success = self.stream_single_game(app_id, game_name)
                        if success:
                            successful_publishes += 1
                            logger.info(f"📤 ✅ {game_name} ({app_id})")
                        else:
                            logger.warning(f"⚠️ Failed: {game_name} ({app_id})")
                        
                        # Respectful delay between API calls
                        time.sleep(1)
                        
                    except Exception as e:
                        logger.error(f"💥 Error processing {game_name} ({app_id}): {e}")
                
                # Cycle summary
                cycle_duration = time.time() - cycle_start
                logger.info(f"📊 Cycle #{cycle_count} complete: {successful_publishes}/{len(game_list)} "
                           f"games published in {cycle_duration:.1f}s")
                
                # Wait for next cycle
                sleep_time = max(0, interval_seconds - cycle_duration)
                if sleep_time > 0:
                    logger.info(f"😴 Waiting {sleep_time:.1f}s until next cycle...\n")
                    time.sleep(sleep_time)
                else:
                    logger.warning("⚡ Cycle took longer than interval!\n")
                    
        except KeyboardInterrupt:
            logger.info("🛑 Streaming stopped by user")
        except Exception as e:
            logger.error(f"💥 Streaming error: {e}")
            raise
        finally:
            self.close()
    
    def close(self):
        """Clean shutdown of Kafka producer."""
        try:
            if hasattr(self, 'producer'):
                self.producer.flush()  # Send any pending messages
                self.producer.close()
                logger.info("🔒 Kafka producer closed cleanly")
        except Exception as e:
            logger.error(f"❌ Error closing producer: {e}")

# Popular games for streaming (based on your CS2 success!)
POPULAR_GAMES = [
    (730, "Counter-Strike 2"),      # Your proven working case
    (1172620, "Sea of Thieves"),    # Popular multiplayer
    (431960, "Wallpaper Engine"),   # Always active users  
    (1938090, "Call of Duty"),      # AAA title
    (271590, "GTA V"),              # Consistently popular
    (252490, "Rust"),               # Survival game
    (1245620, "ELDEN RING"),        # Popular single player
    (1086940, "Baldur's Gate 3"),   # RPG hit
]

def main():
    """
    Main execution function with different modes.
    """
    logger.info("🎮 Steam Real-Time Analytics Producer Starting...")
    
    try:
        producer = SteamPlayerProducer()
        
        # Test mode: Single message (like your current DAG)
        logger.info("🧪 Testing single message...")
        success = producer.stream_single_game(730, "Counter-Strike 2")
        if success:
            logger.info("✅ Single message test successful!")
        else:
            logger.error("❌ Single message test failed!")
            return
        
        # Give user choice
        print("\n" + "="*50)
        print("Choose streaming mode:")
        print("1. Single test message only (safe)")
        print("2. Continuous streaming (real-time pipeline)")
        print("="*50)
        
        choice = input("Enter choice (1-2): ").strip()
        
        if choice == "2":
            # Real-time streaming mode
            logger.info("🚀 Starting continuous streaming mode...")
            producer.stream_multiple_games(POPULAR_GAMES, interval_seconds=30)
        else:
            logger.info("✅ Test complete! Check Kafka UI to see your message.")
            producer.close()
            
    except Exception as e:
        logger.error(f"💥 Producer error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()