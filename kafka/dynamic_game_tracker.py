import json
import time
import requests
from kafka import KafkaProducer
from datetime import datetime, timezone
import logging
import os
import psycopg2
from typing import Dict, List, Optional
import threading
from concurrent.futures import ThreadPoolExecutor

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DynamicGameTracker:
    """
    Dynamic real-time game tracking system.
    Monitors user-selected games for multiple metrics:
    - Player counts
    - Price changes
    - Review sentiment
    - News and updates
    """
    
    def __init__(self):
        # Kafka setup
        kafka_servers = [os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9093')]
        self.producer = KafkaProducer(
            bootstrap_servers=kafka_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: str(k).encode('utf-8') if k else None,
            acks='all',
            retries=3,
            batch_size=16384,
            compression_type='gzip'
        )
        
        # Database setup
        self.db_connection = psycopg2.connect(
            host=os.getenv('POSTGRES_HOST', 'postgres'),
            database=os.getenv('POSTGRES_DB', 'airflow'),
            user=os.getenv('POSTGRES_USER', 'airflow'),
            password=os.getenv('POSTGRES_PASSWORD', 'airflow')
        )
        
        # Steam API configuration
        self.steam_api_key = os.getenv('STEAM_API_KEY', '')
        
        # Tracking state
        self.active_games = set()
        self.tracking_enabled = False
        self.tracking_threads = {}
        
        logger.info("Dynamic Game Tracker initialized successfully")
    
    def setup_tracking_tables(self):
        """Create tables for real-time tracking data."""
        try:
            cur = self.db_connection.cursor()
            
            # Real-time player counts table
            cur.execute("""
                CREATE TABLE IF NOT EXISTS steam_player_tracking (
                    id SERIAL PRIMARY KEY,
                    app_id INTEGER NOT NULL,
                    game_name VARCHAR(500),
                    player_count INTEGER NOT NULL,
                    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
                    api_response_time_ms FLOAT,
                    source VARCHAR(50) DEFAULT 'dynamic_tracker'
                    
                 
                )
            """)
            
            # Price tracking table
            cur.execute("""
                CREATE TABLE IF NOT EXISTS steam_price_tracking (
                    id SERIAL PRIMARY KEY,
                    app_id INTEGER NOT NULL,
                    game_name VARCHAR(500),
                    price_current DECIMAL(10,2),
                    price_original DECIMAL(10,2),
                    discount_percent INTEGER,
                    currency VARCHAR(10),
                    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
                    price_change_detected BOOLEAN DEFAULT FALSE
                    
                    
                )
            """)
            
            # Review sentiment tracking
            cur.execute("""
                CREATE TABLE IF NOT EXISTS steam_review_tracking (
                    id SERIAL PRIMARY KEY,
                    app_id INTEGER NOT NULL,
                    game_name VARCHAR(500),
                    review_id VARCHAR(100) UNIQUE,
                    review_text TEXT,
                    sentiment_score FLOAT,
                    sentiment_label VARCHAR(20),
                    review_helpful INTEGER,
                    review_timestamp TIMESTAMP WITH TIME ZONE,
                    processed_timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                    
                    
                )
            """)
            
            # Game tracking configuration
            cur.execute("""
                CREATE TABLE IF NOT EXISTS game_tracking_config (
                    app_id INTEGER PRIMARY KEY,
                    game_name VARCHAR(500),
                    tracking_enabled BOOLEAN DEFAULT TRUE,
                    track_players BOOLEAN DEFAULT TRUE,
                    track_prices BOOLEAN DEFAULT TRUE,
                    track_reviews BOOLEAN DEFAULT TRUE,
                    update_interval_minutes INTEGER DEFAULT 15,
                    last_updated TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                    added_by VARCHAR(100) DEFAULT 'system'
                )
            """)
            
            self.db_connection.commit()
            cur.close()
            logger.info("Tracking tables created successfully")
            
        except Exception as e:
            logger.error(f"Failed to create tracking tables: {e}")
            self.db_connection.rollback()
            raise
    
    def get_tracked_games(self) -> List[Dict]:
        """Get list of games currently enabled for tracking."""
        try:
            cur = self.db_connection.cursor()
            cur.execute("""
                SELECT app_id, game_name, track_players, track_prices, track_reviews, update_interval_minutes
                FROM game_tracking_config 
                WHERE tracking_enabled = TRUE
                ORDER BY last_updated DESC
            """)
            
            results = cur.fetchall()
            cur.close()
            
            games = []
            for row in results:
                games.append({
                    'app_id': row[0],
                    'game_name': row[1],
                    'track_players': row[2],
                    'track_prices': row[3],
                    'track_reviews': row[4],
                    'update_interval': row[5]
                })
            
            return games
            
        except Exception as e:
            logger.error(f"Failed to get tracked games: {e}")
            return []
    
    def add_game_to_tracking(self, app_id: int, game_name: str = None, **options):
        """Add a game to active tracking."""
        try:
            # Get game name from registry if not provided
            if not game_name:
                cur = self.db_connection.cursor()
                cur.execute("SELECT name FROM steam_game_registry WHERE app_id = %s", (app_id,))
                result = cur.fetchone()
                cur.close()
                
                if result:
                    game_name = result[0]
                else:
                    game_name = f"Game_{app_id}"
            
            # Insert or update tracking configuration
            cur = self.db_connection.cursor()
            cur.execute("""
                INSERT INTO game_tracking_config 
                (app_id, game_name, tracking_enabled, track_players, track_prices, track_reviews, update_interval_minutes)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (app_id) DO UPDATE SET
                tracking_enabled = EXCLUDED.tracking_enabled,
                track_players = EXCLUDED.track_players,
                track_prices = EXCLUDED.track_prices,
                track_reviews = EXCLUDED.track_reviews,
                update_interval_minutes = EXCLUDED.update_interval_minutes,
                last_updated = CURRENT_TIMESTAMP
            """, (
                app_id,
                game_name,
                options.get('enabled', True),
                options.get('track_players', True),
                options.get('track_prices', True),
                options.get('track_reviews', True),
                options.get('interval', 15)
            ))
            
            self.db_connection.commit()
            cur.close()
            
            logger.info(f"Added {game_name} (ID: {app_id}) to tracking")
            return True
            
        except Exception as e:
            logger.error(f"Failed to add game {app_id} to tracking: {e}")
            self.db_connection.rollback()
            return False
    
    def fetch_player_count(self, app_id: int) -> Optional[Dict]:
        """Fetch current player count for a game."""
        url = f"https://api.steampowered.com/ISteamUserStats/GetNumberOfCurrentPlayers/v1/?appid={app_id}"
        
        try:
            start_time = time.time()
            response = requests.get(url, timeout=10)
            api_response_time = (time.time() - start_time) * 1000
            
            response.raise_for_status()
            data = response.json()
            player_count = data['response']['player_count']
            
            return {
                'app_id': app_id,
                'player_count': player_count,
                'timestamp': datetime.now(timezone.utc).isoformat(),
                'api_response_time_ms': round(api_response_time, 2),
                'source': 'dynamic_tracker'
            }
            
        except Exception as e:
            logger.error(f"Failed to fetch player count for app {app_id}: {e}")
            return None
    
    def fetch_price_info(self, app_id: int) -> Optional[Dict]:
            """Fetch current pricing information for a game."""
            url = f"https://store.steampowered.com/api/appdetails"
            params = {'appids': app_id}
            
            try:
                response = requests.get(url, params=params, timeout=10)
                response.raise_for_status()
                
                data = response.json()
                
                if str(app_id) in data and data[str(app_id)]['success']:
                    app_data = data[str(app_id)]['data']
                    price_overview = app_data.get('price_overview')
                    
                    if price_overview:
                        return {
                            'app_id': app_id,
                            'price_current': price_overview.get('final', 0) / 100,
                            'price_original': price_overview.get('initial', 0) / 100,
                            'discount_percent': price_overview.get('discount_percent', 0),
                            'currency': price_overview.get('currency', 'USD'),
                            'timestamp': datetime.now(timezone.utc).isoformat()
                        }
                    else:
                        # Free game or no pricing info
                        return {
                            'app_id': app_id,
                            'price_current': 0.0,
                            'price_original': 0.0,
                            'discount_percent': 0,
                            'currency': 'USD',
                            'timestamp': datetime.now(timezone.utc).isoformat()
                        }
                
                return None
                
            except Exception as e:
                logger.error(f"Failed to fetch price info for app {app_id}: {e}")
                return None
        
    def fetch_reviews(self, app_id: int, count: int = 100) -> List[Dict]:
        """Fetch reviews for a game."""
        url = f"https://store.steampowered.com/appreviews/{app_id}"
        params = {
            'json': 1,
            'language': 'english',
            'num_per_page': count
        }
        
        try:
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            logger.info(f"API Response for {app_id}: success={data.get('success')}, review_count={len(data.get('reviews', []))}")

            reviews = []
            
            if data.get('success') == 1:
                for review in data.get('reviews', []):
                    reviews.append({
                        'app_id': app_id,
                        'review_id': review.get('recommendationid'),
                        'review_text': review.get('review', ''),
                        'review_helpful': review.get('votes_helpful', 0),
                        'review_timestamp': datetime.fromtimestamp(
                            review.get('timestamp_created', 0), 
                            timezone.utc
                        ).isoformat() if review.get('timestamp_created') else None
                    })
            
            return reviews
            
        except Exception as e:
            logger.error(f"Failed to fetch reviews for app {app_id}: {e}")
            return []
    
    def needs_initial_collection(self, app_id: int) -> bool:
        """Check if we've done initial review collection for this game."""
        try:
            cur = self.db_connection.cursor()
            cur.execute("""
                SELECT COUNT(*) FROM steam_review_tracking 
                WHERE app_id = %s AND review_text IS NOT NULL
            """, (app_id,))
            
            result = cur.fetchone()
            cur.close()
            
            return (result[0] if result else 0) < 10  # Collect if fewer than 10 reviews
            
        except Exception as e:
            logger.error(f"Failed to check collection status: {e}")
            return True

    def store_player_data(self, player_data: Dict, game_name: str):
        """Store player count data in database."""
        try:
            cur = self.db_connection.cursor()
            cur.execute("""
                INSERT INTO steam_player_tracking 
                (app_id, game_name, player_count, timestamp, api_response_time_ms, source)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (
                player_data['app_id'],
                game_name,
                player_data['player_count'],
                player_data['timestamp'],
                player_data['api_response_time_ms'],
                player_data['source']
            ))
            
            self.db_connection.commit()
            cur.close()
            
        except Exception as e:
            logger.error(f"Failed to store player data: {e}")
            self.db_connection.rollback()
    
    def store_price_data(self, price_data: Dict, game_name: str):
        """Store price data and detect changes."""
        try:
            cur = self.db_connection.cursor()
            
            # Check if price changed
            cur.execute("""
                SELECT price_current FROM steam_price_tracking 
                WHERE app_id = %s 
                ORDER BY timestamp DESC LIMIT 1
            """, (price_data['app_id'],))
            
            result = cur.fetchone()
            price_changed = False
            
            if result:
                previous_price = float(result[0]) if result[0] else 0.0
                current_price = price_data['price_current']
                price_changed = abs(previous_price - current_price) > 0.01
            
            # Insert new price record
            cur.execute("""
                INSERT INTO steam_price_tracking 
                (app_id, game_name, price_current, price_original, discount_percent, currency, timestamp, price_change_detected)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                price_data['app_id'],
                game_name,
                price_data['price_current'],
                price_data['price_original'],
                price_data['discount_percent'],
                price_data['currency'],
                price_data['timestamp'],
                price_changed
            ))
            
            self.db_connection.commit()
            cur.close()
            
            if price_changed:
                logger.info(f"Price change detected for {game_name}: ${price_data['price_current']}")
            
        except Exception as e:
            logger.error(f"Failed to store price data: {e}")
            self.db_connection.rollback()

    def store_reviews_in_database(self, reviews: List[Dict], app_id: int, game_name: str):
        """Store reviews directly in database for sentiment processing."""
        try:
            cur = self.db_connection.cursor()
            
            for review in reviews:
                cur.execute("""
                    INSERT INTO steam_review_tracking 
                    (app_id, game_name, review_id, review_text, review_helpful, review_timestamp)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (review_id) DO NOTHING
                """, (
                    app_id,
                    game_name,
                    review.get('review_id'),
                    review.get('review_text', ''),
                    review.get('review_helpful', 0),
                    review.get('review_timestamp')
                ))
            
            self.db_connection.commit()
            cur.close()
            logger.info(f"Stored {len(reviews)} reviews in database for {game_name}")
            
        except Exception as e:
            logger.error(f"Failed to store reviews in database: {e}")
            self.db_connection.rollback()        
    
    def publish_tracking_event(self, event_type: str, app_id: int, data: Dict):
        """Publish tracking event to Kafka."""
        try:
            logger.info(f"Publishing {event_type} event for app {app_id}")
            
            event = {
                'event_type': event_type,
                'app_id': app_id,
                'timestamp': datetime.now(timezone.utc).isoformat(),
                'data': data
            }
            
            # Debug logging
            logger.info(f"Event keys: {list(event.keys())}")
            logger.info(f"Data type: {type(data)}")
            if event_type == 'review_batch':
                reviews = data.get('reviews', [])
                logger.info(f"Review count in data: {len(reviews)}")
                if reviews:
                    logger.info(f"First review keys: {list(reviews[0].keys()) if reviews else 'None'}")
            
            # Test serialization
            try:
                serialized = json.dumps(event)
                logger.info(f"Serialization successful, size: {len(serialized)} bytes")
            except Exception as se:
                logger.error(f"Serialization failed: {se}")
                return

            topic_map = {
                'player_count': 'steam-player-counts',
                'price_update': 'steam-price-updates',
                'review_batch': 'steam-reviews'
            }
            
            topic = topic_map.get(event_type, 'steam-tracking-events')
            logger.info(f"Sending to topic: {topic}")

            future = self.producer.send(topic, key=str(app_id), value=event)
            result = future.get(timeout=10)  # Wait for send to complete
            self.producer.flush()  # Force immediate delivery
            
            logger.info(f"Successfully published {event_type} for app {app_id}: {result}")
            
        except Exception as e:
            logger.error(f"Failed to publish tracking event: {e}")
    
    def track_single_game(self, game_config: Dict):
        """Track a single game according to its configuration."""
        app_id = game_config['app_id']
        game_name = game_config['game_name']
        
        try:
            # Track player count
            if game_config['track_players']:
                player_data = self.fetch_player_count(app_id)
                if player_data:
                    self.store_player_data(player_data, game_name)
                    self.publish_tracking_event('player_count', app_id, player_data)
                    logger.info(f"Player tracking: {game_name} - {player_data['player_count']:,} players")
            
            # Track pricing
            if game_config['track_prices']:
                price_data = self.fetch_price_info(app_id)
                if price_data:
                    self.store_price_data(price_data, game_name)
                    self.publish_tracking_event('price_update', app_id, price_data)
            
            # Track reviews (less frequent)
            # Track reviews
            if game_config['track_reviews']:
                if self.needs_initial_collection(app_id):
                    logger.info(f"Initial review collection for {game_name}")
                    reviews = self.fetch_reviews(app_id, 100)  # Get 100 reviews initially
                else:
                    reviews = self.fetch_reviews(app_id, 10)   # Check for new ones
                
                if reviews:
                    self.publish_tracking_event('review_batch', app_id, {'reviews': reviews})

                    self.store_reviews_in_database(reviews, app_id, game_name)

            
            
        except Exception as e:
            logger.error(f"Error tracking game {game_name} ({app_id}): {e}")
    
    def start_tracking(self):
        """Start the dynamic tracking system."""
        logger.info("Starting dynamic game tracking system...")
        
        # Setup database tables
        self.setup_tracking_tables()
        
        # Get games to track
        tracked_games = self.get_tracked_games()
        
        if not tracked_games:
            logger.warning("No games configured for tracking")
            return
        
        logger.info(f"Tracking {len(tracked_games)} games")
        
        self.tracking_enabled = True
        
        # Schedule tracking for each game
        def tracking_loop():
            while self.tracking_enabled:
                start_time = time.time()
                
                # Refresh tracked games list
                current_games = self.get_tracked_games()
                
                # Track each game
                with ThreadPoolExecutor(max_workers=5) as executor:
                    futures = []
                    
                    for game_config in current_games:
                        future = executor.submit(self.track_single_game, game_config)
                        futures.append(future)
                    
                    # Wait for all tracking to complete
                    for future in futures:
                        try:
                            future.result(timeout=30)  # 30 second timeout per game
                        except Exception as e:
                            logger.error(f"Game tracking failed: {e}")
                
                # Calculate next cycle timing
                cycle_duration = time.time() - start_time
                sleep_time = max(60, 300 - cycle_duration)  # Minimum 1 minute, target 5 minutes
                
                logger.info(f"Tracking cycle complete in {cycle_duration:.1f}s. Next cycle in {sleep_time:.1f}s")
                
                if self.tracking_enabled:
                    time.sleep(sleep_time)
        
        # Start tracking in separate thread
        tracking_thread = threading.Thread(target=tracking_loop, daemon=True)
        tracking_thread.start()
        
        logger.info("Dynamic tracking system started successfully")
    
    def stop_tracking(self):
        """Stop the tracking system."""
        self.tracking_enabled = False
        logger.info("Tracking system stopped")
    
    def close(self):
        """Clean shutdown."""
        try:
            self.stop_tracking()
            if self.producer:
                self.producer.flush()
                self.producer.close()
            if self.db_connection:
                self.db_connection.close()
            logger.info("Dynamic Game Tracker closed cleanly")
        except Exception as e:
            logger.error(f"Error during cleanup: {e}")

def main():
    """Main execution with different modes."""
    import sys
    
    logger.info("Dynamic Game Tracker Starting...")
    
    try:
        tracker = DynamicGameTracker()
        
        # Add some popular games for testing
        popular_games = [
            (730, "Counter-Strike 2"),
            (570, "Dota 2"),
            (440, "Team Fortress 2"),
            (1172620, "Sea of Thieves"),
            (271590, "GTA V")
        ]
        
        # Add games to tracking
        for app_id, game_name in popular_games:
            tracker.add_game_to_tracking(app_id, game_name)
        
        logger.info(f"Added {len(popular_games)} games to tracking")
        
        # Start tracking
        tracker.start_tracking()
        
        # Keep running
        try:
            while True:
                time.sleep(60)
                logger.info("Tracking system running...")
        except KeyboardInterrupt:
            logger.info("Shutdown requested by user")
        
    except Exception as e:
        logger.error(f"Tracker failed: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if 'tracker' in locals():
            tracker.close()

if __name__ == "__main__":
    main()
