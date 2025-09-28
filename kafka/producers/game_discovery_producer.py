import json
import time
import requests
from kafka import KafkaProducer
from datetime import datetime, timezone
import logging
import os
import psycopg2
from typing import Dict, List, Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class GameDiscoveryProducer:
    """
    Comprehensive Steam game discovery and metadata collection service.
    Pulls from multiple Steam APIs to build a complete game registry.
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
        
        # API endpoints
        self.steam_api_key = os.getenv('STEAM_API_KEY', '')  # Add your Steam API key
        self.api_endpoints = {
            'app_list': 'https://api.steampowered.com/ISteamApps/GetAppList/v2/',
            'app_details': 'https://store.steampowered.com/api/appdetails',
            'app_reviews': f'https://store.steampowered.com/appreviews/{{app_id}}?json=1&filter=recent&language=english&num_per_page=20',
            'steamspy': 'https://steamspy.com/api.php'
        }
        
        logger.info("Game Discovery Producer initialized successfully")
    
    def create_game_registry_table(self):
        """Create comprehensive game registry table."""
        try:
            cur = self.db_connection.cursor()
            cur.execute("""
                CREATE TABLE IF NOT EXISTS steam_game_registry (
                    app_id INTEGER PRIMARY KEY,
                    name VARCHAR(500),
                    type VARCHAR(100),
                    is_free BOOLEAN,
                    short_description TEXT,
                    detailed_description TEXT,
                    about_the_game TEXT,
                    
                    -- Pricing info
                    price_current DECIMAL(10,2),
                    price_original DECIMAL(10,2),
                    discount_percent INTEGER,
                    currency VARCHAR(10),
                    
                    -- Game metadata
                    release_date DATE,
                    developer VARCHAR(500),
                    publisher VARCHAR(500),
                    platforms JSONB,
                    categories JSONB,
                    genres JSONB,
                    tags JSONB,
                    languages JSONB,
                    
                    -- Media
                    header_image VARCHAR(1000),
                    screenshots JSONB,
                    movies JSONB,
                    
                    -- Requirements
                    pc_requirements JSONB,
                    mac_requirements JSONB,
                    linux_requirements JSONB,
                    
                    -- External data
                    metacritic_score INTEGER,
                    metacritic_url VARCHAR(500),
                    
                    -- SteamSpy estimates
                    owners_estimate VARCHAR(100),
                    owners_variance INTEGER,
                    players_forever INTEGER,
                    players_2weeks INTEGER,
                    average_forever INTEGER,
                    average_2weeks INTEGER,
                    median_forever INTEGER,
                    median_2weeks INTEGER,
                    
                    -- Tracking metadata
                    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    data_quality_score FLOAT,
                    tracking_enabled BOOLEAN DEFAULT FALSE
                    
                    --Note: Indexes will be created separately
                )
            """)
            
            self.db_connection.commit()
        
            # Create indexes separately
            cur = self.db_connection.cursor()
            indexes = [
                "CREATE INDEX IF NOT EXISTS idx_name ON steam_game_registry (name)",
                "CREATE INDEX IF NOT EXISTS idx_developer ON steam_game_registry (developer)",
                "CREATE INDEX IF NOT EXISTS idx_publisher ON steam_game_registry (publisher)",
                "CREATE INDEX IF NOT EXISTS idx_release_date ON steam_game_registry (release_date)",
                "CREATE INDEX IF NOT EXISTS idx_tracking_enabled ON steam_game_registry (tracking_enabled)"
            ]
            
            for index_sql in indexes:
                cur.execute(index_sql)
            
            self.db_connection.commit()
            cur.close()
            logger.info("Game registry table created successfully")
            
        except Exception as e:
            logger.error(f"Failed to create game registry table: {e}")
            self.db_connection.rollback()
            raise
    
    def fetch_complete_app_list(self) -> List[Dict]:
        """
        Fetch complete Steam application list.
        Returns list of all apps with basic info.
        """
        try:
            response = requests.get(self.api_endpoints['app_list'], timeout=30)
            response.raise_for_status()
            
            data = response.json()
            apps = data['applist']['apps']
            
            logger.info(f"Retrieved {len(apps)} apps from Steam API")
            return apps
            
        except Exception as e:
            logger.error(f"Failed to fetch Steam app list: {e}")
            return []
    
    def fetch_app_details(self, app_id: int) -> Optional[Dict]:
        """
        Fetch detailed information for a specific Steam app.
        Uses Store API for comprehensive metadata.
        """
        try:
            params = {
                'appids': app_id,
                
            }
            
            response = requests.get(
                self.api_endpoints['app_details'],
                params=params,
                timeout=10
            )
            response.raise_for_status()
            
            data = response.json()
            
            if str(app_id) in data and data[str(app_id)]['success']:
                app_data = data[str(app_id)]['data']
                
                # Structure the data
                structured_data = {
                    'app_id': app_id,
                    'name': app_data.get('name', ''),
                    'type': app_data.get('type', ''),
                    'is_free': app_data.get('is_free', False),
                    'short_description': app_data.get('short_description', ''),
                    'detailed_description': app_data.get('detailed_description', ''),
                    'about_the_game': app_data.get('about_the_game', ''),
                    
                    # Pricing
                    'price_current': None,
                    'price_original': None,
                    'discount_percent': None,
                    'currency': None,
                    
                    # Metadata
                    'release_date': app_data.get('release_date', {}).get('date'),
                    'developer': ', '.join(app_data.get('developers', [])),
                    'publisher': ', '.join(app_data.get('publishers', [])),
                    'platforms': json.dumps(app_data.get('platforms', {})),
                    'categories': json.dumps([cat.get('description') for cat in app_data.get('categories', [])]),
                    'genres': json.dumps([genre.get('description') for genre in app_data.get('genres', [])]),
                    'languages': json.dumps(app_data.get('supported_languages', '')),
                    
                    # Media
                    'header_image': app_data.get('header_image', ''),
                    'screenshots': json.dumps([ss.get('path_full') for ss in app_data.get('screenshots', [])]),
                    'movies': json.dumps([movie.get('webm', {}).get('max') for movie in app_data.get('movies', [])]),
                    
                    # Requirements
                    'pc_requirements': json.dumps(app_data.get('pc_requirements', {})),
                    'mac_requirements': json.dumps(app_data.get('mac_requirements', {})),
                    'linux_requirements': json.dumps(app_data.get('linux_requirements', {})),
                    
                    # External ratings
                    'metacritic_score': app_data.get('metacritic', {}).get('score'),
                    'metacritic_url': app_data.get('metacritic', {}).get('url'),
                }
                
                # Handle pricing
                price_overview = app_data.get('price_overview')
                if price_overview:
                    structured_data.update({
                        'price_current': price_overview.get('final') / 100 if price_overview.get('final') else None,
                        'price_original': price_overview.get('initial') / 100 if price_overview.get('initial') else None,
                        'discount_percent': price_overview.get('discount_percent'),
                        'currency': price_overview.get('currency')
                    })
                
                return structured_data
                
            else:
                logger.warning(f"No data available for app {app_id}")
                return None
                
        except Exception as e:
            logger.error(f"Failed to fetch details for app {app_id}: {e}")
            return None
    
    def fetch_steamspy_data(self, app_id: int) -> Optional[Dict]:
        """
        Fetch SteamSpy ownership and player data for an app.
        """
        try:
            params = {
                'request': 'appdetails',
                'appid': app_id
            }
            
            response = requests.get(
                self.api_endpoints['steamspy'],
                params=params,
                timeout=10
            )
            response.raise_for_status()
            
            data = response.json()
            
            if 'error' not in data:
                return {
                    'owners_estimate': data.get('owners'),
                    'owners_variance': data.get('owners_variance'),
                    'players_forever': data.get('players_forever'),
                    'players_2weeks': data.get('players_2weeks'),
                    'average_forever': data.get('average_forever'),
                    'average_2weeks': data.get('average_2weeks'),
                    'median_forever': data.get('median_forever'),
                    'median_2weeks': data.get('median_2weeks')
                }
            else:
                logger.warning(f"SteamSpy error for app {app_id}: {data.get('error')}")
                return None
                
        except Exception as e:
            logger.error(f"Failed to fetch SteamSpy data for app {app_id}: {e}")
            return None
    
    def calculate_data_quality_score(self, app_data: Dict) -> float:
        """
        Calculate data quality score based on completeness and reliability.
        Score from 0.0 to 1.0
        """
        score = 0.0
        total_checks = 0
        
        # Basic info checks (30% weight)
        basic_fields = ['name', 'type', 'short_description', 'developer', 'publisher']
        for field in basic_fields:
            total_checks += 1
            if app_data.get(field) and str(app_data[field]).strip():
                score += 0.06  # 30% / 5 fields
        
        # Pricing info (20% weight)
        if app_data.get('price_current') is not None:
            score += 0.2
        total_checks += 1
        
        # Media content (20% weight)
        media_fields = ['header_image', 'screenshots']
        for field in media_fields:
            total_checks += 1
            if app_data.get(field):
                score += 0.1  # 20% / 2 fields
        
        # Categories and genres (15% weight)
        taxonomy_fields = ['categories', 'genres']
        for field in taxonomy_fields:
            total_checks += 1
            if app_data.get(field) and app_data[field] != '[]':
                score += 0.075  # 15% / 2 fields
        
        # Release date (15% weight)
        if app_data.get('release_date'):
            score += 0.15
        total_checks += 1
        
        return min(score, 1.0)  # Cap at 1.0
    
    def store_game_data(self, app_data: Dict, steamspy_data: Optional[Dict] = None):
        """Store comprehensive game data in registry."""
        try:
            # Merge SteamSpy data if available
            if steamspy_data:
                app_data.update(steamspy_data)
            
            # Calculate data quality score
            app_data['data_quality_score'] = self.calculate_data_quality_score(app_data)
            
            # Insert or update database
            cur = self.db_connection.cursor()
            
            # Build dynamic INSERT/UPDATE query
            columns = list(app_data.keys())
            placeholders = [f"%({col})s" for col in columns]
            update_columns = [f"{col} = EXCLUDED.{col}" for col in columns if col != 'app_id']
            
            query = f"""
                INSERT INTO steam_game_registry ({', '.join(columns)})
                VALUES ({', '.join(placeholders)})
                ON CONFLICT (app_id) DO UPDATE SET
                {', '.join(update_columns)},
                last_updated = CURRENT_TIMESTAMP
            """
            
            cur.execute(query, app_data)
            self.db_connection.commit()
            cur.close()
            
            logger.info(f"Stored data for {app_data['name']} (ID: {app_data['app_id']}) - Quality: {app_data['data_quality_score']:.2f}")
            
        except Exception as e:
            logger.error(f"Failed to store game data for {app_data.get('app_id')}: {e}")
            self.db_connection.rollback()
    
    def publish_discovery_event(self, app_data: Dict):
        """Publish game discovery event to Kafka for downstream processing."""
        try:
            event = {
                'event_type': 'game_discovered',
                'timestamp': datetime.now(timezone.utc).isoformat(),
                'app_id': app_data['app_id'],
                'name': app_data['name'],
                'data_quality_score': app_data['data_quality_score'],
                'discovery_source': 'steam_store_api'
            }
            
            self.producer.send(
                'game-discovery-events',
                key=str(app_data['app_id']),
                value=event
            )
            
        except Exception as e:
            logger.error(f"Failed to publish discovery event: {e}")
    
    def discover_games_batch(self, app_list: List[Dict], start_idx: int = 0, batch_size: int = 100):
        """
        Process a batch of games for discovery.
        Rate-limited and error-tolerant.
        """
        logger.info(f"Processing batch starting at index {start_idx}, size {batch_size}")
        
        end_idx = min(start_idx + batch_size, len(app_list))
        batch = app_list[start_idx:end_idx]
        
        successful_discoveries = 0
        
        for i, app_info in enumerate(batch):
            try:
                app_id = app_info['appid']
                
                # Skip non-game apps (DLC, videos, etc.)
                if not app_info.get('name') or len(app_info['name']) < 2:
                    continue
                
                logger.info(f"Processing {i+1}/{len(batch)}: {app_info['name']} ({app_id})")
                
                # Fetch detailed app data
                app_data = self.fetch_app_details(app_id)
                if not app_data:
                    continue
                
                # Fetch SteamSpy data (optional, rate limited)
                steamspy_data = None
                if successful_discoveries % 10 == 0:  # Only every 10th app to respect SteamSpy limits
                    steamspy_data = self.fetch_steamspy_data(app_id)
                    if steamspy_data:
                        time.sleep(1)  # SteamSpy rate limiting
                
                # Store and publish
                self.store_game_data(app_data, steamspy_data)
                self.publish_discovery_event(app_data)
                
                successful_discoveries += 1
                
                # Rate limiting for Steam Store API
                time.sleep(1.5)  # ~40 requests per minute
                
            except Exception as e:
                logger.error(f"Error processing app {app_info.get('appid', 'unknown')}: {e}")
                continue
        
        logger.info(f"Batch complete: {successful_discoveries}/{len(batch)} games processed successfully")
        return successful_discoveries
    
    def run_full_discovery(self, max_games: Optional[int] = None):
        """
        Run complete game discovery process.
        """
        logger.info("Starting comprehensive Steam game discovery...")
        
        # Setup database
        self.create_game_registry_table()
        
        # Fetch complete app list
        app_list = self.fetch_complete_app_list()
        if not app_list:
            logger.error("Failed to fetch app list, aborting discovery")
            return
        
        # Limit games if specified
        if max_games:
            app_list = app_list[:max_games]
            logger.info(f"Limited discovery to {max_games} games")
        
        # Process in batches
        batch_size = 100
        total_processed = 0
        
        for start_idx in range(0, len(app_list), batch_size):
            try:
                processed = self.discover_games_batch(app_list, start_idx, batch_size)
                total_processed += processed
                
                # Progress logging
                progress = ((start_idx + min(batch_size, len(app_list) - start_idx)) / len(app_list)) * 100
                logger.info(f"Overall progress: {progress:.1f}% ({total_processed} games discovered)")
                
                # Batch delay for rate limiting
                if start_idx + batch_size < len(app_list):
                    logger.info("Waiting 30 seconds between batches...")
                    time.sleep(30)
                    
            except KeyboardInterrupt:
                logger.info("Discovery interrupted by user")
                break
            except Exception as e:
                logger.error(f"Batch error at index {start_idx}: {e}")
                continue
        
        logger.info(f"Discovery complete! {total_processed} games discovered and stored")
    
    def close(self):
        """Clean shutdown."""
        try:
            if self.producer:
                self.producer.flush()
                self.producer.close()
            if self.db_connection:
                self.db_connection.close()
            logger.info("Game Discovery Producer closed cleanly")
        except Exception as e:
            logger.error(f"Error during cleanup: {e}")

def main():
    """Main execution with different modes."""
    import sys
    
    logger.info("Steam Game Discovery Service Starting...")
    
    try:
        discovery = GameDiscoveryProducer()
        
        # Get mode from command line args
        mode = sys.argv[1] if len(sys.argv) > 1 else 'sample'
        
        if mode == 'full':
            # Full discovery (will take days)
            discovery.run_full_discovery()
        elif mode == 'sample':
            # Sample discovery for testing (100 games)
            discovery.run_full_discovery(max_games=100)
        elif mode == 'test':
            # Quick test (10 games)
            discovery.run_full_discovery(max_games=10)
        else:
            print("Usage: python game_discovery_producer.py [full|sample|test]")
            return
        
    except Exception as e:
        logger.error(f"Discovery service failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
