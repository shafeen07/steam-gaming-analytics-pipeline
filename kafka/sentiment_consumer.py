import json
import time
import logging
import os
import psycopg2
from datetime import datetime, timezone
from typing import Dict, List, Optional
from kafka import KafkaConsumer
import openai
from transformers import pipeline, AutoTokenizer, AutoModelForSequenceClassification
import torch
import threading
from concurrent.futures import ThreadPoolExecutor
import requests
from dataclasses import dataclass

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def safe_json_deserializer(m):
    """Safely deserialize JSON from bytes or string."""
    try:
        if isinstance(m, bytes):
            return json.loads(m.decode('utf-8'))
        elif isinstance(m, str):
            return json.loads(m)
        else:
            logger.warning(f"Unexpected message type: {type(m)}")
            return {}
    except Exception as e:
        logger.error(f"JSON deserialization error: {e}")
        return {}

def safe_key_deserializer(k):
    """Safely deserialize key from bytes or string."""
    try:
        if k is None:
            return None
        elif isinstance(k, bytes):
            return k.decode('utf-8')
        elif isinstance(k, str):
            return k
        else:
            return str(k)
    except Exception as e:
        logger.error(f"Key deserialization error: {e}")
        return None
    
@dataclass
class SentimentResult:
    """Structure for sentiment analysis results."""
    sentiment_label: str
    sentiment_score: float
    confidence: float
    processing_time_ms: float

class SentimentAnalysisConsumer:
    """
    LLM-powered sentiment analysis consumer for Steam reviews.
    Supports multiple LLM backends: OpenAI GPT, Anthropic Claude, and local models.
    """
    
    def __init__(self):
        # Kafka setup
        kafka_servers = [os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9093')]
        self.consumer = KafkaConsumer(
            'steam-reviews',
            bootstrap_servers=kafka_servers,
            value_deserializer=safe_json_deserializer,
            key_deserializer=safe_key_deserializer,
            group_id='sentiment-analysis-group-v2', #changed to v2
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            consumer_timeout_ms=1000
        )
        
        # Database setup
        self.db_connection = psycopg2.connect(
            host=os.getenv('POSTGRES_HOST', 'postgres'),
            database=os.getenv('POSTGRES_DB', 'airflow'),
            user=os.getenv('POSTGRES_USER', 'airflow'),
            password=os.getenv('POSTGRES_PASSWORD', 'airflow')
        )
        
        # LLM Configuration
        self.llm_backend = os.getenv('LLM_BACKEND', 'local')  # 'openai', 'anthropic', 'local'
        self.openai_api_key = os.getenv('OPENAI_API_KEY', '')
        self.anthropic_api_key = os.getenv('ANTHROPIC_API_KEY', '')
        
        # Initialize sentiment analysis models
        self.local_model = None
        self.local_tokenizer = None
        self.batch_size = int(os.getenv('BATCH_SIZE', '5'))
        self.max_workers = int(os.getenv('MAX_WORKERS', '3'))
        
        # Performance tracking
        self.processed_reviews = 0
        self.processing_errors = 0
        self.start_time = time.time()
        
        self._initialize_models()
        logger.info(f"Sentiment Analysis Consumer initialized with {self.llm_backend} backend")
    
    def _initialize_models(self):
        """Initialize the selected LLM backend."""
        if self.llm_backend == 'openai':
            if not self.openai_api_key:
                logger.warning("OpenAI API key not found, falling back to local model")
                self.llm_backend = 'local'
            else:
                openai.api_key = self.openai_api_key
                logger.info("OpenAI GPT-4 initialized for sentiment analysis")
        
        elif self.llm_backend == 'anthropic':
            if not self.anthropic_api_key:
                logger.warning("Anthropic API key not found, falling back to local model")
                self.llm_backend = 'local'
            else:
                logger.info("Anthropic Claude initialized for sentiment analysis")
        
        if self.llm_backend == 'local':
            self._initialize_local_model()
    
    def _initialize_local_model(self):
        """Initialize local Hugging Face sentiment model."""
        try:
            # Use a high-quality sentiment model optimized for gaming/entertainment content
            model_name = "cardiffnlp/twitter-roberta-base-sentiment-latest"
            
            logger.info(f"Loading local sentiment model: {model_name}")
            
            self.local_tokenizer = AutoTokenizer.from_pretrained(model_name)
            self.local_model = AutoModelForSequenceClassification.from_pretrained(model_name)
            
            # Use GPU if available
            if torch.cuda.is_available():
                self.local_model = self.local_model.cuda()
                logger.info("Using GPU for local model inference")
            
            self.sentiment_pipeline = pipeline(
                "sentiment-analysis",
                model=self.local_model,
                tokenizer=self.local_tokenizer,
                device=0 if torch.cuda.is_available() else -1
            )
            
            logger.info("Local sentiment model loaded successfully")
            
        except Exception as e:
            logger.error(f"Failed to load local model: {e}")
            # Fallback to a simpler model
            try:
                logger.info("Falling back to DistilBERT sentiment model")
                self.sentiment_pipeline = pipeline("sentiment-analysis", 
                                                 model="distilbert-base-uncased-finetuned-sst-2-english")
                logger.info("Fallback model loaded successfully")
            except Exception as fallback_error:
                logger.error(f"Failed to load fallback model: {fallback_error}")
                raise
    
    def analyze_sentiment_openai(self, review_text: str) -> SentimentResult:
        """Analyze sentiment using OpenAI GPT-4."""
        start_time = time.time()
        
        try:
            prompt = f"""
            Analyze the sentiment of this Steam game review. Respond with only a JSON object containing:
            - sentiment: "positive", "negative", or "neutral"
            - score: float between -1.0 (very negative) and 1.0 (very positive)
            - confidence: float between 0.0 and 1.0
            
            Review: "{review_text[:500]}"  # Limit to 500 chars for API efficiency
            
            JSON:
            """
            
            response = openai.ChatCompletion.create(
                model="gpt-4",
                messages=[
                    {"role": "system", "content": "You are a sentiment analysis expert for gaming reviews. Return only valid JSON."},
                    {"role": "user", "content": prompt}
                ],
                max_tokens=100,
                temperature=0.1
            )
            
            result_text = response.choices[0].message.content.strip()
            result = json.loads(result_text)
            
            processing_time = (time.time() - start_time) * 1000
            
            return SentimentResult(
                sentiment_label=result['sentiment'],
                sentiment_score=float(result['score']),
                confidence=float(result['confidence']),
                processing_time_ms=processing_time
            )
            
        except Exception as e:
            logger.error(f"OpenAI sentiment analysis failed: {e}")
            # Fallback to neutral sentiment
            return SentimentResult(
                sentiment_label="neutral",
                sentiment_score=0.0,
                confidence=0.0,
                processing_time_ms=(time.time() - start_time) * 1000
            )
    
    def analyze_sentiment_anthropic(self, review_text: str) -> SentimentResult:
        """Analyze sentiment using Anthropic Claude."""
        start_time = time.time()
        
        try:
            headers = {
                'Content-Type': 'application/json',
                'x-api-key': self.anthropic_api_key,
                'anthropic-version': '2023-06-01'
            }
            
            prompt = f"""
            Analyze the sentiment of this Steam game review. Respond with only a JSON object:
            {{"sentiment": "positive/negative/neutral", "score": float(-1.0 to 1.0), "confidence": float(0.0 to 1.0)}}
            
            Review: "{review_text[:500]}"
            """
            
            data = {
                "model": "claude-3-sonnet-20240229",
                "max_tokens": 100,
                "messages": [
                    {"role": "user", "content": prompt}
                ]
            }
            
            response = requests.post(
                'https://api.anthropic.com/v1/messages',
                headers=headers,
                json=data,
                timeout=10
            )
            
            response.raise_for_status()
            result_text = response.json()['content'][0]['text']
            result = json.loads(result_text)
            
            processing_time = (time.time() - start_time) * 1000
            
            return SentimentResult(
                sentiment_label=result['sentiment'],
                sentiment_score=float(result['score']),
                confidence=float(result['confidence']),
                processing_time_ms=processing_time
            )
            
        except Exception as e:
            logger.error(f"Anthropic sentiment analysis failed: {e}")
            return SentimentResult(
                sentiment_label="neutral",
                sentiment_score=0.0,
                confidence=0.0,
                processing_time_ms=(time.time() - start_time) * 1000
            )
    
    def analyze_sentiment_local(self, review_text: str) -> SentimentResult:
        """Analyze sentiment using local Hugging Face model."""
        start_time = time.time()
        
        try:
            # Truncate text to model's max length
            text = review_text[:512]  # Most models handle 512 tokens well
            
            result = self.sentiment_pipeline(text)[0]
            
            # Convert to standardized format
            label = result['label'].lower()
            score = result['score']
            
            # Map different model outputs to standard format
            if label in ['positive', 'pos', 'label_2']:
                sentiment_label = 'positive'
                sentiment_score = score
            elif label in ['negative', 'neg', 'label_0']:
                sentiment_label = 'negative'
                sentiment_score = -score
            else:
                sentiment_label = 'neutral'
                sentiment_score = 0.0
            
            processing_time = (time.time() - start_time) * 1000
            
            return SentimentResult(
                sentiment_label=sentiment_label,
                sentiment_score=sentiment_score,
                confidence=score,
                processing_time_ms=processing_time
            )
            
        except Exception as e:
            logger.error(f"Local sentiment analysis failed: {e}")
            return SentimentResult(
                sentiment_label="neutral",
                sentiment_score=0.0,
                confidence=0.0,
                processing_time_ms=(time.time() - start_time) * 1000
            )
    
    def analyze_sentiment(self, review_text: str) -> SentimentResult:
        """Route sentiment analysis to the configured backend."""
        if self.llm_backend == 'openai':
            return self.analyze_sentiment_openai(review_text)
        elif self.llm_backend == 'anthropic':
            return self.analyze_sentiment_anthropic(review_text)
        else:
            return self.analyze_sentiment_local(review_text)
    
    def process_review_batch(self, reviews: List[Dict]) -> List[Dict]:
        """Process a batch of reviews with sentiment analysis."""
        processed_reviews = []
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all reviews for processing
            future_to_review = {
                executor.submit(self.analyze_sentiment, review['review_text']): review 
                for review in reviews if review.get('review_text')
            }
            
            # Collect results
            for future in future_to_review:
                try:
                    review = future_to_review[future]
                    sentiment_result = future.result(timeout=30)
                    
                    # Combine review data with sentiment analysis
                    processed_review = {
                        **review,
                        'sentiment_score': sentiment_result.sentiment_score,
                        'sentiment_label': sentiment_result.sentiment_label,
                        'sentiment_confidence': sentiment_result.confidence,
                        'processing_time_ms': sentiment_result.processing_time_ms,
                        'processed_timestamp': datetime.now(timezone.utc).isoformat(),
                        'llm_backend': self.llm_backend
                    }
                    
                    processed_reviews.append(processed_review)
                    self.processed_reviews += 1
                    
                except Exception as e:
                    logger.error(f"Failed to process review: {e}")
                    self.processing_errors += 1
        
        return processed_reviews
    
    def store_sentiment_data(self, processed_reviews: List[Dict]):
        """Store processed reviews with sentiment data in PostgreSQL."""
        if not processed_reviews:
            return
        
        try:
            cur = self.db_connection.cursor()
            
            # Batch insert processed reviews
            insert_sql = """
                INSERT INTO steam_review_tracking 
                (app_id, game_name, review_id, review_text, sentiment_score, sentiment_label, 
                 review_helpful, review_timestamp, processed_timestamp, sentiment_confidence, 
                 processing_time_ms, llm_backend)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (review_id) DO UPDATE SET
                sentiment_score = EXCLUDED.sentiment_score,
                sentiment_label = EXCLUDED.sentiment_label,
                sentiment_confidence = EXCLUDED.sentiment_confidence,
                processed_timestamp = EXCLUDED.processed_timestamp,
                processing_time_ms = EXCLUDED.processing_time_ms,
                llm_backend = EXCLUDED.llm_backend
            """
            
            # Prepare batch data
            batch_data = []
            for review in processed_reviews:
                batch_data.append((
                    review.get('app_id'),
                    review.get('game_name', ''),
                    review.get('review_id'),
                    review.get('review_text', '')[:2000],  # Truncate if needed
                    review.get('sentiment_score', 0.0),
                    review.get('sentiment_label', 'neutral'),
                    review.get('review_helpful', 0),
                    review.get('review_timestamp'),
                    review.get('processed_timestamp'),
                    review.get('sentiment_confidence', 0.0),
                    review.get('processing_time_ms', 0.0),
                    review.get('llm_backend', self.llm_backend)
                ))
            
            # Execute batch insert
            cur.executemany(insert_sql, batch_data)
            self.db_connection.commit()
            cur.close()
            
            logger.info(f"Stored {len(processed_reviews)} sentiment-analyzed reviews")
            
        except Exception as e:
            logger.error(f"Failed to store sentiment data: {e}")
            self.db_connection.rollback()
    
    def enhance_review_table(self):
        """Add sentiment analysis columns to existing review table."""
        try:
            cur = self.db_connection.cursor()
            
            # Add sentiment analysis columns if they don't exist
            alter_statements = [
                "ALTER TABLE steam_review_tracking ADD COLUMN IF NOT EXISTS sentiment_confidence FLOAT DEFAULT 0.0",
                "ALTER TABLE steam_review_tracking ADD COLUMN IF NOT EXISTS processing_time_ms FLOAT DEFAULT 0.0",
                "ALTER TABLE steam_review_tracking ADD COLUMN IF NOT EXISTS llm_backend VARCHAR(20) DEFAULT 'local'",
                "CREATE INDEX IF NOT EXISTS idx_sentiment_label ON steam_review_tracking(sentiment_label)",
                "CREATE INDEX IF NOT EXISTS idx_sentiment_score ON steam_review_tracking(sentiment_score)",
                "CREATE INDEX IF NOT EXISTS idx_app_sentiment ON steam_review_tracking(app_id, sentiment_label)"
            ]
            
            for statement in alter_statements:
                cur.execute(statement)
            
            self.db_connection.commit()
            cur.close()
            
            logger.info("Enhanced review table with sentiment analysis columns")
            
        except Exception as e:
            logger.error(f"Failed to enhance review table: {e}")
            self.db_connection.rollback()
    
    def log_performance_stats(self):
        """Log processing performance statistics."""
        runtime = time.time() - self.start_time
        rate = self.processed_reviews / runtime if runtime > 0 else 0
        
        logger.info(f"Performance Stats: {self.processed_reviews} reviews processed, "
                   f"{self.processing_errors} errors, {rate:.2f} reviews/sec")
    
    def start_consuming(self):
        """Start consuming review messages from Kafka."""
        logger.info("Starting sentiment analysis consumer...")
        
        # Enhance database schema
        self.enhance_review_table()
        
        review_batch = []
        last_batch_time = time.time()
        batch_timeout = 30  # Process batch every 30 seconds
        
        # Debug logging to see what's happening
        logger.info("DEBUG: About to start consumer loop")
        logger.info(f"DEBUG: Consumer topics: {self.consumer.subscription()}")
        logger.info(f"DEBUG: Consumer group: {self.consumer.config['group_id']}")
        
        poll_count = 0
        
        try:
            logger.info("DEBUG: Entering consumer.poll() loop")

            self.consumer.subscribe(['steam-reviews'])
            logger.info("DEBUG: Explicitly subscribed to steam-reviews")

            logger.info("DEBUG: Waiting for partition assignment...")
            partitions = self.consumer.assignment()
            logger.info(f"DEBUG: Initial partitions assigned: {partitions}")

            # Force partition assignment if empty
            if not partitions:
                logger.info("DEBUG: No partitions assigned, calling poll(0) to trigger assignment...")
                self.consumer.poll(0)  # This triggers partition assignment
                partitions = self.consumer.assignment()
                logger.info(f"DEBUG: Partitions after assignment poll: {partitions}")
            
            logger.info("DEBUG: Starting main polling loop...")
            
            while True:
                # Use poll() instead of iterator to get better debugging
                message_batch = self.consumer.poll(timeout_ms=100, max_records = 10)
                poll_count += 1
                
                logger.info(f"DEBUG: Poll #{poll_count} - Got {len(message_batch)} topic-partitions with messages")
                
                if not message_batch:
                    logger.info("DEBUG: No messages in this poll cycle, continuing...")
                    if poll_count % 10 == 0:
                        logger.info(f"DEBUG: Completed {poll_count} poll cycles, still waiting for messages...")
                    continue
                    
                for topic_partition, messages in message_batch.items():
                    logger.info(f"DEBUG: Processing {len(messages)} messages from {topic_partition}")
                    
                    for message in messages:
                        logger.info(f"DEBUG: Processing message - offset: {message.offset}, key: {message.key}")
                        
                        try:
                            event_data = message.value
                            logger.info(f"DEBUG: Deserialized successfully, event_type: {event_data.get('event_type') if isinstance(event_data, dict) else 'NOT_DICT'}")
                            
                            # Extract reviews from the event
                            if event_data.get('event_type') == 'review_batch':
                                reviews = event_data.get('data', {}).get('reviews', [])
                                
                                # Add app_id and game metadata to each review
                                app_id = event_data.get('app_id')
                                for review in reviews:
                                    review['app_id'] = app_id
                                    # Get game name from database or set default
                                    review['game_name'] = f"Game_{app_id}"
                                
                                review_batch.extend(reviews)
                                
                                logger.info(f"Added {len(reviews)} reviews to batch (total: {len(review_batch)})")
                            
                            # Process batch when it reaches target size or timeout
                            current_time = time.time()
                            batch_ready = (len(review_batch) >= self.batch_size or 
                                        (review_batch and current_time - last_batch_time > batch_timeout))
                            
                            if batch_ready:
                                logger.info(f"Processing batch of {len(review_batch)} reviews")
                                
                                # Process sentiment analysis
                                processed_reviews = self.process_review_batch(review_batch)
                                
                                # Store in database
                                self.store_sentiment_data(processed_reviews)
                                
                                # Reset batch
                                review_batch = []
                                last_batch_time = current_time
                                
                                # Log performance stats every 100 processed reviews
                                if self.processed_reviews % 100 == 0:
                                    self.log_performance_stats()
                        
                        except Exception as e:
                            logger.error(f"DEBUG: Error processing message: {e}")
                            self.processing_errors += 1
        
        except KeyboardInterrupt:
            logger.info("Shutdown requested by user")
        except Exception as e:
            logger.error(f"Consumer error: {e}")
        finally:
            self.cleanup()
    
    def cleanup(self):
        """Clean shutdown of consumer."""
        try:
            logger.info("Shutting down sentiment analysis consumer...")
            
            # Log final stats
            self.log_performance_stats()
            
            if self.consumer:
                self.consumer.close()
            
            if self.db_connection:
                self.db_connection.close()
            
            logger.info("Sentiment analysis consumer shut down cleanly")
            
        except Exception as e:
            logger.error(f"Error during cleanup: {e}")

def main():
    """Main execution function."""
    logger.info("Starting LLM Sentiment Analysis Consumer...")
    
    try:
        consumer = SentimentAnalysisConsumer()
        consumer.start_consuming()
        
    except Exception as e:
        logger.error(f"Consumer failed to start: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()