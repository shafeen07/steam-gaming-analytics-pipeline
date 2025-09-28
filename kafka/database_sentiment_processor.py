import json
import time
import logging
import os
import psycopg2
from datetime import datetime, timezone
from typing import Dict, List, Optional
from transformers import pipeline, AutoTokenizer, AutoModelForSequenceClassification
import torch
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@dataclass
class SentimentResult:
    """Structure for sentiment analysis results."""
    sentiment_label: str
    sentiment_score: float
    confidence: float
    processing_time_ms: float

class DatabaseSentimentProcessor:
    """
    Direct database sentiment analysis processor.
    Reads unprocessed reviews from PostgreSQL and adds sentiment analysis.
    """
    
    def __init__(self):
        # Database setup
        self.db_connection = psycopg2.connect(
            host=os.getenv('POSTGRES_HOST', 'postgres'),
            database=os.getenv('POSTGRES_DB', 'airflow'),
            user=os.getenv('POSTGRES_USER', 'airflow'),
            password=os.getenv('POSTGRES_PASSWORD', 'airflow')
        )
        
        # LLM Configuration
        self.llm_backend = os.getenv('LLM_BACKEND', 'local')
        self.batch_size = int(os.getenv('BATCH_SIZE', '10'))
        self.max_workers = 1 #int(os.getenv('MAX_WORKERS', '3'))
        
        # Initialize sentiment analysis models
        self.local_model = None
        self.local_tokenizer = None
        self.sentiment_pipeline = None
        
        # Performance tracking
        self.processed_reviews = 0
        self.processing_errors = 0
        self.start_time = time.time()
        
        self._initialize_models()
        self._setup_database()
        logger.info(f"Database Sentiment Processor initialized with {self.llm_backend} backend")
    
    def _initialize_models(self):
        """Initialize the local sentiment analysis model."""
        try:
            # Use a high-quality sentiment model
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
    
    def _setup_database(self):
        """Ensure database tables have the necessary sentiment columns."""
        try:
            cur = self.db_connection.cursor()
            
            # Add sentiment analysis columns if they don't exist
            alter_statements = [
                "ALTER TABLE steam_review_tracking ADD COLUMN IF NOT EXISTS sentiment_confidence FLOAT DEFAULT 0.0",
                "ALTER TABLE steam_review_tracking ADD COLUMN IF NOT EXISTS processing_time_ms FLOAT DEFAULT 0.0",
                "ALTER TABLE steam_review_tracking ADD COLUMN IF NOT EXISTS llm_backend VARCHAR(20) DEFAULT 'local'",
                "ALTER TABLE steam_review_tracking ADD COLUMN IF NOT EXISTS processing_method VARCHAR(20) DEFAULT 'database'",
                "CREATE INDEX IF NOT EXISTS idx_sentiment_unprocessed ON steam_review_tracking(id) WHERE sentiment_label IS NULL",
                "CREATE INDEX IF NOT EXISTS idx_sentiment_label ON steam_review_tracking(sentiment_label)",
                "CREATE INDEX IF NOT EXISTS idx_sentiment_score ON steam_review_tracking(sentiment_score)",
                "CREATE INDEX IF NOT EXISTS idx_app_sentiment ON steam_review_tracking(app_id, sentiment_label)"
            ]
            
            for statement in alter_statements:
                cur.execute(statement)
            
            self.db_connection.commit()
            cur.close()
            
            logger.info("Database setup completed - sentiment columns ready")
            
        except Exception as e:
            logger.error(f"Failed to setup database: {e}")
            self.db_connection.rollback()
            raise
    
    def get_unprocessed_reviews(self, limit: int = 50) -> List[Dict]:
        """Get reviews that haven't been processed for sentiment analysis."""
        try:
            cur = self.db_connection.cursor()
            
            # Get reviews without sentiment analysis
            query = """
                SELECT id, app_id, game_name, review_id, review_text, review_helpful, review_timestamp
                FROM steam_review_tracking
                WHERE sentiment_label IS NULL 
                    AND review_text IS NOT NULL 
                    AND review_text != ''
                ORDER BY id ASC
                LIMIT %s
            """
            
            cur.execute(query, (limit,))
            results = cur.fetchall()
            cur.close()
            
            reviews = []
            for row in results:
                reviews.append({
                    'id': row[0],
                    'app_id': row[1],
                    'game_name': row[2],
                    'review_id': row[3],
                    'review_text': row[4],
                    'review_helpful': row[5],
                    'review_timestamp': row[6]
                })
            
            logger.info(f"Found {len(reviews)} unprocessed reviews")
            return reviews
            
        except Exception as e:
            logger.error(f"Failed to get unprocessed reviews: {e}")
            return []
    
    def analyze_sentiment_local(self, review_text: str) -> SentimentResult:
        """Analyze sentiment using local Hugging Face model."""
        start_time = time.time()
        
        try:
            # Truncate text to model's max length
            tokens = self.local_tokenizer.encode(review_text, max_length=512, truncation=True)
            text = self.local_tokenizer.decode(tokens, skip_special_tokens=True)
            
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
    
    def process_review_batch(self, reviews: List[Dict]) -> List[Dict]:
        """Process a batch of reviews with sentiment analysis."""
        processed_reviews = []
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all reviews for processing
            future_to_review = {
                executor.submit(self.analyze_sentiment_local, review['review_text']): review 
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
                        'processed_timestamp': datetime.now(timezone.utc),
                        'llm_backend': self.llm_backend,
                        'processing_method': 'database'
                    }
                    
                    processed_reviews.append(processed_review)
                    self.processed_reviews += 1
                    
                except Exception as e:
                    logger.error(f"Failed to process review: {e}")
                    self.processing_errors += 1
        
        return processed_reviews
    
    def update_sentiment_data(self, processed_reviews: List[Dict]):
        """Update the database with sentiment analysis results."""
        if not processed_reviews:
            return
        
        try:
            cur = self.db_connection.cursor()
            
            # Update existing reviews with sentiment data
            update_sql = """
                UPDATE steam_review_tracking SET
                    sentiment_score = %s,
                    sentiment_label = %s,
                    sentiment_confidence = %s,
                    processing_time_ms = %s,
                    processed_timestamp = %s,
                    llm_backend = %s,
                    processing_method = %s
                WHERE id = %s
            """
            
            # Prepare batch data
            batch_data = []
            for review in processed_reviews:
                batch_data.append((
                    review.get('sentiment_score', 0.0),
                    review.get('sentiment_label', 'neutral'),
                    review.get('sentiment_confidence', 0.0),
                    review.get('processing_time_ms', 0.0),
                    review.get('processed_timestamp'),
                    review.get('llm_backend', self.llm_backend),
                    review.get('processing_method', 'database'),
                    review.get('id')
                ))
            
            # Execute batch update
            cur.executemany(update_sql, batch_data)
            self.db_connection.commit()
            cur.close()
            
            logger.info(f"Updated {len(processed_reviews)} reviews with sentiment analysis")
            
        except Exception as e:
            logger.error(f"Failed to update sentiment data: {e}")
            self.db_connection.rollback()
    
    def get_processing_stats(self) -> Dict:
        """Get current processing statistics."""
        try:
            cur = self.db_connection.cursor()
            
            # Get overall stats
            stats_query = """
                SELECT 
                    COUNT(*) as total_reviews,
                    COUNT(*) FILTER (WHERE sentiment_label IS NOT NULL) as processed_reviews,
                    COUNT(*) FILTER (WHERE sentiment_label IS NULL) as pending_reviews,
                    COUNT(*) FILTER (WHERE sentiment_label = 'positive') as positive_reviews,
                    COUNT(*) FILTER (WHERE sentiment_label = 'negative') as negative_reviews,
                    COUNT(*) FILTER (WHERE sentiment_label = 'neutral') as neutral_reviews,
                    AVG(sentiment_score) FILTER (WHERE sentiment_label IS NOT NULL) as avg_sentiment,
                    AVG(processing_time_ms) FILTER (WHERE processing_time_ms > 0) as avg_processing_time
                FROM steam_review_tracking
                WHERE review_text IS NOT NULL
            """
            
            cur.execute(stats_query)
            result = cur.fetchone()
            cur.close()
            
            if result:
                total, processed, pending, positive, negative, neutral, avg_sentiment, avg_time = result
                
                stats = {
                    'total_reviews': total or 0,
                    'processed_reviews': processed or 0,
                    'pending_reviews': pending or 0,
                    'positive_reviews': positive or 0,
                    'negative_reviews': negative or 0,
                    'neutral_reviews': neutral or 0,
                    'avg_sentiment_score': round(avg_sentiment, 3) if avg_sentiment else 0.0,
                    'avg_processing_time_ms': round(avg_time, 2) if avg_time else 0.0,
                    'processing_rate': self.processed_reviews / ((time.time() - self.start_time) / 60) if time.time() > self.start_time else 0,
                    'session_processed': self.processed_reviews,
                    'session_errors': self.processing_errors
                }
                
                return stats
            
        except Exception as e:
            logger.error(f"Failed to get processing stats: {e}")
        
        return {}
    
    def run_processing_cycle(self):
        """Run a single processing cycle."""
        logger.info("Starting sentiment analysis processing cycle...")
        
        # Get unprocessed reviews
        unprocessed_reviews = self.get_unprocessed_reviews(self.batch_size)
        
        if not unprocessed_reviews:
            logger.info("No unprocessed reviews found")
            return
        
        logger.info(f"Processing {len(unprocessed_reviews)} reviews")
        
        # Process sentiment analysis
        processed_reviews = self.process_review_batch(unprocessed_reviews)
        
        # Update database
        self.update_sentiment_data(processed_reviews)
        
        # Log statistics
        stats = self.get_processing_stats()
        logger.info(f"Cycle complete. Processed: {stats.get('session_processed', 0)}, "
                   f"Pending: {stats.get('pending_reviews', 0)}, "
                   f"Rate: {stats.get('processing_rate', 0):.2f} reviews/min")
    
    def run_continuous(self, cycle_interval: int = 30):
        """Run continuous processing with specified interval."""
        logger.info(f"Starting continuous sentiment processing (interval: {cycle_interval}s)")
        
        try:
            while True:
                self.run_processing_cycle()
                
                # Check if there are more reviews to process
                stats = self.get_processing_stats()
                pending = stats.get('pending_reviews', 0)
                
                if pending == 0:
                    logger.info("All reviews processed. Waiting for new reviews...")
                    time.sleep(cycle_interval * 2)  # Longer wait when nothing to process
                else:
                    logger.info(f"{pending} reviews remaining. Next cycle in {cycle_interval}s")
                    time.sleep(cycle_interval)
                    
        except KeyboardInterrupt:
            logger.info("Processing stopped by user")
        except Exception as e:
            logger.error(f"Processing error: {e}")
        finally:
            self.cleanup()
    
    def cleanup(self):
        """Clean shutdown."""
        try:
            if self.db_connection:
                self.db_connection.close()
            logger.info("Database Sentiment Processor closed cleanly")
        except Exception as e:
            logger.error(f"Error during cleanup: {e}")

def main():
    """Main execution function."""
    logger.info("Starting Database Sentiment Analysis Processor...")
    
    try:
        processor = DatabaseSentimentProcessor()
        
        # Show initial stats
        stats = processor.get_processing_stats()
        logger.info(f"Initial state: {stats.get('total_reviews', 0)} total reviews, "
                   f"{stats.get('pending_reviews', 0)} pending processing")
        
        # Run continuous processing
        processor.run_continuous(cycle_interval=30)
        
    except Exception as e:
        logger.error(f"Processor failed to start: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()