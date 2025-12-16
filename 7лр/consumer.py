import os
import json
import logging
import time
from datetime import datetime
from kafka import KafkaConsumer
from kafka.errors import KafkaError

# ===== CONFIGURATION =====
KAFKA_CONFIG = {
    'bootstrap_servers': [os.getenv('KAFKA_BROKER_HOST', 'kafka:9092')],
    'topic': os.getenv('KAFKA_TOPIC', 'book_events'),
    'dead_letter_topic': os.getenv('DEAD_LETTER_TOPIC', 'dead_letter_events'),
    'group_id': 'book-processor-group'
}

# ===== LOGGING SETUP =====
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('logs/consumer.log', encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)

# ===== STATISTICS =====
processing_stats = {
    'total_messages': 0,
    'by_action': {},
    'errors': 0,
    'start_time': None
}

def update_stats(action, success=True):
    """Update processing statistics"""
    processing_stats['total_messages'] += 1
    
    if action not in processing_stats['by_action']:
        processing_stats['by_action'][action] = 0
    processing_stats['by_action'][action] += 1
    
    if not success:
        processing_stats['errors'] += 1

def print_stats():
    """Print processing statistics"""
    if processing_stats['start_time']:
        uptime = time.time() - processing_stats['start_time']
        logger.info(f"""
📊 Consumer Statistics:
   Total messages: {processing_stats['total_messages']}
   Uptime: {uptime:.2f} seconds
   Messages per second: {processing_stats['total_messages'] / uptime:.2f}
   Errors: {processing_stats['errors']}
   By action: {processing_stats['by_action']}
        """)

# ===== MESSAGE PROCESSING =====
def process_book_event(message):
    """Process book event message"""
    try:
        event = message.value
        action = event.get('action', 'unknown')
        book_id = event.get('book_id', 'N/A')
        timestamp = event.get('timestamp', 'N/A')
        user_ip = event.get('user_ip', 'N/A')
        
        logger.info(f"📨 Received event: {action.upper()} for book {book_id}")
        
        # Process different actions
        if action == 'book_created':
            book_data = event.get('data', {})
            title = book_data.get('title', 'N/A')
            author = book_data.get('author', 'N/A')
            logger.info(f"✅ NEW BOOK: '{title}' by {author} (ID: {book_id})")
            
        elif action == 'book_updated':
            book_data = event.get('data', {})
            title = book_data.get('title', 'N/A')
            logger.info(f"✏️ UPDATED BOOK: '{title}' (ID: {book_id})")
            
        elif action == 'book_deleted':
            book_data = event.get('data', {})
            title = book_data.get('title', 'N/A')
            logger.info(f"🗑️ DELETED BOOK: '{title}' (ID: {book_id})")
            
        else:
            logger.warning(f"❓ Unknown action: {action}")
        
        # Simulate some processing logic
        process_business_logic(event)
        
        update_stats(action, success=True)
        return True
        
    except Exception as e:
        logger.error(f"❌ Error processing message: {e}")
        update_stats(action, success=False)
        return False

def process_business_logic(event):
    """Simulate business logic processing"""
    # Example: Calculate processing time based on action
    processing_times = {
        'book_created': 0.1,
        'book_updated': 0.05,
        'book_deleted': 0.02
    }
    
    action = event.get('action')
    sleep_time = processing_times.get(action, 0.01)
    time.sleep(sleep_time)
    
    # Example: Validate data
    if action == 'book_created':
        book_data = event.get('data', {})
        if not book_data.get('title') or not book_data.get('author'):
            logger.warning("⚠️ Incomplete book data in creation event")

# ===== DEAD LETTER QUEUE CONSUMER =====
def setup_dead_letter_consumer():
    """Setup consumer for dead letter queue"""
    try:
        dlq_consumer = KafkaConsumer(
            KAFKA_CONFIG['dead_letter_topic'],
            bootstrap_servers=KAFKA_CONFIG['bootstrap_servers'],
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='dead-letter-processor',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        return dlq_consumer
    except Exception as e:
        logger.error(f"❌ Failed to setup DLQ consumer: {e}")
        return None

def process_dead_letter_messages(dlq_consumer):
    """Process messages from dead letter queue"""
    if not dlq_consumer:
        return
    
    try:
        for message in dlq_consumer:
            dead_event = message.value
            error = dead_event.get('error', 'Unknown error')
            original_action = dead_event.get('action', 'unknown')
            
            logger.error(f"💀 DEAD LETTER: {original_action} - Error: {error}")
            
            # Here you could:
            # 1. Send notification
            # 2. Log to special error log
            # 3. Attempt reprocessing
            # 4. Store in database for manual review
            
    except Exception as e:
        logger.error(f"❌ Error in DLQ processing: {e}")

# ===== MAIN CONSUMER SETUP =====
def create_consumer():
    """Create Kafka consumer with retry logic"""
    max_retries = 20
    for attempt in range(max_retries):
        try:
            consumer = KafkaConsumer(
                KAFKA_CONFIG['topic'],
                bootstrap_servers=KAFKA_CONFIG['bootstrap_servers'],
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                group_id=KAFKA_CONFIG['group_id'],
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                session_timeout_ms=30000,
                heartbeat_interval_ms=10000
            )
            
            # Test connection
            # consumer.topics()
            logger.info("✅ Kafka consumer created successfully")
            return consumer
            
        except Exception as e:
            logger.warning(f"⚠️ Consumer creation attempt {attempt + 1}/{max_retries} failed: {e}")
            if attempt == max_retries - 1:
                logger.error("❌ Failed to create Kafka consumer")
                return None
            time.sleep(3)

def run_consumer():
    """Main consumer loop"""
    logger.info("🚀 Starting Kafka Consumer...")
    processing_stats['start_time'] = time.time()
    
    # Create main consumer
    consumer = create_consumer()
    if not consumer:
        return
    
    # Create DLQ consumer (optional)
    dlq_consumer = setup_dead_letter_consumer()
    
    logger.info(f"👂 Listening to topic: {KAFKA_CONFIG['topic']}")
    if dlq_consumer:
        logger.info(f"👂 Also listening to DLQ: {KAFKA_CONFIG['dead_letter_topic']}")
    
    try:
        # Main message processing loop
        for message in consumer:
            success = process_book_event(message)
            
            # Print stats every 10 messages
            if processing_stats['total_messages'] % 10 == 0:
                print_stats()
                
    except KeyboardInterrupt:
        logger.info("⏹️ Consumer stopped by user")
    except KafkaError as e:
        logger.error(f"❌ Kafka error: {e}")
    except Exception as e:
        logger.error(f"❌ Unexpected error: {e}")
    finally:
        if consumer:
            consumer.close()
        if dlq_consumer:
            dlq_consumer.close()
        
        print_stats()
        logger.info("🔚 Consumer shutdown complete")

if __name__ == '__main__':
    run_consumer()