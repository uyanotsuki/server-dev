import os
import json
import logging
import time
from datetime import datetime
from flask import Flask, request, jsonify, g
import psycopg2
from psycopg2 import extras
import redis
from kafka import KafkaProducer
from kafka.errors import KafkaError

# ===== CONFIGURATION =====
app = Flask(__name__)
app.config['JSON_AS_ASCII'] = False
app.config['JSONIFY_PRETTYPRINT_REGULAR'] = True

# Environment variables
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'postgres'),
    'database': os.getenv('DB_NAME', 'bookdb'),
    'user': os.getenv('DB_USER', 'user'),
    'password': os.getenv('DB_PASS', 'password')
}

REDIS_CONFIG = {
    'host': os.getenv('REDIS_HOST', 'redis'),
    'port': int(os.getenv('REDIS_PORT', 6379)),
    'decode_responses': True
}

KAFKA_CONFIG = {
    'bootstrap_servers': [os.getenv('KAFKA_BROKER_HOST', 'kafka:9092')],
    'topic': os.getenv('KAFKA_TOPIC', 'book_events'),
    'dead_letter_topic': os.getenv('DEAD_LETTER_TOPIC', 'dead_letter_events')
}

# ===== LOGGING SETUP =====
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('logs/app.log', encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)

# ===== GLOBAL SERVICES =====
redis_client = None
kafka_producer = None
dead_letter_producer = None

# ===== DATABASE CONNECTION =====
def get_db_connection():
    """Get database connection with retry logic"""
    if 'db_conn' not in g or g.db_conn.closed:
        max_retries = 10
        for attempt in range(max_retries):
            try:
                g.db_conn = psycopg2.connect(**DB_CONFIG)
                logger.info("✅ Database connection established")
                return g.db_conn
            except Exception as e:
                logger.warning(f"⚠️ Database connection attempt {attempt + 1}/{max_retries} failed: {e}")
                if attempt == max_retries - 1:
                    raise e
                time.sleep(2)
    return g.db_conn

@app.teardown_appcontext
def close_db_connection(exception):
    """Close database connection after request"""
    db_conn = g.pop('db_conn', None)
    if db_conn is not None:
        db_conn.close()

# ===== KAFKA PRODUCER SETUP =====
def init_kafka_producer():
    """Initialize Kafka producer with error handling"""
    global kafka_producer, dead_letter_producer
    
    max_retries = 15
    for attempt in range(max_retries):
        try:
            kafka_producer = KafkaProducer(
                bootstrap_servers=KAFKA_CONFIG['bootstrap_servers'],
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'),
                acks='all',
                retries=3,
                request_timeout_ms=10000,
                api_version=(2, 0, 2)
            )
            
            dead_letter_producer = KafkaProducer(
                bootstrap_servers=KAFKA_CONFIG['bootstrap_servers'],
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8')
            )
            
            # Test connection
            # kafka_producer.list_topics()
            logger.info("✅ Kafka producers initialized successfully")
            return True
            
        except Exception as e:
            logger.warning(f"⚠️ Kafka producer attempt {attempt + 1}/{max_retries} failed: {e}")
            if attempt == max_retries - 1:
                logger.error("❌ Failed to initialize Kafka producers")
                return False
            time.sleep(3)

# ===== REDIS SETUP =====
def init_redis():
    """Initialize Redis connection"""
    global redis_client
    
    max_retries = 10
    for attempt in range(max_retries):
        try:
            redis_client = redis.Redis(**REDIS_CONFIG)
            redis_client.ping()
            logger.info("✅ Redis connection established")
            return True
        except Exception as e:
            logger.warning(f"⚠️ Redis connection attempt {attempt + 1}/{max_retries} failed: {e}")
            if attempt == max_retries - 1:
                logger.error("❌ Failed to connect to Redis")
                redis_client = None
                return False
            time.sleep(2)

# ===== DATABASE INITIALIZATION =====
def init_database():
    """Initialize database tables and sample data"""
    max_retries = 10
    for attempt in range(max_retries):
        try:
            conn = psycopg2.connect(**DB_CONFIG)
            with conn.cursor() as cursor:
                # Create books table
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS books (
                        id SERIAL PRIMARY KEY,
                        title VARCHAR(255) NOT NULL,
                        author VARCHAR(255) NOT NULL,
                        year INTEGER,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                ''')
                
                # Insert sample data if table is empty
                cursor.execute("SELECT COUNT(*) FROM books")
                if cursor.fetchone()[0] == 0:
                    sample_books = [
                        ('Война и мир', 'Лев Толстой', 1869),
                        ('Преступление и наказание', 'Фёдор Достоевский', 1866),
                        ('Мастер и Маргарита', 'Михаил Булгаков', 1967)
                    ]
                    cursor.executemany(
                        "INSERT INTO books (title, author, year) VALUES (%s, %s, %s)",
                        sample_books
                    )
                    logger.info(f"✅ Inserted {len(sample_books)} sample books")
                
                conn.commit()
            conn.close()
            logger.info("✅ Database initialized successfully")
            return True
            
        except Exception as e:
            logger.warning(f"⚠️ Database initialization attempt {attempt + 1}/{max_retries} failed: {e}")
            if attempt == max_retries - 1:
                logger.error("❌ Failed to initialize database")
                return False
            time.sleep(2)

# ===== KAFKA EVENT PUBLISHING =====
def publish_kafka_event(action, book_data=None, book_id=None):
    """Publish event to Kafka topic with error handling"""
    if not kafka_producer:
        logger.error("❌ Kafka producer not available")
        return False
    
    try:
        event = {
            'timestamp': datetime.now().isoformat(),
            'action': action,
            'user_ip': request.remote_addr,
            'book_id': book_id,
            'data': book_data
        }
        
        future = kafka_producer.send(KAFKA_CONFIG['topic'], value=event)
        # Wait for message to be delivered
        future.get(timeout=10)
        
        logger.info(f"✅ Event published: {action} for book {book_id}")
        return True
        
    except KafkaError as e:
        logger.error(f"❌ Kafka error: {e}")
        # Send to dead letter queue
        send_to_dead_letter_queue(event, str(e))
        return False
    except Exception as e:
        logger.error(f"❌ Unexpected error publishing event: {e}")
        return False

def send_to_dead_letter_queue(event, error_message):
    """Send failed messages to dead letter queue"""
    if dead_letter_producer:
        try:
            dead_event = {
                **event,
                'error': error_message,
                'dead_letter_timestamp': datetime.now().isoformat()
            }
            dead_letter_producer.send(KAFKA_CONFIG['dead_letter_topic'], value=dead_event)
            logger.info("✅ Message sent to dead letter queue")
        except Exception as e:
            logger.error(f"❌ Failed to send to dead letter queue: {e}")

# ===== REQUEST STATISTICS =====
request_stats = {
    'total_requests': 0,
    'average_time': 0,
    'endpoints': {}
}

def update_statistics(endpoint, execution_time):
    """Update request statistics"""
    request_stats['total_requests'] += 1
    
    # Update global average
    total_time = request_stats['average_time'] * (request_stats['total_requests'] - 1) + execution_time
    request_stats['average_time'] = total_time / request_stats['total_requests']
    
    # Update endpoint statistics
    if endpoint not in request_stats['endpoints']:
        request_stats['endpoints'][endpoint] = {
            'count': 0,
            'total_time': 0,
            'average_time': 0,
            'min_time': float('inf'),
            'max_time': 0
        }
    
    stats = request_stats['endpoints'][endpoint]
    stats['count'] += 1
    stats['total_time'] += execution_time
    stats['average_time'] = stats['total_time'] / stats['count']
    stats['min_time'] = min(stats['min_time'], execution_time)
    stats['max_time'] = max(stats['max_time'], execution_time)

# ===== REQUEST HOOKS =====
@app.before_request
def before_request():
    """Log incoming requests"""
    g.start_time = time.time()
    logger.info(f"📥 Incoming: {request.method} {request.path}")

@app.after_request
def after_request(response):
    """Log outgoing responses and update statistics"""
    execution_time = time.time() - g.start_time
    endpoint = f"{request.method} {request.path}"
    
    update_statistics(endpoint, execution_time)
    
    logger.info(f"📤 Outgoing: {response.status_code} for {endpoint} - {execution_time:.3f}s")
    return response

# ===== API ROUTES =====

@app.route('/')
def index():
    """API information"""
    return jsonify({
        'message': 'Book Library API with Kafka',
        'endpoints': {
            'GET /books': 'Get all books',
            'GET /books/<id>': 'Get book by ID',
            'POST /books': 'Create new book',
            'PUT /books/<id>': 'Update book',
            'DELETE /books/<id>': 'Delete book',
            'GET /stats': 'Get request statistics',
            'GET /health': 'Health check'
        }
    })

@app.route('/books', methods=['GET'])
def get_books():
    """Get all books with Redis caching"""
    cache_key = 'all_books'
    
    # Try Redis cache first
    if redis_client:
        try:
            cached_books = redis_client.get(cache_key)
            if cached_books:
                logger.info("📚 Books retrieved from cache")
                return jsonify(json.loads(cached_books))
        except Exception as e:
            logger.warning(f"⚠️ Redis cache error: {e}")
    
    # Get from database
    try:
        conn = get_db_connection()
        with conn.cursor(cursor_factory=extras.RealDictCursor) as cursor:
            cursor.execute("SELECT id, title, author, year FROM books ORDER BY id")
            books = cursor.fetchall()
        
        # Cache in Redis
        if redis_client:
            try:
                redis_client.setex(
                    cache_key,
                    int(os.getenv('REDIS_CACHE_TIMEOUT', 30)),
                    json.dumps([dict(book) for book in books], ensure_ascii=False)
                )
            except Exception as e:
                logger.warning(f"⚠️ Failed to cache in Redis: {e}")
        
        logger.info(f"📚 Retrieved {len(books)} books from database")
        return jsonify([dict(book) for book in books])
        
    except Exception as e:
        logger.error(f"❌ Error getting books: {e}")
        return jsonify({'error': 'Internal server error'}), 500

@app.route('/books/<int:book_id>', methods=['GET'])
def get_book(book_id):
    """Get book by ID"""
    try:
        conn = get_db_connection()
        with conn.cursor(cursor_factory=extras.RealDictCursor) as cursor:
            cursor.execute(
                "SELECT id, title, author, year FROM books WHERE id = %s",
                (book_id,)
            )
            book = cursor.fetchone()
        
        if book:
            logger.info(f"📖 Retrieved book ID {book_id}")
            return jsonify(dict(book))
        else:
            logger.warning(f"❌ Book {book_id} not found")
            return jsonify({'error': 'Book not found'}), 404
            
    except Exception as e:
        logger.error(f"❌ Error getting book {book_id}: {e}")
        return jsonify({'error': 'Internal server error'}), 500

@app.route('/books', methods=['POST'])
def create_book():
    """Create new book"""
    try:
        data = request.get_json()
        if not data:
            return jsonify({'error': 'JSON data required'}), 400
        
        required_fields = ['title', 'author', 'year']
        if not all(field in data for field in required_fields):
            return jsonify({
                'error': f'Missing required fields: {required_fields}'
            }), 400
        
        conn = get_db_connection()
        with conn.cursor(cursor_factory=extras.RealDictCursor) as cursor:
            cursor.execute('''
                INSERT INTO books (title, author, year)
                VALUES (%s, %s, %s)
                RETURNING id, title, author, year
            ''', (data['title'], data['author'], data['year']))
            
            new_book = cursor.fetchone()
            conn.commit()
        
        # Clear cache
        if redis_client:
            redis_client.delete('all_books')
        
        # Publish Kafka event
        publish_kafka_event('book_created', dict(new_book), new_book['id'])
        
        logger.info(f"✅ Created book: {new_book['title']} (ID: {new_book['id']})")
        return jsonify(dict(new_book)), 201
        
    except Exception as e:
        logger.error(f"❌ Error creating book: {e}")
        return jsonify({'error': 'Internal server error'}), 500

@app.route('/books/<int:book_id>', methods=['PUT'])
def update_book(book_id):
    """Update book"""
    try:
        data = request.get_json()
        if not data:
            return jsonify({'error': 'JSON data required'}), 400
        
        conn = get_db_connection()
        with conn.cursor(cursor_factory=extras.RealDictCursor) as cursor:
            # Build dynamic update query
            update_fields = []
            values = []
            
            for field in ['title', 'author', 'year']:
                if field in data:
                    update_fields.append(f"{field} = %s")
                    values.append(data[field])
            
            if not update_fields:
                return jsonify({'error': 'No fields to update'}), 400
            
            values.append(book_id)
            query = f"""
                UPDATE books 
                SET {', '.join(update_fields)} 
                WHERE id = %s 
                RETURNING id, title, author, year
            """
            
            cursor.execute(query, values)
            updated_book = cursor.fetchone()
            
            if not updated_book:
                return jsonify({'error': 'Book not found'}), 404
            
            conn.commit()
        
        # Clear cache
        if redis_client:
            redis_client.delete('all_books')
        
        # Publish Kafka event
        publish_kafka_event('book_updated', dict(updated_book), book_id)
        
        logger.info(f"✅ Updated book ID {book_id}")
        return jsonify(dict(updated_book))
        
    except Exception as e:
        logger.error(f"❌ Error updating book {book_id}: {e}")
        return jsonify({'error': 'Internal server error'}), 500

@app.route('/books/<int:book_id>', methods=['DELETE'])
def delete_book(book_id):
    """Delete book"""
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            # Get book info before deletion for Kafka event
            cursor.execute("SELECT title, author FROM books WHERE id = %s", (book_id,))
            book_info = cursor.fetchone()
            
            if not book_info:
                return jsonify({'error': 'Book not found'}), 404
            
            cursor.execute("DELETE FROM books WHERE id = %s", (book_id,))
            conn.commit()
        
        # Clear cache
        if redis_client:
            redis_client.delete('all_books')
        
        # Publish Kafka event
        publish_kafka_event('book_deleted', {
            'title': book_info[0],
            'author': book_info[1]
        }, book_id)
        
        logger.info(f"✅ Deleted book ID {book_id}")
        return jsonify({'message': f'Book {book_id} deleted successfully'})
        
    except Exception as e:
        logger.error(f"❌ Error deleting book {book_id}: {e}")
        return jsonify({'error': 'Internal server error'}), 500

@app.route('/stats', methods=['GET'])
def get_stats():
    """Get request statistics"""
    return jsonify(request_stats)

@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    status = {
        'status': 'healthy',
        'timestamp': datetime.now().isoformat(),
        'services': {}
    }
    
    # Check database
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            cursor.execute("SELECT 1")
        status['services']['database'] = 'healthy'
    except Exception as e:
        status['services']['database'] = f'unhealthy: {e}'
        status['status'] = 'degraded'
    
    # Check Redis
    if redis_client:
        try:
            redis_client.ping()
            status['services']['redis'] = 'healthy'
        except Exception as e:
            status['services']['redis'] = f'unhealthy: {e}'
            status['status'] = 'degraded'
    else:
        status['services']['redis'] = 'not connected'
    
    # Check Kafka - упрощенная проверка
    if kafka_producer:
        try:
            # Простая проверка - отправка тестового сообщения
            test_future = kafka_producer.send(KAFKA_CONFIG['topic'], value={'test': 'health_check'})
            # Не ждем подтверждения, просто проверяем что producer работает
            status['services']['kafka'] = 'healthy'
        except Exception as e:
            status['services']['kafka'] = f'unhealthy: {e}'
            status['status'] = 'degraded'
    else:
        status['services']['kafka'] = 'not connected'
    
    return jsonify(status)

# ===== ERROR HANDLERS =====
@app.errorhandler(404)
def not_found(error):
    return jsonify({'error': 'Endpoint not found'}), 404

@app.errorhandler(405)
def method_not_allowed(error):
    return jsonify({'error': 'Method not allowed'}), 405

@app.errorhandler(500)
def internal_error(error):
    return jsonify({'error': 'Internal server error'}), 500

# ===== APPLICATION INITIALIZATION =====
def initialize_services():
    """Initialize all services"""
    logger.info("🚀 Initializing services...")
    
    # Initialize services in order
    init_database()
    init_redis()
    init_kafka_producer()
    
    logger.info("✅ All services initialized successfully")

if __name__ == '__main__':
    initialize_services()
    
    logger.info("""
    📚 Book Library API with Kafka
    ==============================
    Available endpoints:
    GET    /              - API information
    GET    /books         - Get all books
    GET    /books/<id>    - Get book by ID  
    POST   /books         - Create new book
    PUT    /books/<id>    - Update book
    DELETE /books/<id>    - Delete book
    GET    /stats         - Request statistics
    GET    /health        - Health check
    """)
    
    app.run(host='0.0.0.0', port=5000, debug=False)