from flask import Flask, request, jsonify, g
import logging
import time
import os
import glob
import psycopg2 
from psycopg2 import extras # Нужен для удобного получения результата в виде словаря
import redis 
import json # Для хранения сложных объектов (список книг) в Redis
from datetime import datetime

# --- Настройки PostgreSQL (читаются из переменных окружения Docker Compose/ .env) ---
# Хосты db и cache - это имена сервисов в docker-compose.yml
DB_HOST = os.environ.get('DB_HOST', 'db')
DB_NAME = os.environ.get('DB_NAME', 'mydatabase')
DB_USER = os.environ.get('DB_USER', 'user')
DB_PASS = os.environ.get('DB_PASS', 'password')

# --- Настройки Redis ---
REDIS_HOST = os.environ.get('REDIS_HOST', 'cache') # Имя сервиса 'cache' в docker-compose
REDIS_PORT = int(os.environ.get('REDIS_PORT', 6379))
REDIS_CACHE_TIMEOUT = 30 # Время жизни кэша в секундах
REDIS_KEY_ALL_BOOKS = 'all_books'

# --- Настройки Логирования ---
LOG_FILE = 'books_app.log' # Используем имя из твоего файла
MAX_LOG_SIZE = 5 * 1024 * 1024  # 5 MB
BACKUP_COUNT = 3 

# --- Инициализация Flask и Логирования ---
app = Flask(__name__)
app.config['JSON_AS_ASCII'] = False

# [Оставляем класс SimpleFileHandler из предыдущих шагов, если он был в lab5.py,
# если нет, используем стандартную настройку, чтобы не менять твой код сильно]
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE, encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


# --- Глобальные объекты для БД и Redis ---
redis_client = None

def get_db_connection():
    """Возвращает существующее соединение с БД или создает новое."""
    if 'db_conn' not in g or g.db_conn.closed:
        try:
            g.db_conn = psycopg2.connect(
                host=DB_HOST,
                database=DB_NAME,
                user=DB_USER,
                password=DB_PASS
            )
            logger.info("Установлено новое соединение с PostgreSQL.")
        except Exception as e:
            logger.error(f"Ошибка при подключении к PostgreSQL: {e}")
            # Внутри Docker Compose контейнер 'db' должен быть доступен
            raise e
    return g.db_conn

@app.teardown_appcontext
def close_db_connection(exception):
    """Закрывает соединение с БД после каждого запроса."""
    db_conn = g.pop('db_conn', None)
    if db_conn is not None:
        db_conn.close()
        logger.debug("Соединение с PostgreSQL закрыто.")


def init_db_and_redis():
    """Инициализирует БД (таблицы) и Redis (соединение) с попытками подключения."""
    global redis_client
    max_retries = 15 
    db_connected = False
    redis_connected = False
    
    # 1. Подключение к PostgreSQL с ретраями
    for i in range(max_retries):
        if db_connected: break
        try:
            conn = psycopg2.connect(host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASS, connect_timeout=3)
            with conn.cursor() as cursor:
                # Создание таблицы
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS books (
                        id SERIAL PRIMARY KEY,
                        title VARCHAR(255) NOT NULL,
                        author VARCHAR(255) NOT NULL,
                        year INTEGER
                    )
                ''')
                conn.commit()
                
                # Добавление начальных данных
                cursor.execute("SELECT COUNT(*) FROM books")
                if cursor.fetchone()[0] == 0:
                    cursor.execute('''
                        INSERT INTO books (title, author, year) VALUES 
                        (%s, %s, %s), (%s, %s, %s)
                    ''', [
                        ('Война и мир', 'Лев Толстой', 1869),
                        ('Преступление и наказание', 'Фёдор Достоевский', 1866)
                    ])
                    conn.commit()
                    logger.info("Добавлены начальные данные в базу PostgreSQL.")
                
            conn.close()
            db_connected = True
            logger.info("Успешное подключение и инициализация PostgreSQL.")
        except Exception as e:
            logger.warning(f"Ожидание PostgreSQL. Попытка {i+1}/{max_retries}. Ошибка: {e}")
            time.sleep(2)
            
    if not db_connected:
        logger.error("Не удалось подключиться к PostgreSQL после нескольких попыток. Книги будут недоступны.")
        
    # 2. Подключение к Redis с ретраями
    for i in range(max_retries):
        if redis_connected: break
        try:
            # Используем host 'cache', как указано в docker-compose.yml
            redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True, socket_timeout=3)
            redis_client.ping()
            redis_connected = True
            logger.info("Успешное подключение к Redis.")
        except Exception as e:
            logger.warning(f"Ожидание Redis. Попытка {i+1}/{max_retries}. Ошибка: {e}")
            time.sleep(2)
            
    if not redis_connected:
        logger.error("Не удалось подключиться к Redis после нескольких попыток. Кэширование будет отключено.")
        redis_client = None

# --- Логирование и Статистика (без изменений) ---
# [Здесь находится твой код для request_stats, log_request_info, log_response_info, update_statistics]
# ... (Оставил твой код статистики без изменений)
request_stats = {
    'total_requests': 0,
    'average_time': 0,
    'endpoints': {}
}

def log_request_info():
    logger.info(f"Входящий запрос: {request.method} {request.path}")

def log_response_info(response, status_code, execution_time=None):
    if execution_time is not None:
        logger.info(f"Исходящий ответ: {status_code} для {request.method} {request.path} - Время: {execution_time:.3f} сек")
    else:
        logger.info(f"Исходящий ответ: {status_code} для {request.method} {request.path}")
    return response

def update_statistics(endpoint, execution_time):
    request_stats['total_requests'] += 1
    
    total_time = request_stats['average_time'] * (request_stats['total_requests'] - 1) + execution_time
    request_stats['average_time'] = total_time / request_stats['total_requests']
    
    if endpoint not in request_stats['endpoints']:
        request_stats['endpoints'][endpoint] = {
            'count': 0,
            'total_time': 0,
            'average_time': 0,
            'min_time': float('inf'),
            'max_time': 0
        }
    
    endpoint_stats = request_stats['endpoints'][endpoint]
    endpoint_stats['count'] += 1
    endpoint_stats['total_time'] += execution_time
    endpoint_stats['average_time'] = endpoint_stats['total_time'] / endpoint_stats['count']
    endpoint_stats['min_time'] = min(endpoint_stats['min_time'], execution_time)
    endpoint_stats['max_time'] = max(endpoint_stats['max_time'], execution_time)


app_started = False

# Функция, которая выполняется перед первым запросом
@app.before_request
def before_first_request():
    global app_started
    if not app_started:
        logger.info("=" * 50)
        logger.info("Flask приложение 'Библиотека' запущено и готово к запросам")
        
        # Получаем количество книг из базы (PostgreSQL)
        try:
            conn = get_db_connection()
            with conn.cursor() as cursor:
                cursor.execute("SELECT COUNT(*) FROM books")
                books_count = cursor.fetchone()[0]
                logger.info(f"Текущее количество книг (PostgreSQL): {books_count}")
        except Exception as e:
            # Не страшно, если не смогли подключиться, главное что Flask работает
            logger.error(f"Не удалось получить начальное количество книг из БД: {e}")
            
        logger.info("=" * 50)
        app_started = True

# --- API Эндпоинты (Обновлены для работы с PostgreSQL и Redis) ---

# 1. ПОЛУЧИТЬ ВСЕ КНИГИ (с кешированием Redis)
@app.route('/books', methods=['GET'])
def get_books():
    start_time = time.time()
    log_request_info()
    endpoint = "GET /books"
    
    try:
        # 1. Попытка получить из Redis
        if redis_client:
            cached_books_json = redis_client.get(REDIS_KEY_ALL_BOOKS)
            if cached_books_json:
                logger.info("Запрос на получение всех книг - Использован кеш Redis.")
                response_data = json.loads(cached_books_json)
                response = jsonify(response_data)
                
                execution_time = time.time() - start_time
                update_statistics(endpoint, execution_time)
                log_response_info(response, 200, execution_time)
                return response
        
        # 2. Если нет в кеше, получаем из PostgreSQL
        conn = get_db_connection()
        # Используем RealDictCursor для получения результата в виде списка словарей
        with conn.cursor(cursor_factory=extras.RealDictCursor) as cursor: 
            cursor.execute("SELECT id, title, author, year FROM books ORDER BY id")
            books_list = cursor.fetchall()
        
        logger.info(f"Запрос на получение всех книг. Найдено: {len(books_list)}")
        
        response = jsonify(books_list)
        
        # 3. Кешируем в Redis
        if redis_client:
            redis_client.set(REDIS_KEY_ALL_BOOKS, json.dumps(books_list), ex=REDIS_CACHE_TIMEOUT) 
            logger.info("Результат запроса /books сохранен в кеше Redis.")
            
        execution_time = time.time() - start_time
        update_statistics(endpoint, execution_time)
        log_response_info(response, 200, execution_time)
        
        return response
    except Exception as e:
        execution_time = time.time() - start_time
        update_statistics(endpoint, execution_time)
        logger.error(f"Ошибка при получении книг: {str(e)}")
        return jsonify({"error": "Внутренняя ошибка сервера"}), 500

# 2. ПОЛУЧИТЬ ОДНУ КНИГУ
@app.route('/books/<int:book_id>', methods=['GET'])
def get_one_book(book_id):
    start_time = time.time()
    log_request_info()
    endpoint = f"GET /books/{book_id}"
    
    try:
        conn = get_db_connection()
        with conn.cursor(cursor_factory=extras.RealDictCursor) as cursor:
            cursor.execute("SELECT id, title, author, year FROM books WHERE id = %s", (book_id,))
            book = cursor.fetchone()
        
        if book:
            logger.info(f"Книга с ID {book_id} найдена: {book['title']}")
            response = jsonify(book)
            execution_time = time.time() - start_time
            update_statistics(endpoint, execution_time)
            log_response_info(response, 200, execution_time)
            return response
        else:
            logger.warning(f"Книга с ID {book_id} не найдена")
            execution_time = time.time() - start_time
            update_statistics(endpoint, execution_time)
            return jsonify({"error": "Нет такой книги"}), 404
            
    except Exception as e:
        execution_time = time.time() - start_time
        update_statistics(endpoint, execution_time)
        logger.error(f"Ошибка при получении книги {book_id}: {str(e)}")
        return jsonify({"error": "Внутренняя ошибка сервера"}), 500

# 3. ДОБАВИТЬ КНИГУ (сброс кеша)
@app.route('/books', methods=['POST'])
def add_book():
    start_time = time.time()
    log_request_info()
    endpoint = "POST /books"
    
    conn = None
    try:
        data = request.get_json()
        logger.info(f"Запрос на создание новой книги с данными: {data}")
        
        required_fields = ['title', 'author', 'year']
        if not data or any(f not in data for f in required_fields):
            logger.warning("Попытка создания книги без всех обязательных полей")
            execution_time = time.time() - start_time
            update_statistics(endpoint, execution_time)
            return jsonify({"error": f"Нужны {', '.join(required_fields)}"}), 400
        
        conn = get_db_connection()
        with conn.cursor(cursor_factory=extras.RealDictCursor) as cursor:
            # RETURNING id позволяет получить ID новой записи
            cursor.execute('''
                INSERT INTO books (title, author, year) VALUES (%s, %s, %s)
                RETURNING id
            ''', (data['title'], data['author'], data['year']))
            
            new_id = cursor.fetchone()['id']
            conn.commit()
        
        # Сброс кеша Redis после изменения данных
        if redis_client:
            redis_client.delete(REDIS_KEY_ALL_BOOKS)
            logger.info(f"Сброшен кеш '{REDIS_KEY_ALL_BOOKS}' в Redis после POST запроса.")
        
        logger.info(f"Создана новая книга: ID={new_id}, Название='{data['title']}'")
        
        response = jsonify({"id": new_id, **data})
        execution_time = time.time() - start_time
        update_statistics(endpoint, execution_time)
        log_response_info(response, 201, execution_time)
        
        return response, 201
        
    except Exception as e:
        if conn:
            conn.rollback() # Откат транзакции в случае ошибки
        execution_time = time.time() - start_time
        update_statistics(endpoint, execution_time)
        logger.error(f"Ошибка при создании книги: {str(e)}")
        return jsonify({"error": "Внутренняя ошибка сервера"}), 500

# 4. ИЗМЕНИТЬ КНИГУ (сброс кеша)
@app.route('/books/<int:book_id>', methods=['PUT'])
def update_book(book_id):
    start_time = time.time()
    log_request_info()
    endpoint = f"PUT /books/{book_id}"
    
    conn = None
    try:
        data = request.get_json()
        logger.info(f"Запрос на обновление книги ID={book_id} с данными: {data}")
        
        conn = get_db_connection()
        
        # Составляем запрос на обновление динамически
        update_parts = []
        update_values = []
        
        if 'title' in data:
            update_parts.append("title = %s")
            update_values.append(data['title'])
        if 'author' in data:
            update_parts.append("author = %s")
            update_values.append(data['author'])
        if 'year' in data:
            update_parts.append("year = %s")
            update_values.append(data['year'])

        if not update_parts:
            # Если нет данных для обновления, просто возвращаем 200
            return jsonify({"message": "Нет данных для обновления"}), 200

        update_values.append(book_id)
        
        sql_query = f"UPDATE books SET {', '.join(update_parts)} WHERE id = %s RETURNING id, title, author, year"
        
        with conn.cursor(cursor_factory=extras.RealDictCursor) as cursor:
            cursor.execute(sql_query, update_values)
            updated_book = cursor.fetchone()
            
            if not updated_book:
                conn.rollback()
                logger.warning(f"Попытка обновления несуществующей книги ID={book_id}")
                execution_time = time.time() - start_time
                update_statistics(endpoint, execution_time)
                return jsonify({"error": "Нет такой книги"}), 404
            
            conn.commit()
        
        # Сброс кеша Redis
        if redis_client:
            redis_client.delete(REDIS_KEY_ALL_BOOKS)
            logger.info(f"Сброшен кеш '{REDIS_KEY_ALL_BOOKS}' в Redis после PUT запроса.")
        
        logger.info(f"Книга ID={book_id} успешно обновлена")
        
        response = jsonify(updated_book)
        execution_time = time.time() - start_time
        update_statistics(endpoint, execution_time)
        log_response_info(response, 200, execution_time)
        
        return response
        
    except Exception as e:
        if conn:
            conn.rollback()
        execution_time = time.time() - start_time
        update_statistics(endpoint, execution_time)
        logger.error(f"Ошибка при обновлении книги: {str(e)}")
        return jsonify({"error": "Внутренняя ошибка сервера"}), 500

# 5. УДАЛИТЬ КНИГУ (сброс кеша)
@app.route('/books/<int:book_id>', methods=['DELETE'])
def delete_book(book_id):
    start_time = time.time()
    log_request_info()
    endpoint = f"DELETE /books/{book_id}"
    
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            cursor.execute("DELETE FROM books WHERE id = %s", (book_id,))
            deleted_count = cursor.rowcount
            conn.commit()
        
        if deleted_count == 0:
            logger.warning(f"Попытка удаления несуществующей книги ID={book_id}")
            execution_time = time.time() - start_time
            update_statistics(endpoint, execution_time)
            return jsonify({"error": "Нет такой книги"}), 404
        
        # Сброс кеша Redis
        if redis_client:
            redis_client.delete(REDIS_KEY_ALL_BOOKS)
            logger.info(f"Сброшен кеш '{REDIS_KEY_ALL_BOOKS}' в Redis после DELETE запроса.")
            
        logger.info(f"Книга ID={book_id} успешно удалена.")
        
        response = jsonify({"message": f"Книга с ID {book_id} удалена"})
        execution_time = time.time() - start_time
        update_statistics(endpoint, execution_time)
        log_response_info(response, 200, execution_time)
        
        return response, 200
        
    except Exception as e:
        if conn:
            conn.rollback()
        execution_time = time.time() - start_time
        update_statistics(endpoint, execution_time)
        logger.error(f"Ошибка при удалении книги: {str(e)}")
        return jsonify({"error": "Внутренняя ошибка сервера"}), 500
        
# [Остальной код (logs/info, stats, errorhandler 404/405) оставлен без изменений]

# Эндпоинт для получения информации о логах 
@app.route('/logs/info', methods=['GET'])
def get_logs_info():
    # ... (твой код)
    try:
        base_name = LOG_FILE.replace('.log', '')
        log_files = []
        
        for f in glob.glob(f"{base_name}*.log") + glob.glob(LOG_FILE): # Включаем текущий файл
            if os.path.exists(f):
                file_size = os.path.getsize(f)
                log_files.append({
                    "filename": os.path.basename(f),
                    "size_bytes": file_size,
                    "size_mb": round(file_size / (1024 * 1024), 2),
                })
        
        log_files.sort(key=lambda x: x['filename'])
        
        return jsonify({
            "max_size_mb": MAX_LOG_SIZE / (1024 * 1024),
            "backup_count": BACKUP_COUNT,
            "log_files": log_files
        }), 200
        
    except Exception as e:
        return jsonify({"error": f"Ошибка при получении информации о логах: {str(e)}"}), 500

# Эндпоинт для статистики
@app.route('/stats', methods=['GET'])
def get_stats():
    return jsonify(request_stats), 200


# Обработчик ошибок
@app.errorhandler(404)
def not_found(error):
    start_time = time.time()
    logger.warning(f"Запрос к несуществующему маршруту: {request.path}")
    
    response = jsonify({"error": "Маршрут не найден"}), 404
    execution_time = time.time() - start_time
    update_statistics(f"ERROR {request.path}", execution_time)
    
    return response

@app.errorhandler(405)
def method_not_allowed(error):
    start_time = time.time()
    logger.warning(f"Неподдерживаемый метод {request.method} для маршрута {request.path}")
    
    response = jsonify({"error": "Метод не разрешен"}), 405
    execution_time = time.time() - start_time
    update_statistics(f"ERROR {request.path}", execution_time)
    
    return response

# ЗАПУСК
if __name__ == '__main__':
    init_db_and_redis()
    logger.info("Запуск Flask приложения 'Библиотека'...")
    print("Сервер запущен: http://localhost:5000")
    print("\n📖 Что можно делать:")
    print("GET    /books          - посмотреть все книги")
    print("GET    /books/1        - посмотреть книгу 1")
    print("POST   /books          - добавить книгу") 
    print("PUT    /books/1        - изменить книгу 1")
    print("DELETE /books/1        - удалить книгу 1")
    print("GET    /health         - проверить сервер")
    print("GET    /stats          - статистика выполнения запросов")
    
    app.run(host='0.0.0.0', port=5000, debug=False)