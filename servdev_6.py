from flask import Flask, request, jsonify
import logging
import time

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('books_app.log', encoding='utf-8'),
        logging.StreamHandler()  # Также выводим логи в консоль
    ]
)
logger = logging.getLogger(__name__)

app = Flask(__name__)

books = [
    {"id": 1, "title": "Война и мир", "author": "Лев Толстой", "year": 1869},
    {"id": 2, "title": "Преступление и наказание", "author": "Фёдор Достоевский", "year": 1866}
]

# для первого запроса
app_started = False

# словарь для хранения статистики
request_stats = {
    'total_requests': 0,
    'average_time': 0,
    'endpoints': {}
}

def log_request_info():
    # Логирование информации о входящем запросе
    logger.info(f"Входящий запрос: {request.method} {request.path} - IP: {request.remote_addr}")
    if request.method in ['POST', 'PUT'] and request.is_json:
        logger.debug(f"Тело запроса: {request.get_json()}")

def log_response_info(response, status_code, execution_time=None):
    # Логирование информации об исходящем ответе
    if execution_time is not None:
        logger.info(f"Исходящий ответ: {status_code} для {request.method} {request.path} - Время выполнения: {execution_time:.3f} сек")
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

def measure_execution_time(func_name, endpoint):
    start_time = time.time()
    execution_time = time.time() - start_time
    update_statistics(endpoint, execution_time)
    return execution_time

# Функция, которая выполняется перед каждым запросом
@app.before_request
def before_first_request():
    global app_started
    if not app_started:
        logger.info("=" * 50)
        logger.info("Flask приложение 'Библиотека' запущено и готово к запросам")
        logger.info(f"Текущее количество книг: {len(books)}")
        logger.info("=" * 50)
        app_started = True

# 1. ПОЛУЧИТЬ ВСЕ КНИГИ
@app.route('/books', methods=['GET'])
def get_books():
    start_time = time.time()
    log_request_info()
    
    try:
        logger.info("Запрос на получение всех книг")
        logger.info(f"Найдено книг: {len(books)}")
        
        response = jsonify(books)
        execution_time = time.time() - start_time
        
        # Обновляем статистику
        update_statistics("GET /books", execution_time)
        log_response_info(response, 200, execution_time)
        
        return response
    except Exception as e:
        execution_time = time.time() - start_time
        update_statistics("GET /books", execution_time)
        logger.error(f"Ошибка при получении всех книг: {str(e)} - Время: {execution_time:.3f} сек")
        return jsonify({"error": "Внутренняя ошибка сервера"}), 500

# 2. ПОЛУЧИТЬ ОДНУ КНИГУ
@app.route('/books/<int:book_id>', methods=['GET'])
def get_one_book(book_id):
    start_time = time.time()
    log_request_info()
    
    try:
        logger.info(f"Запрос на получение книги с ID: {book_id}")
        for book in books:
            if book['id'] == book_id:
                logger.info(f"Книга с ID {book_id} найдена: {book['title']}")
                
                response = jsonify(book)
                execution_time = time.time() - start_time
                update_statistics(f"GET /books/{book_id}", execution_time)
                log_response_info(response, 200, execution_time)
                
                return response
        
        logger.warning(f"Книга с ID {book_id} не найдена")
        execution_time = time.time() - start_time
        update_statistics(f"GET /books/{book_id}", execution_time)
        return jsonify({"error": "Нет такой книги"}), 404
        
    except Exception as e:
        execution_time = time.time() - start_time
        update_statistics(f"GET /books/{book_id}", execution_time)
        logger.error(f"Ошибка при получении книги {book_id}: {str(e)} - Время: {execution_time:.3f} сек")
        return jsonify({"error": "Внутренняя ошибка сервера"}), 500

# 3. ДОБАВИТЬ КНИГУ
@app.route('/books', methods=['POST'])
def add_book():
    start_time = time.time()
    log_request_info()
    
    try:
        data = request.get_json()
        logger.info(f"Запрос на создание новой книги с данными: {data}")
        
        # Проверяем что все поля есть
        if not data or 'title' not in data or 'author' not in data or 'year' not in data:
            logger.warning("Попытка создания книги без всех обязательных полей")
            execution_time = time.time() - start_time
            update_statistics("POST /books", execution_time)
            return jsonify({"error": "Нужны title, author и year"}), 400
        
        # Создаем новую книгу
        new_id = max([book['id'] for book in books]) + 1
        new_book = {
            'id': new_id,
            'title': data['title'],
            'author': data['author'],
            'year': data['year']
        }
        
        books.append(new_book)
        logger.info(f"Создана новая книга: ID={new_id}, Название='{data['title']}', Автор='{data['author']}'")
        
        response = jsonify(new_book)
        execution_time = time.time() - start_time
        update_statistics("POST /books", execution_time)
        log_response_info(response, 201, execution_time)
        
        return response, 201
        
    except Exception as e:
        execution_time = time.time() - start_time
        update_statistics("POST /books", execution_time)
        logger.error(f"Ошибка при создании книги: {str(e)} - Время: {execution_time:.3f} сек")
        return jsonify({"error": "Внутренняя ошибка сервера"}), 500

# 4. ИЗМЕНИТЬ КНИГУ
@app.route('/books/<int:book_id>', methods=['PUT'])
def update_book(book_id):
    start_time = time.time()
    log_request_info()
    
    try:
        data = request.get_json()
        logger.info(f"Запрос на обновление книги ID={book_id} с данными: {data}")
        
        for book in books:
            if book['id'] == book_id:
                old_title = book['title']
                old_author = book['author']
                old_year = book['year']
                
                # Меняем только то, что пришло в запросе
                if 'title' in data:
                    book['title'] = data['title']
                if 'author' in data:
                    book['author'] = data['author']
                if 'year' in data:
                    book['year'] = data['year']
                
                logger.info(f"Книга ID={book_id} обновлена: "
                           f"Название: '{old_title}' -> '{book['title']}', "
                           f"Автор: '{old_author}' -> '{book['author']}', "
                           f"Год: {old_year} -> {book['year']}")
                
                response = jsonify(book)
                execution_time = time.time() - start_time
                update_statistics(f"PUT /books/{book_id}", execution_time)
                log_response_info(response, 200, execution_time)
                
                return response
        
        logger.warning(f"Попытка обновления несуществующей книги ID={book_id}")
        execution_time = time.time() - start_time
        update_statistics(f"PUT /books/{book_id}", execution_time)
        return jsonify({"error": "Нет такой книги"}), 404
        
    except Exception as e:
        execution_time = time.time() - start_time
        update_statistics(f"PUT /books/{book_id}", execution_time)
        logger.error(f"Ошибка при обновлении книги {book_id}: {str(e)} - Время: {execution_time:.3f} сек")
        return jsonify({"error": "Внутренняя ошибка сервера"}), 500

# 5. УДАЛИТЬ КНИГУ
@app.route('/books/<int:book_id>', methods=['DELETE'])
def delete_book(book_id):
    start_time = time.time()
    log_request_info()
    
    try:
        logger.info(f"Запрос на удаление книги с ID: {book_id}")
        for i, book in enumerate(books):
            if book['id'] == book_id:
                deleted = books.pop(i)
                logger.info(f"Книга ID={book_id} ('{book['title']}') успешно удалена")
                
                response = jsonify({"message": "Книга удалена", "book": deleted})
                execution_time = time.time() - start_time
                update_statistics(f"DELETE /books/{book_id}", execution_time)
                log_response_info(response, 200, execution_time)
                
                return response
        
        logger.warning(f"Попытка удаления несуществующей книги ID={book_id}")
        execution_time = time.time() - start_time
        update_statistics(f"DELETE /books/{book_id}", execution_time)
        return jsonify({"error": "Нет такой книги"}), 404
        
    except Exception as e:
        execution_time = time.time() - start_time
        update_statistics(f"DELETE /books/{book_id}", execution_time)
        logger.error(f"Ошибка при удалении книги {book_id}: {str(e)} - Время: {execution_time:.3f} сек")
        return jsonify({"error": "Внутренняя ошибка сервера"}), 500

# 6. ПРОВЕРКА СЕРВЕРА
@app.route('/health', methods=['GET'])
def health():
    start_time = time.time()
    log_request_info()
    
    try:
        logger.info("Запрос проверки сервера")
        response = jsonify({"status": "OK", "books_count": len(books)})
        
        execution_time = time.time() - start_time
        update_statistics("GET /health", execution_time)
        log_response_info(response, 200, execution_time)
        
        return response
    except Exception as e:
        execution_time = time.time() - start_time
        update_statistics("GET /health", execution_time)
        logger.error(f"Ошибка при проверке: {str(e)} - Время: {execution_time:.3f} сек")
        return jsonify({"error": "Внутренняя ошибка сервера"}), 500

# 7. СТАТИСТИКА ВЫПОЛНЕНИЯ ЗАПРОСОВ
@app.route('/stats', methods=['GET'])
def get_stats():
    start_time = time.time()
    log_request_info()
    
    try:
        logger.info("Запрос статистики выполнения")
        
        stats_summary = {
            'total_requests': request_stats['total_requests'],
            'average_execution_time_sec': round(request_stats['average_time'], 3),
            'endpoints': {}
        }
        
        for endpoint, endpoint_stat in request_stats['endpoints'].items():
            stats_summary['endpoints'][endpoint] = {
                'request_count': endpoint_stat['count'],
                'average_execution_time_sec': round(endpoint_stat['average_time'], 3),
                'min_execution_time_sec': round(endpoint_stat['min_time'], 3),
                'max_execution_time_sec': round(endpoint_stat['max_time'], 3)
            }
        
        response = jsonify(stats_summary)
        execution_time = time.time() - start_time
        update_statistics("GET /stats", execution_time)
        log_response_info(response, 200, execution_time)
        
        return response
    except Exception as e:
        execution_time = time.time() - start_time
        update_statistics("GET /stats", execution_time)
        logger.error(f"Ошибка при получении статистики: {str(e)} - Время: {execution_time:.3f} сек")
        return jsonify({"error": "Внутренняя ошибка сервера"}), 500

# Обработчик ошибок для несуществующих маршрутов
@app.errorhandler(404)
def not_found(error):
    start_time = time.time()
    logger.warning(f"Запрос к несуществующему маршруту: {request.path}")
    
    response = jsonify({"error": "Маршрут не найден"}), 404
    execution_time = time.time() - start_time
    update_statistics(f"ERROR {request.path}", execution_time)
    
    return response

# Обработчик ошибок для методов, которые не разрешены
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
    
    app.run(debug=True, port=5000)