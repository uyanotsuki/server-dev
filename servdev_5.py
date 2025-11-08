from flask import Flask, request, jsonify

app = Flask(__name__)

# Наши книги (вместо базы данных)
books = [
    {"id": 1, "title": "Война и мир", "author": "Лев Толстой", "year": 1869},
    {"id": 2, "title": "Преступление и наказание", "author": "Фёдор Достоевский", "year": 1866}
]

# 1. ПОЛУЧИТЬ ВСЕ КНИГИ
@app.route('/books', methods=['GET'])
def get_books():
    return jsonify(books)

# 2. ПОЛУЧИТЬ ОДНУ КНИГУ
@app.route('/books/<int:book_id>', methods=['GET'])
def get_one_book(book_id):
    for book in books:
        if book['id'] == book_id:
            return jsonify(book)
    return jsonify({"error": "Нет такой книги"}), 404

# 3. ДОБАВИТЬ КНИГУ
@app.route('/books', methods=['POST'])
def add_book():
    data = request.get_json()
    
    # Проверяем что все поля есть
    if not data or 'title' not in data or 'author' not in data or 'year' not in data:
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
    return jsonify(new_book), 201

# 4. ИЗМЕНИТЬ КНИГУ
@app.route('/books/<int:book_id>', methods=['PUT'])
def update_book(book_id):
    for book in books:
        if book['id'] == book_id:
            data = request.get_json()
            
            # Меняем только то, что пришло в запросе
            if 'title' in data:
                book['title'] = data['title']
            if 'author' in data:
                book['author'] = data['author']
            if 'year' in data:
                book['year'] = data['year']
            
            return jsonify(book)
    
    return jsonify({"error": "Нет такой книги"}), 404

# 5. УДАЛИТЬ КНИГУ
@app.route('/books/<int:book_id>', methods=['DELETE'])
def delete_book(book_id):
    for i, book in enumerate(books):
        if book['id'] == book_id:
            deleted = books.pop(i)
            return jsonify({"message": "Книга удалена", "book": deleted})
    
    return jsonify({"error": "Нет такой книги"}), 404

# 6. ПРОВЕРКА СЕРВЕРА
@app.route('/health', methods=['GET'])
def health():
    return jsonify({"status": "OK", "books_count": len(books)})

# ЗАПУСК
if __name__ == '__main__':
    print("Сервер запущен: http://localhost:5000")
    print("\n📖 Что можно делать:")
    print("GET    /books          - посмотреть все книги")
    print("GET    /books/1        - посмотреть книгу 1")
    print("POST   /books          - добавить книгу") 
    print("PUT    /books/1        - изменить книгу 1")
    print("DELETE /books/1        - удалить книгу 1")
    print("GET    /health         - проверить сервер")
    
    app.run(debug=True, port=5000)