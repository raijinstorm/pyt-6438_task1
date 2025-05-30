# README
## Installation
1. Clone the repository:
   ```bash
   git clone https://github.com/raijinstorm/pyt-6438_task1.git
   cd pyt-6438_task1
   ```
2. Create a virtual environment:
   ```bash
   python -m venv venv
   source venv/bin/activate 
   ```
3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
4. Run the script:
   ```bash
   python3 query_db.py --students data/students.json --rooms data/rooms.json --format xml --queries_folder queries --output_folder output --record_teg_xml element --allow_rewrite yes
   ```


## Условие задачи
С использованием базы MySQL (или другая реляционная БД, например, PostgreSQL) создать схему данных соответствующую файлам во вложении (связь многие к одному)

Написать скрипт, целью которого будет загрузка этих двух файлов и запись данных в базу

## Необходимые запросы к БД

Список комнат и количество студентов в каждой из них

5 комнат, где самый маленький средний возраст студентов

5 комнат с самой большой разницей в возрасте студентов

Список комнат где живут разнополые студенты

## Требования и замечания

Предложить варианты оптимизации запросов с использования индексов

В результате надо сгенерировать SQL запрос который добавить нужные индексы

Выгрузить результат в формате JSON или XML

Всю "математику" делать стоит на уровне БД.

Командный интерфейс должен поддерживать следующие входные параметры

- students (путь к файлу студентов)

- rooms (путь к файлу комнат)

- format (выходной формат: xml или json)

- использовать ООП и SOLID.

- отсутствие использования ORM (использовать SQL)
