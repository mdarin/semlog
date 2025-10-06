```sh
# Делаем скрипты исполняемыми
chmod +x log_search_client.py
chmod +x advanced_search.py

# 1. Интерактивный поиск
python3 log_search_client.py

# 2. Поиск через командную строку
python3 advanced_search.py "ошибки базы данных"

# 3. Поиск с фильтрами
python3 advanced_search.py "медленные запросы" --level ERROR --hours 24 --limit 5

# 4. Только ошибки за последние 2 часа
python3 advanced_search.py "проблемы с сетью" --level ERROR --hours 2

# 5. Поиск в конкретной коллекции
python3 advanced_search.py "авторизация" --collection auth-service-logs

# 6. Статистика коллекции
python3 advanced_search.py --stats --collection universal-logs

# 7. Найти похожие логи
python3 advanced_search.py --similar-to 123456789

# 8. Экспорт результатов
python3 advanced_search.py "memory leak" --export search_results.json

# Тестовый пример после добавления логов
echo "Test logs..." | python3 universal_processor.py
python3 log_search_client.py
```
