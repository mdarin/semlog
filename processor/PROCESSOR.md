```sh
# 1. Docker контейнер

docker logs -f my-app 2>&1 | python3 universal_processor.py docker-logs

# 2. Системные логи

tail -f /var/log/syslog | python3 universal_processor.py system-logs

# 3. Приложение Node.js

node app.js 2>&1 | python3 universal_processor.py node-app

# 4. Python приложение

python my_script.py 2>&1 | python3 universal_processor.py python-app

# 5. Nginx логи

tail -f /var/log/nginx/access.log | python3 universal_processor.py nginx-access

# 6. Несколько источников одновременно

{ docker logs -f service1 & docker logs -f service2 & } | python3 universal_processor.py multi-service

# Протестируем на примере

echo "Hello world
[ERROR] Database connection failed
User login successful
WARNING: High memory usage
{\"level\": \"INFO\", \"message\": \"Server started\", \"timestamp\": \"2024-01-15T10:30:00Z\"}" | python3 universal_processor.py test-logs
```
