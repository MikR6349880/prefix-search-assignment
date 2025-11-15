# Используем официальный образ Python 3.12 slim как базовый
FROM python:3.12-slim

# Устанавливаем рабочую директорию внутри контейнера
WORKDIR /app

# Копируем файл зависимостей (если бы он был, например, requirements.txt)
# COPY requirements.txt .
# RUN pip install --no-cache-dir -r requirements.txt

# В нашем случае, устанавливаем зависимости напрямую
# Устанавливаем requests, который нужен для взаимодействия с OpenSearch
RUN pip install --no-cache-dir requests

# Копируем все файлы проекта в рабочую директорию контейнера
# Это включает search_engine.py, setup_elasticsearch.py, evaluate_coverage.py, и т.д.
COPY . .

# CMD указывает команду, которая выполнится при запуске контейнера
# Мы объединяем выполнение setup_elasticsearch.py и search_engine.py в одну команду
# Используем shell-скрипт для последовательного выполнения и ожидания OpenSearch
# (Хотя в идеале, setup_elasticsearch.py должен сам ждать готовности OpenSearch)
CMD ["sh", "-c", "python setup_elasticsearch.py && python search_engine.py"]
