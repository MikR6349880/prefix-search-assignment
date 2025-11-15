# setup_elasticsearch.py

import xml.etree.ElementTree as ET
import requests
import time
import logging
from typing import Dict, List

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Конфигурация OpenSearch (аналогично Elasticsearch)
OPENSEARCH_HOST = "http://localhost:9200"
OPENSEARCH_INDEX_NAME = "catalog_products"

# Определение маппинга индекса
# Используем стандартные анализаторы OpenSearch, избегая плагинов
INDEX_MAPPING = {
    "settings": {
        "number_of_shards": 1,
        "number_of_replicas": 0,
        "analysis": {
            "analyzer": {
                # Упрощённый анализатор, использующий стандартные фильтры OpenSearch
                "default_analyzer": {
                    "type": "custom",
                    "tokenizer": "standard",
                    "filter": [
                        "lowercase",
                        "stop", # Фильтр стоп-слов (использует встроенные, например, _english_)
                        "snowball" # Лемматизация (использует встроенные, например, English)
                    ],
                    "char_filter": [
                        "html_strip"
                    ]
                },
                # Аналайзер для префиксного поиска (ngram) на основе стандартных токенов
                "autocomplete_analyzer": {
                    "type": "custom",
                    "tokenizer": "keyword", # Используем keyword для ngram
                    "filter": [
                        "lowercase",
                        "edge_ngram_filter" # Переименовали из 'edge_ngram' для ясности
                    ]
                }
            },
            "filter": {
                "edge_ngram_filter": { # Переименовали из 'edge_ngram'
                    "type": "edge_ngram",
                    "min_gram": 1,
                    "max_gram": 20,
                    "token_chars": ["letter"]
                },
                "stop": {
                    "type": "stop",
                    # Используем встроенный список стоп-слов. OpenSearch не всегда имеет _russian_.
                    # Для русского языка может потребоваться плагин или кастомный список.
                    # Пока используем _english_ как пример или оставим пустым.
                    # "stopwords": "_russian_" # <- Может не работать без плагина
                    "stopwords": "_english_" # <- Используем английские стоп-слова как пример
                },
                "snowball": {
                    "type": "snowball",
                    # Язык для лемматизации. OpenSearch не всегда имеет Russian.
                    # "language": "Russian" # <- Может не работать без плагина
                    "language": "English" # <- Используем English как пример
                }
            }
        }
    },
    "mappings": {
        "properties": {
            "name": {
                "type": "text",
                "analyzer": "default_analyzer", # Используем упрощённый
                "fields": {
                    "autocomplete": {
                        "type": "text",
                        "analyzer": "autocomplete_analyzer"
                    },
                    "keyword": {
                        "type": "keyword",
                        "ignore_above": 256
                    }
                }
            },
            "brand": {
                "type": "text",
                "analyzer": "default_analyzer",
                "fields": {
                    "autocomplete": {
                        "type": "text",
                        "analyzer": "autocomplete_analyzer"
                    },
                    "keyword": {
                        "type": "keyword",
                        "ignore_above": 256
                    }
                }
            },
            "category": {
                "type": "text",
                "analyzer": "default_analyzer",
                "fields": {
                    "autocomplete": {
                        "type": "text",
                        "analyzer": "autocomplete_analyzer"
                    },
                    "keyword": {
                        "type": "keyword",
                        "ignore_above": 256
                    }
                }
            },
            "price": {
                "type": "double"
            },
            "url": {
                "type": "keyword"
            },
            "store": {
                "type": "keyword"
            }
        }
    }
}

def wait_for_opensearch():
    """Ждём, пока OpenSearch станет доступен."""
    logger.info("Ожидание запуска OpenSearch...")
    for _ in range(120): # Попробуем 120 раз с интервалом 5 секунд = 10 мин
        try:
            response = requests.get(OPENSEARCH_HOST)
            if response.status_code == 200:
                logger.info("OpenSearch доступен.")
                return True
        except requests.exceptions.RequestException:
            pass
        time.sleep(5)
    logger.error("OpenSearch не стал доступен за отведённое время.")
    return False

def create_index():
    """Создаёт индекс с заданным маппингом."""
    url = f"{OPENSEARCH_HOST}/{OPENSEARCH_INDEX_NAME}"
    response = requests.put(url, json=INDEX_MAPPING, headers={"Content-Type": "application/json"})
    if response.status_code in [200, 201]:
        logger.info(f"Индекс '{OPENSEARCH_INDEX_NAME}' создан или уже существует.")
        return True
    else:
        logger.error(f"Ошибка при создании индекса: {response.status_code} - {response.text}")
        return False

def load_catalog_to_opensearch(xml_path: str):
    """Загружает товары из XML в OpenSearch."""
    logger.info(f"Загрузка каталога из {xml_path} в OpenSearch...")
    try:
        tree = ET.parse(xml_path)
        root = tree.getroot()

        bulk_actions = []
        for product_elem in root.findall('product'):
            product_data = {}
            for child in product_elem:
                product_data[child.tag] = child.text

            # Добавляем действие индексации в bulk-запрос
            action = {"index": {"_index": OPENSEARCH_INDEX_NAME}}
            bulk_actions.append(action)
            bulk_actions.append(product_data)

        # Отправляем bulk-запрос
        if bulk_actions:
            bulk_url = f"{OPENSEARCH_HOST}/_bulk"
            # Объединяем список действий в одну строку, разделённую \n, с учётом экранирования
            bulk_body_lines = []
            for item in bulk_actions:
                import json
                bulk_body_lines.append(json.dumps(item, ensure_ascii=False))
            bulk_body = "\n".join(bulk_body_lines) + "\n"
            response = requests.post(bulk_url, data=bulk_body, headers={"Content-Type": "application/x-ndjson"})
            if response.status_code == 200:
                logger.info(f"Загружено {len(bulk_actions)//2} товаров в OpenSearch.")
                # Принудительно обновляем индекс, чтобы изменения стали видны
                refresh_url = f"{OPENSEARCH_HOST}/{OPENSEARCH_INDEX_NAME}/_refresh"
                requests.post(refresh_url)
                logger.info("Индекс обновлён.")
                return True
            else:
                logger.error(f"Ошибка при bulk-загрузке: {response.status_code} - {response.text}")
                return False

    except ET.ParseError as e:
        logger.error(f"Ошибка парсинга XML: {e}")
        return False
    except FileNotFoundError:
        logger.error(f"Файл не найден: {xml_path}")
        return False

if __name__ == "__main__":
    if not wait_for_opensearch():
        exit(1)

    if not create_index():
        exit(1)

    if not load_catalog_to_opensearch("data/catalog_products.xml"):
        exit(1)

    logger.info("Настройка OpenSearch завершена.")
