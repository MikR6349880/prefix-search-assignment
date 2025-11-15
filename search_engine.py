# search_engine.py

import xml.etree.ElementTree as ET
# import pandas as pd # Не используется
import re
from typing import List, Dict, Tuple
import logging
import requests # Импортируем requests для Elasticsearch/OpenSearch
import time
import csv # Импортируем csv для run_evaluation

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Конфигурация OpenSearch (используем тот же хост, что и для Elasticsearch)
OPENSEARCH_HOST = "http://localhost:9200"
OPENSEARCH_INDEX_NAME = "catalog_products"

class ElasticsearchSearchEngine:
    def __init__(self):
        """
        Инициализирует движок поиска, взаимодействуя с OpenSearch.
        Предполагается, что индекс уже создан и заполнен.
        """
        logger.info(f"Инициализация ElasticsearchSearchEngine (для OpenSearch), подключение к {OPENSEARCH_HOST}")

    def search(self, prefix: str, top_k: int = 10) -> List[Dict[str, str]]:
        """
        Выполняет поиск в OpenSearch по префиксу.
        Использует multi_match с разными полями.
        Попытка использовать fuzziness (может отличаться в OpenSearch).
        Использует ngram-анализатор для префиксного поиска.
        """
        logger.info(f"Выполняется OpenSearch-поиск для префикса: '{prefix}'")
        # Запрос к OpenSearch
        query_body = {
            "query": {
                "bool": {
                    "should": [
                        # Поиск с fuzziness для опечаток (может потребоваться настройка)
                        {
                            "multi_match": {
                                "query": prefix,
                                "fields": ["name.autocomplete", "brand.autocomplete", "category.autocomplete"],
                                "type": "best_fields", # Ищем лучшие совпадения
                                "fuzziness": "AUTO", # Позволяет опечатки (проверить совместимость)
                                "prefix_length": 1 # Минимальная длина префикса для fuzziness
                            }
                        },
                        # Также ищем по основному анализатору
                        {
                            "multi_match": {
                                "query": prefix,
                                "fields": ["name", "brand", "category"],
                                "type": "best_fields"
                            }
                        }
                    ]
                }
            },
            "size": top_k,
            "_source": ["name", "brand", "category", "price", "url", "store"] # Возвращаем только нужные поля
        }

        search_url = f"{OPENSEARCH_HOST}/{OPENSEARCH_INDEX_NAME}/_search"
        try:
            response = requests.post(search_url, json=query_body, headers={"Content-Type": "application/json"})
            response.raise_for_status() # Вызовет исключение, если статус != 200

            results = response.json()
            hits = results.get('hits', {}).get('hits', [])
            products = [hit.get('_source') for hit in hits]
            logger.info(f"Найдено {len(products)} результатов для '{prefix}'")
            return products

        except requests.exceptions.RequestException as e:
            logger.error(f"Ошибка при поиске в OpenSearch: {e}")
            return []
        except Exception as e:
            logger.error(f"Неожиданная ошибка при обработке результата OpenSearch: {e}")
            return []


def run_evaluation(search_engine, queries_csv_path: str, output_csv_path: str):
    """
    Запускает оценку поискового движка на основе CSV с префиксами.
    Принимает объект search_engine с методом search().
    """
    logger.info(f"Запуск оценки. Префиксы: {queries_csv_path}, Вывод: {output_csv_path}")

    with open(queries_csv_path, 'r', encoding='utf-8') as f_in, \
         open(output_csv_path, 'w', newline='', encoding='utf-8') as f_out:

        reader = csv.DictReader(f_in)
        fieldnames = ['query', 'site', 'type', 'notes', 'top_3', 'top_3_score', 'latency_ms', 'judgement']
        writer = csv.DictWriter(f_out, fieldnames=fieldnames)
        writer.writeheader()

        for row in reader:
            query = row['query']
            site = row['site']
            query_type = row['type']
            notes = row['notes']

            # Замеряем время поиска
            start_time = time.time()
            results = search_engine.search(query, top_k=3) # Получаем топ-3
            end_time = time.time()
            latency_ms = (end_time - start_time) * 1000

            # Формируем строки для top_3
            # Используем 'name' как идентификатор, как и раньше
            top_3_ids = "|".join([prod.get('name', 'unknown_name') for prod in results])
            # top_3_score оставляем пустым, как раньше
            top_3_scores = "|".join([""] * len(results))

            writer.writerow({
                'query': query,
                'site': site,
                'type': query_type,
                'notes': notes,
                'top_3': top_3_ids,
                'top_3_score': top_3_scores,
                'latency_ms': f"{latency_ms:.2f}",
                'judgement': '' # Оставляем пустым
            })

    logger.info(f"Оценка завершена. Результаты записаны в {output_csv_path}")


if __name__ == "__main__":
    print("DEBUG: Запуск main блока search_engine.py") # <-- Добавить эту строку
    # --- Запуск оценки с использованием ElasticsearchSearchEngine ---
    print("\n--- Запуск оценки с использованием ElasticsearchSearchEngine ---")
    # Создаём движок
    es_search_engine = ElasticsearchSearchEngine()

    # Запускаем оценку
    queries_path = "data/prefix_queries.csv"
    output_path = "reports/elasticsearch_evaluation_results.csv"
    print(f"DEBUG: Попытка запустить run_evaluation с {queries_path} в {output_path}") # <-- Добавить
    run_evaluation(es_search_engine, queries_path, output_path)
    print(f"DEBUG: run_evaluation завершена. Результаты сохранены в {output_path}") # <-- Добавить
    print(f"Оценка завершена. Результаты сохранены в {output_path}")

    # Пример использования (закомментировано на время отладки, если нужно)
    # print("\n--- Тестовый поиск (Elasticsearch) ---")
    # example_prefix = "ма"
    # es_results = es_search_engine.search(example_prefix, top_k=5)
    # for i, product in enumerate(es_results):
    #     print(f"{i+1}. {product.get('name')} - {product.get('brand')} - {product.get('category')}")
