import csv
import logging

# Настройка логирования (опционально, для отладки)
# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
# logger = logging.getLogger(__name__)

def is_result_relevant(query: str, notes: str, top_3_results: str) -> bool:
    """
    Проверяет, является ли хотя бы один из top_3 результатов релевантным для query и notes.
    Это упрощённая логика. В идеале - ручная оценка.
    """
    # Приводим к нижнему регистру для простоты
    query_lower = query.lower()
    notes_lower = notes.lower()
    results_lower = top_3_results.lower()

    # Проверяем, содержит ли хотя бы один из результатов часть запроса или ключевые слова из notes
    # Это базовая эвристика, может не учитывать транслитерацию, опечатки, семантику.
    # Пример: query "йогурт гр" -> результаты с "йогурт" и "греческий"
    # Пример: notes "butter/oil" -> результаты с "масло"
    # Пример: notes "чай" (через xfq) -> результаты с "чай" (если найдены)

    # Простая проверка: содержится ли query (или его части) в результатах?
    # Или содержатся ли ключевые слова из notes в результатах?
    # Разбиваем query и notes на токены для грубого сравнения
    import re
    query_tokens = re.split(r'\s+|[^\w]', query_lower)
    query_tokens = [token for token in query_tokens if token]
    notes_tokens = re.split(r'\s+|[^\w]', notes_lower)
    notes_tokens = [token for token in notes_tokens if token]

    # Проверяем, содержит ли хотя бы один результат токены из query или notes
    for result_part in results_lower.split('|'): # Разбиваем top_3 по |
        result_part_lower = result_part.strip()
        # Проверяем, содержит ли результат токены из query
        if any(token in result_part_lower for token in query_tokens):
            return True
        # Проверяем, содержит ли результат токены из notes (например, 'чай', 'масло', 'йогурт')
        if any(token in result_part_lower for token in notes_tokens):
            return True

    # Если ни одна проверка не сработала, считаем результат нерелевантным
    # Это грубая эвристика. xfq -> чай не сработает, так как "чай" не содержится в результатах.
    # xfqyfz сервиз -> xfqyfz (токен) не найдёт "сервиз".
    # "кар тофель" -> "картофель" в результатах: "кар" не найдётся как токен в "картофель".
    # Это показывает, что автоматическая оценка сложна.

    # Попробуем более простую эвристику: если в top_3 есть хоть какие-то результаты,
    # и query/notes содержат общеупотребительные слова, которые могут совпасть.
    # Это всё ещё не идеально.
    # Для более точной оценки нужна ручная проверка или более умная модель.

    # Более простой способ: если top_3 не пустой, и query/notes не слишком общие/абстрактные,
    # считать релевантным. Но это тоже может быть неточно.
    # xfq -> 0 результатов -> нерелевантно (это правильно).
    # xfqyfz сервиз -> 0 результатов -> нерелевантно (это правильно).
    # adapter usb c -> результаты с "usb c" -> релевантно (это правильно).
    # йогурт гр -> результаты с "йогурт греческий" -> релевантно (это правильно).
    # кар тофель -> (предположим, что OpenSearch нашёл "Картофель фри") -> релевантно (если "картофель" в результатах).

    # Давайте улучшим эвристику: если есть результаты и они содержат *что-то* похожее на query/notes.
    # Используем более мягкую проверку.
    # Если результаты пусты, запрос точно нерелевантен.
    if not results_lower.strip():
        return False

    # Проверим снова, но с более широким подходом.
    # Проверим, содержит ли хотя бы одна часть результата *часть* query или notes.
    # Это может помочь с "кар" -> "картофель".
    # Разбиваем результат на слова и сравниваем с токенами запроса/заметки.
    result_words = re.split(r'\s+|[^\w]', result_part_lower)
    result_words = [word for word in result_words if word]
    for res_word in result_words:
        for q_token in query_tokens:
            if q_token in res_word or res_word in q_token: # Проверка "кар" в "картофель" или "картофель" в "кар"
                return True
        for n_token in notes_tokens:
            if n_token in res_word or res_word in n_token:
                return True

    # Если после всех проверок не нашли совпадений, считаем нерелевантным.
    return False


def evaluate_coverage(csv_path: str):
    """
    Оценивает покрытие релевантных open запросов.
    """
    total_open = 0
    relevant_open = 0

    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            query_type = row['type']
            if query_type != 'open':
                continue # Пропускаем hidden запросы

            total_open += 1
            query = row['query']
            notes = row['notes']
            top_3 = row['top_3']

            # Проверяем релевантность
            if is_result_relevant(query, notes, top_3):
                relevant_open += 1
                # print(f"RELEVANT: '{query}' -> {top_3}") # Опционально: логировать релевантные
            # else:
            #     print(f"NOT RELEVANT: '{query}' -> {top_3}") # Опционально: логировать нерелевантные

    if total_open > 0:
        coverage_percentage = (relevant_open / total_open) * 100
        print(f"Всего open запросов: {total_open}")
        print(f"Релевантных open запросов: {relevant_open}")
        print(f"Процент покрытия (релевантные open): {coverage_percentage:.2f}%")
        return coverage_percentage
    else:
        print("Не найдено open запросов в файле.")
        return 0


if __name__ == "__main__":
    report_path = "reports/elasticsearch_evaluation_results.csv"
    evaluate_coverage(report_path)

