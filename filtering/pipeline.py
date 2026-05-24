import warnings
from datetime import datetime
from typing import Set, Dict, List, Tuple, Optional
from filtering.istorage import IStorage
from utils.logger import get_logger

logger = get_logger("KnowledgePipeline")


class KnowledgePipeline:
    def __init__(self, config, processor, storage: IStorage):
        self.config = config
        self.processor = processor
        self.storage = storage

        # Оперативная память (Инвертированный кэш состояния cleaned_db)
        self.unique_urls: Set[str] = set()
        self.text_to_db_id: Dict[str, int] = {}
        self.search_index: Dict[str, Set[int]] = {}
        self.ordered_all_texts: List[Tuple[Set[str], int]] = []

        self.num_saved_texts = 0

    def init_memory_from_db(self):
        """Инициализирует кэш памяти на основе уже сохраненных ранее очищенных документов."""
        existing_docs = self.storage.load_existing_cleaned_data()
        for doc in existing_docs:
            self._register_in_memory(doc["id"], doc["url"], doc["content"])

    def _register_in_memory(self, db_id: int, url: str, content: str):
        clean_url = " ".join(url.strip().lower().split()).strip()
        self.unique_urls.add(clean_url)

        norm_text = self.processor.normalize_text(content)
        if norm_text:
            self.text_to_db_id[norm_text] = db_id
            words = set(norm_text.split())
            if len(words) >= self.config.min_num_words:
                idx = len(self.ordered_all_texts)
                self.ordered_all_texts.append((words, db_id))
                for word in words:
                    self.search_index.setdefault(word, set()).add(idx)

    def _clear_document_everywhere(self, db_id: int):
        """Выселяет дубликат из физического хранилища СУБД и ОЗУ-индексов."""
        # Удаление происходит внутри текущей транзакции.
        # Читатели все еще видят старый документ, пока мы не сделаем итоговый commit в updater.py!
        self.storage.delete_document(db_id)

        self.text_to_db_id = {
            t: bid for t, bid in self.text_to_db_id.items() if bid != db_id
        }
        old_ordered = self.ordered_all_texts
        self.ordered_all_texts = []
        self.search_index = {}

        for words, bid in old_ordered:
            if bid == db_id:
                continue
            idx = len(self.ordered_all_texts)
            self.ordered_all_texts.append((words, bid))
            for word in words:
                self.search_index.setdefault(word, set()).add(idx)

    def _find_soft_duplicate_id(self, words_of_text: Set[str]) -> int | None:
        indices_to_check = set()
        for word in words_of_text:
            if word in self.search_index:
                indices_to_check |= self.search_index[word]

        for idx in indices_to_check:
            other_words, db_id = self.ordered_all_texts[idx]
            num_common = len(other_words & words_of_text)
            if num_common > 0:
                precision = num_common / len(other_words)
                recall = num_common / len(words_of_text)
                f1 = 2.0 * precision * recall / (precision + recall)
                if f1 >= self.config.max_f1:
                    return db_id
        return None

    def process_knowledge_base(self, new_records: List[dict]):
        """
        Основной цикл инкрементальной обработки пачки данных из ОЗУ.
        Вся функция выполняется строго в одной транзакции self.storage.
        """

        for sample in new_records:
            raw_text = sample["content"].strip()
            url = sample["url"].strip()
            source_type = sample["content_source"]
            doc_date = datetime.fromisoformat(sample["doc_date"])
            scrapped_at = datetime.fromisoformat(sample["scrapped_at"])

            if not url:
                logger.warning("Sample without URL")
                continue

            # =================================================================
            # ЭТАП 1: РАБОТА С RAW_DOCUMENTS (Сохраняем всё, что реально новое)
            # =================================================================

            # Проверяем точный дубликат в RAW
            find_duplicate = self.storage.is_exact_raw_duplicate(url, raw_text)
            if find_duplicate:
               logger.warning(f"Find duplicate for {url}")
               continue 
                
            raw_id = self.storage.write_raw_document(
                url, raw_text, source_type, doc_date, scrapped_at
            )

            # =================================================================
            # ЭТАП 2: ВАЛИДАЦИЯ И ОБРАБОТКА ДЛЯ CLEANED_DOCUMENTS (БАЗА ЗНАНИЙ)
            # =================================================================

            # 1. Валидация текста на "мусор"
            if not self.processor.is_good_text(raw_text):
                continue

            # 2. Нормализация текста (стемминг/лемматизация)
            norm_text = self.processor.normalize_text(raw_text)
            if not norm_text:
                continue

            # 3. Проверка длины текста в словах
            words = set(norm_text.split())
            if len(words) < self.config.min_num_words:
                continue

            # 4. Поиск дубликатов в оперативной памяти (индексах cleaned)
            duplicate_id = None
            if norm_text in self.text_to_db_id:
                duplicate_id = self.text_to_db_id[norm_text]
            elif source_type == "web":
                duplicate_id = self._find_soft_duplicate_id(words)

            # 5. Алгоритм вытеснения (Удаляем старый cleaned, если нашли совпадение)
            if duplicate_id is not None:
                warnings.warn(
                    f"Обнаружен дубликат (ID: {duplicate_id}) для нового RAW_ID: {raw_id}. Вытеснение..."
                )
                self._clear_document_everywhere(duplicate_id)

            # 6. Очистка текста от эмодзи/тегов для чтения нейросетью и запись в cleaned_db
            cleaned_text = self.processor.clean_for_reading(raw_text)
            new_cleaned_id = self.storage.write_cleaned_document(raw_id, cleaned_text)

            # 7. Регистрируем свежие данные в ОЗУ-кэше
            self._register_in_memory(new_cleaned_id, url, cleaned_text)
            self.num_saved_texts += 1
