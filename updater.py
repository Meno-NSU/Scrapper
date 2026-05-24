import argparse
import sys
from pathlib import Path
from typing import Optional, List
from dotenv import load_dotenv

from filtering.db_storage import DatabaseStorage
from filtering.knowledge_cfg import KnowledgeConfig
from filtering.text_processor import TextProcessor
from filtering.pipeline import KnowledgePipeline
from utils.jsonl_rw import JsonlReader
from utils.logger import get_logger

logger = get_logger("updater")


def parse_args() -> argparse.Namespace:
    """Парсит аргументы командной строки для пайплайна обновлений."""
    parser = argparse.ArgumentParser(
        description="Пайплайн импорта: читает сырые данные из файла, чистит их и обновляет СУБД."
    )
    parser.add_argument(
        "--file",
        type=str,
        default=None,
        help="Путь к конкретному jsonl-файлу с сырыми данными. Если не задан — берется самый свежий из папки scrapped_data.",
    )
    parser.add_argument(
        "--data-dir",
        type=str,
        default="scrapped_data",
        help="Путь к папке с дампами коллектора (используется для автопоиска файла).",
    )
    return parser.parse_args()


def resolve_target_file(args: argparse.Namespace, base_dir: Path) -> Optional[Path]:
    """
    Определяет, какой файл нужно обрабатывать.
    Если передан конкретный файл — берет его. Иначе ищет самый свежий scrapped_*.jsonl.
    """
    if args.file:
        target = Path(args.file)
        return target if target.is_absolute() else base_dir / target

    search_dir = base_dir / args.data_dir
    if not search_dir.exists():
        logger.error(f"Директория для поиска файлов не существует: {search_dir}")
        return None

    # Сортируем файлы по имени (благодаря формату YYYYMMDD_HHMMSS свежий файл всегда будет последним)
    scrapped_files = sorted(search_dir.glob("scrapped_*.jsonl"))
    if not scrapped_files:
        logger.error(
            f"В папке {search_dir} не найдено файлов scrapped_*.jsonl для обработки."
        )
        return None

    most_recent_file = scrapped_files[-1]
    logger.info(
        f"Файл для обработки выбран автоматически (самый свежий): {most_recent_file.name}"
    )
    return most_recent_file


def run_pipeline(records: List[dict]):
    """Инициализирует пайплайн обработки и проводит дедупликацию/сохранение в БД."""
    knowledge_config = KnowledgeConfig()
    processor = TextProcessor(knowledge_config)

    # Весь прогон делаем строго внутри ОДНОГО контекстного менеджера (одна изолированная транзакция)
    with DatabaseStorage() as storage:
        pipeline = KnowledgePipeline(knowledge_config, processor, storage)

        # Подгружаем существующие документы в ОЗУ для F1-дедупликации
        logger.info("Синхронизация ОЗУ-индексов с актуальным состоянием базы...")
        pipeline.init_memory_from_db()

        # Прокачиваем прочитанную из файла пачку через фильтры, дедупликацию и алгоритм вытеснения
        logger.info("Запуск валидации, очистки и вытеснения дубликатов...")
        pipeline.process_knowledge_base(records)

    logger.info(
        f"=== Импорт завершен! Добавлено/обновлено документов в БД: {pipeline.num_saved_texts} ==="
    )


def main():
    BASE_DIR = Path(__file__).resolve().parent
    load_dotenv()

    args = parse_args()

    # 1. Поиск целевого файла на диске
    target_file = resolve_target_file(args, BASE_DIR)

    if not target_file or not target_file.exists():
        logger.error("Целевой файл для обработки не найден. Скрипт остановлен.")
        sys.exit(1)

    logger.info("=== Старт Пайплайна Обновления БД (Диск -> База) ===")

    logger.info(f"Чтение сырых данных из файла: {target_file}")

    # 2. Чтение данных из jsonl-файла без удержания открытого дескриптора
    reader = JsonlReader(target_file)
    total_records = reader.count_total()

    if total_records == 0:
        logger.warning(f"Файл {target_file.name} пуст. Нет данных для импорта в СУБД.")
        return

    logger.info(
        f"Загружено записей из файла: {total_records}. Открытие транзакции к БД..."
    )

    # 3. Превращаем итератор файла в список и передаем в пайплайн обработки
    raw_records = list(reader.read_dicts())
    run_pipeline(raw_records)


if __name__ == "__main__":
    main()
