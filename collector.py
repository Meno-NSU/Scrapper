import argparse
import datetime
import os
from pathlib import Path
from typing import Dict, Tuple, Optional
from dotenv import load_dotenv

from scrapper.scrapper import run_scrapper, VkCfg, WebCfg
from utils.jsonl_rw import JsonlWriter
from utils.targeted_jsonl_w import TargetedJsonlWriter
from utils.logger import get_logger
from filtering.db_storage import DatabaseStorage

from crawlers import crawl_nsu_vk_knowledge as cvk
from crawlers import crawl_nsu_web_knowledge as cweb

logger = get_logger("collector")


def parse_args() -> argparse.Namespace:
    """Парсит аргументы командной строки."""
    parser = argparse.ArgumentParser(
        description="Сборщик сырых данных из ВК и Web в единый структурированный файл."
    )
    parser.add_argument(
        "--urls-dir",
        type=str,
        required=True,
        help="Путь к папке с файлами ссылок vk_urls.json и web_urls.json (относительно корня проекта)",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        required=True,
        help="Путь к папке, куда сохранится результирующий скомпилированный файл",
    )
    parser.add_argument(
        "--vk", action="store_true", help="Явно запустить сбор данных из ВКонтакте"
    )
    parser.add_argument(
        "--vk-cutoff",
        type=str,
        default=None,
        help="Принудительная дата старта для ВК (YYYY-MM-DD). Если не задана — рассчитывается автоматически по БД.",
    )
    parser.add_argument(
        "--web", action="store_true", help="Явно запустить сбор данных с WEB-сайтов"
    )
    return parser.parse_args()


def resolve_enabled_sources(args: argparse.Namespace) -> Tuple[bool, bool]:
    """
    Определяет, какие источники должны быть включены.
    """
    return args.vk, args.web


def get_vk_cutoff_date(manual_cutoff: Optional[str]) -> str:
    """Возвращает дату отсечения для ВК (из аргументов или вычисляет на основе БД)."""
    if manual_cutoff:
        logger.info(f"Дата отсечения ВК задана вручную: {manual_cutoff}")
        return manual_cutoff

    try:
        with DatabaseStorage() as storage:
            max_date = storage.get_max_doc_date()
        if max_date:
            # Откат на 2 дня назад для учета возможных обновлений/редактирований старых постов
            cutoff_date = (max_date - datetime.timedelta(days=2)).strftime("%Y-%m-%d")
            logger.info(
                f"Авто-дата отсечения ВК на основе последней записи в БД (-2 дня): {cutoff_date}"
            )
            return cutoff_date

        logger.info("Таблица raw_documents пуста. ВК запустит полный сбор истории.")
    except Exception as e:
        logger.warning(
            f"Не удалось связаться с БД для авто-расчета даты ({e}). Режим: 'сбор с нуля'."
        )

    return "None"


def filter_already_scrapped_urls(raw_urls: Dict[str, str]) -> Dict[str, str]:
    """ФУНКЦИЯ НА БУДУЩЕЕ: Фильтрация списка URL для WEB-источников."""
    return raw_urls


def prepare_file_configs(
    token: str,
    cutoff_date_str: str,
    urls_dir: Path,
    shared_writer: JsonlWriter,
    vk_enabled: bool,
    web_enabled: bool,
) -> Tuple[Optional[VkCfg], Optional[WebCfg]]:
    """Инициализирует и размечает конфигурации для краулеров ВК и Веба."""
    vk_cfg, web_cfg = None, None

    if vk_enabled:
        vk_urls_file = urls_dir.joinpath("vk_urls.json")
        if not vk_urls_file.exists():
            logger.warning(
                f"Файл конфигурации URL для ВК не найден: {vk_urls_file}. Пропускаем VK."
            )
        else:
            groups_dict = cvk.extract_groups(vk_urls_file)
            vk_targeted_writer = TargetedJsonlWriter(shared_writer, source_type="news")
            vk_cfg = VkCfg(
                vk_service_token=token,
                vk_cutoff_date=cutoff_date_str,
                vk_groups=groups_dict,
                output_writer=vk_targeted_writer,
            )
            logger.info("Источник VK (news) успешно инициализирован.")

    if web_enabled:
        web_urls_file = urls_dir.joinpath("web_urls.json")
        if not web_urls_file.exists():
            logger.warning(
                f"Файл конфигурации URL для WEB не найден: {web_urls_file}. Пропускаем WEB."
            )
        else:
            urls_list = cweb.extract_urls(web_urls_file)
            filtered_urls = filter_already_scrapped_urls(urls_list)
            web_targeted_writer = TargetedJsonlWriter(shared_writer, source_type="web")
            web_cfg = WebCfg(
                web_urls=filtered_urls,
                output_writer=web_targeted_writer,
            )
            logger.info(
                f"Источник WEB (main) успешно инициализирован. Ссылок к обходу: {len(filtered_urls)}"
            )

    return vk_cfg, web_cfg


def main():
    BASE_DIR = Path(__file__).resolve().parent
    load_dotenv()

    # 1. Сбор базовых параметров запуска
    args = parse_args()
    urls_dir = BASE_DIR.joinpath(args.urls_dir)
    output_dir = BASE_DIR.joinpath(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # 2. Определение активных источников
    vk_enabled, web_enabled = resolve_enabled_sources(args)
    if not vk_enabled and not web_enabled:
        logger.warning("Сбор прерван: все источники отключены.")
        return

    logger.info("=== Запуск Сборщика Данных (Сеть -> Единый Файл) ===")
    logger.info(f"Папка конфигурации URL: {urls_dir}")
    logger.info(f"Папка сохранения результатов: {output_dir}")

    # 3. Подготовка параметров аутентификации и дат
    cutoff_date_str = get_vk_cutoff_date(args.vk_cutoff) if vk_enabled else "None"

    token = os.getenv("VK_SERVICE_TOKEN")
    if vk_enabled and not token:
        raise ValueError(
            "❌ Критическая ошибка: В файле .env отсутствует VK_SERVICE_TOKEN"
        )

    # 4. Инициализация выходного файла-приемника
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    scrapped_file_path = output_dir / f"scrapped_{timestamp}.jsonl"
    shared_writer = JsonlWriter(scrapped_file_path)

    # 5. Инициализация конфигураций краулеров
    vk_cfg, web_cfg = prepare_file_configs(
        token=token or "",
        cutoff_date_str=cutoff_date_str,
        urls_dir=urls_dir,
        shared_writer=shared_writer,
        vk_enabled=vk_enabled,
        web_enabled=web_enabled,
    )

    # Безопасное завершение, если конфигурации собрать не удалось
    if not vk_cfg and not web_cfg:
        logger.warning(
            "Сбор прерван: отсутствуют файлы конфигураций ссылок или источники не созданы."
        )
        shared_writer.__exit__(None, None, None)
        if scrapped_file_path.exists():
            scrapped_file_path.unlink()
        return

    logger.info(
        f"Краулеры запущены. Поток сырых данных пишется в: {scrapped_file_path}"
    )
    with shared_writer:
        run_scrapper(vk_cfg=vk_cfg, web_cfg=web_cfg)

    logger.info("=== Сборщик данных успешно завершил работу! ===")


if __name__ == "__main__":
    main()
