import json
from pathlib import Path
import asyncio
from crawl4ai import (
    BrowserConfig,
    CrawlerRunConfig,
    DefaultMarkdownGenerator,
    AsyncWebCrawler,
    PruningContentFilter,
)
from crawl4ai.models import CrawlResultContainer
from crawl4ai.processors.pdf import PDFCrawlerStrategy
from tqdm import tqdm
import warnings
import time
import datetime

from utils.dict_rw import DictWriter
from utils.jsonl_rw import JsonlWriter
from utils.logger import get_logger
from utils.pdf_scrapper import PDFContentSmarterScraper

logger = get_logger(__name__)


def extract_urls(urls_fname: Path) -> dict[str, str]:
    with urls_fname.open(mode="r", encoding="utf-8", errors="ignore") as fp:
        url_data = json.load(fp)

    url_dict = {}

    for doc_name, doc_url in url_data.items():
        # Если URL еще нет или текущее имя длиннее того, что уже сохранено
        if doc_url not in url_dict or len(doc_name) > len(url_dict[doc_url]):
            url_dict[doc_url] = doc_name

    # Форматируем название
    for doc_url in url_dict:
        url_dict[doc_url] = " ".join(url_dict[doc_url].strip().split()).strip()

    logger.info(f"Извлечено {len(url_dict)} url")
    return url_dict


def get_configs():
    browser_config = BrowserConfig(verbose=False)
    run_config = CrawlerRunConfig(
        markdown_generator=DefaultMarkdownGenerator(
            content_filter=PruningContentFilter(threshold=0.6),
            options={"ignore_links": True},
        ),
        word_count_threshold=10,  # Minimum words per content block
        excluded_tags=["form", "header"],
        exclude_external_links=True,  # Remove external links
        remove_overlay_elements=True,  # Remove popups/modals
        process_iframes=True,
        verbose=False,
    )
    pdf_config = CrawlerRunConfig(
        markdown_generator=DefaultMarkdownGenerator(
            content_filter=PruningContentFilter(threshold=0.6),
            options={"ignore_links": True},
        ),
        word_count_threshold=10,  # Minimum words per content block
        excluded_tags=["form", "header"],
        exclude_external_links=True,  # Remove external links
        remove_overlay_elements=True,  # Remove popups/modals
        process_iframes=True,
        verbose=False,
        scraping_strategy=PDFContentSmarterScraper(),
    ) 

    return {"browser": browser_config, "run": run_config, "pdf": pdf_config}


async def crawl_web_knowledge(url_dict: dict[str, str], out: DictWriter, configs: dict):
    success_count = 0
    fail_count = 0

    url_list = sorted(list(url_dict.keys()))

    # 2. Сбор данных
    async with AsyncWebCrawler(config=configs["browser"]) as crawler:
        for doc_url in tqdm(url_list, desc=f"Сбор данных с Web источников"):
            try:
                if doc_url.endswith(".pdf"):
                    config = configs["pdf"]
                else:
                    config = configs["run"]
                result: CrawlResultContainer = await crawler.arun(
                    url=doc_url, config=config
                )

                if result.success or getattr(result, "metadata", None):
                    jsonified_result = {
                        "url": doc_url,
                        "name": url_dict[doc_url],
                        "content": result.markdown.fit_markdown,
                        "doc_date": str(datetime.datetime.now()),
                        "scrapped_at": str(datetime.datetime.now()),
                    }
                    out.write_dict(jsonified_result)
                    success_count += 1
                else:
                    fail_count += 1
                    # Генерируем предупреждение, но не останавливаем скрипт
                    warnings.warn(
                        f"FAIL {doc_url}: Status={result.status_code}, Error={result.error_message}"
                    )
            except Exception as e:
                fail_count += 1
                logger.info(f"EXCEPTION {doc_url}: {e}")

    # 3. Итоговый отчет
    logger.info("-" * 40)
    logger.info(f"🎉 Готово!")
    logger.info(f"Всего URLs: {len(url_list)}")
    logger.info(f"✅ Успешно: {success_count}")
    if fail_count > 0:
        logger.info(f"⚠️ Ошибок: {fail_count} (см. предупреждения выше)")
    else:
        logger.info(f"Ошибок: 0")


async def main():
    BASE = Path(__file__).resolve().parent.parent

    RESOURCES_DIR = BASE.joinpath("urls")
    SCRAPPED_DATA_DIR = BASE.joinpath("scrapped_data")
    logger.info(f"isdir({RESOURCES_DIR}) = {RESOURCES_DIR.is_dir()}")
    logger.info(f"isdir({SCRAPPED_DATA_DIR}) = {SCRAPPED_DATA_DIR.is_dir()}")

    url_fname = RESOURCES_DIR.joinpath("web_urls.json")
    logger.info(f"isfile({url_fname}) = {url_fname.is_file()}")

    # 1. Формируем имя файла с текущей датой
    current_date = datetime.datetime.now().strftime("%Y-%m-%d")
    filename = f"web_scrapped_{current_date}.jsonl"
    output = SCRAPPED_DATA_DIR.joinpath(filename)

    url_dict = extract_urls(url_fname)
    out_writer = JsonlWriter(output)
    await crawl_web_knowledge(url_dict, out_writer, get_configs())

    logger.info(f"Файл: {output}")


if __name__ == "__main__":
    asyncio.run(main())
