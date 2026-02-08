import json
import logging
from pathlib import Path
import asyncio
from crawl4ai import *
from tqdm import tqdm
import warnings
import time
import datetime

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger(__name__)

def _extract_urls(urls_fname: Path) -> dict[str, str]:
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

    return {"browser": browser_config, "run": run_config}


async def crawl_web_knowledge(url_fname: Path, output: Path, configs: dict):
    success_count = 0
    fail_count = 0

    # 1. Извлечение urls из json файла
    url_dict = _extract_urls(url_fname)
    url_list = sorted(list(url_dict.keys()))

    # 2. Сбор данных
    with open(output, mode="w", encoding="utf-8") as fp:
        async with AsyncWebCrawler(config=configs["browser"]) as crawler:
            for doc_url in tqdm(url_list, desc=f"Сбор данных с Web источников"):
                try:
                    result = await crawler.arun(url=doc_url, config=configs["run"])

                    if result.success:
                        jsonified_result = {
                            "url": doc_url,
                            "name": url_dict[doc_url],
                            "content": result.markdown.fit_markdown,
                            "date": None,  # Для веб-страниц часто нет явной даты публикации
                            "collection_date": int(time.time()),
                        }
                        fp.write(
                            json.dumps(jsonified_result, ensure_ascii=False) + "\n"
                        )
                        fp.flush()  # Сохраняем сразу
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
        
    logger.info(f"Файл: {output}")

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

    await crawl_web_knowledge(url_fname, output, get_configs())

if __name__ == "__main__":
    asyncio.run(main())
