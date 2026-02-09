import asyncio
import datetime
from pathlib import Path
from typing import Iterator
from dotenv import load_dotenv
import os
import yaml
from crawlers import crawl_nsu_vk_knowledge as cvk
from crawlers import crawl_nsu_web_knowledge as cweb
import merge_knowledge as mk
import filter_knowledge as fk
from utils.logger import get_logger

logger = get_logger("scrapper")


def delete_files(files: Iterator[Path]) -> None:
    for file in files:
        file.unlink()
        logger.info(f"\tУдален: {file}")


def _clear_data_before_crawling(directory: Path) -> None:
    delete_files(directory.rglob("*.jsonl"))


def crawl_vk_data(urls_dir: Path, output_dir: Path, config: dict):
    token = os.getenv("VK_SERVICE_TOKEN")
    if token is None:
        raise ValueError("❌ В .env файле не задан VK_SERVICE_TOKEN")

    urls_file = urls_dir.joinpath("vk_urls.json")
    output_file = output_dir.joinpath("vk_scrapped.jsonl")

    cutoff_date = None
    if config["VK_CUTOFF_DATE"] is not None and config["VK_CUTOFF_DATE"] != "None":
        cutoff_date = int(
            datetime.datetime.strptime(
                str(config["VK_CUTOFF_DATE"]), "%Y-%m-%d"
            ).timestamp()
        )

    cvk.crawl_vk_knowledge(token, urls_file, output_file, cutoff_date)


async def craw_web_data(urls_dir: Path, output_dir: Path, config: dict):
    url_fname = urls_dir.joinpath("web_urls.json")

    current_date = datetime.datetime.now().strftime("%Y-%m-%d")
    filename = f"web_scrapped_{current_date}.jsonl"
    output_file = output_dir.joinpath(filename)

    await cweb.crawl_web_knowledge(url_fname, output_file, cweb.get_configs())


def run_scrapper():
    BASE = Path(__file__).resolve().parent
    load_dotenv()

    config: dict = dict()
    default_config: dict = dict()
    CONFIG_PATH = BASE.joinpath("config.yaml")
    DEFAULT_CONFIG_PATH = BASE.joinpath("default_config.yaml")
    with (
        open(CONFIG_PATH, "r") as f_config,
        open(DEFAULT_CONFIG_PATH, "r") as f_def_config,
    ):
        config = yaml.safe_load(f_config)
        default_config = yaml.safe_load(f_def_config)

    if config is None:
        logger.info("Пропускаем Scrapper")
        return

    if config.get("scrapper", None) is None:
        config["scrapper"] = dict()

    config = default_config["scrapper"] | config["scrapper"]

    URLS_DIR = BASE.joinpath(config["URLS_DIR"])
    OUTPUT_DIR = BASE.joinpath(config["OUTPUT_DIR"])

    if config["CLEAR_BEFORE_CRAWL"]:
        logger.info(f"Очищение {OUTPUT_DIR} от .jsonl перед сбором данных")
        _clear_data_before_crawling(OUTPUT_DIR)

    logger.info("Сбор данных с ВК...")
    crawl_vk_data(URLS_DIR, OUTPUT_DIR, config)
    logger.info("Сбор данных с web-источников...")
    asyncio.run(craw_web_data(URLS_DIR, OUTPUT_DIR, config))

    merged_knowledge = OUTPUT_DIR.joinpath("merged_latest_knowledge.jsonl")
    files_dict = mk.get_latest_files(OUTPUT_DIR)

    mk.merge_jsonl_files(list(files_dict.values()), merged_knowledge)

    filtered_output = OUTPUT_DIR.joinpath("filtered_merged_latest_knowledge.jsonl")
    fk.process(merged_knowledge, filtered_output, fk.get_pipeline())
    if not config["SAVE_TEMP_FILES"]:
        logger.info("Удаление временных файлов:")
        delete_files(iter(list(files_dict.values()) + [merged_knowledge]))


def main():
    run_scrapper()


if __name__ == "__main__":
    main()
