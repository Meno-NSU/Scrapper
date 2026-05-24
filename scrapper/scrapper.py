import asyncio
import datetime
import os
from pathlib import Path
from typing import Iterator
from attr import dataclass
from dotenv import load_dotenv
import yaml
from crawlers import crawl_nsu_vk_knowledge as cvk
from crawlers import crawl_nsu_web_knowledge as cweb
from utils.jsonl_rw import JsonlWriter
from utils.web_cfg import WebCfg
from utils.logger import get_logger
from utils.vk_cfg import VkCfg
import scrapper.filter_knowledge as fk
import scrapper.merge_knowledge as mk

logger = get_logger("scrapper")

@dataclass(frozen=True)
class ScrapperCfg:
    vk_service_token: str
    vk_cutoff_date: str | None = None
    urls_dir: Path = Path()
    output_dir: Path = Path()
    clear_before_crawl: bool = False
    save_temp_files: bool = True

def delete_files(files: Iterator[Path]) -> None:
    for file in files:
        file.unlink()
        logger.info(f"\tУдален: {file}")


def _clear_data_before_crawling(directory: Path) -> None:
    delete_files(directory.rglob("*.jsonl"))


def crawl_vk_data(cfg: VkCfg):
    token = cfg.vk_service_token

    cutoff_date = None
    if cfg.vk_cutoff_date is not None and cfg.vk_cutoff_date != "None":
        cutoff_date = int(
            datetime.datetime.strptime(str(cfg.vk_cutoff_date), "%Y-%m-%d").timestamp()
        )

    with cfg.output_writer as writer:
        cvk.crawl_vk_knowledge(token, cfg.vk_groups, writer, cutoff_date)


async def craw_web_data(cfg: WebCfg):
    with cfg.output_writer as writer:
        await cweb.crawl_web_knowledge(cfg.web_urls, writer, cweb.get_configs())


def run_scrapper(vk_cfg: VkCfg | None = None, web_cfg: WebCfg | None = None):
    if vk_cfg:
        logger.info("Сбор данных с ВК...")
        crawl_vk_data(vk_cfg)
    if web_cfg:
        logger.info("Сбор данных с web-источников...")
        asyncio.run(craw_web_data(web_cfg))


def get_vk_cfg(cfg: ScrapperCfg) -> tuple[VkCfg, Path]:
    URLS_FILE = cfg.urls_dir.joinpath("vk_urls.json")
    groups_dict = cvk.extract_groups(URLS_FILE)
    output = cfg.output_dir.joinpath("vk_scrapped.jsonl")

    return VkCfg(
        vk_service_token=cfg.vk_service_token,
        vk_cutoff_date=cfg.vk_cutoff_date,
        vk_groups=groups_dict,
        output_writer=JsonlWriter(output),
    ), output


def rename_vk_scrapped_file(vk_cfg: VkCfg, output: Path):
    min_date = vk_cfg.output_writer.get_value("min_date")
    max_date = vk_cfg.output_writer.get_value("max_date")
    new_name = output.stem + f"_{min_date}_to_{max_date}" + output.suffix
    new_path = output.parent / new_name

    output.rename(new_path)


def get_web_cfg(cfg: ScrapperCfg) -> WebCfg:
    URLS_FILE = cfg.urls_dir.joinpath("web_urls.json")
    urls = cweb.extract_urls(URLS_FILE)

    current_date = datetime.datetime.now().strftime("%Y%m%d")
    filename = f"web_scrapped_{current_date}.jsonl"
    output = cfg.output_dir.joinpath(filename)
    output_writer = JsonlWriter(output)

    return WebCfg(
        web_urls=urls,
        output_writer=output_writer,
    )


def main():
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

    config_dict = default_config["scrapper"] | config["scrapper"]

    URLS_DIR = BASE.joinpath(config_dict["URLS_DIR"])
    OUTPUT_DIR = BASE.joinpath(config_dict["OUTPUT_DIR"])
    token = os.getenv("VK_SERVICE_TOKEN")
    if token is None:
        raise ValueError("❌ В .env файле не задан VK_SERVICE_TOKEN")

    cfg = ScrapperCfg(
        vk_service_token=token,
        vk_cutoff_date=config_dict["VK_CUTOFF_DATE"],
        urls_dir=URLS_DIR,
        output_dir=OUTPUT_DIR,
        clear_before_crawl=config_dict["CLEAR_BEFORE_CRAWL"],
        save_temp_files=config_dict["SAVE_TEMP_FILES"],
    )

    if cfg.clear_before_crawl:
        logger.info(f"Очищение {OUTPUT_DIR} от .jsonl перед сбором данных")
        _clear_data_before_crawling(OUTPUT_DIR)

    vk_cfg, scrapped_vk_file = get_vk_cfg(cfg)
    web_cfg = get_web_cfg(cfg)

    run_scrapper(vk_cfg=vk_cfg, web_cfg=web_cfg)
    rename_vk_scrapped_file(vk_cfg, scrapped_vk_file)

    merged_knowledge = cfg.output_dir.joinpath("merged_latest_knowledge.jsonl")
    files_dict = mk.get_latest_files(cfg.output_dir)

    mk.merge_jsonl_files(list(files_dict.values()), merged_knowledge)

    filtered_output = cfg.output_dir.joinpath("filtered_merged_latest_knowledge.jsonl")
    fk.process(merged_knowledge, filtered_output, fk.get_pipeline())
    if cfg.save_temp_files:
        logger.info("Удаление временных файлов:")
        delete_files(iter([merged_knowledge]))


if __name__ == "__main__":
    main()
