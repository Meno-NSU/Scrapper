import json
import logging
from pathlib import Path
from collections.abc import Callable
from tqdm import tqdm
import re

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger(__name__)


def _delete_empty_content(item: dict) -> dict | None:
    if not item["content"]:
        return None

    return item

def _remove_emojis(item: dict):
    emoj = re.compile("["
        u"\U0001F600-\U0001F64F"  # emoticons
        u"\U0001F300-\U0001F5FF"  # symbols & pictographs
        u"\U0001F680-\U0001F6FF"  # transport & map symbols
        u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
        u"\U00002500-\U00002BEF"  # chinese char
        u"\U00002702-\U000027B0"
        u"\U000024C2-\U0001F251"
        u"\U0001f926-\U0001f937"
        u"\U00010000-\U0010ffff"
        u"\u2640-\u2642" 
        u"\u2600-\u2B55"
        u"\u200d"
        u"\u23cf"
        u"\u23e9"
        u"\u231a"
        u"\ufe0f"  # dingbats
        u"\u3030"
                      "]+", re.UNICODE)
    item['content'] = re.sub(emoj, '', item['content'])
    return item


def get_pipeline() -> list[Callable]:
    return [_remove_emojis, _delete_empty_content]


def process(input_file: Path, output_file: Path, pipeline: list[Callable]):
    logger.info("Подсчет строк...")
    with open(input_file, "r", encoding="utf-8") as f:
        total_lines = sum(1 for _ in f)

    filtered_count = 0
    with (
        open(input_file, "r", encoding="utf-8") as f_in,
        open(output_file, "w", encoding="utf-8") as f_out,
    ):
        for line in tqdm(f_in, total=total_lines, desc="Фильтрация", unit="lines"):
            line = line.strip()
            if not line:
                continue

            try:
                item = json.loads(line)
            except json.JSONDecodeError:
                continue

            for transform in pipeline:
                item = transform(item)
                if item is None:
                    break

            if item is not None:
                filtered_count += 1
                json_line = json.dumps(item, ensure_ascii=False)
                f_out.write(json_line + "\n")

    logger.info(f"После фильтрации осталось {filtered_count} из {total_lines} записей")


def main():
    BASE = Path(__file__).resolve().parent
    SCRAPPED_DATA_DIR = BASE.joinpath("scrapped_data")
    INPUT = SCRAPPED_DATA_DIR.joinpath("merged_latest_knowledge.jsonl")
    OUTPUT = SCRAPPED_DATA_DIR.joinpath("filtered_merged_latest_knowledge.jsonl")

    process(INPUT, OUTPUT, get_pipeline())


if __name__ == "__main__":
    main()
