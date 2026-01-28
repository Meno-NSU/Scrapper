import json
from pathlib import Path
from collections.abc import Callable
from tqdm import tqdm


def delete_empty_content(item: dict) -> dict | None:
    if not item["content"]:
        return None

    return item


def get_pipeline() -> list[Callable]:
    return [delete_empty_content]


def process(input_file: Path, output_file: Path, pipeline: list[Callable]):
    print("Подсчет строк...")
    with open(input_file, "r", encoding="utf-8") as f:
        total_lines = sum(1 for _ in f)

    filtered_count = 0
    with (
        open(input_file, "r", encoding="utf-8") as f_in,
        open(output_file, "w", encoding="utf-8") as f_out,
    ):
        for line in tqdm(f_in, total=total_lines, desc="Обработка JSONL", unit="lines"):
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

    print(f"Осталось {filtered_count} из {total_lines} записей")


def main():
    BASE = Path().cwd()
    SCRAPPED_DATA_DIR = BASE.joinpath("scrapped_data")
    INPUT = SCRAPPED_DATA_DIR.joinpath("merged_latest_knowledge.jsonl")
    OUTPUT = SCRAPPED_DATA_DIR.joinpath("filtered_merged_latest_knowledge.jsonl")

    process(INPUT, OUTPUT, get_pipeline())


if __name__ == "__main__":
    main()
