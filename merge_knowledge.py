from pathlib import Path
from datetime import datetime

def _is_date(date_str: str):
    try:
        # Указываем формат, который мы ожидаем (ГГГГ-ММ-ДД)
        datetime.strptime(date_str, "%Y-%m-%d")
        return True
    except ValueError:
        return False

def _print_files(files_dict: dict[str, Path]) -> None:
    if(not files_dict):
        print("Файлы для соединения не найдены")
        return

    print("Сливаем данные из файлов:")
    for resource_name, path in files_dict.items():
        print(f"Ресурс: {resource_name}, путь до файла: {path}")

def get_latest_files(directory: Path) -> dict[str, Path]:
    latest = {}

    for file in directory.glob("*.jsonl"):
        # 1. Разбиваем имя по '_' и берем последний кусок: '2026-01-27.jsonl'
        last_part = file.name.split("_")[-1]

        # 2. Отрезаем '.jsonl', получаем чистую дату: '2026-01-27'
        date_str = last_part.replace(".jsonl", "")

        # 3. Определяем тип (vk или web)
        prefix = file.name.split("_")[0]
        # 4. Сравниваем строки (ISO даты YYYY-MM-DD отлично сравниваются как строки)
        if _is_date(date_str) and (
            prefix not in latest or date_str > latest[prefix]["date"]
        ):
            latest[prefix] = {"path": file, "date": date_str}

    return {k: Path(v["path"]) for k, v in latest.items()}


def merge_jsonl_files(input_files: list[Path], output_path: Path):
    with output_path.open("w", encoding="utf-8") as outfile:
        for file_path in input_files:
            with file_path.open("r", encoding="utf-8") as infile:
                for line in infile:
                    # Проверяем, не пустая ли строка, и записываем
                    if line.strip():
                        outfile.write(line.rstrip() + "\n")

    print(f"✅ Успешно смерджено {len(input_files)} файлов в: {output_path}")


def main():
    BASE = Path(__file__).resolve().parent
    SCRAPPED_DATA_DIR = BASE.joinpath("scrapped_data")
    OUTPUT = SCRAPPED_DATA_DIR.joinpath("merged_latest_knowledge.jsonl")
    files_dict = get_latest_files(SCRAPPED_DATA_DIR)
    _print_files(files_dict)
    merge_jsonl_files(list(files_dict.values()), OUTPUT)

if __name__ == "__main__":
    main()
