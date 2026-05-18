import json
from pathlib import Path
from typing import Iterator

from utils.dict_rw import DictReader, DictWriter


class JsonlReader(DictReader):
    def __init__(self, path: Path):
        self.path = path

    def read_dicts(self) -> Iterator[dict]:
        with open(self.path, "r", encoding="utf-8") as f:
            for line in f:
                if line.strip():
                    yield json.loads(line)

    def count_total(self) -> int:
        if not self.path or not self.path.exists():
            return 0
        with open(self.path, "r", encoding="utf-8") as f:
            return sum(1 for _ in f)


class JsonlWriter(DictWriter):
    def __init__(self, path: Path):
        super().__init__()
        self.path = path
        self._file = open(self.path, "w", encoding="utf-8")

    def write_dict(self, item: dict) -> None:
        if self._file:
            self._file.write(json.dumps(item, ensure_ascii=False) + "\n")

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._file:
            self._file.close()
