from typing import Iterator

from utils.dict_rw import DictReader, DictWriter


class MemoryDictReader(DictReader):
    def __init__(self, data_list: list[dict]):
        self.data = data_list

    def read_dicts(self) -> Iterator[dict]:
        for item in self.data:
            yield item

    def count_total(self) -> int:
        return len(self.data)


class MemoryDictWriter(DictWriter):
    def __init__(
        self,
    ):
        super().__init__()
        self.target_list = list()

    def write_dict(self, item: dict) -> None:
        self.target_list.append(item.copy())

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass
