from typing import Iterator


class DictReader:
    def read_dicts(self) -> Iterator[dict]: ...
    def count_total(self) -> int: ...


class DictWriter:
    def __init__(self):
        self._meta: dict = dict()

    def write_dict(self, item: dict) -> None: ...
    
    def save_kv(self, key, value) -> None:
        self._meta[key] = value

    def get_value(self, key):
        return self._meta.get(key)

    def __exit__(self, exc_type, exc_val, exc_tb): ...
