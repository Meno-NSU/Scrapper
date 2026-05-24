from datetime import datetime
from typing import Protocol, List


class IStorage(Protocol):
    def __enter__(self) -> "IStorage": ...

    def __exit__(self, exc_type, exc_val, exc_tb) -> None: ...

    # === ЭТАП 1: РАБОТА С СЫРЫМИ ДАННЫМИ (RAW) ===
    def is_exact_raw_duplicate(self, url: str, text: str) -> bool: ...

    def write_raw_document(
        self,
        url: str,
        raw_text: str,
        source_type: str,
        file_date: datetime,
        scrapped_at: datetime,
    ) -> int: ...

    # === ЭТАП 2: РАБОТА С ОЧИЩЕННЫМИ ДАННЫМИ (CLEANED) ===
    def load_existing_cleaned_data(self) -> List[dict]: ...

    def write_cleaned_document(
        self,
        raw_id: int,
        cleaned_text: str,
    ) -> int: ...

    def delete_document(self, cleaned_id: int) -> None: ...
