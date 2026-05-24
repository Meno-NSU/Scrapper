from utils.dict_rw import DictWriter
from utils.jsonl_rw import JsonlWriter


class TargetedJsonlWriter(DictWriter):
    """
    Прокси-врайтер. Оборачивает базовый файловый врайтер и автоматически 
    добавляет ко всем записям метаданные источника перед сохранением.
    """
    def __init__(self, base_writer: JsonlWriter, source_type: str):
        super().__init__()
        self.base_writer = base_writer
        self.source_type = source_type

    def write_dict(self, item: dict) -> None:
        # Создаем копию, чтобы не мутировать исходные данные в краулере
        enriched_item = item.copy()
        
        # Гарантируем стандартизированный формат для updater.py
        enriched_item["content_source"] = self.source_type
        
        self.base_writer.write_dict(enriched_item)

    # Делегируем управление контекстом базовому файловому врайтеру
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass  # Базовый врайтер закроется сам в run_collector