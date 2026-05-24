from typing import Iterator, Any


class DictReader:
    def read_dicts(self) -> Iterator[dict]:
        """Генератор, поочередно возвращающий словари."""
        raise NotImplementedError("Метод read_dicts должен быть переопределен.")

    def count_total(self) -> int:
        """Возвращает общее количество доступных записей."""
        raise NotImplementedError("Метод count_total должен быть переопределен.")


class DictWriter:
    def __init__(self):
        self._meta: dict = dict()

    def __enter__(self) -> "DictWriter":
        """Позволяет использовать врайтер в конструкции 'with'."""
        return self

    def write_dict(self, item: dict) -> None:
        """Записывает один словарь в таргет (файл, память и т.д.)."""
        raise NotImplementedError("Метод write_dict должен быть переопределен.")

    def save_kv(self, key: Any, value: Any) -> None:
        """Сохраняет метаданные в рамках сессии записи."""
        self._meta[key] = value

    def get_value(self, key: Any) -> Any:
        """Получает сохраненные метаданные по ключу."""
        return self._meta.get(key)

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Закрывает ресурсы при выходе из контекстного менеджера."""
        pass
