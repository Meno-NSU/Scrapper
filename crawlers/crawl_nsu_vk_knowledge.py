import json
import vk_api
import os
from pathlib import Path
import time
import datetime
from urllib.parse import urlparse
from vk_api.vk_api import VkApiMethod
from typing import TextIO, Optional
from tqdm import tqdm
from dotenv import load_dotenv

from utils.logger import get_logger

logger = get_logger(__name__)


def _get_group_domain(url):
    """Извлекает domain группы из ссылки"""
    return urlparse(url).path.strip("/")


def _get_posts(vk: VkApiMethod, domain: str, count: int, offset: int):
    # Добавляем filter='owner' чтобы получать посты именно от имени группы,
    # а не все подряд (хотя по умолчанию usually 'all').
    response = vk.wall.get(domain=domain, count=count, offset=offset, filter="owner")
    posts = response.get("items", [])
    return posts


def _to_output_dict(post: dict, name: str) -> dict:
    result = dict()
    result["url"] = f"https://vk.com/wall{post['owner_id']}_{post['id']}"
    result["name"] = name
    result["content"] = post.get("text", " ")

    result["date"] = post.get("date")
    result["collection_date"] = int(time.time())

    # Дополнительные поля для отладки, если нужно
    if "is_pinned" in post:
        result["is_pinned"] = post["is_pinned"]

    return result


def _collect_data(
    vk: VkApiMethod,
    domain: str,
    title: str,
    out: TextIO,
    batch_size: int = 100,
    cutoff_date: Optional[int] = None,
) -> tuple[Optional[int], Optional[int]]:
    """
    Собирает данные и возвращает (min_date, max_date) для собранных постов
    """
    offset = 0
    saved_count = 0
    should_stop = False
    min_date = None
    max_date = None

    # Итеративно скачиваем посты, пока они есть и не достигли cutoff_date
    with tqdm(desc=f'Извлечение данных из группы "{title}"') as pbar:
        while True:
            if should_stop:
                break

            posts = _get_posts(vk, domain, count=batch_size, offset=offset)

            # Если постов нет, значит дошли до конца
            if not posts:
                break

            for post in posts:
                post_date = post.get("date")
                is_pinned = post.get("is_pinned", 0) == 1

                # Важно: закрепленный пост (pinned) может быть старым.
                # Мы его сохраняем, но НЕ прерываем сбор, если он старше cutoff_date.
                # Прерываем только на ОБЫЧНЫХ постах (хронологических).
                if not is_pinned:
                    # Проверяем, не старше ли пост, чем cutoff_date
                    if cutoff_date is not None and post_date < cutoff_date:
                        should_stop = True
                        break  # Break inner loop

                # Обновляем даты (включая pinned в статистике)
                if min_date is None or post_date < min_date:
                    min_date = post_date
                if max_date is None or post_date > max_date:
                    max_date = post_date

                # Сохраняем
                out_post = _to_output_dict(post, title)
                json_line = json.dumps(out_post, ensure_ascii=False)

                out.write(json_line + "\n")
                saved_count += 1

            # Принудительно сбрасываем буфер на диск
            out.flush()

            offset += len(posts)
            pbar.update(len(posts))

            # Небольшая пауза
            time.sleep(0.3)

    # Форматирование дат для красивого вывода в лог
    min_str = (
        datetime.datetime.fromtimestamp(min_date).strftime("%Y-%m-%d %H:%M:%S")
        if min_date is not None
        else "N/A"
    )
    max_str = (
        datetime.datetime.fromtimestamp(max_date).strftime("%Y-%m-%d %H:%M:%S")
        if max_date is not None
        else "N/A"
    )

    logger.info(
        f"✅ Сохранено {saved_count} постов. Диапазон дат: {min_str} - {max_str}"
    )

    return min_date, max_date


def _autorize(token: str | None) -> VkApiMethod:
    vk_session = vk_api.VkApi(token=token)
    vk: VkApiMethod = vk_session.get_api()
    return vk


def _get_groups(filepath: Path) -> dict:
    with open(filepath, "r", encoding="utf-8") as f:
        groups_dict = json.load(f)
    return groups_dict


def _save_posts(
    vk: VkApiMethod,
    groups_dict: dict,
    output_filepath: Path,
    cutoff_unix_date: int | None = None,
    posts_per_prequest: int = 100,
):
    cutoff_info = ""
    if cutoff_unix_date is not None:
        cutoff_info = f" (с {datetime.datetime.fromtimestamp(cutoff_unix_date)})"

    logger.info(f"Найдено групп: {len(groups_dict)}")
    logger.info(f"Начинаем сбор в {output_filepath}{cutoff_info}...")

    # Отслеживаем общие минимальную и максимальную дату
    global_min_date = None
    global_max_date = None

    # Используем 'w' для перезаписи файла при новом запуске.
    # Файл vk_scrapped.jsonl будет содержать актуальные результаты прогона.
    with open(output_filepath, "w", encoding="utf-8") as f_out:
        for title, link in groups_dict.items():
            domain = _get_group_domain(link)
            logger.info(f"Извлечение данных из группы {title}...")

            try:
                min_date, max_date = _collect_data(
                    vk,
                    domain,
                    title,
                    f_out,
                    posts_per_prequest,
                    cutoff_unix_date,
                )

                # Обновляем глобальные даты
                if min_date is not None:
                    if global_min_date is None or min_date < global_min_date:
                        global_min_date = min_date

                if max_date is not None:
                    if global_max_date is None or max_date > global_max_date:
                        global_max_date = max_date

            except vk_api.exceptions.ApiError as e:
                logger.info(f"\n⚠️ Ошибка API ({title}): {e}")
            except Exception as e:
                logger.info(f"\n⚠️ Ошибка ({title}): {e}")

    min_date_str = "nan"
    if global_min_date is not None:
        min_date_str = datetime.datetime.fromtimestamp(global_min_date).strftime(
            "%Y-%m-%d"
        )

    max_date_str = "nan"
    if global_max_date is not None:
        max_date_str = datetime.datetime.fromtimestamp(global_max_date).strftime(
            "%Y-%m-%d"
        )

    # Формируем имя с датами
    new_name = (
        output_filepath.stem
        + f"_{min_date_str}_to_{max_date_str}"
        + output_filepath.suffix
    )
    new_path = output_filepath.parent / new_name

    output_filepath.rename(new_path)
    logger.info(f"🎉 Готово! Данные сохранены в {output_filepath}")
    logger.info(f"   Диапазон: с {min_date_str} по {max_date_str}")


def crawl_vk_knowledge(
    vk_token: str,
    urls_filepath: Path,
    output_filepath: Path,
    cutoff_unix_date: int | None,
    posts_per_prequest: int = 100,
):
    # 1. Авторизация
    try:
        vk = _autorize(vk_token)
    except Exception as e:
        logger.info(f"❌ Ошибка авторизации: {e}")
        return

    # 2. Чтение списка групп
    try:
        groups_dict = _get_groups(urls_filepath)
    except FileNotFoundError:
        logger.info(f"❌ Файл {urls_filepath} не найден.")
        return

    _save_posts(vk, groups_dict, output_filepath, cutoff_unix_date, posts_per_prequest)


def main():
    BASE = Path(__file__).resolve().parent.parent
    RESOURCES_DIR = BASE.joinpath("urls")
    SCRAPPED_DATA_DIR = BASE.joinpath("scrapped_data")

    # Задаём конфигурацию
    load_dotenv()

    # Сервисный ключ доступа
    VK_SERVICE_TOKEN = os.getenv("VK_SERVICE_TOKEN")
    if VK_SERVICE_TOKEN is None:
        raise ValueError("❌ В .env файле не задан VK_SERVICE_TOKEN")

    # Имя входного файла
    INPUT_FILE = RESOURCES_DIR.joinpath("vk_urls.json")

    # Файл для сохранения результатов (В crawl_vk_knowledge к названию файла добавятся даты)
    OUTPUT = SCRAPPED_DATA_DIR.joinpath("vk_scrapped.jsonl")

    # Дата, НАЧИНАЯ С КОТОРОЙ скраппить посты (Unix timestamp)
    #
    # Скрипт собирает все посты от текущего момента назад до этой даты.
    # None = скраппить все посты за всё время
    # Пример: 1609459200 для 2021-01-01
    # Можно использовать: int(datetime.datetime(2020, 1, 1).timestamp())
    CUTOFF_DATE = None  # int(datetime.datetime(2026, 1, 1).timestamp())

    # Количество постов для скачивания (максимум 100 за один запрос)
    POSTS_PER_REQUEST = 100

    crawl_vk_knowledge(
        VK_SERVICE_TOKEN, INPUT_FILE, OUTPUT, CUTOFF_DATE, POSTS_PER_REQUEST
    )


if __name__ == "__main__":
    main()
