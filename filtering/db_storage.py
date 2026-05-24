import os
import datetime
from typing import List, Optional
import psycopg
from dotenv import load_dotenv

load_dotenv()


class DatabaseStorage:
    def __init__(self):
        self.conn_str = (
            f"dbname={os.getenv('DB_NAME')} "
            f"user={os.getenv('DB_USER')} "
            f"password={os.getenv('DB_PASSWORD')} "
            f"host={os.getenv('DB_HOST', '127.0.0.1')} "
            f"port={os.getenv('DB_PORT', '5432')}"
        )
        self.conn = None
        self.cur = None

    def __enter__(self) -> "DatabaseStorage":
        self.conn = psycopg.connect(self.conn_str)
        self.cur = self.conn.cursor()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        if exc_type is not None:
            if self.conn:
                self.conn.rollback()
                print(f"[Storage] Транзакция отменена из-за ошибки: {exc_val}")
        else:
            if self.conn:
                self.conn.commit()  # Единый коммит на всю сессию пайплайна
        if self.cur:
            self.cur.close()
        if self.conn:
            self.conn.close()

    # ==========================================
    # СЛУЖЕБНЫЕ МЕТОДЫ И ИНКРЕМЕНТЫ
    # ==========================================

    def get_max_doc_date(self) -> Optional[datetime.date]:
        """Возвращает максимальную дату публикации из сырых документов для инкрементального сбора."""
        if not self.cur:
            raise RuntimeError("База данных не инициализирована.")
        try:
            self.cur.execute("SELECT MAX(doc_date) FROM raw_documents;")
            res = self.cur.fetchone()
            return res[0].date() if res and res[0] else None
        except Exception as e:
            print(f"[Storage] Не удалось получить max_doc_date: {e}")
            return None

    # ==========================================
    # ЭТАП 1: РАБОТА С СЫРЫМИ ДАННЫМИ (RAW)
    # ==========================================

    def is_exact_raw_duplicate(self, url: str, text: str) -> bool:
        if not self.cur:
            raise RuntimeError("База данных не инициализирована.")
        self.cur.execute(
            "SELECT 1 FROM raw_documents WHERE url = %s AND content = %s LIMIT 1;",
            (url, text),
        )
        return self.cur.fetchone() is not None

    def write_raw_document(
        self,
        url: str,
        raw_text: str,
        source_type: str,
        doc_date: datetime.datetime,
        scrapped_at: datetime.datetime,
    ) -> int:
        if not self.cur:
            raise RuntimeError("База данных не инициализирована.")

        self.cur.execute(
            """
            INSERT INTO raw_documents (content_source, url, content, doc_date, scrapped_at)
            VALUES (%s, %s, %s, %s, %s) RETURNING id;
            """,
            (source_type, url, raw_text, str(doc_date), str(scrapped_at)),
        )
        return self.cur.fetchone()[0]

    # ==========================================
    # ЭТАП 2: РАБОТА С ОЧИЩЕННЫМИ ДАННЫМИ (CLEANED)
    # ==========================================

    def load_existing_cleaned_data(self) -> List[dict]:
        if not self.cur:
            raise RuntimeError("База данных не инициализирована.")
        self.cur.execute(
            """
            SELECT c.id, r.url, c.filtered_content 
            FROM cleaned_documents c
            JOIN raw_documents r ON c.raw_id = r.id;
            """
        )
        return [
            {"id": row[0], "url": row[1], "content": row[2]}
            for row in self.cur.fetchall()
        ]

    def write_cleaned_document(self, raw_id: int, cleaned_text: str) -> int:
        if not self.cur:
            raise RuntimeError("База данных не инициализирована.")

        self.cur.execute(
            """
            INSERT INTO cleaned_documents (raw_id, filtered_content)
            VALUES (%s, %s) RETURNING id;
            """,
            (raw_id, cleaned_text),
        )

        return self.cur.fetchone()[0]

    def delete_document(self, cleaned_id: int) -> None:
        if not self.cur:
            raise RuntimeError("База данных не инициализирована.")

        self.cur.execute("DELETE FROM cleaned_documents WHERE id = %s;", (cleaned_id,))
        print(f"[Storage] Документ (Cleaned ID: {cleaned_id} удален.")
