import os
from datetime import datetime
from typing import List, Dict, Any
import psycopg
from dotenv import load_dotenv

load_dotenv()


class NewsRepository:
    def __init__(self):
        self.conn_str = (
            f"dbname={os.getenv('DB_NAME')} "
            f"user={os.getenv('DB_USER')} "
            f"password={os.getenv('DB_PASSWORD')} "
            f"host={os.getenv('DB_HOST', '127.0.0.1')} "
            f"port={os.getenv('DB_PORT', '5432')}"
        )

    def get_updated_news(self, from_time: datetime | None) -> List[Dict[str, Any]]:
        """
        Вытаскивает очищенные новости вместе с их URL и датой публикации.
        """
        query = """
            SELECT 
                c.id AS document_id,
                c.filtered_content AS content,
                r.url AS url,
                r.doc_date AS doc_date
            FROM cleaned_documents c
            JOIN raw_documents r ON c.raw_id = r.id
            WHERE r.content_source = 'news'
        """

        params = []
        if from_time is not None:
            query += " AND r.doc_date >= %s"
            params.append(from_time)

        query += " ORDER BY r.doc_date DESC;"

        with psycopg.connect(self.conn_str) as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)

                result = []
                for row in cur.fetchall():
                    result.append({
                        "document_id": row[0],
                        "content": row[1],
                        "url": row[2],
                        "doc_date": row[3].isoformat() if isinstance(row[3], datetime) else row[3]
                    })
                return result
