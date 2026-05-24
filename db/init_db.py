import os
import psycopg
from dotenv import load_dotenv

# Загружаем переменные из файла .env
load_dotenv()

# Собираем конфигурацию из переменных окружения
DB_CONFIG = {
    "dbname": os.getenv("DB_NAME", "nsu_scrapper_db"),
    "user": os.getenv("DB_USER", "meno"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST", "127.0.0.1"),
    "port": os.getenv("DB_PORT", "5432"),
}


def create_tables():
    if not DB_CONFIG["password"]:
        print("Ошибка: Переменная DB_PASSWORD не найдена в файле .env или окружении!")
        return

    conn_str = (
        f"dbname={DB_CONFIG['dbname']} "
        f"user={DB_CONFIG['user']} "
        f"password={DB_CONFIG['password']} "
        f"host={DB_CONFIG['host']} "
        f"port={DB_CONFIG['port']}"
    )

    try:
        with psycopg.connect(conn_str) as conn:
            with conn.cursor() as cur:
                print(
                    "Подключение к PostgreSQL успешно выполнено (данные взяты из .env)."
                )

                cur.execute("""
                    CREATE TABLE IF NOT EXISTS raw_documents (
                        id SERIAL PRIMARY KEY,
                        content_source VARCHAR(50) NOT NULL,
                        url TEXT NOT NULL,
                        content TEXT NOT NULL,
                        doc_date TIMESTAMP NOT NULL,
                        scrapped_at TIMESTAMP NOT NULL
                    );
                """)
                print("Таблица 'raw_documents' проверена/создана.")

                cur.execute("""
                    CREATE TABLE IF NOT EXISTS cleaned_documents (
                        id SERIAL PRIMARY KEY,
                        raw_id INT NOT NULL,
                        filtered_content TEXT NOT NULL,
                        CONSTRAINT fk_raw_document 
                            FOREIGN KEY (raw_id) 
                            REFERENCES raw_documents(id) 
                            ON DELETE RESTRICT
                    );
                """)
                print("Таблица 'cleaned_documents' проверена/создана.")

                # cur.execute("""
                #     CREATE TABLE IF NOT EXISTS document_chunks (
                #         document_id INT NOT NULL,
                #         chunk_index INT NOT NULL,
                #         start_char INT NOT NULL,
                #         end_char INT NOT NULL,
                #         PRIMARY KEY (document_id, chunk_index),
                #         CONSTRAINT fk_cleaned_document 
                #             FOREIGN KEY (document_id) 
                #             REFERENCES cleaned_documents(id) 
                #             ON DELETE CASCADE
                #     );
                # """)
                # print("Таблица 'document_chunks' проверена/создана.")

                # cur.execute("""
                #     CREATE TABLE IF NOT EXISTS inverted_index (
                #         word VARCHAR(64) NOT NULL,
                #         document_id INT NOT NULL,
                #         PRIMARY KEY (word, document_id),
                #         CONSTRAINT fk_index_document 
                #             FOREIGN KEY (document_id) 
                #             REFERENCES cleaned_documents(id) 
                #             ON DELETE CASCADE
                #     );
                # """)
                # print("Таблица 'inverted_index' проверена/создана.")

                conn.commit()
                print("\nСтруктура БД успешно синхронизирована!")

    except Exception as error:
        print(f"Ошибка при работе с PostgreSQL: {error}")


if __name__ == "__main__":
    create_tables()
