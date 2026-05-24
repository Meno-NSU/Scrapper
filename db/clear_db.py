import os
import psycopg
from dotenv import load_dotenv

load_dotenv()

DB_CONFIG = {
    "dbname": os.getenv("DB_NAME", "nsu_scrapper_db"),
    "user": os.getenv("DB_USER", "meno"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST", "127.0.0.1"),
    "port": os.getenv("DB_PORT", "5432")
}

def drop_tables():
    if not DB_CONFIG["password"]:
        print("Ошибка: Переменная DB_PASSWORD не найдена в файле .env!")
        return

    conn_str = (
        f"dbname={DB_CONFIG['dbname']} "
        f"user={DB_CONFIG['user']} "
        f"password={DB_CONFIG['password']} "
        f"host={DB_CONFIG['host']} "
        f"port={DB_CONFIG['port']}"
    )
    
    # Список таблиц для удаления (в порядке от зависимых к главным)
    tables = ["cleaned_documents", "raw_documents"]
    
    # Подтверждение в терминале от греха подальше
    confirm = input("Ты уверен, что хочешь УДАЛИТЬ все таблицы и данные? (y/n): ")
    if confirm.lower() != 'y':
        print("Удаление отменено.")
        return

    try:
        with psycopg.connect(conn_str) as conn:
            with conn.cursor() as cur:
                print("\nНачинаю удаление таблиц...")
                
                # Используем CASCADE, чтобы связи foreign key не блокировали удаление
                for table in tables:
                    cur.execute(f"DROP TABLE IF EXISTS {table} CASCADE;")
                    print(f"Таблица '{table}' успешно удалена.")
                
                conn.commit()
                print("\nБаза данных абсолютно чиста!")

    except Exception as error:
        print(f"Ошибка при удалении: {error}")

if __name__ == "__main__":
    drop_tables()