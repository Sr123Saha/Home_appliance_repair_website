import sqlite3
import pandas as pd
from pathlib import Path

# =====================
# Пути к файлам
# =====================

BASE_DIR = Path(__file__).resolve().parent

USERS_CSV = BASE_DIR / "inputDataUsers.csv"      # Исправлено: используем Path
REQUESTS_CSV = BASE_DIR / "inputDataRequests.csv"  # Исправлено: используем Path
COMMENTS_CSV = BASE_DIR / "inputDataComments.csv"  # Исправлено: используем Path

DB_NAME = BASE_DIR / "home_service.db"



# =====================
# Создание БД
# =====================

def create_db():
    """Создание схемы БД с правильными именами колонок"""
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    # Таблица пользователей
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            fio TEXT,
            phone TEXT,
            login TEXT UNIQUE,
            password TEXT,
            user_type TEXT,
            is_active INTEGER DEFAULT 1,
            registration_date TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # Таблица заявок
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS requests (
            request_id INTEGER PRIMARY KEY AUTOINCREMENT,
            start_date TEXT,
            climate_tech_type TEXT,
            climate_tech_model TEXT,
            problem_description TEXT,
            request_status TEXT,
            completion_date TEXT,
            repair_parts TEXT,
            master_id INTEGER,
            client_id INTEGER,
            FOREIGN KEY(master_id) REFERENCES users(user_id),
            FOREIGN KEY(client_id) REFERENCES users(user_id)
        )
    """)

    # Таблица комментариев
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS comments (
            comment_id INTEGER PRIMARY KEY AUTOINCREMENT,
            message TEXT,
            master_id INTEGER,
            request_id INTEGER,
            FOREIGN KEY(master_id) REFERENCES users(user_id),
            FOREIGN KEY(request_id) REFERENCES requests(request_id)
        )
    """)

    conn.commit()
    conn.close()
    print("✅ База данных создана/обновлена")


# =====================
# Импорт пользователей
# =====================

def import_users():
    """Импорт пользователей из CSV в БД"""
    
    if not USERS_CSV.exists():
        print(f"⚠️  CSV не найден: {USERS_CSV}")
        return

    print("📥 Импорт пользователей...")

    df = pd.read_csv(USERS_CSV, sep=";", encoding="utf-8-sig")
    df.columns = df.columns.str.strip().str.lower()

    # Переименуем столбцы из CSV в ожидаемые БД
    df = df.rename(columns={
        "userid": "user_id",
        "id": "user_id",
        "type": "user_type",
        "role": "user_type"
    })

    # Убедимся что есть все нужные колонки
    required_cols = ["user_id", "fio", "phone", "login", "password", "user_type"]
    for col in required_cols:
        if col not in df.columns:
            df[col] = None

    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    for _, row in df.iterrows():
        try:
            cursor.execute("""
                INSERT OR REPLACE INTO users 
                    (user_id, fio, phone, login, password, user_type)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (
                row["user_id"],
                row["fio"],
                row["phone"],
                row["login"],
                row["password"],
                row["user_type"]
            ))
        except Exception as e:
            print(f"⚠️  Ошибка при импорте пользователя: {e}")

    conn.commit()
    conn.close()
    print("✅ Пользователи загружены")


# =====================
# Импорт заявок
# =====================

def import_requests():
    """Импорт заявок из CSV в БД"""
    
    if not REQUESTS_CSV.exists():
        print(f"⚠️  CSV не найден: {REQUESTS_CSV}")
        return

    print("📥 Импорт заявок...")

    df = pd.read_csv(REQUESTS_CSV, sep=";", encoding="utf-8-sig")
    df.columns = df.columns.str.strip().str.lower()

    # Переименуем столбцы
    df = df.rename(columns={
        "startdate": "start_date",
        "start_date": "start_date",
        "hometechtype": "climate_tech_type",
        "climatetechtype": "climate_tech_type",
        "hometechmodel": "climate_tech_model",
        "climatetechmodel": "climate_tech_model",
        "problemdescryption": "problem_description",
        "problemdescription": "problem_description",
        "requeststatus": "request_status",
        "completiondate": "completion_date",
        "repairparts": "repair_parts",
        "masterid": "master_id",
        "clientid": "client_id"
    })

    # Заменяем null-ы на None
    df = df.replace({"null": None, "NULL": None, pd.NA: None})

    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    for _, row in df.iterrows():
        try:
            cursor.execute("""
                INSERT INTO requests
                    (start_date, climate_tech_type, climate_tech_model, 
                    problem_description, request_status, completion_date,
                    repair_parts, master_id, client_id)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                row.get("start_date"),
                row.get("climate_tech_type"),
                row.get("climate_tech_model"),
                row.get("problem_description"),
                row.get("request_status"),
                row.get("completion_date"),
                row.get("repair_parts"),
                row.get("master_id"),
                row.get("client_id")
            ))
        except Exception as e:
            print(f"⚠️  Ошибка при импорте заявки: {e}")

    conn.commit()
    conn.close()
    print("✅ Заявки загружены")


# =====================
# Импорт комментариев
# =====================

def import_comments():
    """Импорт комментариев из CSV в БД"""
    
    if not COMMENTS_CSV.exists():
        print(f"⚠️  CSV не найден: {COMMENTS_CSV}")
        return

    print("📥 Импорт комментариев...")

    df = pd.read_csv(COMMENTS_CSV, sep=";", encoding="utf-8-sig")
    df.columns = df.columns.str.strip().str.lower()

    df = df.rename(columns={
        "message": "message",
        "masterid": "master_id",
        "requestid": "request_id"
    })

    df = df.replace({"null": None, "NULL": None, pd.NA: None})

    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    for _, row in df.iterrows():
        try:
            cursor.execute("""
                INSERT INTO comments (message, master_id, request_id)
                VALUES (?, ?, ?)
            """, (
                row.get("message"),
                row.get("master_id"),
                row.get("request_id")
            ))
        except Exception as e:
            print(f"⚠️  Ошибка при импорте комментария: {e}")

    conn.commit()
    conn.close()
    print("✅ Комментарии загружены")


# =====================
# MAIN
# =====================

def main():
    create_db()
    import_users()
    import_requests()
    import_comments()
    print("✅✅✅ Импорт завершён УСПЕШНО ✅✅✅")


if __name__ == "__main__":
    main()
