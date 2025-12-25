"""
Скрипт для запуска веб-приложения с проверкой зависимостей и БД
"""
import os
import sys

def check_dependencies():
    required = ['flask', 'qrcode', 'PIL']
    missing = []

    for package in required:
        try:
            if package == 'PIL':
                __import__('PIL')
            else:
                __import__(package)
        except ImportError:
            missing.append(package)

    if missing:
        print("=" * 60)
        print("ОШИБКА: Не установлены необходимые пакеты!")
        print("pip install -r requirements.txt")
        print("Отсутствуют:", ", ".join(missing))
        print("=" * 60)
        return False

    return True


def check_database():
    db_name = "home_service.db"
    if not os.path.exists(db_name):
        print("=" * 60)
        print("ОШИБКА: База данных не найдена!")
        print(f"Нет файла: {db_name}")
        print("=" * 60)
        return False
    return True


def main():
    print("Проверка готовности проекта...")

    if not check_dependencies():
        sys.exit(1)

    if not check_database():
        sys.exit(1)

    print("=" * 60)
    print("Запуск веб-приложения")
    print("http://127.0.0.1:5000/")
    print("=" * 60)
    print("Тестовые пользователи:")
    print("login1 / pass1 — Менеджер")
    print("login7 / pass7 — Мастер")
    print("login8 / pass8 — Заказчик")
    print("=" * 60)

    from web_app import app
    app.run(debug=True)


if __name__ == "__main__":
    main()
