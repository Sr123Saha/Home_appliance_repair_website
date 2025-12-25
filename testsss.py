import os
import sys
import pytest
from web_app import app, DB_NAME

# Настройка путей, чтобы тесты видели основной модуль
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

@pytest.fixture(scope="module")
def test_client():
    """Настройка тестового клиента Flask."""
    assert os.path.exists(DB_NAME), f"БД {DB_NAME} не найдена. Сначала создайте её."
    app.config["TESTING"] = True
    with app.test_client() as client:
        yield client

def test_login_and_access(test_client):
    """Комплексная проверка: анонимный доступ, вход и защищенные страницы."""
    
    # 1. Проверка редиректа анонима
    res = test_client.get("/requests")
    assert res.status_code in (301, 302)
    assert "/login" in res.headers.get("Location", "")

    # 2. Ошибка при неверном логине
    res = test_client.post("/login", data={"login": "err", "password": "err"}, follow_redirects=True)
    assert "Неверный логин или пароль" in res.get_data(as_text=True)

    # 3. Успешный вход менеджера (используем login1/pass1 из вашего примера)
    res = test_client.post("/login", data={"login": "login1", "password": "pass1"}, follow_redirects=True)
    assert res.status_code == 200
    assert "Список заявок" in res.get_data(as_text=True)

    # 4. Доступ к статистике для авторизованного
    res = test_client.get("/stats")
    assert "Статистика" in res.get_data(as_text=True)

    # 5. Доступ к управлению пользователями
    res = test_client.get("/users/manage")
    assert "Управление пользователями" in res.get_data(as_text=True)

def test_logout(test_client):
    """Проверка выхода из системы."""
    test_client.get("/logout", follow_redirects=True)
    res = test_client.get("/requests")
    assert res.status_code in (301, 302) # Снова редирект после выхода