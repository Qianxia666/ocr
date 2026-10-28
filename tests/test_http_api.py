"""HTTP / WebSocket 端到端回归测试。"""
import json
import uuid
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from models.database import db_manager


@pytest.fixture(scope="module")
def client():
    from main import app

    with TestClient(app) as test_client:
        yield test_client


def _register(client, username, password, token=None):
    payload = {"username": username, "password": password}
    if token:
        payload["token"] = token
    return client.post("/api/auth/register", json=payload)


def _login(client, username, password):
    response = client.post("/api/auth/login", json={"username": username, "password": password})
    assert response.status_code == 200, response.text
    return response


def test_basic_endpoints(client):
    assert client.get("/healthz").status_code == 200
    assert client.get("/").status_code == 200
    assert client.get("/static/favicon.ico").status_code == 200
    assert client.get("/login").status_code == 200
    assert client.get("/api/announcement/config").status_code == 200
    assert client.get("/api/nonexistent-endpoint").status_code == 404
    assert client.get("/api/admin/users/list").status_code == 401


def test_register_and_login_admin(client):
    response = _register(client, "admin_tester", "AdminPass123")
    assert response.status_code == 200, response.text
    data = response.json()
    assert data["status"] == "success"

    _login(client, "admin_tester", "AdminPass123")
    current = client.get("/api/user/current")
    assert current.status_code == 200
    assert current.json()["data"]["username"] == "admin_tester"
    assert current.json()["data"]["is_admin"] is True

    quota = client.get("/api/user/quota")
    assert quota.status_code == 200
    assert quota.json()["data"]["remaining_pages"] > 0


def test_registration_rejects_too_long_password(client):
    response = client.post(
        "/api/auth/register",
        json={"username": "long_pwd_user", "password": "a" * 129},
    )
    assert response.status_code == 400
    assert "128" in response.json()["detail"]


def test_redemption_code_flow(client, tmp_path: Path):
    generated = client.post(
        "/api/admin/codes/generate",
        json={"pages": 5, "max_uses": 1, "batch_count": 1},
    )
    assert generated.status_code == 200, generated.text
    code = generated.json()["code"]

    assert _register(client, "normal_tester", "UserPass123").status_code == 200
    _login(client, "normal_tester", "UserPass123")

    redeemed = client.post("/api/user/redeem", json={"code": code})
    assert redeemed.status_code == 200, redeemed.text
    assert redeemed.json()["pages_granted"] == 5

    duplicate = client.post("/api/user/redeem", json={"code": code})
    assert duplicate.status_code == 400

    invalid = client.post("/api/user/redeem", json={"code": "not-a-real-code"})
    assert invalid.status_code == 400


def test_admin_user_management(client):
    _login(client, "admin_tester", "AdminPass123")

    users = client.get("/api/admin/users/list", params={"search": "normal_tester"})
    assert users.status_code == 200
    payload = users.json()
    assert payload["total"] == 1
    assert payload["data"][0]["username"] == "normal_tester"
    target_id = payload["data"][0]["id"]

    assert client.post(f"/api/admin/users/{target_id}/disable").status_code == 200
    assert client.post(f"/api/admin/users/{target_id}/enable").status_code == 200

    missing = client.post(f"/api/admin/users/{uuid.uuid4()}/disable")
    assert missing.status_code in (404, 500)


def _seed_task(sqlite_path: str, task_id: str, user_id: str, file_name: str, content: str) -> None:
    """直接写入数据库，用于构造任务列表/搜索场景。"""
    import sqlite3

    conn = sqlite3.connect(sqlite_path)
    try:
        conn.execute(
            "INSERT INTO tasks (id, user_id, task_type, status, file_name, file_size, total_pages, created_at) "
            "VALUES (?, ?, 'image_ocr', 'pending', ?, 10, 1, '2026-01-01T00:00:00+00:00')",
            (task_id, user_id, file_name),
        )
        conn.execute(
            "INSERT INTO page_results (task_id, page_number, status, content, created_at) "
            "VALUES (?, 1, 'completed', ?, '2026-01-01T00:00:00+00:00')",
            (task_id, content),
        )
        conn.commit()
    finally:
        conn.close()


def test_admin_task_search_is_scoped(client):
    _login(client, "admin_tester", "AdminPass123")

    admin = client.get("/api/user/current").json()["data"]
    users = client.get("/api/admin/users/list", params={"search": "normal_tester"}).json()["data"]
    normal_id = users[0]["id"]

    _seed_task(db_manager.db_path, "admin-task-1", admin["user_id"], "admin.png", "unique-banana-content")
    _seed_task(db_manager.db_path, "normal-task-1", normal_id, "normal.png", "unique-apple-content")

    scoped = client.get(
        "/api/admin/tasks/list",
        params={"user_id": normal_id, "search": "unique-banana-content"},
    )
    assert scoped.status_code == 200
    assert scoped.json()["data"] == []
    assert scoped.json()["total"] == 0

    by_content = client.get("/api/admin/tasks/list", params={"search": "unique-banana-content"})
    assert by_content.status_code == 200
    ids = [task["id"] for task in by_content.json()["data"]]
    assert ids == ["admin-task-1"]
    assert by_content.json()["total"] == 1

    by_name = client.get("/api/admin/tasks/list", params={"search": "normal_tester"})
    assert by_name.status_code == 200
    assert [task["id"] for task in by_name.json()["data"]] == ["normal-task-1"]
    assert by_name.json()["total"] == 1


def test_quota_cannot_drop_below_used_pages(client):
    _login(client, "admin_tester", "AdminPass123")
    users = client.get("/api/admin/users/list", params={"search": "normal_tester"}).json()["data"]
    target_id = users[0]["id"]

    import sqlite3

    conn = sqlite3.connect(db_manager.db_path)
    try:
        conn.execute("UPDATE users SET used_pages = 3 WHERE id = ?", (target_id,))
        conn.commit()
    finally:
        conn.close()

    response = client.post(
        f"/api/admin/users/{target_id}/quota",
        json={"action": "set", "pages": 1},
    )
    assert response.status_code == 400
    assert "已使用" in response.json()["detail"]


def test_registration_token_flow(client):
    _login(client, "admin_tester", "AdminPass123")

    created = client.post(
        "/api/admin/registration-tokens/create",
        json={"max_uses": 1, "description": "pytest token"},
    )
    assert created.status_code == 200, created.text
    token = created.json().get("token") or created.json().get("data", {}).get("token")
    assert token

    assert client.post("/api/admin/settings/registration", json={"enabled": False}).status_code == 200

    denied = _register(client, "tokenless_user", "UserPass123")
    assert denied.status_code == 400

    accepted = _register(client, "token_user", "UserPass123", token=token)
    assert accepted.status_code == 200, accepted.text

    reused = _register(client, "token_user_2", "UserPass123", token=token)
    assert reused.status_code == 400

    assert client.post("/api/admin/settings/registration", json={"enabled": True}).status_code == 200


def test_upload_rejects_wrong_content_type(client):
    _login(client, "admin_tester", "AdminPass123")
    response = client.post(
        "/process/image",
        files={"file": ("note.txt", b"hello", "text/plain")},
    )
    assert response.status_code == 400


def test_websocket_ping_pong(client):
    with client.websocket_connect("/ws") as websocket:
        established = websocket.receive_json()
        assert established["type"] == "connection_established"

        websocket.send_text(json.dumps({"type": "ping"}))
        pong = websocket.receive_json()
        assert pong["type"] == "pong"


def test_logout_invalidates_session(client):
    _login(client, "admin_tester", "AdminPass123")
    assert client.get("/api/user/current").status_code == 200
    assert client.post("/api/auth/logout").status_code == 200
    assert client.get("/api/user/current").status_code == 401
