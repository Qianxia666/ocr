"""针对已修复缺陷的回归测试。"""
from pathlib import Path

import pytest

import core.password_hashing as password_hashing
from core.password_hashing import hash_password, verify_password
from models.database import (
    DatabaseManager,
    PageResultModel,
    TaskModel,
    TaskType,
    UserAgreementModel,
    IPTrackingModel,
)
from models.user import RedemptionCodeModel, RegistrationTokenModel, UserModel


def test_long_password_does_not_crash_bcrypt():
    """bcrypt 只接受 72 字节以内的密码，超长密码必须被安全处理。"""
    long_password = "长密码" * 60  # UTF-8 下远超 72 字节
    assert len(long_password.encode("utf-8")) > 72

    encoded = hash_password(long_password)
    assert verify_password(long_password, encoded)
    assert not verify_password(long_password + "x", encoded)


def test_scrypt_fallback_roundtrip(monkeypatch):
    """未安装 bcrypt 时使用 scrypt 后备实现。"""
    monkeypatch.setattr(password_hashing, "_bcrypt", None)
    encoded = hash_password("fallback-secret")
    assert encoded.startswith("$scrypt$v1$")
    assert verify_password("fallback-secret", encoded)
    assert not verify_password("wrong", encoded)


@pytest.mark.asyncio
async def test_task_search_does_not_leak_other_users(tmp_path: Path):
    """管理员按用户筛选并搜索内容时，不得通过 OR 优先级泄漏其他用户任务。"""
    db = DatabaseManager(str(tmp_path / "filter.db"))
    await db.initialize()
    users = UserModel(db)
    u1 = await users.create_user("alice", "secret123")
    u2 = await users.create_user("bob", "secret123")
    tasks = TaskModel(db)
    pages = PageResultModel(db)

    async def add_task(task_id, user_id, file_name, content):
        await tasks.create_task(
            task_id=task_id,
            task_type=TaskType.IMAGE_OCR,
            file_name=file_name,
            file_size=100,
            total_pages=1,
            user_id=user_id,
        )
        await pages.create_page_result(task_id, 1, "completed")
        await pages.update_page_result(task_id, 1, "completed", content=content)

    await add_task("task-1", u1, "alice.png", "apple")
    await add_task("task-2", u2, "bob.png", "banana")

    filtered = await tasks.get_all_tasks_with_users(user_id_filter=u1, search_query="banana")
    assert filtered == []
    assert await tasks.count_tasks(user_id_filter=u1, search_query="banana") == 0

    by_content = await tasks.get_all_tasks_with_users(search_query="banana")
    assert [t["id"] for t in by_content] == ["task-2"]
    assert await tasks.count_tasks(search_query="banana") == 1

    by_username = await tasks.get_all_tasks_with_users(search_query="alice")
    assert [t["id"] for t in by_username] == ["task-1"]
    assert await tasks.count_tasks(search_query="alice") == 1


@pytest.mark.asyncio
async def test_ip_abuse_settings_survive_restart(tmp_path: Path):
    """启动时只能补齐缺失配置，不能覆盖管理员已保存的设置。"""
    db = DatabaseManager(str(tmp_path / "settings.db"))
    await db.initialize()
    tracker = IPTrackingModel(db)

    assert await tracker.update_settings(enabled=False, threshold=9, days=30)
    assert await tracker.reset_to_env_defaults()

    settings = await tracker.get_settings()
    assert settings["enabled"] is False
    assert settings["threshold"] == 9
    assert settings["days"] == 30


@pytest.mark.asyncio
async def test_toggle_and_delete_missing_records_return_false(tmp_path: Path):
    db = DatabaseManager(str(tmp_path / "toggles.db"))
    await db.initialize()
    codes = RedemptionCodeModel(db)
    tokens = RegistrationTokenModel(db)

    assert not await codes.activate_code("missing-code")
    assert not await codes.deactivate_code("missing-code")
    assert not await tokens.activate_token("missing-token")
    assert not await tokens.deactivate_token("missing-token")
    assert not await tokens.delete_token("missing-token")


@pytest.mark.asyncio
async def test_user_agreement_acceptance_returns_bool(tmp_path: Path):
    db = DatabaseManager(str(tmp_path / "agreement.db"))
    await db.initialize()
    users = UserModel(db)
    uid = await users.create_user("agreement_user", "secret123")
    agreements = UserAgreementModel(db)

    agreement_id = await agreements.create_agreement("测试协议内容")
    assert agreement_id
    assert await agreements.has_user_accepted_current(uid) is False
    assert await agreements.record_acceptance(uid, agreement_id)
    assert await agreements.has_user_accepted_current(uid) is True


@pytest.mark.asyncio
async def test_duplicate_redemption_is_blocked_by_db_constraint(tmp_path: Path):
    """即使绕过业务层检查，数据库唯一索引也要阻止重复兑换入账。"""
    db = DatabaseManager(str(tmp_path / "unique.db"))
    await db.initialize()
    users = UserModel(db)
    uid = await users.create_user("redeemer", "secret123")
    codes = RedemptionCodeModel(db)
    code = await codes.create_code(uid, pages=5, max_uses=2)
    assert code

    ok, _, pages = await codes.use_code(code, uid)
    assert ok and pages == 5

    async with db.get_connection() as conn:
        with pytest.raises(Exception):
            await conn.execute(
                "INSERT INTO redemption_history (user_id, code, pages_granted, redeemed_at) "
                "VALUES (?, ?, ?, ?)",
                (uid, code, 5, "2026-01-01T00:00:00+00:00"),
            )
            await conn.commit()
