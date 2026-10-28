import asyncio
from pathlib import Path

import pytest

from core.auth import AuthManager
from core.password_hashing import hash_password, verify_password
from models.database import DatabaseManager
from models.user import UserModel, RedemptionCodeModel, RegistrationTokenModel


def test_password_hash_roundtrip():
    encoded = hash_password('secret123')
    assert encoded != 'secret123'
    assert verify_password('secret123', encoded)
    assert not verify_password('wrong', encoded)


@pytest.mark.asyncio
async def test_database_auth_quota_and_codes(tmp_path: Path):
    db = DatabaseManager(str(tmp_path / 'tasks.db'))
    await db.initialize()
    users = UserModel(db)
    auth = AuthManager(users, db)

    ok, _, admin_id = await auth.register('admin_user', 'secret123')
    assert ok and admin_id
    admin = await users.get_user(admin_id)
    assert admin['is_admin'] is True

    ok, _, user_id = await auth.register('normal_user', 'secret123')
    assert ok and user_id
    user = await users.get_user(user_id)
    assert user['is_admin'] is False
    assert await users.authenticate('normal_user', 'secret123')
    assert await users.authenticate('normal_user', 'bad-pass') is None

    assert await users.add_pages(user_id, 3)
    assert await users.deduct_pages(user_id, 2)
    assert not await users.deduct_pages(user_id, 2)
    quota = await users.get_quota(user_id)
    assert quota['remaining_pages'] == 1

    codes = RedemptionCodeModel(db)
    code = await codes.create_code(admin_id, pages=5, max_uses=1)
    assert code
    first = await codes.use_code(code, user_id)
    second = await codes.use_code(code, user_id)
    assert first[0] is True and first[2] == 5
    assert second[0] is False


@pytest.mark.asyncio
async def test_registration_token_single_use_is_atomic(tmp_path: Path):
    db = DatabaseManager(str(tmp_path / 'tokens.db'))
    await db.initialize()
    users = UserModel(db)
    admin_id = await users.create_user('admin', 'secret123', is_admin=True)
    u1 = await users.create_user('u1x', 'secret123')
    u2 = await users.create_user('u2x', 'secret123')
    tokens = RegistrationTokenModel(db)
    token = await tokens.create_token(admin_id, max_uses=1)
    assert token

    results = await asyncio.gather(
        tokens.use_token(token, u1, 'u1x'),
        tokens.use_token(token, u2, 'u2x'),
    )
    assert sum(1 for ok, _ in results if ok) == 1
