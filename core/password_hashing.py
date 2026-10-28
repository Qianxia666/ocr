"""Password hashing helpers with bcrypt preference and a secure stdlib fallback."""
from __future__ import annotations

import base64
import hashlib
import hmac
import os

try:  # pragma: no cover - depends on deployment environment
    import bcrypt as _bcrypt
except ImportError:  # pragma: no cover
    _bcrypt = None

_SCRYPT_PREFIX = "$scrypt$v1$"
# bcrypt 只处理前 72 字节，超长密码会直接抛 ValueError。
# 参考 bcrypt-sha256 方案：先做 sha256 摘要再交给 bcrypt，并用前缀区分旧格式。
_BCRYPT_SHA256_PREFIX = "$bcrypt-sha256$"
_BCRYPT_MAX_BYTES = 72


def _prepare_bcrypt_input(raw: bytes) -> tuple[bytes, bool]:
    """返回 (bcrypt 输入, 是否使用了 sha256 预摘要)。"""
    if len(raw) <= _BCRYPT_MAX_BYTES:
        return raw, False
    return hashlib.sha256(raw).digest(), True


def hash_password(password: str) -> str:
    raw = password.encode("utf-8")
    if _bcrypt is not None:
        candidate, prehashed = _prepare_bcrypt_input(raw)
        hashed = _bcrypt.hashpw(candidate, _bcrypt.gensalt()).decode("utf-8")
        return _BCRYPT_SHA256_PREFIX + hashed if prehashed else hashed

    # 无 bcrypt 时使用 scrypt（本身没有长度限制）
    salt = os.urandom(16)
    digest = hashlib.scrypt(raw, salt=salt, n=2**14, r=8, p=1, dklen=32)
    return _SCRYPT_PREFIX + base64.urlsafe_b64encode(salt).decode() + "$" + base64.urlsafe_b64encode(digest).decode()


def verify_password(password: str, encoded: str) -> bool:
    raw = password.encode("utf-8")

    if encoded.startswith(_SCRYPT_PREFIX):
        try:
            payload = encoded[len(_SCRYPT_PREFIX):]
            salt_text, digest_text = payload.split("$", 1)
            salt = base64.urlsafe_b64decode(salt_text.encode())
            expected = base64.urlsafe_b64decode(digest_text.encode())
            actual = hashlib.scrypt(raw, salt=salt, n=2**14, r=8, p=1, dklen=len(expected))
            return hmac.compare_digest(actual, expected)
        except (ValueError, TypeError):
            return False

    if _bcrypt is None:
        # Existing bcrypt hashes require the bcrypt dependency. Never pretend a
        # password matched when the verifier is unavailable.
        return False

    if encoded.startswith(_BCRYPT_SHA256_PREFIX):
        candidate, _ = _prepare_bcrypt_input(raw)
        # 超长密码使用 sha256 预摘要；短密码仍按原文校验（理论上不会出现，
        # 但保留兼容性，避免因输入长度变化导致误判）。
        try:
            return _bcrypt.checkpw(candidate, encoded[len(_BCRYPT_SHA256_PREFIX):].encode("utf-8"))
        except (ValueError, TypeError):
            return False

    try:
        candidate, _ = _prepare_bcrypt_input(raw)
        return _bcrypt.checkpw(candidate, encoded.encode("utf-8"))
    except (ValueError, TypeError):
        return False
