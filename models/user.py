import aiosqlite
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any, List
import json
import logging
import uuid
import secrets
import bcrypt

logger = logging.getLogger(__name__)


class UserModel:
    """用户模型,提供用户相关的数据库操作"""

    def __init__(self, db_manager):
        self.db_manager = db_manager

    def hash_password(self, password: str) -> str:
        """对密码进行哈希加密"""
        salt = bcrypt.gensalt()
        hashed = bcrypt.hashpw(password.encode('utf-8'), salt)
        return hashed.decode('utf-8')

    def verify_password(self, password: str, password_hash: str) -> bool:
        """验证密码是否正确"""
        try:
            return bcrypt.checkpw(password.encode('utf-8'), password_hash.encode('utf-8'))
        except Exception as e:
            logger.error(f"密码验证失败: {e}")
            return False

    async def create_user(self,
                         username: str,
                         password: str,
                         is_admin: bool = False,
                         total_pages: int = 0) -> Optional[str]:
        """创建新用户，返回用户ID"""
        try:
            user_id = str(uuid.uuid4())
            password_hash = self.hash_password(password)
            session_token = secrets.token_urlsafe(32)

            async with self.db_manager.get_connection() as db:
                await db.execute("""
                    INSERT INTO users (
                        id, username, password_hash, is_admin, total_pages, used_pages,
                        created_at, last_login, session_token
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    user_id,
                    username,
                    password_hash,
                    1 if is_admin else 0,
                    total_pages,
                    0,
                    datetime.now(timezone.utc).isoformat(),
                    datetime.now(timezone.utc).isoformat(),
                    session_token
                ))
                await db.commit()
                logger.info(f"用户 {username} ({user_id[:8]}...) 创建成功 (管理员: {is_admin})")
                return user_id
        except Exception as e:
            logger.error(f"创建用户失败 {username}: {e}")
            return None

    async def authenticate(self, username: str, password: str) -> Optional[Dict[str, Any]]:
        """验证用户名和密码，返回用户信息"""
        try:
            user = await self.get_user_by_username(username)
            if not user:
                return None

            # 检查用户是否被禁用
            if user.get('is_disabled', False):
                logger.warning(f"用户 {username} 已被禁用，拒绝登录")
                return None

            if self.verify_password(password, user['password_hash']):
                # 更新最后登录时间
                await self.update_last_login(user['id'])
                return user
            return None
        except Exception as e:
            logger.error(f"用户认证失败 {username}: {e}")
            return None

    async def get_user(self, user_id: str) -> Optional[Dict[str, Any]]:
        """根据ID获取用户信息"""
        try:
            async with self.db_manager.get_connection() as db:
                db.row_factory = aiosqlite.Row
                async with db.execute("""
                    SELECT * FROM users WHERE id = ?
                """, (user_id,)) as cursor:
                    row = await cursor.fetchone()
                    if row:
                        user = dict(row)
                        # 转换布尔值
                        user['is_admin'] = bool(user['is_admin'])
                        user['is_disabled'] = bool(user.get('is_disabled', 0))
                        return user
                    return None
        except Exception as e:
            logger.error(f"获取用户失败 {user_id}: {e}")
            return None

    async def get_user_by_session(self, session_token: str) -> Optional[Dict[str, Any]]:
        """根据Session Token获取用户信息"""
        try:
            async with self.db_manager.get_connection() as db:
                db.row_factory = aiosqlite.Row
                async with db.execute("""
                    SELECT * FROM users WHERE session_token = ?
                """, (session_token,)) as cursor:
                    row = await cursor.fetchone()
                    if row:
                        user = dict(row)
                        user['is_admin'] = bool(user['is_admin'])
                        user['is_disabled'] = bool(user.get('is_disabled', 0))
                        return user
                    return None
        except Exception as e:
            logger.error(f"根据Session获取用户失败: {e}")
            return None

    async def get_user_by_username(self, username: str) -> Optional[Dict[str, Any]]:
        """根据用户名获取用户信息"""
        try:
            async with self.db_manager.get_connection() as db:
                db.row_factory = aiosqlite.Row
                async with db.execute("""
                    SELECT * FROM users WHERE username = ?
                """, (username,)) as cursor:
                    row = await cursor.fetchone()
                    if row:
                        user = dict(row)
                        user['is_admin'] = bool(user['is_admin'])
                        user['is_disabled'] = bool(user.get('is_disabled', 0))
                        return user
                    return None
        except Exception as e:
            logger.error(f"根据用户名获取用户失败 {username}: {e}")
            return None

    async def update_last_login(self, user_id: str) -> bool:
        """更新最后登录时间"""
        try:
            async with self.db_manager.get_connection() as db:
                await db.execute("""
                    UPDATE users SET last_login = ? WHERE id = ?
                """, (datetime.now(timezone.utc).isoformat(), user_id))
                await db.commit()
                return True
        except Exception as e:
            logger.error(f"更新登录时间失败 {user_id}: {e}")
            return False

    async def update_session_token(self, user_id: str, session_token: Optional[str]) -> bool:
        """更新用户的Session Token（None 表示清除）"""
        try:
            async with self.db_manager.get_connection() as db:
                await db.execute("""
                    UPDATE users SET session_token = ? WHERE id = ?
                """, (session_token, user_id))
                await db.commit()
                return True
        except Exception as e:
            logger.error(f"更新Session Token失败 {user_id}: {e}")
            return False

    async def add_pages(self, user_id: str, pages: int) -> bool:
        """增加用户的总页数"""
        try:
            async with self.db_manager.get_connection() as db:
                await db.execute("""
                    UPDATE users SET total_pages = total_pages + ? WHERE id = ?
                """, (pages, user_id))
                await db.commit()
                logger.info(f"用户 {user_id} 增加 {pages} 页配额")
                return True
        except Exception as e:
            logger.error(f"增加页数失败 {user_id}: {e}")
            return False

    async def deduct_pages(self, user_id: str, pages: int) -> bool:
        """扣除用户使用的页数"""
        try:
            async with self.db_manager.get_connection() as db:
                # 检查余额
                async with db.execute("""
                    SELECT total_pages, used_pages FROM users WHERE id = ?
                """, (user_id,)) as cursor:
                    row = await cursor.fetchone()
                    if not row:
                        return False

                    total_pages, used_pages = row
                    if used_pages + pages > total_pages:
                        logger.warning(f"用户 {user_id} 配额不足")
                        return False

                # 扣除页数
                await db.execute("""
                    UPDATE users SET used_pages = used_pages + ? WHERE id = ?
                """, (pages, user_id))
                await db.commit()
                logger.info(f"用户 {user_id} 扣除 {pages} 页")
                return True
        except Exception as e:
            logger.error(f"扣除页数失败 {user_id}: {e}")
            return False


    async def refund_pages(self, user_id: str, pages: int) -> bool:
        """返还用户已扣除的页数"""
        if pages <= 0:
            return True
        try:
            async with self.db_manager.get_connection() as db:
                await db.execute("""
                    UPDATE users
                    SET used_pages = CASE
                        WHEN used_pages >= ? THEN used_pages - ?
                        ELSE 0
                    END
                    WHERE id = ?
                """, (pages, pages, user_id))
                await db.commit()
                logger.info(f"用户 {user_id} 返还 {pages} 页配额")
                return True
        except Exception as e:
            logger.error(f"返还页数失败 {user_id}: {e}")
            return False

    async def get_quota(self, user_id: str) -> Optional[Dict[str, int]]:
        """获取用户配额信息"""
        try:
            async with self.db_manager.get_connection() as db:
                async with db.execute("""
                    SELECT total_pages, used_pages FROM users WHERE id = ?
                """, (user_id,)) as cursor:
                    row = await cursor.fetchone()
                    if row:
                        total_pages, used_pages = row
                        return {
                            'total_pages': total_pages,
                            'used_pages': used_pages,
                            'remaining_pages': total_pages - used_pages
                        }
                    return None
        except Exception as e:
            logger.error(f"获取配额失败 {user_id}: {e}")
            return None

    async def set_quota(self, user_id: str, total_pages: int) -> bool:
        """设置用户总配额"""
        try:
            async with self.db_manager.get_connection() as db:
                await db.execute("""
                    UPDATE users SET total_pages = ? WHERE id = ?
                """, (total_pages, user_id))
                await db.commit()
                logger.info(f"用户 {user_id} 配额设置为 {total_pages} 页")
                return True
        except Exception as e:
            logger.error(f"设置配额失败 {user_id}: {e}")
            return False

    async def get_all_users(self, limit: int = 100, offset: int = 0) -> List[Dict[str, Any]]:
        """获取所有用户列表"""
        try:
            async with self.db_manager.get_connection() as db:
                db.row_factory = aiosqlite.Row
                async with db.execute("""
                    SELECT id, username, is_admin, is_disabled, total_pages, used_pages,
                           created_at, last_login
                    FROM users
                    ORDER BY created_at DESC
                    LIMIT ? OFFSET ?
                """, (limit, offset)) as cursor:
                    rows = await cursor.fetchall()
                    users = []
                    for row in rows:
                        user = dict(row)
                        user['is_admin'] = bool(user['is_admin'])
                        user['is_disabled'] = bool(user.get('is_disabled', 0))
                        user['remaining_pages'] = user['total_pages'] - user['used_pages']
                        users.append(user)
                    return users
        except Exception as e:
            logger.error(f"获取用户列表失败: {e}")
            return []

    async def count_users(self) -> int:
        """统计用户总数"""
        try:
            async with self.db_manager.get_connection() as db:
                async with db.execute("SELECT COUNT(*) FROM users") as cursor:
                    row = await cursor.fetchone()
                    return row[0] if row else 0
        except Exception as e:
            logger.error(f"统计用户数失败: {e}")
            return 0

    async def disable_user(self, user_id: str) -> bool:
        """禁用用户"""
        try:
            async with self.db_manager.get_connection() as db:
                await db.execute("""
                    UPDATE users SET is_disabled = 1 WHERE id = ?
                """, (user_id,))
                await db.commit()
                logger.info(f"用户 {user_id} 已被禁用")
                return True
        except Exception as e:
            logger.error(f"禁用用户失败 {user_id}: {e}")
            return False

    async def enable_user(self, user_id: str) -> bool:
        """启用用户"""
        try:
            async with self.db_manager.get_connection() as db:
                await db.execute("""
                    UPDATE users SET is_disabled = 0 WHERE id = ?
                """, (user_id,))
                await db.commit()
                logger.info(f"用户 {user_id} 已被启用")
                return True
        except Exception as e:
            logger.error(f"启用用户失败 {user_id}: {e}")
            return False


class RedemptionCodeModel:
    """兑换码模型,提供兑换码相关的数据库操作"""

    def __init__(self, db_manager):
        self.db_manager = db_manager

    def generate_code(self) -> str:
        """生成唯一兑换码"""
        # 使用secrets生成安全的随机码
        return f"OCR-{secrets.token_hex(8).upper()}"

    async def create_code(self,
                         created_by: str,
                         pages: int,
                         max_uses: int = 1,
                         expires_days: Optional[int] = None,
                         description: Optional[str] = None) -> Optional[str]:
        """创建兑换码"""
        try:
            code = self.generate_code()
            expires_at = None
            if expires_days:
                expires_at = (datetime.now(timezone.utc) + timedelta(days=expires_days)).isoformat()

            async with self.db_manager.get_connection() as db:
                await db.execute("""
                    INSERT INTO redemption_codes (
                        code, pages, max_uses, used_count, created_by,
                        created_at, expires_at, is_active, description
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    code,
                    pages,
                    max_uses,
                    0,
                    created_by,
                    datetime.now(timezone.utc).isoformat(),
                    expires_at,
                    1,
                    description
                ))
                await db.commit()
                logger.info(f"兑换码 {code} 创建成功 (页数: {pages}, 用途: {max_uses})")
                return code
        except Exception as e:
            import traceback
            logger.error(f"创建兑换码失败: {e}\n{traceback.format_exc()}")
            return None

    async def get_code(self, code: str) -> Optional[Dict[str, Any]]:
        """获取兑换码信息"""
        try:
            async with self.db_manager.get_connection() as db:
                db.row_factory = aiosqlite.Row
                async with db.execute("""
                    SELECT * FROM redemption_codes WHERE code = ?
                """, (code,)) as cursor:
                    row = await cursor.fetchone()
                    if row:
                        code_info = dict(row)
                        code_info['is_active'] = bool(code_info['is_active'])
                        return code_info
                    return None
        except Exception as e:
            logger.error(f"获取兑换码失败 {code}: {e}")
            return None

    async def validate_code(self, code: str) -> tuple[bool, str]:
        """验证兑换码是否有效"""
        code_info = await self.get_code(code)

        if not code_info:
            return False, "兑换码不存在"

        if not code_info['is_active']:
            return False, "兑换码已被禁用"

        if code_info['used_count'] >= code_info['max_uses']:
            return False, "兑换码已达到最大使用次数"

        if code_info['expires_at']:
            expires_at = datetime.fromisoformat(code_info['expires_at'])
            if datetime.now(timezone.utc) > expires_at:
                return False, "兑换码已过期"

        return True, "有效"

    async def use_code(self, code: str, user_id: str) -> tuple[bool, str, int]:
        """使用兑换码"""
        # 验证兑换码
        is_valid, message = await self.validate_code(code)
        if not is_valid:
            return False, message, 0

        try:
            # 检查用户是否已经兑换过该兑换码
            async with self.db_manager.get_connection() as db:
                async with db.execute("""
                    SELECT COUNT(*) FROM redemption_history
                    WHERE user_id = ? AND code = ?
                """, (user_id, code)) as cursor:
                    row = await cursor.fetchone()
                    if row and row[0] > 0:
                        logger.warning(f"用户 {user_id} 尝试重复兑换 {code}")
                        return False, "您不能重复兑换该兑换码", 0

            code_info = await self.get_code(code)
            pages = code_info['pages']

            async with self.db_manager.get_connection() as db:
                # 增加使用次数
                await db.execute("""
                    UPDATE redemption_codes SET used_count = used_count + 1
                    WHERE code = ?
                """, (code,))

                # 记录兑换历史
                await db.execute("""
                    INSERT INTO redemption_history (
                        user_id, code, pages_granted, redeemed_at
                    ) VALUES (?, ?, ?, ?)
                """, (
                    user_id,
                    code,
                    pages,
                    datetime.now(timezone.utc).isoformat()
                ))

                await db.commit()
                logger.info(f"用户 {user_id} 兑换码 {code} 成功,获得 {pages} 页")
                return True, "兑换成功", pages
        except Exception as e:
            logger.error(f"使用兑换码失败 {code}: {e}")
            return False, f"兑换失败: {str(e)}", 0

    async def deactivate_code(self, code: str) -> bool:
        """禁用兑换码"""
        try:
            async with self.db_manager.get_connection() as db:
                await db.execute("""
                    UPDATE redemption_codes SET is_active = 0 WHERE code = ?
                """, (code,))
                await db.commit()
                logger.info(f"兑换码 {code} 已禁用")
                return True
        except Exception as e:
            logger.error(f"禁用兑换码失败 {code}: {e}")
            return False

    async def activate_code(self, code: str) -> bool:
        """激活兑换码"""
        try:
            async with self.db_manager.get_connection() as db:
                await db.execute("""
                    UPDATE redemption_codes SET is_active = 1 WHERE code = ?
                """, (code,))
                await db.commit()
                logger.info(f"兑换码 {code} 已激活")
                return True
        except Exception as e:
            logger.error(f"激活兑换码失败 {code}: {e}")
            return False

    async def get_all_codes(self, created_by: Optional[str] = None, limit: int = 100, offset: int = 0) -> List[Dict[str, Any]]:
        """获取所有兑换码列表"""
        try:
            async with self.db_manager.get_connection() as db:
                db.row_factory = aiosqlite.Row

                if created_by:
                    query = """
                        SELECT * FROM redemption_codes
                        WHERE created_by = ?
                        ORDER BY created_at DESC
                        LIMIT ? OFFSET ?
                    """
                    params = (created_by, limit, offset)
                else:
                    query = """
                        SELECT * FROM redemption_codes
                        ORDER BY created_at DESC
                        LIMIT ? OFFSET ?
                    """
                    params = (limit, offset)

                async with db.execute(query, params) as cursor:
                    rows = await cursor.fetchall()
                    codes = []
                    for row in rows:
                        code_info = dict(row)
                        code_info['is_active'] = bool(code_info['is_active'])
                        code_info['remaining_uses'] = code_info['max_uses'] - code_info['used_count']
                        codes.append(code_info)
                    return codes
        except Exception as e:
            logger.error(f"获取兑换码列表失败: {e}")
            return []

    async def count_codes(self) -> int:
        """统计兑换码总数"""
        try:
            async with self.db_manager.get_connection() as db:
                async with db.execute("SELECT COUNT(*) FROM redemption_codes") as cursor:
                    row = await cursor.fetchone()
                    return row[0] if row else 0
        except Exception as e:
            logger.error(f"统计兑换码数失败: {e}")
            return 0

    async def get_redemption_history(self, user_id: Optional[str] = None, code: Optional[str] = None, limit: int = 100) -> List[Dict[str, Any]]:
        """获取兑换历史"""
        try:
            async with self.db_manager.get_connection() as db:
                db.row_factory = aiosqlite.Row

                if user_id:
                    query = """
                        SELECT * FROM redemption_history
                        WHERE user_id = ?
                        ORDER BY redeemed_at DESC LIMIT ?
                    """
                    params = (user_id, limit)
                elif code:
                    query = """
                        SELECT * FROM redemption_history
                        WHERE code = ?
                        ORDER BY redeemed_at DESC LIMIT ?
                    """
                    params = (code, limit)
                else:
                    query = """
                        SELECT * FROM redemption_history
                        ORDER BY redeemed_at DESC LIMIT ?
                    """
                    params = (limit,)

                async with db.execute(query, params) as cursor:
                    rows = await cursor.fetchall()
                    return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"获取兑换历史失败: {e}")
            return []

    async def get_redemption_history_with_users(self, code: Optional[str] = None, limit: int = 100) -> List[Dict[str, Any]]:
        """获取兑换历史（包含用户信息）"""
        try:
            async with self.db_manager.get_connection() as db:
                db.row_factory = aiosqlite.Row

                if code:
                    query = """
                        SELECT
                            rh.id, rh.user_id, rh.code, rh.pages_granted, rh.redeemed_at,
                            u.username, u.id as user_uuid
                        FROM redemption_history rh
                        LEFT JOIN users u ON rh.user_id = u.id
                        WHERE rh.code = ?
                        ORDER BY rh.redeemed_at DESC LIMIT ?
                    """
                    params = (code, limit)
                else:
                    query = """
                        SELECT
                            rh.id, rh.user_id, rh.code, rh.pages_granted, rh.redeemed_at,
                            u.username, u.id as user_uuid
                        FROM redemption_history rh
                        LEFT JOIN users u ON rh.user_id = u.id
                        ORDER BY rh.redeemed_at DESC LIMIT ?
                    """
                    params = (limit,)

                async with db.execute(query, params) as cursor:
                    rows = await cursor.fetchall()
                    return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"获取兑换历史（含用户信息）失败: {e}")
            return []


class RegistrationTokenModel:
    """注册令牌模型,提供注册令牌相关的数据库操作"""

    def __init__(self, db_manager):
        self.db_manager = db_manager

    def generate_token(self) -> str:
        """生成唯一注册令牌"""
        return secrets.token_urlsafe(32)

    async def create_token(self,
                          created_by: str,
                          max_uses: int = 1,
                          expires_days: Optional[int] = None,
                          description: Optional[str] = None) -> Optional[str]:
        """创建注册令牌"""
        try:
            token = self.generate_token()
            expires_at = None
            if expires_days:
                expires_at = (datetime.now(timezone.utc) + timedelta(days=expires_days)).isoformat()

            async with self.db_manager.get_connection() as db:
                await db.execute("""
                    INSERT INTO registration_tokens (
                        token, max_uses, used_count, created_by,
                        created_at, expires_at, is_active, description
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    token,
                    max_uses,
                    0,
                    created_by,
                    datetime.now(timezone.utc).isoformat(),
                    expires_at,
                    1,
                    description
                ))
                await db.commit()
                logger.info(f"注册令牌创建成功 (最大使用次数: {max_uses})")
                return token
        except Exception as e:
            logger.error(f"创建注册令牌失败: {e}")
            return None

    async def get_token(self, token: str) -> Optional[Dict[str, Any]]:
        """获取注册令牌信息"""
        try:
            async with self.db_manager.get_connection() as db:
                db.row_factory = aiosqlite.Row
                async with db.execute("""
                    SELECT * FROM registration_tokens WHERE token = ?
                """, (token,)) as cursor:
                    row = await cursor.fetchone()
                    if row:
                        token_info = dict(row)
                        token_info['is_active'] = bool(token_info['is_active'])
                        return token_info
                    return None
        except Exception as e:
            logger.error(f"获取注册令牌失败: {e}")
            return None

    async def validate_token(self, token: str) -> tuple[bool, str]:
        """验证注册令牌是否有效"""
        token_info = await self.get_token(token)

        if not token_info:
            return False, "注册令牌不存在"

        if not token_info['is_active']:
            return False, "注册令牌已被禁用"

        if token_info['used_count'] >= token_info['max_uses']:
            # 自动关闭达到上限的令牌
            await self.deactivate_token(token)
            return False, "注册令牌已达到最大使用次数"

        if token_info['expires_at']:
            expires_at = datetime.fromisoformat(token_info['expires_at'])
            if datetime.now(timezone.utc) > expires_at:
                return False, "注册令牌已过期"

        return True, "有效"

    async def use_token(self, token: str, user_id: str, username: str, user_uuid: str) -> tuple[bool, str]:
        """使用注册令牌并记录使用者信息"""
        # 验证令牌
        is_valid, message = await self.validate_token(token)
        if not is_valid:
            return False, message

        try:
            from datetime import datetime, timezone
            async with self.db_manager.get_connection() as db:
                # 增加使用次数
                await db.execute("""
                    UPDATE registration_tokens SET used_count = used_count + 1
                    WHERE token = ?
                """, (token,))

                # 记录使用历史
                await db.execute("""
                    INSERT INTO registration_token_history (
                        user_id, token, username, user_uuid, registered_at
                    ) VALUES (?, ?, ?, ?, ?)
                """, (user_id, token, username, user_uuid, datetime.now(timezone.utc).isoformat()))

                # 检查是否达到最大使用次数,如果达到则自动关闭
                async with db.execute("""
                    SELECT used_count, max_uses FROM registration_tokens WHERE token = ?
                """, (token,)) as cursor:
                    row = await cursor.fetchone()
                    if row:
                        used_count, max_uses = row
                        if used_count >= max_uses:
                            await db.execute("""
                                UPDATE registration_tokens SET is_active = 0 WHERE token = ?
                            """, (token,))

                await db.commit()
                logger.info(f"注册令牌使用成功: user={username}, token={token}")
                return True, "使用成功"
        except Exception as e:
            logger.error(f"使用注册令牌失败: {e}")
            return False, f"使用失败: {str(e)}"

    async def deactivate_token(self, token: str) -> bool:
        """禁用注册令牌"""
        try:
            async with self.db_manager.get_connection() as db:
                await db.execute("""
                    UPDATE registration_tokens SET is_active = 0 WHERE token = ?
                """, (token,))
                await db.commit()
                logger.info(f"注册令牌已禁用")
                return True
        except Exception as e:
            logger.error(f"禁用注册令牌失败: {e}")
            return False

    async def activate_token(self, token: str) -> bool:
        """激活注册令牌"""
        try:
            async with self.db_manager.get_connection() as db:
                await db.execute("""
                    UPDATE registration_tokens SET is_active = 1 WHERE token = ?
                """, (token,))
                await db.commit()
                logger.info(f"注册令牌已激活")
                return True
        except Exception as e:
            logger.error(f"激活注册令牌失败: {e}")
            return False

    async def get_all_tokens(self, created_by: Optional[str] = None, limit: int = 100, offset: int = 0) -> List[Dict[str, Any]]:
        """获取所有注册令牌列表"""
        try:
            async with self.db_manager.get_connection() as db:
                db.row_factory = aiosqlite.Row

                if created_by:
                    query = """
                        SELECT * FROM registration_tokens
                        WHERE created_by = ?
                        ORDER BY created_at DESC
                        LIMIT ? OFFSET ?
                    """
                    params = (created_by, limit, offset)
                else:
                    query = """
                        SELECT * FROM registration_tokens
                        ORDER BY created_at DESC
                        LIMIT ? OFFSET ?
                    """
                    params = (limit, offset)

                async with db.execute(query, params) as cursor:
                    rows = await cursor.fetchall()
                    tokens = []
                    for row in rows:
                        token_info = dict(row)
                        token_info['is_active'] = bool(token_info['is_active'])
                        token_info['remaining_uses'] = token_info['max_uses'] - token_info['used_count']
                        tokens.append(token_info)
                    return tokens
        except Exception as e:
            logger.error(f"获取注册令牌列表失败: {e}")
            return []

    async def count_tokens(self) -> int:
        """统计注册令牌总数"""
        try:
            async with self.db_manager.get_connection() as db:
                async with db.execute("SELECT COUNT(*) FROM registration_tokens") as cursor:
                    row = await cursor.fetchone()
                    return row[0] if row else 0
        except Exception as e:
            logger.error(f"统计注册令牌数失败: {e}")
            return 0

    async def delete_token(self, token: str) -> bool:
        """删除注册令牌"""
        try:
            async with self.db_manager.get_connection() as db:
                await db.execute("""
                    DELETE FROM registration_tokens WHERE token = ?
                """, (token,))
                await db.commit()
                logger.info(f"注册令牌已删除")
                return True
        except Exception as e:
            logger.error(f"删除注册令牌失败: {e}")
            return False

    async def get_token_history(self) -> list:
        """获取所有令牌使用历史记录"""
        try:
            async with self.db_manager.get_connection() as db:
                db.row_factory = aiosqlite.Row
                async with db.execute("""
                    SELECT * FROM registration_token_history
                    ORDER BY registered_at DESC
                """) as cursor:
                    rows = await cursor.fetchall()
                    return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"获取令牌使用历史失败: {e}")
            return []
