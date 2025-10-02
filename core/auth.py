import uuid
import secrets
import logging
from typing import Optional, Dict, Any
from functools import wraps
from fastapi import Request, HTTPException, Response
from datetime import datetime, timezone

logger = logging.getLogger(__name__)


class AuthManager:
    """认证管理器,处理用户Session和权限验证"""

    def __init__(self, user_model, db_manager):
        self.user_model = user_model
        self.db_manager = db_manager
        self.session_cookie_name = "ocr_session"

    def generate_session_token(self) -> str:
        """生成安全的Session Token"""
        return secrets.token_urlsafe(32)

    async def login(self, username: str, password: str, response: Response) -> Optional[Dict[str, Any]]:
        """
        用户登录
        - 验证用户名和密码
        - 设置Session Cookie
        - 返回用户信息
        """
        user = await self.user_model.authenticate(username, password)
        if not user:
            return None

        # 生成新的session token
        new_session_token = self.generate_session_token()
        await self.user_model.update_session_token(user['id'], new_session_token)

        # 设置Cookie
        response.set_cookie(
            key=self.session_cookie_name,
            value=new_session_token,
            path='/',
            max_age=30 * 24 * 60 * 60,  # 30天
            httponly=True,
            samesite='lax'
        )

        logger.info(f"用户 {username} 登录成功")
        return user

    async def register(self, username: str, password: str) -> tuple[bool, str, Optional[str]]:
        """
        用户注册
        - 检查用户名是否已存在
        - 首个注册用户自动成为管理员
        - 返回: (成功, 消息, 用户ID)
        """
        # 检查用户名是否已存在
        existing_user = await self.user_model.get_user_by_username(username)
        if existing_user:
            return False, "用户名已存在", None

        # 检查是否是首个用户
        user_count = await self.user_model.count_users()
        is_first_user = (user_count == 0)
        is_admin = is_first_user
        initial_pages = 1000000 if is_admin else 0  # 管理员获得100万页初始配额

        # 创建用户
        user_id = await self.user_model.create_user(
            username=username,
            password=password,
            is_admin=is_admin,
            total_pages=initial_pages
        )

        if not user_id:
            return False, "用户创建失败", None

        if is_first_user:
            logger.info(f"首个用户 {username} 注册成功,已设置为管理员")
            return True, "注册成功！您是第一个用户，已自动获得管理员权限", user_id
        else:
            logger.info(f"新用户 {username} 注册成功")
            return True, "注册成功", user_id

    async def logout(self, response: Response, request: Request = None):
        """用户登出"""
        # 如果提供了 request，先清除数据库中的 session
        if request:
            session_token = request.cookies.get(self.session_cookie_name)
            if session_token:
                # 清除数据库中的 session token
                try:
                    user = await self.user_model.get_user_by_session(session_token)
                    if user:
                        await self.user_model.update_session_token(user['id'], None)
                except Exception as e:
                    logger.error(f"清除数据库 session 失败: {e}")

        # 删除 cookie - 必须匹配设置时的参数
        response.delete_cookie(
            key=self.session_cookie_name,
            path='/',
            samesite='lax'
        )
        logger.info("用户已登出")

    async def get_current_user(self, request: Request) -> Optional[Dict[str, Any]]:
        """获取当前登录用户"""
        session_token = request.cookies.get(self.session_cookie_name)
        if not session_token:
            return None

        user = await self.user_model.get_user_by_session(session_token)
        return user

    async def require_user(self, request: Request) -> Dict[str, Any]:
        """要求用户已登录,否则抛出异常"""
        user = await self.get_current_user(request)
        if not user:
            raise HTTPException(status_code=401, detail="未登录")
        return user

    async def require_admin(self, request: Request) -> Dict[str, Any]:
        """要求管理员权限,否则抛出异常"""
        user = await self.require_user(request)
        if not user.get('is_admin'):
            raise HTTPException(status_code=403, detail="需要管理员权限")
        return user

    async def check_quota(self, user_id: str, required_pages: int) -> tuple[bool, str]:
        """检查用户配额是否充足"""
        quota = await self.user_model.get_quota(user_id)
        if not quota:
            return False, "无法获取用户配额信息"

        if quota['remaining_pages'] < required_pages:
            return False, f"配额不足,需要{required_pages}页,剩余{quota['remaining_pages']}页"

        return True, "配额充足"

    async def deduct_quota(self, user_id: str, pages: int) -> bool:
        """扣除用户配额"""
        success = await self.user_model.deduct_pages(user_id, pages)
        if success:
            logger.info(f"用户 {user_id} 扣除配额 {pages} 页")
        else:
            logger.error(f"用户 {user_id} 配额扣除失败")
        return success


# 全局认证管理器实例(将在main.py中初始化)
auth_manager: Optional[AuthManager] = None


def get_auth_manager() -> AuthManager:
    """获取全局认证管理器实例"""
    if auth_manager is None:
        raise RuntimeError("认证管理器未初始化")
    return auth_manager


async def init_auth_manager(user_model, db_manager):
    """初始化全局认证管理器"""
    global auth_manager
    auth_manager = AuthManager(user_model, db_manager)
    logger.info("认证管理器初始化完成")
    return auth_manager
