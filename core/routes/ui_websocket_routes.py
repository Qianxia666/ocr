from __future__ import annotations

import logging
from typing import Any, Callable, Optional

from fastapi import APIRouter, HTTPException, Request, Response, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, RedirectResponse


def register_ui_websocket_routes(
    app,
    *,
    password: str,
    templates,
    favicon_url: str,
    title: str,
    back_url: str,
    runtime_config: Any,
    api_runtime_config: Any,
    default_model: str,
    get_auth_manager: Callable[[], Any],
    websocket_manager: Any,
    logger: logging.Logger,
) -> None:
    """Register UI pages and websocket routes."""

    router = APIRouter()

    def _get_auth_manager() -> Any:
        auth_manager = get_auth_manager()
        if auth_manager is None:
            raise HTTPException(status_code=503, detail="认证管理器未初始化")
        return auth_manager

    async def _get_current_user(request: Request) -> Optional[dict]:
        auth_manager = _get_auth_manager()
        return await auth_manager.get_current_user(request)

    def _admin_forbidden_response() -> HTMLResponse:
        return HTMLResponse(
            content="<h1>403 - 需要管理员权限</h1><p>只有管理员可以访问此页面。<br><a href='/'>返回主页</a></p>",
            status_code=403,
        )

    async def _render_admin_page(
        request: Request,
        template_name: str,
        page_title: str,
        extra_context: Optional[dict] = None,
    ):
        try:
            user = await _get_current_user(request)
            if not user:
                return RedirectResponse(url="/login", status_code=302)
            if not user.get("is_admin"):
                return _admin_forbidden_response()

            context = {
                "request": request,
                "favicon_url": favicon_url,
                "title": f"{title} - {page_title}",
            }
            if extra_context:
                context.update(extra_context)

            return templates.TemplateResponse(request, template_name, context)
        except HTTPException as exc:
            if exc.status_code == 403:
                return HTMLResponse(
                    content="<h1>403 - 需要管理员权限</h1><p>只有管理员可以访问此页面</p>",
                    status_code=403,
                )
            if exc.status_code == 401:
                return RedirectResponse(url="/login", status_code=302)
            raise

    @router.get("/", response_class=HTMLResponse)
    async def root_page(request: Request, response: Response):
        """根路径 - 检查登录状态"""
        user = await _get_current_user(request)
        if not user:
            from models.database import db_manager

            registration_enabled = True
            try:
                async with db_manager.get_connection() as db:
                    async with db.execute(
                        "SELECT value FROM system_settings WHERE key = 'registration_enabled'"
                    ) as cursor:
                        row = await cursor.fetchone()
                        registration_enabled = row[0] == "true" if row else True
            except Exception as exc:
                logger.error("获取注册状态失败: %s", exc)
                registration_enabled = False

            current_model = api_runtime_config.get_config("model") or default_model

            return templates.TemplateResponse(
                request,
                "landing.html",
                {
                    "request": request,
                    "favicon_url": favicon_url,
                    "title": title,
                    "registration_enabled": registration_enabled,
                    "model": current_model,
                    "max_image_size_mb": runtime_config.max_image_size_mb,
                    "max_pdf_size_mb": runtime_config.max_pdf_size_mb,
                    "max_pdf_pages": runtime_config.max_pdf_pages,
                    "password_prefix": f"/{password}" if password else "",
                },
            )

        if password:
            return RedirectResponse(url=f"/{password}", status_code=302)
        return templates.TemplateResponse(
            request,
            "web.html",
            {
                "request": request,
                "favicon_url": favicon_url,
                "title": title,
                "backurl": back_url,
            },
        )

    @router.get(f"/{password}" if password else "/main", response_class=HTMLResponse)
    async def access_with_password(request: Request, response: Response):
        """受密码保护的主页"""
        user = await _get_current_user(request)
        if not user:
            return RedirectResponse(url="/login", status_code=302)
        return templates.TemplateResponse(
            request,
            "web.html",
            {
                "request": request,
                "favicon_url": favicon_url,
                "title": title,
                "backurl": back_url,
            },
        )

    @router.get("/admin", response_class=HTMLResponse)
    @router.get(f"/{password}/admin" if password else "/admin-disabled", response_class=HTMLResponse)
    async def admin_page(request: Request, response: Response):
        return await _render_admin_page(
            request,
            "admin.html",
            "管理后台",
            {"backurl": back_url},
        )

    @router.get("/admin/codes", response_class=HTMLResponse)
    @router.get(
        f"/{password}/admin/codes" if password else "/admin/codes-disabled",
        response_class=HTMLResponse,
    )
    async def admin_codes_page(request: Request, response: Response):
        return await _render_admin_page(
            request,
            "admin_codes.html",
            "兑换码管理",
            {"password": password},
        )

    @router.get("/admin/users", response_class=HTMLResponse)
    @router.get(
        f"/{password}/admin/users" if password else "/admin/users-disabled",
        response_class=HTMLResponse,
    )
    async def admin_users_page(request: Request, response: Response):
        return await _render_admin_page(
            request,
            "admin_users.html",
            "用户管理",
            {"password_prefix": f"/{password}" if password else ""},
        )

    @router.get("/admin/tasks", response_class=HTMLResponse)
    @router.get(
        f"/{password}/admin/tasks" if password else "/admin/tasks-disabled",
        response_class=HTMLResponse,
    )
    async def admin_tasks_page(request: Request, response: Response):
        return await _render_admin_page(
            request,
            "admin_tasks.html",
            "任务管理",
            {"password_prefix": f"/{password}" if password else ""},
        )

    @router.get("/admin/settings", response_class=HTMLResponse)
    @router.get(
        f"/{password}/admin/settings" if password else "/admin/settings-disabled",
        response_class=HTMLResponse,
    )
    async def admin_settings_page(request: Request, response: Response):
        return await _render_admin_page(
            request,
            "admin_settings.html",
            "系统设置",
            {"password_prefix": f"/{password}" if password else ""},
        )

    @router.get("/admin/registration-tokens", response_class=HTMLResponse)
    @router.get(
        f"/{password}/admin/registration-tokens" if password else "/admin/registration-tokens-disabled",
        response_class=HTMLResponse,
    )
    async def admin_registration_tokens_page(request: Request, response: Response):
        return await _render_admin_page(
            request,
            "admin_registration_tokens.html",
            "令牌注册管理",
            {
                "password_prefix": f"/{password}" if password else "",
                "password": password,
            },
        )

    @router.websocket(f"/{password}/ws" if password else "/ws")
    async def websocket_endpoint(websocket: WebSocket, client_id: str = None):
        """WebSocket连接端点"""
        try:
            auth_manager = _get_auth_manager()
            user = await auth_manager.get_current_user(websocket)
            await websocket.accept()
            actual_client_id = await websocket_manager.connect(
                websocket,
                client_id,
                user_id=user.get("id") if user else None,
                is_admin=bool(user and user.get("is_admin")),
            )
            logger.info("WebSocket客户端连接: %s", actual_client_id)

            try:
                while True:
                    data = await websocket.receive_text()
                    await websocket_manager.handle_client_message(actual_client_id, data)
            except WebSocketDisconnect:
                logger.info("WebSocket客户端断开连接: %s", actual_client_id)
            except (ConnectionResetError, ConnectionError) as exc:
                logger.info("WebSocket客户端连接断开 %s: %s", actual_client_id, type(exc).__name__)
            except Exception as exc:
                logger.error("WebSocket连接异常 %s: %s", actual_client_id, exc)

        except Exception as exc:
            logger.error("WebSocket连接失败: %s", exc)
        finally:
            if "actual_client_id" in locals():
                try:
                    await websocket_manager.disconnect(actual_client_id)
                except Exception as exc:
                    logger.debug("清理连接时发生错误 %s: %s", actual_client_id, exc)

    app.include_router(router)
