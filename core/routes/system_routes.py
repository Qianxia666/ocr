from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Callable

from core import aiosqlite_compat as aiosqlite
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse

from models.database import db_manager

logger = logging.getLogger(__name__)


def register_system_routes(
    app,
    password: str,
    get_task_manager: Callable[[], Any],
    get_auth_manager: Callable[[], Any],
) -> None:
    """Register system health admin routes."""

    protected_prefix = f"/{password}" if password else ""
    public_router = APIRouter()
    protected_router = APIRouter(prefix=protected_prefix)

    def require_task_manager():
        manager = get_task_manager()
        if not manager:
            raise HTTPException(status_code=503, detail="任务管理器未初始化")
        return manager

    async def require_admin(request: Request):
        auth_manager = get_auth_manager()
        if not auth_manager:
            raise HTTPException(status_code=503, detail="认证管理器未初始化")
        return await auth_manager.require_admin(request)

    @public_router.get("/healthz")
    @public_router.get("/health")
    async def health_check():
        """Container health check endpoint without authentication."""
        try:
            require_task_manager()
            try:
                async with aiosqlite.connect(db_manager.db_path) as db:
                    await db.execute("SELECT 1")
            except Exception as db_error:
                return JSONResponse(
                    status_code=503,
                    content={"status": "unhealthy", "reason": f"数据库连接失败: {db_error}"},
                )

            return JSONResponse(
                status_code=200,
                content={"status": "healthy", "timestamp": datetime.now().isoformat()},
            )
        except HTTPException as exc:
            return JSONResponse(
                status_code=exc.status_code,
                content={"status": "unhealthy", "reason": exc.detail},
            )
        except Exception as exc:
            logger.error("health_check_unexpected_error: %s", exc)
            return JSONResponse(
                status_code=503,
                content={"status": "unhealthy", "reason": str(exc)},
            )

    @protected_router.get("/api/system/health")
    async def get_system_health_api(request: Request):
        """Return current system health snapshot."""
        await require_admin(request)
        task_manager = require_task_manager()

        try:
            stats = await task_manager.get_system_stats()
            system_health = stats.get("system_health", {})

            issues = []
            metrics = system_health.get("system_monitor", {})
            if metrics.get("cpu_percent", 0) > 90:
                issues.append("CPU使用率过高")
            if metrics.get("memory_percent", 0) > 90:
                issues.append("内存使用率过高")
            if metrics.get("disk_percent", 0) > 90:
                issues.append("磁盘使用率过高")

            timeout_stats = system_health.get("timeout_manager", {})
            if timeout_stats.get("active_timeouts", 0) > 10:
                issues.append("活跃超时过多")

            error_stats = system_health.get("error_handler", {})
            if error_stats.get("error_rate", 0) > 0.1:
                issues.append("系统错误率过高")

            overall_status = "healthy"
            if issues:
                overall_status = "warning" if len(issues) <= 2 else "critical"

            return JSONResponse(
                {
                    "status": "success",
                    "data": {
                        "overall_status": overall_status,
                        "timestamp": datetime.now().isoformat(),
                        "issues": issues,
                        "system_health": system_health,
                    },
                }
            )
        except Exception as exc:
            logger.error("get_system_health_failed: %s", exc, exc_info=True)
            raise HTTPException(status_code=500, detail="internal server error")

    @protected_router.get("/api/system/metrics")
    async def get_system_metrics_api(request: Request):
        """Return system metrics for dashboard consumption."""
        await require_admin(request)
        task_manager = require_task_manager()

        try:
            stats = await task_manager.get_system_stats()
            system_health = stats.get("system_health", {})

            metrics = {
                "timestamp": datetime.now().isoformat(),
                "system_resources": system_health.get("system_monitor", {}),
                "timeout_stats": system_health.get("timeout_manager", {}),
                "error_stats": system_health.get("error_handler", {}),
                "recovery_stats": system_health.get("recovery_manager", {}),
                "api_performance": system_health.get("system_monitor", {}).get("api_stats", {}),
            }

            return JSONResponse({"status": "success", "data": metrics})
        except Exception as exc:
            logger.error("get_system_metrics_failed: %s", exc, exc_info=True)
            raise HTTPException(status_code=500, detail="internal server error")

    @protected_router.get("/api/system/recovery")
    async def get_recovery_stats_api(request: Request):
        """Return error/recovery statistics."""
        await require_admin(request)
        task_manager = require_task_manager()

        try:
            stats = await task_manager.get_system_stats()
            system_health = stats.get("system_health", {})

            recovery_stats = system_health.get("recovery_manager", {})
            error_stats = system_health.get("error_handler", {})

            return JSONResponse(
                {
                    "status": "success",
                    "data": {
                        "recovery_manager": recovery_stats,
                        "error_handler": error_stats,
                        "summary": {
                            "total_checkpoints": recovery_stats.get("total_checkpoints", 0),
                            "successful_recoveries": recovery_stats.get("successful_recoveries", 0),
                            "failed_recoveries": recovery_stats.get("failed_recoveries", 0),
                            "total_errors_handled": error_stats.get("total_errors_handled", 0),
                            "recovery_success_rate": recovery_stats.get("recovery_success_rate", 0),
                            "error_rate": error_stats.get("error_rate", 0),
                        },
                    },
                }
            )
        except Exception as exc:
            logger.error("get_recovery_stats_failed: %s", exc, exc_info=True)
            raise HTTPException(status_code=500, detail="internal server error")

    app.include_router(public_router)
    app.include_router(protected_router)
