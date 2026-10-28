import asyncio
import logging
import os
import tempfile
from typing import Any, Callable, Optional

from fastapi import APIRouter, HTTPException, Query, Request, Response, UploadFile
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse


def _cleanup_temp_file(path: Optional[str]) -> None:
    if path and os.path.exists(path):
        try:
            os.unlink(path)
        except OSError:
            pass


def _raise_internal_error(logger: logging.Logger, code: str, exc: Exception, **context: Any) -> None:
    context_str = " ".join(f"{key}={value}" for key, value in context.items() if value is not None)
    logger.error("task_basic_route_failed code=%s %s error=%s", code, context_str, exc, exc_info=True)
    raise HTTPException(status_code=500, detail="internal server error", headers={"X-Error-Code": code})


async def _write_upload_to_temp_file(
    upload_file: UploadFile,
    *,
    suffix: str,
    max_file_size_mb: int,
    error_message: str,
) -> tuple[str, float]:
    tmp_file_path: Optional[str] = None
    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp_file:
            tmp_file_path = tmp_file.name
            chunk_size = 1024 * 1024
            total_size = 0
            max_file_size = max_file_size_mb * 1024 * 1024

            while chunk := await upload_file.read(chunk_size):
                total_size += len(chunk)
                if total_size > max_file_size:
                    raise HTTPException(status_code=400, detail=error_message)
                tmp_file.write(chunk)

        if total_size <= 0:
            raise HTTPException(status_code=400, detail="empty file")

        return tmp_file_path, total_size / (1024 * 1024)
    except Exception:
        _cleanup_temp_file(tmp_file_path)
        raise


def register_task_basic_routes(
    app,
    *,
    password: str,
    runtime_config: Any,
    websocket_manager: Any,
    logger: logging.Logger,
    get_task_manager: Callable[[], Any],
    get_auth_manager: Callable[[], Any],
    get_client_ip: Callable[[Request], str],
    check_pdf_info: Callable[[Any], tuple[int, float]],
) -> None:
    """Register synchronous/basic task APIs."""

    router = APIRouter()

    def _require_task_manager():
        manager = get_task_manager()
        if not manager:
            raise HTTPException(status_code=503, detail="task manager not initialized")
        return manager

    def _require_auth_manager():
        auth_manager = get_auth_manager()
        if not auth_manager:
            raise HTTPException(status_code=503, detail="auth manager not initialized")
        return auth_manager

    async def _require_task_access(request: Request, task_id: str):
        """Require a logged-in owner (or admin) for task data endpoints."""
        auth_manager = _require_auth_manager()
        task_manager = _require_task_manager()
        user = await auth_manager.require_user(request)
        task_info = await task_manager.get_task_info(task_id)
        if not task_info:
            raise HTTPException(status_code=404, detail="task not found")
        if not user.get("is_admin") and task_info.get("user_id") != user.get("id"):
            raise HTTPException(status_code=404, detail="task not found")
        return user, task_info

    @router.post(f"/{password}/process/image" if password else "/process/image")
    async def process_image_endpoint(file: UploadFile, request: Request, response: Response):
        _ = response
        task_manager = _require_task_manager()
        auth_manager = _require_auth_manager()

        if not file:
            raise HTTPException(status_code=400, detail="file missing")

        user = await auth_manager.get_current_user(request)
        if not user:
            raise HTTPException(status_code=401, detail="unauthorized")

        if not file.content_type or not file.content_type.startswith("image/"):
            raise HTTPException(status_code=400, detail="不支持的文件类型，请上传图片文件")

        has_quota, quota_message = await auth_manager.check_quota(user["id"], 1)
        if not has_quota:
            raise HTTPException(status_code=403, detail=f"quota exceeded: {quota_message}")

        tmp_file_path = None
        quota_reserved = False
        task_submitted = False
        try:
            tmp_file_path, file_size_mb = await _write_upload_to_temp_file(
                file,
                suffix=".img",
                max_file_size_mb=runtime_config.max_image_size_mb,
                error_message=f"Image file too large (max {runtime_config.max_image_size_mb}MB)",
            )
            client_ip = get_client_ip(request)

            if not await auth_manager.deduct_quota(user["id"], 1):
                raise HTTPException(status_code=403, detail="quota exceeded")
            quota_reserved = True

            task_id = await task_manager.submit_image_task_from_file(
                file_path=tmp_file_path,
                file_name=file.filename or "image",
                metadata={
                    "source": "sync_endpoint",
                    "sync_mode": True,
                    "file_size_mb": file_size_mb,
                    "user_id": user["id"],
                    "client_ip": client_ip,
                },
            )
            task_submitted = True
            tmp_file_path = None

            return JSONResponse(
                {
                    "status": "accepted",
                    "task_id": task_id,
                    "message": "task submitted",
                    "estimated_time": 30,
                    "websocket_url": f"/ws?client_id=sync_{task_id}",
                    "status_url": f"/api/tasks/{task_id}",
                    "result_url": f"/api/tasks/{task_id}/result",
                }
            )
        except HTTPException:
            _cleanup_temp_file(tmp_file_path)
            if quota_reserved and not task_submitted:
                await auth_manager.user_model.refund_pages(user["id"], 1)
            raise
        except Exception as exc:
            _cleanup_temp_file(tmp_file_path)
            if quota_reserved and not task_submitted:
                await auth_manager.user_model.refund_pages(user["id"], 1)
            _raise_internal_error(logger, "TASK_BASIC_SYNC_IMAGE_FAILED", exc, filename=file.filename)

    @router.post(f"/{password}/process/pdf" if password else "/process/pdf")
    async def process_pdf_endpoint(file: UploadFile, request: Request, response: Response):
        _ = response
        task_manager = _require_task_manager()
        auth_manager = _require_auth_manager()

        if not file:
            raise HTTPException(status_code=400, detail="file missing")

        user = await auth_manager.get_current_user(request)
        if not user:
            raise HTTPException(status_code=401, detail="unauthorized")

        tmp_file_path = None
        quota_reserved = False
        task_submitted = False
        page_count = 0
        try:
            tmp_file_path, _ = await _write_upload_to_temp_file(
                file,
                suffix=".pdf",
                max_file_size_mb=runtime_config.max_pdf_size_mb,
                error_message=f"PDF file too large (max {runtime_config.max_pdf_size_mb}MB)",
            )

            try:
                page_count, file_size_mb = await asyncio.to_thread(check_pdf_info, tmp_file_path)
            except Exception as exc:
                logger.warning("task_basic_pdf_inspection_failed filename=%s error=%s", file.filename, exc)
                _cleanup_temp_file(tmp_file_path)
                raise HTTPException(status_code=400, detail="invalid pdf")

            if page_count <= 0:
                raise HTTPException(status_code=400, detail="invalid pdf")
            if page_count > runtime_config.max_pdf_pages:
                _cleanup_temp_file(tmp_file_path)
                raise HTTPException(
                    status_code=400,
                    detail=f"PDF has too many pages (max {runtime_config.max_pdf_pages})",
                )

            has_quota, quota_message = await auth_manager.check_quota(user["id"], page_count)
            if not has_quota:
                _cleanup_temp_file(tmp_file_path)
                raise HTTPException(status_code=403, detail=f"quota exceeded: {quota_message}")

            if not await auth_manager.deduct_quota(user["id"], page_count):
                raise HTTPException(status_code=403, detail="quota exceeded")
            quota_reserved = True

            client_ip = get_client_ip(request)
            task_id = await task_manager.submit_pdf_task_from_file(
                file_path=tmp_file_path,
                file_name=file.filename or "document.pdf",
                metadata={
                    "source": "sync_endpoint",
                    "sync_mode": True,
                    "file_size_mb": file_size_mb,
                    "total_pages": page_count,
                    "user_id": user["id"],
                    "client_ip": client_ip,
                },
            )
            task_submitted = True
            tmp_file_path = None

            return JSONResponse(
                {
                    "status": "accepted",
                    "task_id": task_id,
                    "message": "task submitted",
                    "total_pages": page_count,
                    "file_size_mb": f"{file_size_mb:.2f}",
                    "estimated_time": page_count * 3,
                    "websocket_url": f"/ws?client_id=sync_{task_id}",
                    "status_url": f"/api/tasks/{task_id}",
                    "result_url": f"/api/tasks/{task_id}/result",
                }
            )
        except HTTPException:
            _cleanup_temp_file(tmp_file_path)
            if quota_reserved and not task_submitted:
                await auth_manager.user_model.refund_pages(user["id"], page_count)
            raise
        except Exception as exc:
            _cleanup_temp_file(tmp_file_path)
            if quota_reserved and not task_submitted:
                await auth_manager.user_model.refund_pages(user["id"], page_count)
            _raise_internal_error(logger, "TASK_BASIC_SYNC_PDF_FAILED", exc, filename=file.filename)

    @router.post(f"/{password}/api/tasks/image" if password else "/api/tasks/image")
    async def submit_image_task_api(file: UploadFile, request: Request):
        task_manager = _require_task_manager()
        auth_manager = _require_auth_manager()
        if not file:
            raise HTTPException(status_code=400, detail="file missing")
        user = await auth_manager.require_user(request)
        has_quota, quota_message = await auth_manager.check_quota(user["id"], 1)
        if not has_quota:
            raise HTTPException(status_code=403, detail=f"quota exceeded: {quota_message}")

        tmp_file_path = None
        quota_reserved = False
        task_submitted = False
        try:
            tmp_file_path, file_size_mb = await _write_upload_to_temp_file(
                file,
                suffix=".img",
                max_file_size_mb=runtime_config.max_image_size_mb,
                error_message=f"Image file too large (max {runtime_config.max_image_size_mb}MB)",
            )
            if not await auth_manager.deduct_quota(user["id"], 1):
                raise HTTPException(status_code=403, detail="quota exceeded")
            quota_reserved = True
            task_id = await task_manager.submit_image_task_from_file(
                file_path=tmp_file_path,
                file_name=file.filename or "image",
                metadata={
                    "source": "api",
                    "user_id": user["id"],
                    "client_ip": get_client_ip(request),
                    "file_size_mb": file_size_mb,
                },
            )
            task_submitted = True
            tmp_file_path = None
            return JSONResponse({"status": "success", "task_id": task_id, "message": "task submitted"})
        except HTTPException:
            _cleanup_temp_file(tmp_file_path)
            if quota_reserved and not task_submitted:
                await auth_manager.user_model.refund_pages(user["id"], 1)
            raise
        except Exception as exc:
            _cleanup_temp_file(tmp_file_path)
            if quota_reserved and not task_submitted:
                await auth_manager.user_model.refund_pages(user["id"], 1)
            _raise_internal_error(logger, "TASK_BASIC_IMAGE_SUBMIT_FAILED", exc, filename=file.filename)

    @router.post(f"/{password}/api/tasks/pdf" if password else "/api/tasks/pdf")
    async def submit_pdf_task_api(file: UploadFile, request: Request):
        task_manager = _require_task_manager()
        auth_manager = _require_auth_manager()
        if not file:
            raise HTTPException(status_code=400, detail="file missing")

        tmp_file_path = None
        quota_reserved = False
        task_submitted = False
        page_count = 0
        try:
            tmp_file_path, file_size_mb = await _write_upload_to_temp_file(
                file,
                suffix=".pdf",
                max_file_size_mb=runtime_config.max_pdf_size_mb,
                error_message=f"PDF file too large (max {runtime_config.max_pdf_size_mb}MB)",
            )

            page_count, _ = await asyncio.to_thread(check_pdf_info, tmp_file_path)
            if page_count <= 0:
                raise HTTPException(status_code=400, detail="invalid pdf")
            if page_count > runtime_config.max_pdf_pages:
                _cleanup_temp_file(tmp_file_path)
                raise HTTPException(status_code=400, detail=f"PDF has too many pages (max {runtime_config.max_pdf_pages})")

            user = await auth_manager.require_user(request)
            has_quota, quota_message = await auth_manager.check_quota(user["id"], page_count)
            if not has_quota:
                raise HTTPException(status_code=403, detail=f"quota exceeded: {quota_message}")

            if not await auth_manager.deduct_quota(user["id"], page_count):
                raise HTTPException(status_code=403, detail="quota exceeded")
            quota_reserved = True

            task_id = await task_manager.submit_pdf_task_from_file(
                file_path=tmp_file_path,
                file_name=file.filename or "document.pdf",
                metadata={
                    "source": "api",
                    "user_id": user["id"],
                    "client_ip": get_client_ip(request),
                    "file_size_mb": file_size_mb,
                    "total_pages": page_count,
                },
            )
            task_submitted = True
            tmp_file_path = None
            return JSONResponse({"status": "success", "task_id": task_id, "message": "task submitted"})
        except HTTPException:
            _cleanup_temp_file(tmp_file_path)
            if quota_reserved and not task_submitted:
                await auth_manager.user_model.refund_pages(user["id"], page_count)
            raise
        except Exception as exc:
            _cleanup_temp_file(tmp_file_path)
            if quota_reserved and not task_submitted:
                await auth_manager.user_model.refund_pages(user["id"], page_count)
            _raise_internal_error(logger, "TASK_BASIC_PDF_SUBMIT_FAILED", exc, filename=file.filename)

    @router.get(f"/{password}/api/tasks/{{task_id}}" if password else "/api/tasks/{task_id}")
    async def get_task_info_api(task_id: str, request: Request):
        try:
            _, task_info = await _require_task_access(request, task_id)
            return JSONResponse({"status": "success", "data": task_info})
        except HTTPException:
            raise
        except Exception as exc:
            _raise_internal_error(logger, "TASK_BASIC_INFO_FAILED", exc, task_id=task_id)

    @router.get(f"/{password}/api/tasks/{{task_id}}/result" if password else "/api/tasks/{task_id}/result")
    async def get_task_result_api(task_id: str, request: Request):
        try:
            _, _ = await _require_task_access(request, task_id)
            task_manager = _require_task_manager()
            task_result = await task_manager.get_task_result(task_id)
            if not task_result:
                raise HTTPException(status_code=404, detail="task not found")

            return JSONResponse({"status": "success", "data": task_result})
        except HTTPException:
            raise
        except Exception as exc:
            _raise_internal_error(logger, "TASK_BASIC_RESULT_FAILED", exc, task_id=task_id)

    @router.get(f"/{password}/api/tasks" if password else "/api/tasks")
    async def get_tasks_list_api(
        request: Request,
        status: Optional[str] = Query(None, description="task status filter"),
        limit: int = Query(100, ge=1, le=1000, description="result limit"),
        offset: int = Query(0, ge=0, description="result offset"),
    ):
        _require_task_manager()
        auth_manager = _require_auth_manager()
        from models.database import task_model

        try:
            user = await auth_manager.require_user(request)
            tasks = await task_model.get_recent_tasks(
                limit=limit, offset=offset, user_id=user["id"], status=status
            )
            return JSONResponse({"status": "success", "data": tasks})
        except HTTPException:
            raise
        except Exception as exc:
            _raise_internal_error(logger, "TASK_BASIC_LIST_FAILED", exc, limit=limit, offset=offset)

    @router.get(f"/{password}/api/system/stats" if password else "/api/system/stats")
    async def get_system_stats_api(request: Request):
        task_manager = _require_task_manager()
        auth_manager = _require_auth_manager()
        try:
            await auth_manager.require_admin(request)
            stats = await task_manager.get_system_stats()
            websocket_stats = websocket_manager.get_connection_stats() if websocket_manager else {}
            return JSONResponse(content=jsonable_encoder({
                "status": "success",
                "data": {"task_system": stats, "websocket": websocket_stats},
            }))
        except HTTPException:
            raise
        except Exception as exc:
            _raise_internal_error(logger, "TASK_BASIC_SYSTEM_STATS_FAILED", exc)

    app.include_router(router)
