from __future__ import annotations

import asyncio
import json
import logging
import os
import tempfile
from datetime import datetime
from typing import Any, Callable, Optional

from fastapi import APIRouter, HTTPException, Query, Request, Response, UploadFile
from fastapi.responses import JSONResponse


def _cleanup_temp_file(path: Optional[str], logger: Optional[logging.Logger] = None, context: str = "") -> None:
    if path and os.path.exists(path):
        try:
            os.unlink(path)
        except OSError as exc:
            if logger:
                logger.warning("temp_file_cleanup_failed path=%s context=%s error=%s", path, context, exc)


def _raise_internal_error(logger: logging.Logger, code: str, exc: Exception, **context: Any) -> None:
    context_str = " ".join(f"{key}={value}" for key, value in context.items() if value is not None)
    logger.exception("task_route_failed code=%s %s", code, context_str)
    raise HTTPException(status_code=500, detail=str(exc), headers={"X-Error-Code": code})


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

        return tmp_file_path, total_size / (1024 * 1024)
    except Exception:
        _cleanup_temp_file(tmp_file_path)
        raise


def _calculate_page_statistics(pages: list[dict[str, Any]]) -> dict[str, Any]:
    stats = {
        "total_pages": len(pages),
        "completed_pages": 0,
        "failed_pages": 0,
        "pending_pages": 0,
        "processing_pages": 0,
        "average_processing_time": 0,
        "total_content_length": 0,
        "success_rate": 0,
    }

    if not pages:
        return stats

    total_processing_time = 0.0
    processing_time_count = 0

    for page in pages:
        status = page.get("status", "pending")

        if status == "completed":
            stats["completed_pages"] += 1
            stats["total_content_length"] += page.get("content_length", 0)
        elif status == "failed":
            stats["failed_pages"] += 1
        elif status == "processing":
            stats["processing_pages"] += 1
        else:
            stats["pending_pages"] += 1

        processing_time = page.get("processing_time")
        if processing_time:
            total_processing_time += processing_time
            processing_time_count += 1

    if processing_time_count > 0:
        stats["average_processing_time"] = total_processing_time / processing_time_count

    if stats["total_pages"] > 0:
        stats["success_rate"] = (stats["completed_pages"] / stats["total_pages"]) * 100

    return stats


def register_task_advanced_routes(
    app,
    *,
    password: str,
    runtime_config: Any,
    logger: logging.Logger,
    get_task_manager: Callable[[], Any],
    get_auth_manager: Callable[[], Any],
    get_client_ip: Callable[[Request], str],
    check_pdf_info: Callable[[Any], tuple[int, float]],
) -> None:
    """Register advanced task APIs including batch, export and admin task management."""

    router = APIRouter()

    def _require_task_manager():
        manager = get_task_manager()
        if not manager:
            raise HTTPException(status_code=503, detail="任务管理器未初始化")
        return manager

    def _require_auth_manager():
        auth_manager = get_auth_manager()
        if not auth_manager:
            raise HTTPException(status_code=503, detail="认证管理器未初始化")
        return auth_manager

    async def _inspect_pdf_source(pdf_source: Any) -> tuple[int, float]:
        return await asyncio.to_thread(check_pdf_info, pdf_source)

    @router.get(f"/{password}/api/admin/tasks/list" if password else "/api/admin/tasks/list")
    async def list_all_tasks_api(
        request: Request,
        page: int = Query(1, ge=1),
        page_size: int = Query(100, ge=1, le=1000),
        user_id: Optional[str] = Query(None),
        search: Optional[str] = Query(None),
    ):
        auth_manager = _require_auth_manager()
        try:
            await auth_manager.require_admin(request)
            from models.database import task_model

            offset = (page - 1) * page_size
            tasks = await task_model.get_all_tasks_with_users(
                limit=page_size,
                offset=offset,
                user_id_filter=user_id,
                search_query=search,
            )
            total = await task_model.count_tasks(user_id_filter=user_id, search_query=search)

            return JSONResponse(
                {
                    "status": "success",
                    "data": tasks,
                    "total": total,
                    "page": page,
                    "page_size": page_size,
                    "total_pages": (total + page_size - 1) // page_size,
                }
            )
        except HTTPException:
            raise
        except Exception as exc:
            _raise_internal_error(logger, "TASK_ADMIN_LIST_FAILED", exc, page=page, page_size=page_size, user_id=user_id)

    @router.get(f"/{password}/api/admin/tasks/stats" if password else "/api/admin/tasks/stats")
    async def get_tasks_stats_api(request: Request, user_id: Optional[str] = Query(None)):
        auth_manager = _require_auth_manager()
        try:
            await auth_manager.require_admin(request)
            from models.database import task_model

            stats = await task_model.get_task_stats_by_user(user_id=user_id)
            return JSONResponse({"status": "success", "data": stats})
        except HTTPException:
            raise
        except Exception as exc:
            _raise_internal_error(logger, "TASK_ADMIN_STATS_FAILED", exc, user_id=user_id)

    @router.post(f"/{password}/api/admin/tasks/{{task_id}}/cancel" if password else "/api/admin/tasks/{task_id}/cancel")
    async def cancel_task_api(request: Request, task_id: str):
        auth_manager = _require_auth_manager()
        task_manager = _require_task_manager()
        try:
            await auth_manager.require_admin(request)
            success, message = await task_manager.cancel_task(task_id, reason="管理员手动停止")
            return JSONResponse({"status": "success" if success else "error", "message": message})
        except HTTPException:
            raise
        except Exception as exc:
            _raise_internal_error(logger, "TASK_ADMIN_CANCEL_FAILED", exc, task_id=task_id)

    @router.get(f"/{password}/api/admin/tasks/{{task_id}}/content" if password else "/api/admin/tasks/{task_id}/content")
    async def get_task_content_api(request: Request, task_id: str):
        auth_manager = _require_auth_manager()
        try:
            await auth_manager.require_admin(request)
            from models.database import page_result_model, task_model

            task = await task_model.get_task(task_id)
            if not task:
                raise HTTPException(status_code=404, detail="任务不存在")

            page_results = await page_result_model.get_task_page_results(task_id)
            content_data = {
                "task_id": task_id,
                "file_name": task.get("file_name"),
                "status": task.get("status"),
                "total_pages": task.get("total_pages", 0),
                "processed_pages": task.get("processed_pages", 0),
                "created_at": task.get("created_at"),
                "pages": [
                    {
                        "page_number": page.get("page_number"),
                        "status": page.get("status"),
                        "content": page.get("content", ""),
                        "error_message": page.get("error_message"),
                    }
                    for page in page_results
                ],
            }

            return JSONResponse({"status": "success", "data": content_data})
        except HTTPException:
            raise
        except Exception as exc:
            _raise_internal_error(logger, "TASK_ADMIN_CONTENT_FAILED", exc, task_id=task_id)

    @router.post(f"/{password}/api/async/image" if password else "/api/async/image")
    async def submit_async_image_task(file: UploadFile, request: Request):
        task_manager = _require_task_manager()
        if not file:
            raise HTTPException(status_code=400, detail="未收到文件")
        if not file.content_type or not file.content_type.startswith("image/"):
            raise HTTPException(status_code=400, detail="不支持的文件类型，请上传图片文件")

        tmp_file_path: Optional[str] = None
        try:
            tmp_file_path, file_size_mb = await _write_upload_to_temp_file(
                file,
                suffix=".img",
                max_file_size_mb=runtime_config.max_image_size_mb,
                error_message=f"文件过大，最大支持{runtime_config.max_image_size_mb}MB",
            )
            client_ip = get_client_ip(request)
            task_id = await task_manager.submit_image_task_from_file(
                file_path=tmp_file_path,
                file_name=file.filename or "image",
                metadata={
                    "source": "async_api",
                    "file_type": file.content_type,
                    "original_filename": file.filename,
                    "file_size_mb": file_size_mb,
                    "client_ip": client_ip,
                },
            )
            tmp_file_path = None

            return JSONResponse(
                {
                    "status": "success",
                    "task_id": task_id,
                    "message": "图片任务已提交",
                    "estimated_time": 30,
                    "endpoints": {
                        "status": f"/api/tasks/{task_id}",
                        "result": f"/api/tasks/{task_id}/result",
                        "cancel": f"/api/tasks/{task_id}",
                        "websocket": "/ws",
                    },
                }
            )
        except HTTPException:
            _cleanup_temp_file(tmp_file_path, logger, "submit_async_image_task")
            raise
        except Exception as exc:
            _cleanup_temp_file(tmp_file_path, logger, "submit_async_image_task")
            _raise_internal_error(logger, "TASK_ASYNC_IMAGE_SUBMIT_FAILED", exc, filename=file.filename)

    @router.post(f"/{password}/api/async/pdf" if password else "/api/async/pdf")
    async def submit_async_pdf_task(file: UploadFile, request: Request):
        task_manager = _require_task_manager()
        if not file:
            raise HTTPException(status_code=400, detail="未收到文件")
        if file.content_type != "application/pdf":
            raise HTTPException(status_code=400, detail="不支持的文件类型，请上传PDF文件")

        tmp_file_path: Optional[str] = None
        try:
            tmp_file_path, file_size_mb = await _write_upload_to_temp_file(
                file,
                suffix=".pdf",
                max_file_size_mb=runtime_config.max_pdf_size_mb,
                error_message=f"文件过大，最大支持{runtime_config.max_pdf_size_mb}MB",
            )
            try:
                page_count, _ = await _inspect_pdf_source(tmp_file_path)
            except Exception as exc:
                logger.warning("async_pdf_inspection_failed filename=%s error=%s", file.filename, exc)
                raise HTTPException(status_code=400, detail="PDF文件格式错误或损坏") from exc

            if page_count > runtime_config.max_pdf_pages:
                raise HTTPException(status_code=400, detail=f"PDF页数过多，最大支持{runtime_config.max_pdf_pages}页")

            client_ip = get_client_ip(request)
            task_id = await task_manager.submit_pdf_task_from_file(
                file_path=tmp_file_path,
                file_name=file.filename or "document.pdf",
                metadata={
                    "source": "async_api",
                    "file_type": file.content_type,
                    "original_filename": file.filename,
                    "file_size_mb": file_size_mb,
                    "total_pages": page_count,
                    "client_ip": client_ip,
                },
            )
            tmp_file_path = None

            return JSONResponse(
                {
                    "status": "success",
                    "task_id": task_id,
                    "message": "PDF任务已提交",
                    "total_pages": page_count,
                    "file_size_mb": f"{file_size_mb:.2f}",
                    "estimated_time": page_count * 3,
                    "endpoints": {
                        "status": f"/api/tasks/{task_id}",
                        "result": f"/api/tasks/{task_id}/result",
                        "cancel": f"/api/tasks/{task_id}",
                        "websocket": "/ws",
                    },
                }
            )
        except HTTPException:
            _cleanup_temp_file(tmp_file_path, logger, "submit_async_pdf_task")
            raise
        except Exception as exc:
            _cleanup_temp_file(tmp_file_path, logger, "submit_async_pdf_task")
            _raise_internal_error(logger, "TASK_ASYNC_PDF_SUBMIT_FAILED", exc, filename=file.filename)

    @router.post(f"/{password}/api/async/batch" if password else "/api/async/batch")
    async def submit_batch_tasks(files: list[UploadFile], request: Request):
        task_manager = _require_task_manager()
        if not files:
            raise HTTPException(status_code=400, detail="未收到文件")
        if len(files) > 20:
            raise HTTPException(status_code=400, detail="批量上传最多支持20个文件")

        try:
            client_ip = get_client_ip(request)
            submitted_tasks = []
            failed_files = []

            for file in files:
                tmp_file_path: Optional[str] = None
                try:
                    if file.content_type and file.content_type.startswith("image/"):
                        tmp_file_path, file_size_mb = await _write_upload_to_temp_file(
                            file,
                            suffix=".img",
                            max_file_size_mb=runtime_config.max_image_size_mb,
                            error_message=f"图片文件过大(最大{runtime_config.max_image_size_mb}MB)",
                        )
                        task_id = await task_manager.submit_image_task_from_file(
                            file_path=tmp_file_path,
                            file_name=file.filename or "image",
                            priority=1,
                            metadata={
                                "source": "batch_api",
                                "file_type": file.content_type,
                                "original_filename": file.filename,
                                "file_size_mb": file_size_mb,
                                "client_ip": client_ip,
                            },
                        )
                        submitted_tasks.append(
                            {
                                "task_id": task_id,
                                "filename": file.filename,
                                "type": "image",
                                "estimated_time": 30,
                            }
                        )
                        tmp_file_path = None
                    elif file.content_type == "application/pdf":
                        tmp_file_path, file_size_mb = await _write_upload_to_temp_file(
                            file,
                            suffix=".pdf",
                            max_file_size_mb=runtime_config.max_pdf_size_mb,
                            error_message=f"PDF文件过大(最大{runtime_config.max_pdf_size_mb}MB)",
                        )
                        page_count, _ = await _inspect_pdf_source(tmp_file_path)
                        if page_count > runtime_config.max_pdf_pages:
                            failed_files.append({"filename": file.filename, "error": f"PDF页数过多(最大{runtime_config.max_pdf_pages}页)"})
                            continue

                        task_id = await task_manager.submit_pdf_task_from_file(
                            file_path=tmp_file_path,
                            file_name=file.filename or "document.pdf",
                            priority=1,
                            metadata={
                                "source": "batch_api",
                                "file_type": file.content_type,
                                "original_filename": file.filename,
                                "file_size_mb": file_size_mb,
                                "total_pages": page_count,
                                "client_ip": client_ip,
                            },
                        )
                        submitted_tasks.append(
                            {
                                "task_id": task_id,
                                "filename": file.filename,
                                "type": "pdf",
                                "total_pages": page_count,
                                "estimated_time": page_count * 3,
                            }
                        )
                        tmp_file_path = None
                    else:
                        failed_files.append({"filename": file.filename, "error": "不支持的文件类型"})
                except HTTPException as exc:
                    failed_files.append({"filename": file.filename, "error": str(exc.detail)})
                except Exception as exc:
                    failed_files.append({"filename": file.filename, "error": str(exc)})
                    logger.warning("batch_task_item_failed filename=%s error=%s", file.filename, exc)
                finally:
                    _cleanup_temp_file(tmp_file_path, logger, "submit_batch_tasks")

            total_estimated_time = sum(task.get("estimated_time", 0) for task in submitted_tasks)
            return JSONResponse(
                {
                    "status": "success",
                    "message": f"批量任务提交完成，成功: {len(submitted_tasks)}, 失败: {len(failed_files)}",
                    "submitted_tasks": submitted_tasks,
                    "failed_files": failed_files,
                    "total_tasks": len(submitted_tasks),
                    "total_estimated_time": total_estimated_time,
                    "endpoints": {"batch_status": "/api/tasks/batch/status", "websocket": "/ws"},
                }
            )
        except HTTPException:
            raise
        except Exception as exc:
            _raise_internal_error(logger, "TASK_BATCH_SUBMIT_FAILED", exc, files_count=len(files))

    @router.post(f"/{password}/api/tasks/batch/status" if password else "/api/tasks/batch/status")
    async def get_batch_tasks_status(task_ids: dict):
        task_manager = _require_task_manager()
        task_id_list = task_ids.get("task_ids", [])
        if not task_id_list:
            raise HTTPException(status_code=400, detail="未提供任务ID")
        if len(task_id_list) > 50:
            raise HTTPException(status_code=400, detail="批量查询最多支持50个任务")

        try:
            tasks_status = []
            for task_id in task_id_list:
                try:
                    task_info = await task_manager.get_task_info(task_id)
                    if task_info:
                        tasks_status.append(
                            {
                                "task_id": task_id,
                                "status": task_info["status"],
                                "progress": task_info.get("progress", 0),
                                "created_at": task_info["created_at"],
                                "completed_at": task_info.get("completed_at"),
                                "error_message": task_info.get("error_message"),
                            }
                        )
                    else:
                        tasks_status.append({"task_id": task_id, "status": "not_found", "error_message": "任务不存在"})
                except Exception as exc:
                    logger.warning("batch_status_item_failed task_id=%s error=%s", task_id, exc)
                    tasks_status.append({"task_id": task_id, "status": "error", "error_message": str(exc)})

            status_counts: dict[str, int] = {}
            for task in tasks_status:
                status = task["status"]
                status_counts[status] = status_counts.get(status, 0) + 1

            return JSONResponse(
                {
                    "status": "success",
                    "tasks": tasks_status,
                    "summary": {"total_tasks": len(tasks_status), "status_counts": status_counts},
                }
            )
        except HTTPException:
            raise
        except Exception as exc:
            _raise_internal_error(logger, "TASK_BATCH_STATUS_FAILED", exc, task_count=len(task_id_list))

    @router.get(f"/{password}/api/tasks/{{task_id}}/pages" if password else "/api/tasks/{task_id}/pages")
    async def get_task_pages_api(task_id: str):
        task_manager = _require_task_manager()
        try:
            pages = await task_manager.get_task_pages(task_id)
            if not pages and not await task_manager.get_task_info(task_id):
                raise HTTPException(status_code=404, detail="任务不存在")

            return JSONResponse(
                {
                    "status": "success",
                    "data": {
                        "task_id": task_id,
                        "pages": pages,
                        "total_pages": len(pages),
                        "page_statistics": _calculate_page_statistics(pages),
                    },
                }
            )
        except HTTPException:
            raise
        except Exception as exc:
            _raise_internal_error(logger, "TASK_PAGES_FAILED", exc, task_id=task_id)

    @router.get(f"/{password}/api/tasks/{{task_id}}/pages/{{page_num}}" if password else "/api/tasks/{task_id}/pages/{page_num}")
    async def get_task_page_api(task_id: str, page_num: int):
        task_manager = _require_task_manager()
        if page_num < 1:
            raise HTTPException(status_code=400, detail="页码必须大于0")

        try:
            page_result = await task_manager.get_task_page(task_id, page_num)
            if not page_result:
                task_info = await task_manager.get_task_info(task_id)
                if not task_info:
                    raise HTTPException(status_code=404, detail="任务不存在")
                if page_num > task_info.get("total_pages", 0):
                    raise HTTPException(status_code=404, detail="页码超出范围")
                raise HTTPException(status_code=404, detail="页面结果不存在")

            return JSONResponse(
                {
                    "status": "success",
                    "data": {"task_id": task_id, "page_number": page_num, "page_result": page_result},
                }
            )
        except HTTPException:
            raise
        except Exception as exc:
            _raise_internal_error(logger, "TASK_PAGE_FAILED", exc, task_id=task_id, page_num=page_num)

    @router.get(f"/{password}/api/tasks/{{task_id}}/progress" if password else "/api/tasks/{task_id}/progress")
    async def get_task_progress_detail_api(task_id: str):
        task_manager = _require_task_manager()
        try:
            progress_detail = await task_manager.get_task_progress_detail(task_id)
            if not progress_detail:
                raise HTTPException(status_code=404, detail="任务不存在")
            return JSONResponse({"status": "success", "data": progress_detail})
        except HTTPException:
            raise
        except Exception as exc:
            _raise_internal_error(logger, "TASK_PROGRESS_FAILED", exc, task_id=task_id)

    @router.get(f"/{password}/api/tasks/{{task_id}}/batches" if password else "/api/tasks/{task_id}/batches")
    async def get_task_batches_api(task_id: str):
        task_manager = _require_task_manager()
        try:
            progress_detail = await task_manager.get_task_progress_detail(task_id)
            if not progress_detail:
                raise HTTPException(status_code=404, detail="任务不存在")

            batches = progress_detail.get("batches", [])
            return JSONResponse(
                {
                    "status": "success",
                    "data": {"task_id": task_id, "batches": batches, "total_batches": len(batches)},
                }
            )
        except HTTPException:
            raise
        except Exception as exc:
            _raise_internal_error(logger, "TASK_BATCHES_FAILED", exc, task_id=task_id)

    @router.get(f"/{password}/api/tasks/{{task_id}}/export" if password else "/api/tasks/{task_id}/export")
    async def export_task_result_api(task_id: str, format: str = Query("json", description="导出格式: json, txt, md")):
        task_manager = _require_task_manager()
        if format not in ["json", "txt", "md"]:
            raise HTTPException(status_code=400, detail="不支持的导出格式，支持: json, txt, md")

        try:
            task_result = await task_manager.get_task_result(task_id)
            if not task_result:
                raise HTTPException(status_code=404, detail="任务不存在")
            if task_result["status"] != "completed":
                raise HTTPException(status_code=400, detail="任务未完成，无法导出")

            content = task_result.get("content", "")
            filename = f"task_{task_id}_result"

            if format == "json":
                response_content = json.dumps(
                    {
                        "task_id": task_id,
                        "export_time": datetime.now().isoformat(),
                        "task_info": task_result,
                        "content": content,
                    },
                    ensure_ascii=False,
                    indent=2,
                )
                media_type = "application/json"
                filename += ".json"
            elif format == "txt":
                response_content = content
                media_type = "text/plain"
                filename += ".txt"
            else:
                response_content = f"# Task {task_id} Result\n\n{content}"
                media_type = "text/markdown"
                filename += ".md"

            return Response(
                content=response_content,
                media_type=media_type,
                headers={
                    "Content-Disposition": f"attachment; filename={filename}",
                    "Content-Type": f"{media_type}; charset=utf-8",
                },
            )
        except HTTPException:
            raise
        except Exception as exc:
            _raise_internal_error(logger, "TASK_EXPORT_FAILED", exc, task_id=task_id, format=format)

    app.include_router(router)
