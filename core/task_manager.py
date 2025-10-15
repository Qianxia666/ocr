import asyncio
import uuid
import logging
import time
import fitz  # PyMuPDF
from typing import Dict, Any, Optional, List, Callable
from datetime import datetime, timezone
from io import BytesIO
from PIL import Image
import aiohttp
import base64
import json

from models.database import (
    TaskStatus, TaskType, task_model, page_result_model,
    task_progress_model, page_batch_model,
    init_database, cleanup_database
)
from core.task_queue import (
    TaskQueue, WorkerPool, TaskItem,
    image_task_queue, pdf_task_queue,
    image_worker_pool, pdf_worker_pool
)
from core.page_processor import (
    PageProcessor,
    init_page_processor,
    shutdown_page_processor,
    is_page_processor_ready,
    ensure_page_processor_ready
)
from core.content_moderator import ModerationFailedException
from core.progress_tracker import ProgressTracker, init_progress_tracker, shutdown_progress_tracker, progress_tracker
from core.system_integration import SystemIntegration, init_system_integration, shutdown_system_integration, get_system_integration
from core.timeout_manager import TimeoutLevel
from core.error_handler import RecoveryStrategy
from core.auth import get_auth_manager
from models.websocket_messages import MessageFactory

logger = logging.getLogger(__name__)

def _extract_markdown_from_response(raw: str) -> str:
    """Parse OCR model response and extract Markdown content"""
    if not raw:
        return ''

    cleaned = raw.strip()

    if cleaned.startswith('```') and cleaned.endswith('```'):
        cleaned = cleaned.strip('`')
        if cleaned.lower().startswith('json'):
            cleaned = cleaned[4:]
        cleaned = cleaned.strip()

    try:
        data = json.loads(cleaned)
        if isinstance(data, dict):
            text_value = data.get('content') or data.get('text')
            if isinstance(text_value, str) and text_value.strip():
                return text_value.strip()
        elif isinstance(data, str) and data.strip():
            return data.strip()
    except json.JSONDecodeError as exc:
        logger.debug('OCR响应JSON解析失败: %s', exc)

    return ''

class TaskManager:
    """Task manager coordinating queues and OCR workers."""

    def _get_task_log_prefix(self, task_item: TaskItem) -> str:
        file_name = getattr(task_item, 'file_name', None)
        return f"[{file_name}] " if file_name else ''

    def _task_logger(self, task_item: TaskItem):
        prefix = self._get_task_log_prefix(task_item)

        class _TaskLogger:
            def debug(self_inner, message: str) -> None:
                logger.debug(prefix + message)

            def info(self_inner, message: str) -> None:
                logger.info(prefix + message)

            def warning(self_inner, message: str) -> None:
                logger.warning(prefix + message)

            def error(self_inner, message: str) -> None:
                logger.error(prefix + message)

            def exception(self_inner, message: str) -> None:
                logger.exception(prefix + message)

        return _TaskLogger()

    """任务管理器 - 提供任务创建、查询、控制的高级接口"""

    def __init__(self,
                 image_task_queue: TaskQueue,
                 pdf_task_queue: TaskQueue,
                 image_worker_pool: WorkerPool,
                 pdf_worker_pool: WorkerPool,
                 runtime_config=None,
                 api_config: Optional[Dict[str, Any]] = None,
                 websocket_manager=None):
        """
        初始化任务管理器
        :param image_task_queue: 图片任务队列
        :param pdf_task_queue: PDF任务队列
        :param image_worker_pool: 图片工作者池
        :param pdf_worker_pool: PDF工作者池
        :param runtime_config: 运行时配置对象引用
        :param api_config: API配置
        :param websocket_manager: WebSocket管理器
        """
        self.image_task_queue = image_task_queue
        self.pdf_task_queue = pdf_task_queue
        self.image_worker_pool = image_worker_pool
        self.pdf_worker_pool = pdf_worker_pool
        self.runtime_config = runtime_config  # 保存引用
        self.api_config = api_config or {}
        self.websocket_manager = websocket_manager
        self._initialized = False

        # 设置任务处理器
        self.image_worker_pool.set_task_processor(self._process_task)
        self.pdf_worker_pool.set_task_processor(self._process_task)

        logger.info("任务管理器初始化完成（双队列模式）")

    def _get_configured_max_retries(self) -> int:
        """获取实际生效的最大重试次数配置"""
        if self.runtime_config is not None and hasattr(self.runtime_config, 'max_retries'):
            try:
                value = int(self.runtime_config.max_retries)
            except (TypeError, ValueError):
                value = 3
        else:
            value = 3

        return max(1, value)
    
    async def initialize(self):
        """初始化任务管理器"""
        if self._initialized:
            return
        
        try:
            # 初始化数据库
            try:
                await init_database()
                logger.info("数据库初始化成功")
            except Exception as e:
                logger.error(f"数据库初始化失败: {e}")
                # 数据库是必需的，如果失败则抛出异常
                raise
            
            # 初始化系统集成器
            try:
                await init_system_integration(self.runtime_config, self.websocket_manager)
                logger.info("系统集成器初始化成功")
            except Exception as e:
                logger.warning(f"系统集成器初始化失败: {e}")
                # 系统集成器不是必需的，继续执行
            
            # 初始化页面处理器
            try:
                logger.warning("=== 任务管理器中初始化页面处理器 ===")
                await init_page_processor(self.runtime_config, self.api_config)
                
                # 验证页面处理器状态
                if is_page_processor_ready():
                    logger.info("页面处理器在任务管理器中初始化成功")
                    # 安全地获取页面处理器实例
                    try:
                        from core.page_processor import page_processor
                        logger.warning(f"任务管理器中的页面处理器实例: {page_processor}")
                    except Exception as e:
                        logger.warning(f"获取页面处理器实例时出错: {e}")
                else:
                    logger.warning("页面处理器在任务管理器中初始化可能不完整")
                    # 安全地获取页面处理器实例
                    try:
                        from core.page_processor import page_processor
                        logger.warning(f"任务管理器中的页面处理器实例: {page_processor}")
                    except Exception as e:
                        logger.warning(f"获取页面处理器实例时出错: {e}")
            except Exception as e:
                logger.error(f"页面处理器初始化失败: {e}")
                # 页面处理器不是必需的，继续执行
            
            # 初始化进度跟踪器
            try:
                await init_progress_tracker(self.websocket_manager)
                logger.info("进度跟踪器初始化成功")
            except Exception as e:
                logger.warning(f"进度跟踪器初始化失败: {e}")
                # 进度跟踪器不是必需的，继续执行
            
            # 恢复未完成的任务
            try:
                image_restored = await self.image_task_queue.restore_from_database()
                pdf_restored = await self.pdf_task_queue.restore_from_database()
                logger.info(f"恢复了 {image_restored + pdf_restored} 个未完成的任务 (图片: {image_restored}, PDF: {pdf_restored})")
            except Exception as e:
                logger.warning(f"恢复未完成任务失败: {e}")
                # 继续执行

            # 启动工作者池（图片和PDF分别启动）
            try:
                await self.image_worker_pool.start()
                await self.pdf_worker_pool.start()
                logger.info("图片和PDF工作者池启动成功")
            except Exception as e:
                logger.error(f"工作者池启动失败: {e}")
                # 工作者池是必需的，如果失败则抛出异常
                raise
            
            self._initialized = True
            logger.info("任务管理器初始化成功")
            
        except Exception as e:
            logger.error(f"任务管理器初始化失败: {e}")
            raise
    
    async def shutdown(self):
        """关闭任务管理器"""
        if not self._initialized:
            return

        try:
            # 停止工作者池（图片和PDF分别停止）
            await self.image_worker_pool.stop()
            await self.pdf_worker_pool.stop()

            # 关闭页面处理器
            await shutdown_page_processor()

            # 关闭进度跟踪器
            await shutdown_progress_tracker()

            # 关闭系统集成器
            await shutdown_system_integration()

            # 清理数据库连接
            await cleanup_database()

            self._initialized = False
            logger.info("任务管理器关闭完成")

        except Exception as e:
            logger.error(f"任务管理器关闭失败: {e}")
    
    async def submit_image_task(self,
                               file_data: bytes,
                               file_name: str,
                               priority: int = 0,
                               metadata: Optional[Dict[str, Any]] = None) -> str:
        """
        提交图片OCR任务(从内存数据)
        :param file_data: 文件数据
        :param file_name: 文件名
        :param priority: 优先级
        :param metadata: 元数据
        :return: 任务ID
        """
        task_id = str(uuid.uuid4())

        task_item = TaskItem(
            task_id=task_id,
            task_type=TaskType.IMAGE_OCR,
            file_data=file_data,
            file_name=file_name,
            file_size=len(file_data),
            total_pages=1,
            priority=priority,
            metadata=metadata or {}
        )

        success = await self.image_task_queue.put(task_item)
        if success:
            logger.info(f"图片OCR任务提交成功: {task_id}")

            # 发送任务开始WebSocket消息
            if self.websocket_manager:
                await self._send_task_started_message(task_item)

            return task_id
        else:
            logger.error("任务提交失败")
            raise Exception("任务提交失败")

    async def submit_image_task_from_file(self,
                                         file_path: str,
                                         file_name: str,
                                         priority: int = 0,
                                         metadata: Optional[Dict[str, Any]] = None) -> str:
        """
        提交图片OCR任务(从临时文件)- 节省内存
        :param file_path: 临时文件路径
        :param file_name: 文件名
        :param priority: 优先级
        :param metadata: 元数据
        :return: 任务ID
        """
        import os

        # 验证文件存在
        if not os.path.exists(file_path):
            raise Exception(f"临时文件不存在: {file_path}")

        # 获取文件大小
        file_size = os.path.getsize(file_path)

        task_id = str(uuid.uuid4())

        # 创建任务项,file_data为None,使用file_path
        task_item = TaskItem(
            task_id=task_id,
            task_type=TaskType.IMAGE_OCR,
            file_data=None,  # 不使用内存存储
            file_name=file_name,
            file_size=file_size,
            total_pages=1,
            priority=priority,
            metadata={
                **(metadata or {}),
                'temp_file_path': file_path  # 传递临时文件路径
            }
        )

        success = await self.image_task_queue.put(task_item)
        if success:
            logger.info(f"图片OCR任务提交成功(临时文件模式): {task_id}")

            # 发送任务开始WebSocket消息
            if self.websocket_manager:
                await self._send_task_started_message(task_item)

            return task_id
        else:
            logger.error("图片任务提交失败")
            # 清理临时文件
            try:
                os.unlink(file_path)
            except:
                pass
            raise Exception("任务提交失败")
    
    async def submit_pdf_task(self,
                             file_data: bytes,
                             file_name: str,
                             priority: int = 0,
                             metadata: Optional[Dict[str, Any]] = None) -> str:
        """
        提交PDF OCR任务(从内存数据)
        :param file_data: 文件数据
        :param file_name: 文件名
        :param priority: 优先级
        :param metadata: 元数据
        :return: 任务ID
        """
        # 获取PDF页数
        try:
            pdf_document = fitz.open(stream=file_data, filetype="pdf")
            total_pages = len(pdf_document)
            pdf_document.close()
        except Exception as e:
            logger.error(f"PDF文件解析失败: {e}")
            raise Exception("PDF文件格式错误或损坏")

        task_id = str(uuid.uuid4())

        task_item = TaskItem(
            task_id=task_id,
            task_type=TaskType.PDF_OCR,
            file_data=file_data,
            file_name=file_name,
            file_size=len(file_data),
            total_pages=total_pages,
            priority=priority,
            metadata=metadata or {}
        )

        success = await self.pdf_task_queue.put(task_item)
        if success:
            logger.info(f"PDF OCR任务提交成功: {task_id} ({total_pages}页)")

            # 发送任务开始WebSocket消息
            if self.websocket_manager:
                await self._send_task_started_message(task_item)

            return task_id
        else:
            logger.error("PDF任务提交失败")
            raise Exception("任务提交失败")

    async def submit_pdf_task_from_file(self,
                                       file_path: str,
                                       file_name: str,
                                       priority: int = 0,
                                       metadata: Optional[Dict[str, Any]] = None) -> str:
        """
        提交PDF OCR任务(从临时文件)- 节省内存
        :param file_path: 临时文件路径
        :param file_name: 文件名
        :param priority: 优先级
        :param metadata: 元数据
        :return: 任务ID
        """
        import os

        # 验证文件存在
        if not os.path.exists(file_path):
            raise Exception(f"临时文件不存在: {file_path}")

        # 从元数据获取页数和文件大小
        total_pages = metadata.get('total_pages', 0)
        file_size = os.path.getsize(file_path)

        task_id = str(uuid.uuid4())

        # 创建任务项,file_data为None,使用file_path
        task_item = TaskItem(
            task_id=task_id,
            task_type=TaskType.PDF_OCR,
            file_data=None,  # 不使用内存存储
            file_name=file_name,
            file_size=file_size,
            total_pages=total_pages,
            priority=priority,
            metadata={
                **(metadata or {}),
                'temp_file_path': file_path  # 传递临时文件路径
            }
        )

        success = await self.pdf_task_queue.put(task_item)
        if success:
            logger.info(f"PDF OCR任务提交成功(临时文件模式): {task_id} ({total_pages}页)")

            # 发送任务开始WebSocket消息
            if self.websocket_manager:
                await self._send_task_started_message(task_item)

            return task_id
        else:
            logger.error("PDF任务提交失败")
            # 清理临时文件
            try:
                os.unlink(file_path)
            except:
                pass
            raise Exception("任务提交失败")
    
    async def get_task_info(self, task_id: str) -> Optional[Dict[str, Any]]:
        """获取任务详细信息"""
        task_data = await task_model.get_task(task_id)
        if not task_data:
            return None

        # 根据任务类型获取队列状态
        if task_data['task_type'] == TaskType.IMAGE_OCR.value:
            queue_status = await self.image_task_queue.get_task_status(task_id)
        else:
            queue_status = await self.pdf_task_queue.get_task_status(task_id)

        if queue_status:
            task_data['queue_status'] = queue_status

        # 如果是PDF任务，获取页面结果
        if task_data['task_type'] == TaskType.PDF_OCR.value:
            page_results = await page_result_model.get_task_page_results(task_id)
            task_data['page_results'] = page_results

        return task_data
    
    async def get_task_result(self, task_id: str) -> Optional[Dict[str, Any]]:
        """获取任务结果"""
        # 静默查询,只在失败或完成时才记录日志
        task_data = await self.get_task_info(task_id)
        if not task_data:
            logger.warning(f"任务不存在: {task_id}")
            return None

        # 计算文件大小（MB）
        file_size_mb = round(task_data.get('file_size', 0) / (1024 * 1024), 2) if task_data.get('file_size') else None

        result = {
            'task_id': task_id,
            'status': task_data['status'],
            'progress': task_data.get('progress', 0),
            'created_at': task_data['created_at'],
            'completed_at': task_data.get('completed_at'),
            'error_message': task_data.get('error_message'),
            'file_size_mb': file_size_mb
        }

        if task_data['status'] == TaskStatus.COMPLETED.value:
            # 任务完成时记录一次日志
            if task_data['task_type'] == TaskType.IMAGE_OCR.value:
                # 图片任务结果
                page_results = await page_result_model.get_task_page_results(task_id)

                if page_results:
                    content = page_results[0].get('content', '')
                    result['content'] = content

                    if not content:
                        logger.error(f"图片任务结果内容为空: {task_id}")
                else:
                    logger.error(f"图片任务没有页面结果: {task_id}")
                    result['content'] = ''
            else:
                # PDF任务结果
                page_results = await page_result_model.get_task_page_results(task_id)

                contents = []
                empty_pages = []
                moderation_failed_pages = []
                for page_result in page_results:
                    page_num = page_result.get('page_number')
                    page_status = page_result.get('status')
                    error_message = page_result.get('error_message', '')
                    content = page_result.get('content', '')

                    # 过滤审查失败的页面
                    if page_status == 'cancelled' and error_message == '内容道德审查未通过':
                        moderation_failed_pages.append(page_num)
                        logger.debug(f"过滤审查失败页面 {task_id}-{page_num}")
                        continue

                    if content:
                        contents.append(content)
                    else:
                        empty_pages.append(page_num)

                if moderation_failed_pages:
                    logger.info(f"PDF任务有 {len(moderation_failed_pages)} 个页面审查失败: 页面 {moderation_failed_pages}")
                if empty_pages:
                    logger.error(f"PDF任务有 {len(empty_pages)} 个页面内容为空: 页面 {empty_pages}")

                result['content'] = '\n\n'.join(contents)
                result['page_results'] = page_results

                if not result['content']:
                    logger.error(f"严重: PDF任务所有页面内容都为空! {task_id}")
        elif task_data['status'] == TaskStatus.CANCELLED.value:
            # 审查失败的任务 - 前端不可见内容
            if task_data.get('error_message') == '内容道德审查未通过':
                result['content'] = ''  # 前端看不到内容
            else:
                # 其他取消原因，可以返回内容
                result['content'] = ''
        else:
            result['content'] = ''

        return result
    

    async def cancel_task(self, task_id: str, reason: str = "管理员手动停止") -> tuple[bool, str]:
        """取消指定任务"""
        task = await task_model.get_task(task_id)
        if not task:
            return False, "任务不存在"

        status = task.get('status')
        if status in (TaskStatus.COMPLETED.value, TaskStatus.FAILED.value, TaskStatus.CANCELLED.value):
            return False, "任务已结束"

        task_type_value = task.get('task_type')
        if task_type_value == TaskType.IMAGE_OCR.value:
            queue = self.image_task_queue
            pool = self.image_worker_pool
        else:
            queue = self.pdf_task_queue
            pool = self.pdf_worker_pool

        cancelled_pending = await queue.cancel_task(task_id, reason)
        if cancelled_pending:
            await self._refund_task_quota(task)
            await self._send_task_cancelled_message(task_id, reason)
            return True, "已取消等待中的任务"

        if not pool:
            return False, "任务无法取消"

        cancelled_processing = await pool.cancel_task(task_id, reason)
        if cancelled_processing:
            latest_task = await task_model.get_task(task_id)
            await self._refund_task_quota(latest_task or task)
            await self._send_task_cancelled_message(task_id, reason)
            return True, "已停止任务处理"

        return False, "任务可能已完成，请刷新列表"

    async def get_tasks_list(self, status: Optional[str] = None, limit: int = 100):
        """获取任务列表"""
        if status:
            return await task_model.get_tasks_by_status(status, limit)
        else:
            return await task_model.get_recent_tasks(limit)
    
    
    async def get_task_pages(self, task_id: str) -> List[Dict[str, Any]]:
        """获取任务的分页列表和状态"""
        try:
            page_results = await page_result_model.get_task_page_results(task_id)
            return page_results
        except Exception as e:
            logger.error(f"获取任务分页失败 {task_id}: {e}")
            return []
    
    async def get_task_page(self, task_id: str, page_num: int) -> Optional[Dict[str, Any]]:
        """获取特定页面结果"""
        try:
            page_result = await page_result_model.get_page_result(task_id, page_num)
            return page_result
        except Exception as e:
            logger.error(f"获取任务页面失败 {task_id}-{page_num}: {e}")
            return None
    
    async def get_task_progress_detail(self, task_id: str) -> Dict[str, Any]:
        """获取任务详细进度信息"""
        try:
            # 获取任务基本信息
            task_info = await task_model.get_task(task_id)
            if not task_info:
                return {}
            
            # 获取最新进度
            latest_progress = await task_progress_model.get_latest_progress(task_id)
            
            # 获取批次信息
            batches = await page_batch_model.get_task_batches(task_id)
            
            # 获取页面结果统计
            page_results = await page_result_model.get_task_page_results(task_id)
            page_stats = self._calculate_page_stats(page_results)
            
            return {
                'task_info': task_info,
                'latest_progress': latest_progress,
                'batches': batches,
                'page_statistics': page_stats,
                'detailed_progress': {
                    'total_pages': task_info.get('total_pages', 0),
                    'processed_pages': task_info.get('processed_pages', 0),
                    'failed_pages': task_info.get('failed_pages', 0),
                    'progress_percentage': task_info.get('progress', 0),
                    'processing_rate': task_info.get('processing_rate', 0),
                    'estimated_time_remaining': task_info.get('estimated_time_remaining', 0)
                }
            }
        except Exception as e:
            logger.error(f"获取任务详细进度失败 {task_id}: {e}")
            return {}
    
    
    async def _retry_empty_content_page(self, task_id: str, page_number: int, pdf_document, page_processor) -> bool:
        """重试空内容页面"""
        try:
            logger.info(f" 开始重试空内容页面 {task_id}-{page_number}...")
            
            # 转换页面为图片
            page = pdf_document.load_page(page_number - 1)
            
            # 使用较低的DPI以节省内存
            dpi = 150
            pix = page.get_pixmap(dpi=dpi)
            
            # 流式转换图片数据
            image_data = None
            with BytesIO() as buffer:
                png_data = pix.tobytes("png")
                buffer.write(png_data)
                image_data = buffer.getvalue()
            
            # 立即释放内存
            pix = None
            page = None
            
            # 使用页面处理器重新处理
            content = await page_processor._call_ocr_api_with_retry(image_data, asyncio.Semaphore(1))
            
            if content:
                # 更新页面结果
                await page_result_model.update_page_result(
                    task_id, page_number, "completed",
                    content=content, processing_time=0
                )
                logger.info(f"空内容页面重试成功 {task_id}-{page_number}: 内容长度={len(content)}字符")
                return True
            else:
                logger.error(f"空内容页面重试失败 {task_id}-{page_number}: OCR返回空内容")
                return False
                
        except Exception as e:
            logger.error(f"空内容页面重试异常 {task_id}-{page_number}: {e}")
            return False
    
    def _calculate_page_stats(self, page_results: List[Dict[str, Any]]) -> Dict[str, Any]:
        """计算页面统计信息"""
        stats = {
            'total_pages': len(page_results),
            'completed_pages': 0,
            'failed_pages': 0,
            'pending_pages': 0,
            'processing_pages': 0,
            'average_processing_time': 0,
            'total_content_length': 0,
            'success_rate': 0
        }
        
        if not page_results:
            return stats
        
        total_processing_time = 0
        processing_time_count = 0
        
        for page in page_results:
            status = page.get('status', 'pending')
            
            if status == 'completed':
                stats['completed_pages'] += 1
                stats['total_content_length'] += page.get('content_length', 0)
            elif status == 'failed':
                stats['failed_pages'] += 1
            elif status == 'processing':
                stats['processing_pages'] += 1
            else:
                stats['pending_pages'] += 1
            
            # 计算平均处理时间
            processing_time = page.get('processing_time')
            if processing_time:
                total_processing_time += processing_time
                processing_time_count += 1
        
        # 计算平均处理时间
        if processing_time_count > 0:
            stats['average_processing_time'] = total_processing_time / processing_time_count
        
        # 计算成功率
        if stats['total_pages'] > 0:
            stats['success_rate'] = (stats['completed_pages'] / stats['total_pages']) * 100
        
        return stats
    
    async def get_system_stats(self) -> Dict[str, Any]:
        """获取系统级统计信息"""
        image_queue_stats = await self.image_task_queue.get_queue_stats()
        pdf_queue_stats = await self.pdf_task_queue.get_queue_stats()

        image_worker_stats = await self.image_worker_pool.get_worker_stats()
        pdf_worker_stats = await self.pdf_worker_pool.get_worker_stats()

        system_health = {}
        system_integration = get_system_integration()
        if system_integration:
            system_health = await system_integration.get_system_health()

        return {
            'queues': {
                'image': image_queue_stats,
                'pdf': pdf_queue_stats
            },
            'workers': {
                'image': image_worker_stats,
                'pdf': pdf_worker_stats
            },
            'system_health': system_health,
            'system': {
                'initialized': self._initialized,
                'timestamp': datetime.now(timezone.utc).isoformat()
            }
        }

    async def auto_scale_workers(self):
        """根据队列压力自动调整工作者数量"""
        try:
            queue_pool_pairs = (
                ('image', self.image_task_queue, self.image_worker_pool),
                ('pdf', self.pdf_task_queue, self.pdf_worker_pool),
            )

            for label, queue, pool in queue_pool_pairs:
                queue_stats = await queue.get_queue_stats()
                pending_count = queue_stats['pending_count']
                processing_count = queue_stats['processing_count']

                worker_stats = await pool.get_worker_stats()
                current_workers = worker_stats['total_workers']
                target_workers = current_workers

                if pending_count > max(1, processing_count) * 2:
                    target_workers = min(current_workers + 1, pool.max_workers)
                elif pending_count == 0 and processing_count == 0 and current_workers > pool.min_workers:
                    target_workers = max(current_workers - 1, pool.min_workers)

                if target_workers != current_workers:
                    scaled = await pool.scale_workers(target_workers)
                    if scaled:
                        logger.info(f'自动调整{label}工作者数量到 {target_workers}')
        except Exception as e:
            logger.error(f'自动调整工作者失败: {e}')

    async def _process_task(self, task_item: TaskItem) -> Dict[str, Any]:
        """Process a task with unified logging and monitoring."""
        log = self._task_logger(task_item)
        task_id = task_item.task_id
        task_type = task_item.task_type.value

        log.info(f"=== start task {task_id} ({task_type}) ===")
        log.info(
            f"detail: file={task_item.file_name}, size={task_item.file_size} bytes, "
            f"pages={task_item.total_pages}, priority={task_item.priority}"
        )

        system_integration = get_system_integration()
        timeout_context = None

        try:
            log.info(f"set task {task_id} status to processing")
            await task_model.update_task_status(task_id, TaskStatus.PROCESSING, progress=0.0)
            log.info(f"task {task_id} status updated to processing")

            log.info(f"begin monitoring {task_id}")
            if system_integration:
                timeout_context = await system_integration.monitor_task_execution(
                    task_id, task_item.total_pages
                )
                log.info(f"monitoring enabled for {task_id}")
            else:
                log.warning(f"system integration unavailable, skip monitoring for {task_id}")

            if task_item.task_type == TaskType.PDF_OCR:
                log.info(f"checking page processor for {task_id}")
                if not is_page_processor_ready():
                    log.warning(f"page processor not ready for {task_id}, reinitializing")
                    processor_ready = await ensure_page_processor_ready(self.runtime_config, self.api_config)
                    if not processor_ready:
                        log.error(f"page processor initialization failed for {task_id}; fallback will be used if needed")
                    else:
                        log.info(f"page processor reinitialized for {task_id}")
                else:
                    log.info(f"page processor ready for {task_id}")

            log.info(f"execute task pipeline for {task_id}")
            if task_item.task_type == TaskType.IMAGE_OCR:
                log.info(f"dispatch image OCR for {task_id}")
                result = await self._process_image_task_protected(task_item)
            elif task_item.task_type == TaskType.PDF_OCR:
                log.info(f"dispatch PDF OCR for {task_id}")
                result = await self._process_pdf_task_protected(task_item)
            else:
                msg = f"unsupported task type: {task_item.task_type}"
                log.error(f"{msg} ({task_id})")
                raise Exception(msg)

            log.info(f"task pipeline finished for {task_id}")

            # 检查是否为审查失败（特殊处理）
            is_moderation_failed = isinstance(result, dict) and result.get('moderation_failed') == True

            if system_integration and timeout_context:
                if is_moderation_failed:
                    log.info(f"complete monitoring {task_id} (cancelled - moderation failed)")
                    await system_integration.complete_task_monitoring(
                        task_id, timeout_context, success=False
                    )
                else:
                    log.info(f"complete monitoring {task_id} (success)")
                    await system_integration.complete_task_monitoring(
                        task_id, timeout_context, success=True
                    )

            if is_moderation_failed:
                log.info(f"=== task cancelled {task_id} (moderation failed) ===")
            else:
                log.info(f"=== task finished {task_id} ===")
            return result

        except Exception as exc:
            if system_integration and timeout_context:
                log.info(f"complete monitoring {task_id} (failure)")
                await system_integration.complete_task_monitoring(
                    task_id, timeout_context, success=False
                )

            log.error(f"=== task failed {task_id}: {exc} ===")
            log.exception(f"task failure details {task_id}:")

            try:
                await task_model.update_task_status(
                    task_id, TaskStatus.FAILED,
                    error_message=str(exc)
                )
                log.info(f"task {task_id} status updated to failed")
            except Exception as update_error:
                log.error(f"failed to update task status {task_id}: {update_error}")

            raise
        finally:
            if system_integration and timeout_context:
                try:
                    await system_integration.finalize_task_monitoring(task_id, timeout_context)
                except Exception as finalize_error:
                    log.warning(f"finalize monitoring failed {task_id}: {finalize_error}")
    async def _process_image_task_protected(self, task_item: TaskItem) -> Dict[str, Any]:
        """处理图片OCR任务 - 带保护机制"""
        system_integration = get_system_integration()

        async def _process_image_operation():
            return await self._process_image_task(task_item)

        if system_integration:
            return await system_integration.execute_with_protection(
                task_id=task_item.task_id,
                operation=_process_image_operation,
                timeout_level=TimeoutLevel.PAGE_PROCESSING,
                recovery_strategy=RecoveryStrategy.RETRY,
                max_retries=self._get_configured_max_retries()
            )
        else:
            return await self._process_image_task(task_item)

    async def _process_image_task(self, task_item: TaskItem) -> Dict[str, Any]:
        """Process a single image OCR task."""
        import os

        log = self._task_logger(task_item)
        start_time = time.time()
        temp_file_path = task_item.metadata.get('temp_file_path')
        image_data: Optional[bytes] = None

        try:
            if temp_file_path:
                log.info(f"load image bytes from temp file {temp_file_path}")
                with open(temp_file_path, 'rb') as handle:
                    image_data = handle.read()
            else:
                log.info("load inline image bytes from request payload")
                image_data = task_item.file_data

            if not image_data:
                raise Exception('image data is empty')

            # 压缩图片（如果配置了压缩比例）
            # 只压缩大于1MB的图片，避免对小图片的无谓开销
            MIN_COMPRESS_SIZE = 1 * 1024 * 1024  # 1MB
            if (hasattr(self, 'runtime_config') and self.runtime_config and
                len(image_data) > MIN_COMPRESS_SIZE):
                compression_ratio = getattr(self.runtime_config, 'image_compression_ratio', 1.0)
                if compression_ratio < 1.0:
                    image_data = self._compress_image(image_data, compression_ratio)

            content = await self._call_ocr_api_protected(task_item.task_id, image_data)
            processing_time = time.time() - start_time
            image_data = None

            await page_result_model.create_page_result(task_item.task_id, 1, 'completed')
            await page_result_model.update_page_result(
                task_item.task_id,
                1,
                'completed',
                content=content,
                processing_time=processing_time
            )

            await task_model.update_task_status(
                task_item.task_id,
                TaskStatus.PROCESSING,
                progress=100.0,
                processed_pages=1
            )

            if self.websocket_manager:
                page_message = MessageFactory.create_page_completed(
                    task_item.task_id,
                    1,
                    'completed',
                    content_length=len(content) if content else 0,
                    processing_time=processing_time
                )
                await self.websocket_manager.send_to_task_subscribers(
                    task_item.task_id,
                    page_message.to_dict()
                )

            file_size_mb = round(task_item.file_size / (1024 * 1024), 2)
            result_summary = {
                'total_pages': 1,
                'successful_pages': 1,
                'failed_pages': 0,
                'success_rate': '100.0%',
                'processing_time': processing_time,
                'content_length': len(content) if content else 0,
                'file_size_mb': file_size_mb
            }

            if self.websocket_manager:
                completed_message = MessageFactory.create_task_completed(
                    task_item.task_id,
                    result_summary,
                    content_preview=content[:500] if content else None
                )
                await self.websocket_manager.send_to_task_subscribers(
                    task_item.task_id,
                    completed_message.to_dict()
                )

            log.info(f"image task completed {task_item.task_id} in {processing_time:.2f}s")
            return result_summary

        except ModerationFailedException as moderation_exc:
            # 审查失败异常 - 特殊处理
            processing_time = time.time() - start_time
            log.warning(f"内容道德审查未通过 {task_item.task_id}: {moderation_exc.details}")

            # 保存原始OCR内容到page_result（仅后端可见）
            await page_result_model.create_page_result(task_item.task_id, 1, 'cancelled')
            await page_result_model.update_page_result(
                task_item.task_id,
                1,
                'cancelled',
                content=moderation_exc.original_content,  # 保存原始内容
                error_message='内容道德审查未通过',
                processing_time=processing_time
            )

            # 将任务状态设置为CANCELLED
            await task_model.update_task_status(
                task_item.task_id,
                TaskStatus.CANCELLED,
                progress=0.0,
                processed_pages=0,
                error_message='内容道德审查未通过'
            )

            # 保存审查日志到moderation_logs表
            await self._save_moderation_log(
                task_id=task_item.task_id,
                page_number=1,
                action='block',
                details=moderation_exc.details,
                original_content=moderation_exc.original_content
            )

            # WebSocket通知前端任务已取消
            if self.websocket_manager:
                cancelled_message = MessageFactory.create_task_failed(
                    task_item.task_id,
                    '内容道德审查未通过',
                    failed_page=1
                )
                await self.websocket_manager.send_to_task_subscribers(
                    task_item.task_id,
                    cancelled_message.to_dict()
                )

            log.info(f"任务已取消(审查未通过) {task_item.task_id}")
            # 返回特殊标记，让上层识别为取消而非成功
            return {
                'total_pages': 1,
                'successful_pages': 0,
                'failed_pages': 0,
                'cancelled': True,
                'moderation_failed': True,  # 添加特殊标记
                'reason': '内容道德审查未通过'
            }

        except Exception as exc:
            processing_time = time.time() - start_time

            await page_result_model.create_page_result(task_item.task_id, 1, 'failed')
            await page_result_model.update_page_result(
                task_item.task_id,
                1,
                'failed',
                error_message=str(exc),
                processing_time=processing_time
            )

            if self.websocket_manager:
                failed_message = MessageFactory.create_task_failed(
                    task_item.task_id,
                    str(exc),
                    failed_page=1
                )
                await self.websocket_manager.send_to_task_subscribers(
                    task_item.task_id,
                    failed_message.to_dict()
                )

            raise
        finally:
            pass
    async def _process_pdf_task_protected(self, task_item: TaskItem) -> Dict[str, Any]:
        """处理PDF OCR任务 - 带保护机制"""
        system_integration = get_system_integration()
        
        async def _process_pdf_operation():
            return await self._process_pdf_task(task_item)
        
        if system_integration:
            return await system_integration.execute_with_protection(
                task_id=task_item.task_id,
                operation=_process_pdf_operation,
                timeout_level=TimeoutLevel.TASK_EXECUTION,
                recovery_strategy=RecoveryStrategy.RETRY,
                max_retries=self._get_configured_max_retries()
            )
        else:
            return await self._process_pdf_task(task_item)
    
    async def _process_pdf_task(self, task_item: TaskItem) -> Dict[str, Any]:
        """处理PDF OCR任务 - 使用新的分页处理器"""
        log = self._task_logger(task_item)
        import os
        start_time = time.time()
        temp_file_path = task_item.metadata.get('temp_file_path')

        try:
            log.info(f"=== 开始PDF任务处理 {task_item.task_id} ===")
            log.info(f"任务详情: 文件名={task_item.file_name}, 总页数={task_item.total_pages}, 文件大小={task_item.file_size}字节")

            # 检查是否使用临时文件模式
            if temp_file_path:
                log.info(f"使用临时文件模式处理PDF: {temp_file_path}")

            # 1. 使用分页处理器处理PDF
            log.info(f"检查页面处理器状态 {task_item.task_id}...")

            if not is_page_processor_ready():
                log.warning(f"页面处理器未初始化 {task_item.task_id}，尝试重新初始化...")
                try:
                    await init_page_processor(self.runtime_config, self.api_config)
                    if not is_page_processor_ready():
                        raise Exception("页面处理器重新初始化失败")
                    log.info(f"页面处理器重新初始化成功 {task_item.task_id}")
                except Exception as e:
                    log.error(f"页面处理器重新初始化失败 {task_item.task_id}: {e}")
                    # 不要抛出异常，而是使用内置的PDF处理逻辑
                    log.warning(f" 将使用内置PDF处理逻辑继续处理任务 {task_item.task_id}")
                    return await self._process_pdf_fallback(task_item)
            else:
                log.info(f"页面处理器已就绪 {task_item.task_id}")

            # 2. 更新任务状态为处理中
            log.info(f"更新PDF任务状态为处理中 {task_item.task_id}...")
            update_success = await task_model.update_task_status(
                task_item.task_id, TaskStatus.PROCESSING,
                progress=0.0, processed_pages=0
            )
            if not update_success:
                log.error(f"更新PDF任务状态为处理中失败 {task_item.task_id}")
            else:
                log.info(f"PDF任务状态已更新为处理中 {task_item.task_id}")

            # 3. 开始分页处理，包含实时进度跟踪
            log.info(f"开始分页处理PDF {task_item.task_id} (总页数: {task_item.total_pages})...")

            # 添加保护机制：确保页面处理器可用
            log.warning(f"=== 页面处理器可用性检查 {task_item.task_id} ===")
            
            # 检查并确保页面处理器可用
            processor_available = False
            max_retries = 2
            
            for retry in range(max_retries):
                log.warning(f"尝试 {retry + 1}/{max_retries}: 检查页面处理器状态")

                # 重新导入以获取最新的页面处理器引用
                from core.page_processor import page_processor as current_page_processor

                log.warning(f"当前页面处理器: {current_page_processor}")
                log.warning(f"页面处理器就绪状态: {is_page_processor_ready()}")

                if current_page_processor is not None and is_page_processor_ready():
                    # 检查必要方法
                    if hasattr(current_page_processor, 'process_pdf_with_pagination'):
                        log.info(f"页面处理器可用，包含必要方法")
                        processor_available = True
                        break
                    else:
                        log.error(f"页面处理器缺少 process_pdf_with_pagination 方法")
                else:
                    log.warning(f"页面处理器不可用，尝试重新初始化...")

                    try:
                        init_success = await ensure_page_processor_ready(self.runtime_config, self.api_config)
                        log.info(f"页面处理器重新初始化结果: {init_success}")

                        if init_success:
                            # 等待短暂时间确保初始化完成
                            await asyncio.sleep(0.5)
                            # 重新导入获取更新后的引用
                            from core.page_processor import page_processor as updated_page_processor
                            if updated_page_processor is not None and hasattr(updated_page_processor, 'process_pdf_with_pagination'):
                                log.info(f"页面处理器重新初始化后可用")
                                processor_available = True
                                current_page_processor = updated_page_processor
                                break
                    except Exception as init_error:
                        log.error(f"页面处理器重新初始化异常: {init_error}")

                # 如果不是最后一次尝试，等待一下再重试
                if retry < max_retries - 1:
                    await asyncio.sleep(1)

            if not processor_available:
                log.error(f"页面处理器最终不可用，使用备用处理方式 {task_item.task_id}")
                return await self._process_pdf_fallback(task_item)

            log.warning(f"=== 页面处理器检查通过 {task_item.task_id} ===")
            
            # 使用确认可用的页面处理器
            if temp_file_path:
                # 临时文件模式
                result_summary = await current_page_processor.process_pdf_from_file(
                    task_id=task_item.task_id,
                    file_path=temp_file_path,
                    total_pages=task_item.total_pages,
                    websocket_manager=self.websocket_manager,
                    file_name=task_item.file_name
                )
            else:
                # 传统内存模式
                result_summary = await current_page_processor.process_pdf_with_pagination(
                    task_id=task_item.task_id,
                    file_data=task_item.file_data,
                    total_pages=task_item.total_pages,
                    websocket_manager=self.websocket_manager,
                    file_name=task_item.file_name
                )
            
            log.info(f"分页处理结果摘要: {result_summary}")

            # 4. 更新任务状态为完成
            processing_time = time.time() - start_time
            log.info(f"更新PDF任务状态为完成 {task_item.task_id}...")
            update_success = await task_model.update_task_status(
                task_item.task_id, TaskStatus.COMPLETED,
                progress=100.0, processed_pages=result_summary.get('successful_pages', 0)
            )
            if not update_success:
                log.error(f"更新PDF任务状态为完成失败 {task_item.task_id}")
            else:
                log.info(f"PDF任务状态已更新为完成 {task_item.task_id}")

            # 5. 验证数据库中的页面结果
            log.info(f"验证数据库中的页面结果 {task_item.task_id}...")
            page_results = await page_result_model.get_task_page_results(task_item.task_id)
            log.info(f"数据库中找到 {len(page_results)} 个页面结果")

            successful_pages = 0
            failed_pages = 0
            moderation_failed_pages = 0
            total_content_length = 0

            for page_result in page_results:
                page_status = page_result.get('status')
                error_message = page_result.get('error_message', '')

                if page_status == 'completed':
                    successful_pages += 1
                    content = page_result.get('content', '')
                    content_length = len(content) if content else 0
                    total_content_length += content_length
                    log.info(f"页面 {page_result['page_number']}: 内容长度={content_length}字符, 预览={content[:100] if content else '无内容'}...")
                elif page_status == 'cancelled' and error_message == '内容道德审查未通过':
                    moderation_failed_pages += 1
                    log.warning(f"页面 {page_result['page_number']}: 审查失败")
                else:
                    failed_pages += 1
                    log.error(f"页面 {page_result['page_number']}: 状态={page_status}, 错误={error_message}")

            log.info(f" 页面统计: 成功={successful_pages}, 失败={failed_pages}, 审查失败={moderation_failed_pages}, 总内容长度={total_content_length}字符")

            # 如果所有页面都审查失败，将任务标记为CANCELLED
            if moderation_failed_pages == task_item.total_pages:
                log.warning(f"所有页面审查失败，将任务标记为CANCELLED {task_item.task_id}")
                await task_model.update_task_status(
                    task_item.task_id,
                    TaskStatus.CANCELLED,
                    error_message='所有页面内容道德审查未通过'
                )
                return {
                    'total_pages': task_item.total_pages,
                    'successful_pages': 0,
                    'failed_pages': 0,
                    'moderation_failed': True,
                    'moderation_failed_pages': moderation_failed_pages,
                    'cancelled': True,
                    'reason': '所有页面内容道德审查未通过'
                }

            if total_content_length == 0 and successful_pages > 0:
                log.error(f" 检测到严重问题: 页面显示成功但内容为空！可能是OCR API响应解析失败。")

                # 尝试修复空内容页面
                log.info(f" 开始修复空内容页面 {task_item.task_id}...")
                pdf_document = fitz.open(stream=task_item.file_data, filetype="pdf")

                try:
                    fixed_pages = 0
                    for page_result in page_results:
                        if page_result.get('status') == 'completed' and not page_result.get('content'):
                            page_number = page_result['page_number']
                            # 安全地获取页面处理器实例
                            try:
                                from core.page_processor import page_processor as retry_page_processor
                                retry_success = await self._retry_empty_content_page(
                                    task_item.task_id, page_number, pdf_document, retry_page_processor
                                )
                            except Exception as e:
                                log.error(f"获取页面处理器实例失败 {task_item.task_id}-{page_number}: {e}")
                                retry_success = False
                            if retry_success:
                                fixed_pages += 1
                    
                    if fixed_pages > 0:
                        log.info(f"成功修复 {fixed_pages} 个空内容页面")
                        # 重新获取页面结果
                        page_results = await page_result_model.get_task_page_results(task_item.task_id)

                        # 重新计算统计信息
                        successful_pages = 0
                        failed_pages = 0
                        total_content_length = 0

                        for page_result in page_results:
                            if page_result.get('status') == 'completed':
                                successful_pages += 1
                                content = page_result.get('content', '')
                                content_length = len(content) if content else 0
                                total_content_length += content_length
                            else:
                                failed_pages += 1

                        log.info(f" 修复后页面统计: 成功={successful_pages}, 失败={failed_pages}, 总内容长度={total_content_length}字符")
                    else:
                        log.warning(f"未能修复任何空内容页面")

                except Exception as fix_error:
                    log.error(f"修复空内容页面时出错 {task_item.task_id}: {fix_error}")
                finally:
                    if pdf_document:
                        pdf_document.close()

            # 6. 发送任务完成消息
            if self.websocket_manager:
                log.info(f"发送任务完成WebSocket消息 {task_item.task_id}...")
                completed_message = MessageFactory.create_task_completed(
                    task_item.task_id, result_summary
                )
                await self.websocket_manager.send_to_task_subscribers(
                    task_item.task_id, completed_message.to_dict()
                )
                log.info(f"任务完成消息已发送 {task_item.task_id}")

            # 7. 记录OCR活动用于IP滥用检测
            try:
                task_data = await task_model.get_task(task_item.task_id)
                if task_data and task_data.get('user_id') and task_data.get('client_ip'):
                    from models.database import ip_tracking_model
                    await ip_tracking_model.record_ocr_activity(
                        task_data['user_id'],
                        task_data['client_ip']
                    )
                    log.debug(f"已记录OCR活动: 用户={task_data['user_id']}, IP={task_data['client_ip']}")
            except Exception as ip_error:
                log.error(f"记录OCR活动失败 {task_item.task_id}: {ip_error}")

            log.info(f"=== PDF任务处理完成 {task_item.task_id} (耗时: {processing_time:.2f}s): {result_summary} ===")
            return result_summary

        except Exception as e:
            processing_time = time.time() - start_time
            log.error(f"=== PDF任务处理异常 {task_item.task_id} (耗时: {processing_time:.2f}s): {e} ===")
            log.exception(f"PDF任务处理异常详情 {task_item.task_id}:")
            
            # 更新任务状态为失败
            try:
                log.info(f"更新PDF任务状态为失败 {task_item.task_id}...")
                update_success = await task_model.update_task_status(
                    task_item.task_id, TaskStatus.FAILED,
                    error_message=str(e)
                )
                if not update_success:
                    log.error(f"更新PDF任务状态为失败失败 {task_item.task_id}")
                else:
                    log.info(f"PDF任务状态已更新为失败 {task_item.task_id}")
            except Exception as update_error:
                log.error(f"更新PDF任务状态失败 {task_item.task_id}: {update_error}")

            # 如果页面处理器处理失败，尝试使用备用处理方式
            if "页面处理器" in str(e) or "page_processor" in str(e):
                log.warning(f" 页面处理器处理失败 {task_item.task_id}，尝试使用备用处理方式")
                try:
                    return await self._process_pdf_fallback(task_item)
                except Exception as fallback_error:
                    log.error(f"备用处理方式也失败 {task_item.task_id}: {fallback_error}")
                    raise fallback_error
            raise
        finally:
            # 注意: 不在这里清理临时文件，因为任务可能会重试
            # 临时文件将在任务队列的cleanup回调中清理
            pass
    
    async def _process_pdf_fallback(self, task_item: TaskItem) -> Dict[str, Any]:
        """PDF处理的备用方法 - 当页面处理器不可用时使用"""
        log = self._task_logger(task_item)
        start_time = time.time()

        try:
            log.info(f"=== 开始使用备用方法处理PDF任务 {task_item.task_id} ===")

            # 1. 更新任务状态为处理中
            log.info(f"更新备用PDF任务状态为处理中 {task_item.task_id}...")
            await task_model.update_task_status(
                task_item.task_id, TaskStatus.PROCESSING,
                progress=0.0, processed_pages=0
            )
            log.info(f"备用PDF任务状态已更新为处理中 {task_item.task_id}")

            # 2. 使用原有的PDF处理逻辑
            pdf_document = None
            try:
                pdf_document = fitz.open(stream=task_item.file_data, filetype="pdf")

                successful_pages = 0
                failed_pages = 0

                # 使用信号量控制并发
                semaphore = asyncio.Semaphore(self.api_config.get('concurrency', 3))  # 降低并发数

                # 分批处理策略
                batch_size = 2  # 备用方法使用更小的批次大小
                log.info(f"备用PDF处理批次大小: {batch_size}")

                batch_pages = []
                for page_number in range(1, task_item.total_pages + 1):
                    batch_pages.append(page_number)

                    # 当达到批次大小或最后一页时，处理当前批次
                    if len(batch_pages) >= batch_size or page_number == task_item.total_pages:
                        log.info(f"处理批次 {task_item.task_id}: 页面 {batch_pages}")
                        batch_results = await self._process_page_batch_fallback(
                            task_item.task_id, pdf_document, batch_pages, semaphore
                        )

                        # 统计批次结果
                        for result in batch_results:
                            if result.get('success', False):
                                successful_pages += 1
                            else:
                                failed_pages += 1

                        # 更新任务进度
                        progress = (successful_pages + failed_pages) / task_item.total_pages * 100
                        log.info(f"更新备用PDF任务进度 {task_item.task_id}: {progress:.1f}%")
                        await task_model.update_task_status(
                            task_item.task_id, TaskStatus.PROCESSING,
                            progress=progress, processed_pages=successful_pages + failed_pages
                        )
                        
                        # 清空批次并进行垃圾回收
                        batch_pages = []
                        import gc
                        gc.collect()
                        
                        # 批次间短暂休息，避免内存压力
                        if page_number < task_item.total_pages:
                            await asyncio.sleep(0.2)
                
                # 3. 计算最终结果
                total_time = time.time() - start_time
                success_rate = (successful_pages / task_item.total_pages * 100) if task_item.total_pages > 0 else 0

                # 4. 更新任务状态为完成
                log.info(f"更新备用PDF任务状态为完成 {task_item.task_id}...")
                await task_model.update_task_status(
                    task_item.task_id, TaskStatus.COMPLETED,
                    progress=100.0, processed_pages=successful_pages
                )
                log.info(f"备用PDF任务状态已更新为完成 {task_item.task_id}")

                # 5. 发送任务完成消息
                if self.websocket_manager:
                    log.info(f"发送备用PDF任务完成WebSocket消息 {task_item.task_id}...")
                    result_summary = {
                        'total_pages': task_item.total_pages,
                        'successful_pages': successful_pages,
                        'failed_pages': failed_pages,
                        'success_rate': f'{success_rate:.1f}%',
                        'processing_time': total_time,
                        'strategy_used': 'fallback',
                        'avg_time_per_page': total_time / task_item.total_pages if task_item.total_pages > 0 else 0
                    }

                    completed_message = MessageFactory.create_task_completed(
                        task_item.task_id, result_summary
                    )
                    await self.websocket_manager.send_to_task_subscribers(
                        task_item.task_id, completed_message.to_dict()
                    )
                    log.info(f"备用PDF任务完成消息已发送 {task_item.task_id}")

                # 6. 记录OCR活动用于IP滥用检测
                try:
                    task_data = await task_model.get_task(task_item.task_id)
                    if task_data and task_data.get('user_id') and task_data.get('client_ip'):
                        from models.database import ip_tracking_model
                        await ip_tracking_model.record_ocr_activity(
                            task_data['user_id'],
                            task_data['client_ip']
                        )
                        log.debug(f"已记录OCR活动(备用): 用户={task_data['user_id']}, IP={task_data['client_ip']}")
                except Exception as ip_error:
                    log.error(f"记录OCR活动失败(备用) {task_item.task_id}: {ip_error}")

                log.info(f"=== 备用PDF处理完成 {task_item.task_id}: {result_summary} (耗时: {total_time:.2f}s) ===")
                return result_summary

            finally:
                if pdf_document:
                    pdf_document.close()

        except Exception as e:
            total_time = time.time() - start_time
            log.error(f"=== 备用PDF处理失败 {task_item.task_id} (耗时: {total_time:.2f}s): {e} ===")
            log.exception(f"备用PDF处理失败详情 {task_item.task_id}:")

            # 更新任务状态为失败
            try:
                log.info(f"更新备用PDF任务状态为失败 {task_item.task_id}...")
                await task_model.update_task_status(
                    task_item.task_id, TaskStatus.FAILED,
                    error_message=str(e)
                )
                log.info(f"备用PDF任务状态已更新为失败 {task_item.task_id}")
            except Exception as update_error:
                log.error(f"更新备用PDF任务状态失败 {task_item.task_id}: {update_error}")

            raise
    
    async def _process_page_batch_fallback(self, task_id: str, pdf_document, page_numbers: list, semaphore: asyncio.Semaphore):
        """备用方法：处理页面批次（顺序处理）"""
        page_results = []

        # 顺序处理每一页（改为一个接一个处理，不并发）
        for page_number in page_numbers:
            try:
                result = await self._process_single_page_fallback(task_id, pdf_document, page_number, semaphore)
                page_results.append(result)
            except Exception as e:
                logger.error(f"备用页面处理异常 {task_id}-{page_number}: {e}")
                page_results.append({
                    'success': False,
                    'error': str(e),
                    'processing_time': 0
                })

        return page_results
    
    async def _process_single_page_fallback(self, task_id: str, pdf_document, page_number: int, semaphore: asyncio.Semaphore):
        """备用方法：处理单个PDF页面"""
        page_start_time = time.time()
        
        try:
            async with semaphore:
                # 转换页面为图片
                page = pdf_document.load_page(page_number - 1)
                
                # 使用较低的DPI以节省内存
                dpi = 150
                pix = page.get_pixmap(dpi=dpi)
                
                # 流式转换图片数据
                image_data = None
                with BytesIO() as buffer:
                    png_data = pix.tobytes("png")
                    buffer.write(png_data)
                    image_data = buffer.getvalue()
                
                # 立即释放内存
                pix = None
                page = None
                
                # 调用OCR API
                content = await self._call_ocr_api_protected(task_id, image_data)
                processing_time = time.time() - page_start_time
                
                # 清理图片数据
                image_data = None
                
                # 保存结果到数据库
                await page_result_model.create_page_result(task_id, page_number, "completed")
                await page_result_model.update_page_result(
                    task_id, page_number, "completed",
                    content=content, processing_time=processing_time
                )
                
                logger.debug(f"备用页面处理成功 {task_id}-{page_number}: {processing_time:.2f}s")
                
                return {
                    'success': True,
                    'page_number': page_number,
                    'content_length': len(content) if content else 0,
                    'processing_time': processing_time
                }
                
        except Exception as e:
            processing_time = time.time() - page_start_time
            logger.error(f"备用页面处理失败 {task_id}-{page_number}: {e}")
            
            # 保存错误结果
            await page_result_model.create_page_result(task_id, page_number, "failed")
            await page_result_model.update_page_result(
                task_id, page_number, "failed",
                error_message=str(e), processing_time=processing_time
            )
            
            return {
                'success': False,
                'page_number': page_number,
                'error': str(e),
                'processing_time': processing_time
            }
    
    async def _process_pdf_pages(self, task_item: TaskItem):
        """PDF页面处理生成器 - 优化内存使用和流式处理"""
        pdf_document = None
        try:
            # 使用流式方式打开PDF，避免全部加载到内存
            pdf_document = fitz.open(stream=task_item.file_data, filetype="pdf")
            
            # 使用信号量控制并发
            semaphore = asyncio.Semaphore(self.api_config.get('concurrency', 5))
            
            # 分批处理策略：根据文件大小和页数动态调整批次大小
            batch_size = self._calculate_batch_size(task_item.total_pages, task_item.file_size)
            logger.info(f"PDF处理批次大小: {batch_size}")
            
            batch_pages = []
            for page_number in range(1, task_item.total_pages + 1):
                batch_pages.append(page_number)
                
                # 当达到批次大小或最后一页时，处理当前批次
                if len(batch_pages) >= batch_size or page_number == task_item.total_pages:
                    async for result in self._process_page_batch(
                        task_item.task_id, pdf_document, batch_pages, semaphore
                    ):
                        yield result
                    
                    # 清空批次并进行垃圾回收
                    batch_pages = []
                    # 强制垃圾回收，释放内存
                    import gc
                    gc.collect()
                    
                    # 批次间短暂休息，避免内存压力
                    if page_number < task_item.total_pages:
                        await asyncio.sleep(0.1)
                    
        finally:
            if pdf_document:
                pdf_document.close()
    
    def _calculate_batch_size(self, total_pages: int, file_size: int) -> int:
        """根据文件大小和页数计算最优批次大小"""
        # 基础批次大小
        base_batch_size = 4
        
        # 根据文件大小调整（MB）
        file_size_mb = file_size / (1024 * 1024)
        if file_size_mb > 100:
            base_batch_size = 2  # 大文件使用小批次
        elif file_size_mb < 10:
            base_batch_size = 8  # 小文件使用大批次
        
        # 根据总页数调整
        if total_pages > 100:
            base_batch_size = min(base_batch_size, 3)  # 多页文件限制批次大小
        elif total_pages < 10:
            base_batch_size = min(base_batch_size, total_pages)  # 少页文件不超过总页数
        
        return max(1, base_batch_size)
    
    async def _process_page_batch(self, task_id: str, pdf_document, page_numbers: list, semaphore: asyncio.Semaphore):
        """处理页面批次"""
        batch_tasks = []
        
        for page_number in page_numbers:
            task = self._process_single_pdf_page(task_id, pdf_document, page_number, semaphore)
            batch_tasks.append(task)
        
        # 并发处理批次中的所有页面
        results = await asyncio.gather(*batch_tasks, return_exceptions=True)
        
        for i, result in enumerate(results):
            page_number = page_numbers[i]
            if isinstance(result, Exception):
                yield page_number, None, {
                    'success': False,
                    'error': str(result),
                    'processing_time': 0
                }
            else:
                yield result
    
    async def _process_single_pdf_page(self, task_id: str, pdf_document, page_number: int, semaphore: asyncio.Semaphore):
        """处理单个PDF页面"""
        page_start_time = time.time()
        
        try:
            # 转换页面为图片
            page = pdf_document.load_page(page_number - 1)
            
            # 动态调整DPI以平衡质量和内存使用
            dpi = self._calculate_optimal_dpi(page)
            pix = page.get_pixmap(dpi=dpi)
            
            # 流式转换图片数据
            image_data = None
            with BytesIO() as buffer:
                # 直接从pixmap获取PNG数据，避免PIL转换
                png_data = pix.tobytes("png")
                buffer.write(png_data)
                image_data = buffer.getvalue()
            
            # 立即释放内存
            pix = None
            page = None
            
            # 调用受保护的OCR API
            content = await self._call_ocr_api_protected(task_id, image_data, semaphore)
            processing_time = time.time() - page_start_time
            
            # 清理图片数据
            image_data = None
            
            return page_number, None, {
                'success': True,
                'content': content,
                'processing_time': processing_time
            }
            
        except Exception as e:
            processing_time = time.time() - page_start_time
            logger.error(f"处理PDF页面 {page_number} 失败: {e}")
            return page_number, None, {
                'success': False,
                'error': str(e),
                'processing_time': processing_time
            }
    
    def _calculate_optimal_dpi(self, page) -> int:
        """根据页面大小计算最优DPI"""
        # 获取页面尺寸
        rect = page.rect
        width, height = rect.width, rect.height

        # 基础DPI
        base_dpi = self.api_config.get('pdf_dpi', 200)

        # 根据页面大小调整DPI
        page_area = width * height

        if page_area > 500000:  # 大页面
            return max(150, base_dpi - 50)  # 降低DPI
        elif page_area < 100000:  # 小页面
            return min(300, base_dpi + 50)  # 提高DPI
        else:
            return base_dpi

    def _compress_image(self, image_data: bytes, compression_ratio: float) -> bytes:
        """
        压缩图片到指定比例
        :param image_data: 原始图片数据
        :param compression_ratio: 压缩比例(0.1-1.0)，如0.2表示压缩到原尺寸的20%
        :return: 压缩后的图片数据
        """
        try:
            # 如果压缩比例为1.0，直接返回原图
            if compression_ratio >= 1.0:
                return image_data

            # 限制压缩比例范围
            compression_ratio = max(0.1, min(1.0, compression_ratio))

            # 打开图片
            with BytesIO(image_data) as input_buffer:
                img = Image.open(input_buffer)

                # 计算新尺寸
                original_width, original_height = img.size
                new_width = int(original_width * compression_ratio)
                new_height = int(original_height * compression_ratio)

                # 压缩图片（使用LANCZOS算法保证质量）
                compressed_img = img.resize((new_width, new_height), Image.LANCZOS)

                # 转换为字节
                with BytesIO() as output_buffer:
                    compressed_img.save(output_buffer, format='PNG')
                    compressed_data = output_buffer.getvalue()

                size_saved_percent = (1 - len(compressed_data) / len(image_data)) * 100
                logger.info(f"图片压缩完成: {original_width}x{original_height} -> {new_width}x{new_height}, "
                           f"原大小: {len(image_data)//1024}KB, 压缩后: {len(compressed_data)//1024}KB, "
                           f"节省: {size_saved_percent:.1f}%")

                return compressed_data

        except Exception as e:
            logger.warning(f"图片压缩失败，使用原图: {e}")
            return image_data

    async def _call_ocr_api_protected(self, task_id: str, image_data: bytes,
                                    semaphore: Optional[asyncio.Semaphore] = None) -> str:
        """调用受保护的OCR API"""
        system_integration = get_system_integration()

        async def _api_call_operation():
            return await self._call_ocr_api(task_id, image_data, semaphore)

        if system_integration:
            return await system_integration.protected_api_call(
                task_id, _api_call_operation, max_retries=1
            )
        else:
            return await self._call_ocr_api(task_id, image_data, semaphore)
    
    async def _call_ocr_api(self, task_id: str, image_data: bytes, semaphore: Optional[asyncio.Semaphore] = None) -> str:
        """调用OCR API - 基础实现"""
        system_prompt = """
        You are an expert OCR assistant. Your job is to extract all text from the provided image and convert it into a well-structured, easy-to-read Markdown document that mirrors the intended structure of the original. Follow these precise guidelines:

- Use Markdown headings, paragraphs, lists, and tables to match the document’s hierarchy and flow.  
- For tables, use standard Markdown table syntax and merge cells if needed. If a table has a title, include it as plain text above the table.  
- Render mathematical formulas with LaTeX syntax: use $...$ for inline and $$...$$ for display equations.  
- For images, use the syntax ![descriptive alt text](link) with a clear, descriptive alt text.  
- Remove unnecessary line breaks so that the text flows naturally without awkward breaks.
- Your final Markdown output must be direct text (do not wrap it in code blocks).

Ensure your output is clear, accurate, and faithfully reflects the original image’s content and structure.
        """
        
        if semaphore:
            async with semaphore:
                return await self._make_api_request_with_timeout(task_id, image_data, system_prompt)
        else:
            return await self._make_api_request_with_timeout(task_id, image_data, system_prompt)
    
    async def _save_moderation_log(self, task_id: str, page_number: int, action: str, details: Dict[str, Any], original_content: str = None):
        """保存审查日志到数据库"""
        try:
            from models.database import db_manager
            async with db_manager.get_connection() as db:
                await db.execute("""
                    INSERT INTO moderation_logs
                    (task_id, page_number, action, risk_level, violation_types, reason, original_content, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    task_id,
                    page_number,
                    action,
                    details.get('risk_level', 'high'),
                    json.dumps(details.get('violation_types', []), ensure_ascii=False),
                    details.get('reason', ''),
                    original_content,
                    datetime.now(timezone.utc).isoformat()
                ))
                await db.commit()
                logger.info(f"审查日志已保存 {task_id}-{page_number}")
        except Exception as e:
            logger.error(f"保存审查日志失败 {task_id}: {e}")

    async def _make_api_request_with_timeout(self, task_id: str, image_data: bytes, system_prompt: str, timeout: int = 120) -> str:
        """发起API请求 - 增加超时控制"""
        try:
            return await asyncio.wait_for(
                self._make_api_request(task_id, image_data, system_prompt),
                timeout=timeout
            )
        except asyncio.TimeoutError:
            raise asyncio.TimeoutError(f"API请求超时 (>{timeout}秒)")
    
    async def _make_api_request(self, task_id: str, image_data: bytes, system_prompt: str) -> str:
        """发起API请求（包含审查）"""
        api_base_url = self.api_config.get('api_base_url', 'https://api.openai.com')
        api_key = self.api_config.get('api_key')
        model = self.api_config.get('model', 'gpt-4o')

        if not api_key:
            raise Exception("API密钥未配置")

        encoded_image = base64.b64encode(image_data).decode('utf-8')

        payload: Dict[str, Any] = {
            "messages": [
                {
                    "role": "system",
                    "content": system_prompt
                },
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "text",
                            "text": "Analyze the provided image and output a JSON object with a single string field named \"content\". The value must contain the recognized Markdown-formatted text without additional commentary."
                        },
                        {
                            "type": "image_url",
                            "image_url": {
                                "url": f"data:image/png;base64,{encoded_image}"
                            }
                        }
                    ]
                }
            ],
            "stream": False,
            "model": model,
            "temperature": 0.0,
            "presence_penalty": 0,
            "frequency_penalty": 0,
            "top_p": 1,
        }

        async with aiohttp.ClientSession() as session:
            response = await session.post(
                f"{api_base_url}/v1/chat/completions",
                headers={"Authorization": f"Bearer {api_key}"},
                json=payload,
            )

            if response.status == 200:
                result = await response.json()
                message = None
                choices = result.get('choices')
                if isinstance(choices, list) and choices:
                    message = choices[0].get('message') if isinstance(choices[0], dict) else None

                raw_content: Optional[str] = None
                if isinstance(message, dict):
                    content_field = message.get('content')
                    if isinstance(content_field, list):
                        text_parts: List[str] = []
                        for block in content_field:
                            if not isinstance(block, dict):
                                continue
                            block_type = block.get('type')
                            if block_type in ('text', 'output_text') and isinstance(block.get('text'), str):
                                text_parts.append(block['text'])
                        raw_content = ''.join(text_parts) if text_parts else None
                    elif isinstance(content_field, str):
                        raw_content = content_field

                if not raw_content:
                    raise Exception("OCR模型返回内容为空或结构不支持")

                # 解析返回内容
                parsed_content = _extract_markdown_from_response(raw_content)
                if not parsed_content:
                    raise Exception("OCR返回内容为空或格式不正确")

                # ========== 内容道德审查 ==========
                from core.content_moderator import ContentModerator, ModerationFailedException, get_moderation_config

                # 初始化审查器
                moderator = ContentModerator(get_moderation_config())

                # 查询用户是否为管理员
                user_is_admin = False
                if task_id:
                    try:
                        from models.database import task_model, db_manager
                        task_data = await task_model.get_task(task_id)
                        if task_data and task_data.get('user_id'):
                            # 直接查询数据库获取用户信�?
                            async with db_manager.get_connection() as db:
                                async with db.execute(
                                    "SELECT is_admin FROM users WHERE id = ?",
                                    (task_data['user_id'],)
                                ) as cursor:
                                    user_row = await cursor.fetchone()
                                    user_is_admin = bool(user_row[0]) if user_row else False
                    except Exception as e:
                        logger.warning(f"查询用户管理员权限失�?{task_id}: {e}")

                # 执行审查
                action, moderated_content, details = await moderator.moderate_content(
                    content=parsed_content,
                    task_id=task_id,
                    user_is_admin=user_is_admin
                )
                # 审查通过，返回内�?
                return moderated_content
                # 注意：ModerationFailedException会直接向上传播，由_process_image_task捕获处理
                # ==================================

            else:
                error_detail = ""
                try:
                    error_payload = await response.text()
                    if error_payload:
                        try:
                            error_json = json.loads(error_payload)
                            if isinstance(error_json, dict):
                                error_obj = error_json.get('error')
                                if isinstance(error_obj, dict):
                                    message = error_obj.get('message')
                                    if isinstance(message, str):
                                        error_detail = message.strip()
                                if not error_detail and isinstance(error_json.get('message'), str):
                                    error_detail = error_json['message'].strip()
                        except json.JSONDecodeError:
                            error_detail = error_payload.strip()
                except Exception as error_read_exc:
                    logger.warning(f"读取API错误响应失败 {task_id}: {error_read_exc}")

                detail_suffix = f"，错误信�? {error_detail}" if error_detail else ""
                raise Exception(f"API请求失败, 状态码: {response.status}{detail_suffix}")

    
    async def _refund_task_quota(self, task: Optional[Dict[str, Any]]):
        """根据任务信息返还未消耗的配额"""
        if not task:
            return

        user_id = task.get('user_id')
        if not user_id:
            return

        pages_to_refund = 0
        task_type_value = task.get('task_type')
        if task_type_value == TaskType.IMAGE_OCR.value:
            pages_to_refund = 1
        elif task_type_value == TaskType.PDF_OCR.value:
            total_pages = task.get('total_pages') or 0
            processed_pages = task.get('processed_pages') or 0
            if total_pages:
                pages_to_refund = max(total_pages - processed_pages, 0)
                if pages_to_refund == 0 and task.get('status') != TaskStatus.COMPLETED.value:
                    pages_to_refund = total_pages

        if pages_to_refund <= 0:
            return

        try:
            auth_manager = get_auth_manager()
        except Exception:
            logger.warning("无法获取认证管理器，跳过配额返还")
            return

        try:
            await auth_manager.user_model.refund_pages(user_id, pages_to_refund)
            logger.info(f"任务 {task.get('id', task.get('task_id'))} 取消，已返还 {pages_to_refund} 页配额给用户 {user_id}")
        except Exception as exc:
            logger.error(f"返还用户配额失败 {user_id}: {exc}")

    async def _send_task_cancelled_message(self, task_id: str, reason: str):
        """通知订阅者任务已取消"""
        if not self.websocket_manager:
            return

        try:
            message = MessageFactory.create_task_cancelled(task_id, reason)
            await self.websocket_manager.send_to_task_subscribers(
                task_id, message.to_dict()
            )
            logger.info(f"发送任务取消消息: {task_id}")
        except Exception as exc:
            logger.error(f"发送任务取消消息失败 {task_id}: {exc}")

    async def _send_task_started_message(self, task_item: TaskItem):
        """发送任务开始消息"""
        try:
            if self.websocket_manager:
                # 估算处理时间：图片任务3秒，PDF任务每页3秒
                estimated_time = 3 if task_item.task_type == TaskType.IMAGE_OCR else task_item.total_pages * 3
                
                message = MessageFactory.create_task_started(
                    task_item.task_id,
                    task_item.task_type.value,
                    task_item.file_name,
                    task_item.total_pages,
                    estimated_time
                )
                
                await self.websocket_manager.send_to_task_subscribers(
                    task_item.task_id, message.to_dict()
                )
                
                logger.debug(f"发送任务开始消息: {task_item.task_id}")
        except Exception as e:
            logger.error(f"发送任务开始消息失败 {task_item.task_id}: {e}")
    
    async def _send_error_notification(self, task_id: str, error_type: str,
                                     error_message: str, severity: str = "error"):
        """发送错误通知消息"""
        try:
            if self.websocket_manager:
                message = MessageFactory.create_error_notification(
                    error_type, error_message, task_id, severity
                )
                
                await self.websocket_manager.send_to_task_subscribers(
                    task_id, message.to_dict()
                )
                
                logger.debug(f"发送错误通知消息: {task_id}")
        except Exception as e:
            logger.error(f"发送错误通知消息失败 {task_id}: {e}")

# 创建全局任务管理器实例
def create_task_manager(runtime_config, api_config: Dict[str, Any], websocket_manager=None) -> TaskManager:
    """
    创建任务管理器实例
    :param runtime_config: 运行时配置对象引用
    :param api_config: API配置
    :param websocket_manager: WebSocket管理器
    """
    global image_worker_pool, pdf_worker_pool

    # 从配置中获取并发数
    image_concurrency = api_config.get('image_concurrency', 2)
    pdf_concurrency = api_config.get('pdf_concurrency', 2)

    # 创建工作者池
    image_worker_pool = WorkerPool(
        image_task_queue,
        min_workers=image_concurrency,
        max_workers=image_concurrency,
        worker_timeout=float('inf'),
        name='image'
    )

    pdf_worker_pool = WorkerPool(
        pdf_task_queue,
        min_workers=pdf_concurrency,
        max_workers=pdf_concurrency,
        worker_timeout=float('inf'),
        name='pdf'
    )

    logger.info(f"创建工作者池 - 图片并发数: {image_concurrency}, PDF并发数: {pdf_concurrency}")

    return TaskManager(
        image_task_queue, pdf_task_queue,
        image_worker_pool, pdf_worker_pool,
        runtime_config, api_config, websocket_manager
    )

# 全局任务管理器实例（需要在应用启动时初始化）
task_manager: Optional[TaskManager] = None

async def init_task_manager(runtime_config, api_config: Dict[str, Any], websocket_manager=None):
    """
    初始化全局任务管理器
    :param runtime_config: 运行时配置对象引用
    :param api_config: API配置
    :param websocket_manager: WebSocket管理器
    """
    global task_manager

    # 强制重新初始化，确保每次启动都能正确初始化
    logger.info("开始初始化全局任务管理器（双队列模式）...")

    # 如果已存在，先关闭旧的实例
    if task_manager is not None:
        try:
            logger.info("关闭现有的任务管理器实例...")
            await task_manager.shutdown()
        except Exception as e:
            logger.warning(f"关闭现有任务管理器失败: {e}")

    # 创建新的任务管理器实例
    task_manager = create_task_manager(runtime_config, api_config, websocket_manager)

    # 初始化任务管理器
    await task_manager.initialize()

    # 验证初始化状态
    if task_manager and task_manager._initialized:
        logger.info("全局任务管理器初始化完成（双队列模式）")
    else:
        logger.error("全局任务管理器初始化失败")
        raise Exception("任务管理器初始化验证失败")

async def shutdown_task_manager():
    """关闭全局任务管理器"""
    global task_manager
    if task_manager:
        await task_manager.shutdown()
        task_manager = None
        logger.info("全局任务管理器已关闭")
