import asyncio
import uuid
import logging
import time
import fitz  # PyMuPDF
from typing import Dict, Any, Optional, List, AsyncGenerator, Tuple
from datetime import datetime, timezone
import aiohttp
import json
from enum import Enum

from models.database import (
    TaskStatus, TaskType, task_model, page_result_model,
    task_progress_model, page_batch_model
)
from core.image_utils import compress_image_to_target_ratio
from core.ocr_client import OCRServiceError, request_ocr_markdown
from core.ocr_policy import (
    OCR_SYSTEM_PROMPT,
    OCR_USER_PROMPT,
    build_ocr_request_config,
    resolve_timeout_seconds,
)

logger = logging.getLogger(__name__)

class PageStrategy(Enum):
    """分页策略枚举"""
    AUTO = "auto"        # 自动策略
    FIXED = "fixed"      # 固定大小
    ADAPTIVE = "adaptive"  # 自适应策略

class PageProcessor:
    """分页处理核心类 - 智能分页策略和并行处理"""
    
    def __init__(self, runtime_config=None, api_config: Optional[Dict[str, Any]] = None):
        """
        初始化分页处理器
        :param runtime_config: 运行时配置对象引用(用于动态读取可变配置)
        :param api_config: API静态配置(api_base_url, api_key, model等)
        """
        # 保存runtime_config引用用于动态配置
        self._runtime_config = runtime_config
        # 保存静态API配置
        self.api_config = api_config or {}

        self._fallback_max_concurrent_pages = self.api_config.get('concurrency', 5)
        self._fallback_batch_size = self.api_config.get('batch_size', 4)
        self._fallback_pdf_dpi = self.api_config.get('pdf_dpi', 200)

        # 固定配置(不会变化的)
        try:
            self.retry_delay = max(float(self.api_config.get('retry_delay', 0.5)), 0.1)
        except (TypeError, ValueError):
            self.retry_delay = 0.5
        self.health_check_interval = self.api_config.get('health_check_interval', 300)

        # 会话管理
        self._session = None  # 延迟创建 aiohttp.ClientSession
        self._session_lock = asyncio.Lock()
        self._pdf_render_lock = asyncio.Lock()
        self._last_health_check_ts = 0.0
        self._last_health_check_ok = True

        logger.info(f"分页处理器初始化完成 - 并发: {self.max_concurrent_pages}, 批次: {self.default_batch_size}")

    def _get_runtime_positive_int(self, field_name: str, fallback: int, minimum: int = 1) -> int:
        if self._runtime_config is not None and hasattr(self._runtime_config, field_name):
            try:
                value = int(getattr(self._runtime_config, field_name))
            except (TypeError, ValueError):
                value = fallback
        else:
            value = fallback
        return max(minimum, value)

    @property
    def max_concurrent_pages(self) -> int:
        return self._get_runtime_positive_int(
            'concurrency',
            int(self.api_config.get('concurrency', self._fallback_max_concurrent_pages)),
        )

    @property
    def default_batch_size(self) -> int:
        return self._get_runtime_positive_int(
            'batch_size',
            int(self.api_config.get('batch_size', self._fallback_batch_size)),
        )

    @property
    def pdf_dpi(self) -> int:
        return self._get_runtime_positive_int(
            'pdf_dpi',
            int(self.api_config.get('pdf_dpi', self._fallback_pdf_dpi)),
            minimum=72,
        )

    @property
    def max_retries(self) -> int:
        """动态获取最大重试次数"""
        if self._runtime_config:
            retries = getattr(self._runtime_config, 'max_retries', 3)
            logger.debug(f"从runtime_config读取max_retries: {retries}")
            return retries
        return 3

    @property
    def api_timeout(self) -> int:
        """动态获取API超时时间"""
        timeout = resolve_timeout_seconds(self._runtime_config, 120)
        logger.debug(f"从runtime_config读取api_timeout: {timeout}")
        return timeout

    async def _get_log_prefix_for_task(self, task_id: str) -> str:
        """获取任务的日志前缀（包含文件名）"""
        try:
            task_data = await task_model.get_task(task_id)
            if task_data and task_data.get('file_name'):
                return f"[{task_data['file_name']}] "
        except:
            pass
        return ''

    def calculate_page_strategy(self, task_id: str, total_pages: int, file_size: int) -> Dict[str, Any]:
        """
        计算最优分页策略
        :param task_id: 任务ID
        :param total_pages: 总页数
        :param file_size: 文件大小（字节）
        :return: 分页策略配置
        """
        file_size_mb = file_size / (1024 * 1024)
        
        # 基础策略配置
        strategy = {
            'type': PageStrategy.AUTO,
            'batch_size': self.default_batch_size,
            'max_concurrent_batches': 2,
            'priority_pages': [],  # 优先处理的页面
            'estimated_time_per_page': 3.0,  # 每页预估时间（秒）
            'memory_optimization': False
        }
        
        # 根据文件大小调整策略
        if file_size_mb > 100:  # 大文件（>100MB）
            strategy.update({
                'type': PageStrategy.ADAPTIVE,
                'batch_size': 2,  # 小批次
                'max_concurrent_batches': 1,  # 减少并发
                'memory_optimization': True,
                'estimated_time_per_page': 4.0
            })
            logger.info(f"任务 {task_id} 使用大文件策略: 批次大小=2")
            
        elif file_size_mb < 10:  # 小文件（<10MB）
            strategy.update({
                'type': PageStrategy.FIXED,
                'batch_size': min(8, total_pages),  # 大批次，但不超过总页数
                'max_concurrent_batches': 3,
                'estimated_time_per_page': 2.5
            })
            logger.info(f"任务 {task_id} 使用小文件策略: 批次大小={strategy['batch_size']}")
            
        else:  # 中等文件
            strategy.update({
                'type': PageStrategy.AUTO,
                'batch_size': self.default_batch_size,
                'max_concurrent_batches': 2,
                'estimated_time_per_page': 3.0
            })
            logger.info(f"任务 {task_id} 使用标准策略: 批次大小={strategy['batch_size']}")
        
        # 根据总页数调整
        if total_pages > 100:  # 大量页面
            strategy['batch_size'] = min(strategy['batch_size'], 3)
            strategy['max_concurrent_batches'] = 1
            strategy['estimated_time_per_page'] = 3.5
            logger.info(f"任务 {task_id} 调整为大页数策略")
            
        elif total_pages <= 5:  # 少量页面
            strategy['batch_size'] = total_pages
            strategy['max_concurrent_batches'] = 1
            strategy['estimated_time_per_page'] = 2.0
            logger.info(f"任务 {task_id} 调整为小页数策略")
        
        # 计算预估总时间
        strategy['estimated_total_time'] = int(total_pages * strategy['estimated_time_per_page'])
        
        return strategy
    
    async def create_page_batches(self, task_id: str, total_pages: int, strategy: Dict[str, Any]) -> List[str]:
        """
        创建页面批次
        :param task_id: 任务ID
        :param total_pages: 总页数
        :param strategy: 分页策略
        :return: 批次ID列表
        """
        batch_size = strategy['batch_size']
        batch_ids = []
        batch_number = 1
        
        try:
            for page_start in range(1, total_pages + 1, batch_size):
                page_end = min(page_start + batch_size - 1, total_pages)
                pages_count = page_end - page_start + 1
                
                batch_id = f"{task_id}_batch_{batch_number:03d}"
                
                # 创建批次记录
                success = await page_batch_model.create_batch(
                    batch_id=batch_id,
                    task_id=task_id,
                    batch_number=batch_number,
                    page_start=page_start,
                    page_end=page_end,
                    pages_count=pages_count
                )
                
                if success:
                    batch_ids.append(batch_id)
                else:
                    logger.error(f"批次创建失败 {batch_id}")
                
                batch_number += 1
            
            logger.info(f"任务 {task_id} 创建了 {len(batch_ids)} 个批次")
            return batch_ids
            
        except Exception as e:
            logger.error(f"创建页面批次失败 {task_id}: {e}")
            return []
    
    async def process_pdf_from_file(self,
                                    task_id: str,
                                    file_path: str,
                                    total_pages: int,
                                    websocket_manager=None,
                                    file_name: str = None) -> Dict[str, Any]:
        """
        从临时文件分页处理PDF文档 - 节省内存
        :param task_id: 任务ID
        :param file_path: PDF临时文件路径
        :param total_pages: 总页数
        :param websocket_manager: WebSocket管理器
        :param file_name: 文件名（用于日志前缀）
        :return: 处理结果摘要
        """
        import os
        start_time = time.time()
        log_prefix = f"[{file_name}] " if file_name else ""

        try:
            # 获取文件大小
            file_size = os.path.getsize(file_path)

            # 计算分页策略
            strategy = self.calculate_page_strategy(task_id, total_pages, file_size)
            logger.info(f"{log_prefix}任务 {task_id} 分页策略(临时文件模式): {strategy}")

            # 创建页面批次
            batch_ids = await self.create_page_batches(task_id, total_pages, strategy)
            if not batch_ids:
                raise Exception("创建页面批次失败")

            # 初始化页面结果记录
            await self._initialize_page_results(task_id, total_pages)

            # 开始分批处理
            successful_pages = 0
            failed_pages = 0

            pdf_document = None
            try:
                # 从文件路径打开PDF,而非内存
                pdf_document = await self._open_pdf_document_from_file(file_path)
                logger.info(f"从临时文件打开PDF: {file_path}")

                # 使用信号量控制并发批次
                batch_semaphore = asyncio.Semaphore(strategy['max_concurrent_batches'])
                page_semaphore = asyncio.Semaphore(self.max_concurrent_pages)

                # 顺序处理批次
                batch_results = []
                for batch_id in batch_ids:
                    try:
                        result = await self._process_batch(
                            batch_id, task_id, pdf_document,
                            batch_semaphore, page_semaphore,
                            strategy, websocket_manager
                        )
                        batch_results.append(result)
                    except Exception as e:
                        logger.error(f"批次处理异常 {batch_id}: {e}")
                        batch_results.append(e)

                # 统计结果
                for i, result in enumerate(batch_results):
                    batch_id = batch_ids[i]
                    if isinstance(result, Exception):
                        logger.error(f"批次处理异常 {batch_id}: {result}")
                        batch_info = await page_batch_model.get_batch(batch_id)
                        if batch_info:
                            failed_pages += batch_info['pages_count']
                    else:
                        successful_pages += result.get('successful_pages', 0)
                        failed_pages += result.get('failed_pages', 0)

            finally:
                if pdf_document:
                    await self._close_pdf_document(pdf_document)

            # 计算最终结果
            total_time = time.time() - start_time
            success_rate = (successful_pages / total_pages * 100) if total_pages > 0 else 0

            file_size_mb = round(file_size / (1024 * 1024), 2)
            result_summary = {
                'total_pages': total_pages,
                'successful_pages': successful_pages,
                'failed_pages': failed_pages,
                'success_rate': f'{success_rate:.1f}%',
                'processing_time': total_time,
                'file_size_mb': file_size_mb
            }

            logger.info(f"{log_prefix}分页处理完成(临时文件模式) {task_id}: {result_summary}")
            return result_summary

        except Exception as e:
            logger.error(f"{log_prefix}分页处理失败(临时文件模式) {task_id}: {e}")
            raise

    async def process_pdf_with_pagination(self,
                                        task_id: str,
                                        file_data: bytes,
                                        total_pages: int,
                                        websocket_manager=None,
                                        file_name: str = None) -> Dict[str, Any]:
        """
        分页处理PDF文档(内存模式-兼容旧代码)
        :param task_id: 任务ID
        :param file_data: PDF文件数据
        :param total_pages: 总页数
        :param websocket_manager: WebSocket管理器
        :param file_name: 文件名（用于日志前缀）
        :return: 处理结果摘要
        """
        start_time = time.time()
        log_prefix = f"[{file_name}] " if file_name else ""

        try:
            # 计算分页策略
            strategy = self.calculate_page_strategy(task_id, total_pages, len(file_data))
            logger.info(f"{log_prefix}任务 {task_id} 分页策略: {strategy}")

            # 创建页面批次
            batch_ids = await self.create_page_batches(task_id, total_pages, strategy)
            if not batch_ids:
                raise Exception("创建页面批次失败")

            # 初始化页面结果记录
            await self._initialize_page_results(task_id, total_pages)

            # 开始分批处理
            successful_pages = 0
            failed_pages = 0

            pdf_document = None
            try:
                pdf_document = await self._open_pdf_document_from_bytes(file_data)
                
                # 使用信号量控制并发批次
                batch_semaphore = asyncio.Semaphore(strategy['max_concurrent_batches'])
                page_semaphore = asyncio.Semaphore(self.max_concurrent_pages)
                
                # 顺序处理批次（改为一个接一个处理，不并发）
                batch_results = []
                for batch_id in batch_ids:
                    try:
                        result = await self._process_batch(
                            batch_id, task_id, pdf_document,
                            batch_semaphore, page_semaphore,
                            strategy, websocket_manager
                        )
                        batch_results.append(result)
                    except Exception as e:
                        logger.error(f"批次处理异常 {batch_id}: {e}")
                        batch_results.append(e)
                
                # 统计结果
                for i, result in enumerate(batch_results):
                    batch_id = batch_ids[i]
                    if isinstance(result, Exception):
                        logger.error(f"批次处理异常 {batch_id}: {result}")
                        # 获取批次页面数并计入失败
                        batch_info = await page_batch_model.get_batch(batch_id)
                        if batch_info:
                            failed_pages += batch_info['pages_count']
                    else:
                        successful_pages += result.get('successful_pages', 0)
                        failed_pages += result.get('failed_pages', 0)
                
            finally:
                if pdf_document:
                    await self._close_pdf_document(pdf_document)
            
            # 计算最终结果
            total_time = time.time() - start_time
            success_rate = (successful_pages / total_pages * 100) if total_pages > 0 else 0
            
            file_size_mb = round(len(file_data) / (1024 * 1024), 2)
            result_summary = {
                'total_pages': total_pages,
                'successful_pages': successful_pages,
                'failed_pages': failed_pages,
                'success_rate': f'{success_rate:.1f}%',
                'processing_time': total_time,
                'file_size_mb': file_size_mb
            }
            
            logger.info(f"{log_prefix}分页处理完成 {task_id}: {result_summary}")
            return result_summary

        except Exception as e:
            logger.error(f"{log_prefix}分页处理失败 {task_id}: {e}")
            raise
    
    async def _initialize_page_results(self, task_id: str, total_pages: int):
        """初始化所有页面结果记录"""
        try:
            for page_number in range(1, total_pages + 1):
                await page_result_model.create_page_result(task_id, page_number, "pending")
        except Exception as e:
            logger.error(f"初始化页面结果记录失败 {task_id}: {e}")
            raise
    
    async def _process_batch(self, 
                            batch_id: str,
                            task_id: str,
                            pdf_document,
                            batch_semaphore: asyncio.Semaphore,
                            page_semaphore: asyncio.Semaphore,
                            strategy: Dict[str, Any],
                            websocket_manager=None) -> Dict[str, Any]:
        """
        处理单个批次
        :param batch_id: 批次ID
        :param task_id: 任务ID
        :param pdf_document: PDF文档对象
        :param batch_semaphore: 批次信号量
        :param page_semaphore: 页面信号量
        :param strategy: 处理策略
        :param websocket_manager: WebSocket管理器
        :return: 批次处理结果
        """
        async with batch_semaphore:
            batch_start_time = time.time()

            try:
                # 获取批次信息
                batch_info = await page_batch_model.get_batch(batch_id)
                if not batch_info:
                    raise Exception(f"批次信息不存在: {batch_id}")
                
                page_start = batch_info['page_start']
                page_end = batch_info['page_end']
                pages_count = batch_info['pages_count']
                
                logger.info(f"开始处理批次 {batch_id}: 页面 {page_start}-{page_end}")
                
                # 更新批次状态为处理中
                await page_batch_model.update_batch_status(batch_id, "processing")
                
                # 并发处理批次内的页面（根据CONCURRENCY环境变量）
                page_tasks = []
                for page_number in range(page_start, page_end + 1):
                    task = self._process_single_page(
                        task_id, page_number, pdf_document,
                        page_semaphore, batch_id, strategy,
                        websocket_manager
                    )
                    page_tasks.append(task)

                # 并发执行所有页面任务
                page_results = await asyncio.gather(*page_tasks, return_exceptions=True)
                
                # 统计批次结果
                successful_pages = 0
                failed_pages = 0
                
                for i, result in enumerate(page_results):
                    page_number = page_start + i
                    if isinstance(result, Exception):
                        logger.error(f"页面处理异常 {task_id}-{page_number}: {result}")
                        failed_pages += 1
                        await page_result_model.update_page_result(
                            task_id, page_number, "failed",
                            error_message=str(result),
                            batch_id=batch_id
                        )
                    else:
                        if result.get('success', False):
                            successful_pages += 1
                        else:
                            failed_pages += 1
                
                # 计算批次处理时间
                batch_time = time.time() - batch_start_time
                
                # 更新批次状态
                batch_status = "completed" if failed_pages == 0 else "partial"
                await page_batch_model.update_batch_status(
                    batch_id, batch_status,
                    processed_pages=successful_pages + failed_pages,
                    failed_pages=failed_pages,
                    processing_time=batch_time
                )
                
                # 从batch_id中提取task_id获取文件名
                task_id_from_batch = batch_id.rsplit('_batch_', 1)[0] if '_batch_' in batch_id else task_id
                batch_log_prefix = await self._get_log_prefix_for_task(task_id_from_batch)
                logger.info(f"{batch_log_prefix}批次处理完成 {batch_id}: 成功 {successful_pages}, 失败 {failed_pages}, 耗时 {batch_time:.2f}s")
                
                return {
                    'batch_id': batch_id,
                    'successful_pages': successful_pages,
                    'failed_pages': failed_pages,
                    'processing_time': batch_time
                }
                
            except Exception as e:
                # 更新批次状态为失败
                await page_batch_model.update_batch_status(batch_id, "failed")
                logger.error(f"批次处理失败 {batch_id}: {e}")
                raise
    
    async def _process_single_page(self,
                                  task_id: str,
                                  page_number: int,
                                  pdf_document,
                                  semaphore: asyncio.Semaphore,
                                  batch_id: str,
                                  strategy: Dict[str, Any],
                                  websocket_manager=None) -> Dict[str, Any]:
        """
        处理单个页面
        :param task_id: 任务ID
        :param page_number: 页面号
        :param pdf_document: PDF文档对象
        :param semaphore: 信号量
        :param batch_id: 批次ID
        :param strategy: 处理策略
        :param websocket_manager: WebSocket管理器
        :return: 页面处理结果
        """
        page_start_time = time.time()
        
        try:
            # 更新页面状态为处理中
            await page_result_model.update_page_result(
                task_id, page_number, "processing",
                batch_id=batch_id
            )
            
            image_data = await self._render_page_to_png(pdf_document, page_number, strategy)

            # 压缩图片（如果配置了压缩比例）
            # 只压缩大于1MB的图片，避免对小图片的无谓开销
            MIN_COMPRESS_SIZE = 1 * 1024 * 1024  # 1MB
            if (self._runtime_config and
                hasattr(self._runtime_config, 'image_compression_ratio') and
                len(image_data) > MIN_COMPRESS_SIZE):
                compression_ratio = self._runtime_config.image_compression_ratio
                if compression_ratio < 1.0:
                    image_data = await self._compress_image_async(image_data, compression_ratio)

            # 调用 OCR API 并支持重试
            content = await self._call_ocr_api_with_retry(image_data, semaphore, task_id=task_id)
            processing_time = time.time() - page_start_time
            
            # 更新页面结果
            await page_result_model.update_page_result(
                task_id, page_number, "completed",
                content=content,
                processing_time=processing_time,
                batch_id=batch_id
            )
            
            # 清理图片数据
            image_data = None

            return {
                'success': True,
                'page_number': page_number,
                'content_length': len(content) if content else 0,
                'processing_time': processing_time
            }

        except Exception as e:
            processing_time = time.time() - page_start_time
            logger.error(f"页面处理失败 {task_id}-{page_number}: {e}")
            
            # 更新页面结果为失败
            await page_result_model.update_page_result(
                task_id, page_number, "failed",
                error_message=str(e),
                processing_time=processing_time,
                batch_id=batch_id
            )
            
            return {
                'success': False,
                'page_number': page_number,
                'error': str(e),
                'processing_time': processing_time
            }
    
    def _calculate_optimal_dpi(self, width: float, height: float, strategy: Dict[str, Any]) -> int:
        """根据页面尺寸和策略计算最优 DPI。"""
        page_area = width * height

        # 基础DPI
        base_dpi = self.pdf_dpi

        # 根据内存优化策略调整
        if strategy.get('memory_optimization', False):
            base_dpi = min(base_dpi, 150)  # 降低DPI以节省内存

        # 根据页面大小调整DPI
        if page_area > 500000:  # 大页面
            return max(120, base_dpi - 50)
        elif page_area < 100000:  # 小页面
            return min(250, base_dpi + 30)
        else:
            return base_dpi

    @staticmethod
    def _open_pdf_document_from_file_sync(file_path: str):
        """同步打开 PDF 文件，供线程池调用。"""
        return fitz.open(file_path)

    @staticmethod
    def _open_pdf_document_from_bytes_sync(file_data: bytes):
        """同步从字节流打开 PDF，供线程池调用。"""
        return fitz.open(stream=file_data, filetype="pdf")

    @staticmethod
    def _close_pdf_document_sync(pdf_document) -> None:
        """同步关闭 PDF 文档，供线程池调用。"""
        pdf_document.close()

    def _render_page_to_png_sync(self, pdf_document, page_number: int, strategy: Dict[str, Any]) -> bytes:
        """CPU 密集型 PDF 渲染（同步函数，供线程池调用）。"""
        page = pdf_document.load_page(page_number - 1)
        dpi = self._calculate_optimal_dpi(page.rect.width, page.rect.height, strategy)
        pix = page.get_pixmap(dpi=dpi)
        return pix.tobytes("png")

    async def _open_pdf_document_from_file(self, file_path: str):
        """在线程池中打开 PDF 文件，避免阻塞事件循环。"""
        async with self._pdf_render_lock:
            return await asyncio.to_thread(self._open_pdf_document_from_file_sync, file_path)

    async def _open_pdf_document_from_bytes(self, file_data: bytes):
        """在线程池中从字节流打开 PDF，避免阻塞事件循环。"""
        async with self._pdf_render_lock:
            return await asyncio.to_thread(self._open_pdf_document_from_bytes_sync, file_data)

    async def _close_pdf_document(self, pdf_document) -> None:
        """在线程池中关闭 PDF 文档，避免阻塞事件循环。"""
        async with self._pdf_render_lock:
            await asyncio.to_thread(self._close_pdf_document_sync, pdf_document)

    async def _render_page_to_png(self, pdf_document, page_number: int, strategy: Dict[str, Any]) -> bytes:
        """在线程池中执行 PDF 渲染，避免阻塞事件循环。"""
        async with self._pdf_render_lock:
            return await asyncio.to_thread(
                self._render_page_to_png_sync,
                pdf_document,
                page_number,
                strategy,
            )

    def _compress_image(self, image_data: bytes, compression_ratio: float) -> bytes:
        """
        按目标输出体积比例压缩图片
        :param image_data: 原始图片数据
        :param compression_ratio: 目标输出体积比例(0.1-1.0)，如0.5表示目标为原文件大小的50%
        :return: 压缩后的图片数据
        """
        return compress_image_to_target_ratio(image_data, compression_ratio)

    async def _compress_image_async(self, image_data: bytes, compression_ratio: float) -> bytes:
        """在线程池中执行图片压缩，避免阻塞事件循环"""
        return await asyncio.to_thread(self._compress_image, image_data, compression_ratio)
    async def _call_ocr_api_with_retry(self, image_data: bytes, semaphore: asyncio.Semaphore, task_id: str = None) -> str:
        """Call OCR API with retry-aware error recovery."""
        try:
            async with semaphore:
                return await self._make_ocr_request(image_data, task_id=task_id)
        except OCRServiceError as exc:
            return await self._recover_from_ocr_error(task_id, exc)
        except Exception as exc:
            wrapped = OCRServiceError(
                "OCR_UNEXPECTED_ERROR",
                f"OCR????: {exc}",
                retryable=True,
                details={"exception": repr(exc)},
            )
            return await self._recover_from_ocr_error(task_id, wrapped)

    async def _recover_from_ocr_error(self, task_id: Optional[str], error: OCRServiceError) -> str:
        logger.warning(
            "ocr_request_failed task=%s code=%s retryable=%s detail=%s",
            task_id,
            error.code,
            error.retryable,
            str(error),
        )
        fallback_content = await self._generate_fallback_content(
            {
                "error_code": error.code,
                "retryable": error.retryable,
                "details": error.to_dict(),
            }
        )
        if fallback_content:
            logger.info("OCR failed and fallback content was used")
            return fallback_content
        raise error

    async def _generate_fallback_content(self, context: Optional[Dict[str, Any]] = None) -> Optional[str]:
        """Generate fallback content when OCR is fully unavailable."""
        logger.info("Attempting to generate fallback OCR content...")

        try:
            diagnostics = "???????"
            if context:
                try:
                    diagnostics = json.dumps(context, ensure_ascii=False, indent=2)
                except Exception:
                    diagnostics = str(context)
            fallback_text = (
                "[????] OCR ????????\n\n"
                "????????????????????????????????????????????????\n\n"
                "????:\n"
                f"{diagnostics}"
            )
            logger.warning("Using fallback OCR content")
            return fallback_text
        except Exception as exc:
            logger.error(f"Failed to generate fallback content: {exc}")
            return None

    async def _get_session(self) -> aiohttp.ClientSession:
        """Get or create a shared aiohttp session."""
        if self._session and not self._session.closed:
            return self._session

        async with self._session_lock:
            if self._session and not self._session.closed:
                return self._session
            timeout = aiohttp.ClientTimeout(total=self.api_timeout)
            self._session = aiohttp.ClientSession(timeout=timeout)
            return self._session

    async def _make_ocr_request(self, image_data: bytes, task_id: str = None) -> str:
        """Send OCR request with shared session and response validation."""
        request_config = build_ocr_request_config(
            self.api_config,
            self._runtime_config,
            max_retries=self.max_retries,
        )

        if request_config.api_key:
            precheck_ok = await self._precheck_api_connection(
                request_config.api_base_url,
                request_config.api_key,
            )
            if not precheck_ok:
                logger.warning("API precheck did not succeed; continuing with request")
        else:
            logger.debug("skip API precheck because API key is empty")

        session = await self._get_session()
        parsed_content = await request_ocr_markdown(
            request_config,
            image_data,
            system_prompt=OCR_SYSTEM_PROMPT,
            user_prompt=OCR_USER_PROMPT,
            shared_session=session,
            logger=logger,
            task_id=task_id,
        )
        return parsed_content

    async def close(self) -> None:
        """Close the shared HTTP session held by the page processor."""
        if self._session and not self._session.closed:
            await self._session.close()
        self._session = None
    
    async def _precheck_api_connection(self, api_base_url: str, api_key: str, force: bool = False) -> bool:
        """Run a cached health check against the OCR API"""
        if not api_key:
            self._last_health_check_ok = False
            return False

        now = time.time()
        if not force and (now - self._last_health_check_ts) < self.health_check_interval:
            return self._last_health_check_ok

        self._last_health_check_ts = now

        test_url = f"{api_base_url}/v1/models"
        headers = {"Authorization": f"Bearer {api_key}"}
        timeout = aiohttp.ClientTimeout(total=10)

        try:
            session = await self._get_session()
            async with session.get(test_url, headers=headers, timeout=timeout) as response:
                self._last_health_check_ok = response.status == 200
                if not self._last_health_check_ok:
                    logger.warning(f'API预检查失败: 状态码={response.status}')
                return self._last_health_check_ok
        except Exception as exc:
            self._last_health_check_ok = False
            logger.debug('API预检查异常: %s', exc)
            return False

def create_page_processor(runtime_config, api_config: Dict[str, Any]) -> PageProcessor:
    """
    创建页面处理器实例
    :param runtime_config: 运行时配置对象引用
    :param api_config: API静态配置
    """
    return PageProcessor(runtime_config, api_config)

# 全局页面处理器实例（需要在应用启动时初始化）
page_processor: Optional[PageProcessor] = None
_page_processor_initialized = False  # 添加初始化状态标志

# 添加模块级别的初始化保护
def _ensure_safe_import():
    """确保模块导入时的安全性"""
    global page_processor, _page_processor_initialized
    logger.debug(f"页面处理器模块导入检查: page_processor={page_processor}, _page_processor_initialized={_page_processor_initialized}")
    
    # 如果状态不一致，重置为安全状态
    if _page_processor_initialized and page_processor is None:
        logger.warning("检测到页面处理器状态不一致，重置为未初始化状态")
        _page_processor_initialized = False

# 模块导入时执行安全检查
_ensure_safe_import()

async def init_page_processor(runtime_config, api_config: Dict[str, Any]):
    """
    初始化全局页面处理器
    :param runtime_config: 运行时配置对象引用
    :param api_config: API静态配置
    """
    global page_processor, _page_processor_initialized

    logger.info("=== 开始初始化全局页面处理器 ===")
    logger.warning(f"初始化前状态: page_processor={page_processor}, _page_processor_initialized={_page_processor_initialized}")

    try:
        # 1. 检查必要配置
        logger.info("检查API配置...")
        if not api_config:
            raise ValueError("API配置不能为空")

        api_key = api_config.get('api_key')
        if not api_key:
            logger.warning("API密钥未配置，将影响OCR功能")
        else:
            logger.info("API配置验证通过")

        # 2. 检查当前状态
        logger.info(f"当前页面处理器状态: page_processor={'已存在' if page_processor is not None else '不存在'}, "
                   f"_page_processor_initialized={_page_processor_initialized}")

        # 如果已经初始化，进行更严格的验证
        if _page_processor_initialized and page_processor is not None:
            logger.info("全局页面处理器已经初始化，进行验证...")

            # 验证页面处理器是否可用
            if await _validate_page_processor():
                logger.info("页面处理器验证通过，跳过初始化")
                return
            else:
                logger.error("页面处理器验证失败，需要重新初始化")
                await shutdown_page_processor()

        # 如果存在旧实例，先清理
        if page_processor is not None:
            logger.info("发现旧的页面处理器实例，进行清理...")
            await shutdown_page_processor()

        # 3. 创建新实例
        logger.info("创建新的页面处理器实例...")
        page_processor = create_page_processor(runtime_config, api_config)
        logger.warning(f"创建后状态: page_processor={page_processor}, 类型={type(page_processor) if page_processor else 'None'}")

        # 4. 验证初始化是否成功
        logger.info("验证页面处理器初始化...")
        if page_processor is None:
            logger.error("页面处理器创建失败 - 对象为 None")
            raise Exception("页面处理器创建失败")

        # 验证必要的方法是否存在
        required_methods = ['process_pdf_with_pagination', 'calculate_page_strategy', 'create_page_batches']
        missing_methods = []
        for method in required_methods:
            if not hasattr(page_processor, method):
                missing_methods.append(method)

        if missing_methods:
            logger.error(f"页面处理器缺少必要方法: {missing_methods}")
            raise Exception(f"页面处理器缺少必要方法: {missing_methods}")

        logger.info(f"页面处理器创建成功，包含所有必要方法")

        # 5. 验证页面处理器功能
        if not await _validate_page_processor():
            raise Exception("页面处理器功能验证失败")

        # 6. 设置初始化状态
        _page_processor_initialized = True
        logger.warning(f"初始化完成状态: page_processor={page_processor}, _page_processor_initialized={_page_processor_initialized}")
        logger.info("全局页面处理器初始化完成")

    except Exception as e:
        _page_processor_initialized = False
        page_processor = None
        logger.error(f"全局页面处理器初始化失败: {e}")
        raise

async def _validate_page_processor() -> bool:
    """验证页面处理器是否可用"""
    global page_processor
    
    if page_processor is None:
        logger.warning("页面处理器实例为None")
        return False
    
    try:
        # 检查基本属性
        if not hasattr(page_processor, 'api_config'):
            logger.warning("页面处理器缺少api_config属性")
            return False
        
        if not hasattr(page_processor, 'max_concurrent_pages'):
            logger.warning("页面处理器缺少max_concurrent_pages属性")
            return False
        
        if not hasattr(page_processor, 'default_batch_size'):
            logger.warning("页面处理器缺少default_batch_size属性")
            return False
        
        # 检查API配置
        api_config = page_processor.api_config
        if not api_config:
            logger.warning("页面处理器的API配置为空")
            return False
        
        # 测试API连接
        logger.info("测试API连接...")
        api_connection_ok = await _test_api_connection(api_config)
        if not api_connection_ok:
            logger.warning("API连接测试失败")
            # 不直接返回False，允许继续使用本地处理
        
        logger.info("页面处理器验证通过")
        return True
        
    except Exception as e:
        logger.error(f"页面处理器验证失败: {e}")
        return False

async def _test_api_connection(api_config: Dict[str, Any]) -> bool:
    """测试API连接"""
    try:
        import aiohttp
                
        api_key = api_config.get('api_key')
        if not api_key:
            logger.warning("API密钥未配置，跳过连接测试")
            return True
        
        api_base_url = api_config.get('api_base_url', 'https://api.openai.com')
        logger.info(f"测试API连接: {api_base_url}")
        
        # 创建简单的测试请求
        timeout = aiohttp.ClientTimeout(total=10)  # 10秒超时
        
        async with aiohttp.ClientSession(timeout=timeout) as session:
            # 使用模型列表API作为连接测试
            test_url = f"{api_base_url}/v1/models"
            headers = {"Authorization": f"Bearer {api_key}"}
            
            async with session.get(test_url, headers=headers) as response:
                if response.status == 200:
                    logger.info("API连接测试成功")
                    return True
                else:
                    logger.warning(f"API连接测试失败，状态码: {response.status}")
                    return False
                    
    except Exception as e:
        logger.error(f"API连接测试异常: {e}")
        return False

async def shutdown_page_processor():
    """关闭全局页面处理器并清理资源"""
    global page_processor, _page_processor_initialized
    if page_processor:
        try:
            await page_processor.close()
        except Exception as exc:
            logger.warning(f'关闭页面处理器会话失败: {exc}')
        page_processor = None
        _page_processor_initialized = False
        logger.info('全局页面处理器已关闭')


def is_page_processor_ready() -> bool:
    """检查页面处理器是否已就绪"""
    global page_processor, _page_processor_initialized
    
    # 基础检查
    if page_processor is None:
        logger.error("页面处理器实例为None")
        return False

    if not _page_processor_initialized:
        logger.error("页面处理器初始化标志为False")
        return False
    
    # 检查关键属性
    required_attrs = ['api_config', 'max_concurrent_pages', 'default_batch_size', 'max_retries', 'retry_delay']
    missing_attrs = []
    for attr in required_attrs:
        if not hasattr(page_processor, attr):
            missing_attrs.append(attr)
    
    if missing_attrs:
        logger.error(f"页面处理器缺少必要属性: {missing_attrs}")
        return False

    # 检查关键方法
    required_methods = ['process_pdf_with_pagination', 'calculate_page_strategy', 'create_page_batches']
    missing_methods = []
    for method in required_methods:
        if not hasattr(page_processor, method):
            missing_methods.append(method)

    if missing_methods:
        logger.error(f"页面处理器缺少必要方法: {missing_methods}")
        return False

    # 检查API配置
    if not page_processor.api_config:
        logger.error("页面处理器的API配置为空")
        return False

    return True

async def ensure_page_processor_ready(runtime_config, api_config: Dict[str, Any]) -> bool:
    """
    确保页面处理器已就绪，如果未就绪则尝试重新初始化
    :param runtime_config: 运行时配置对象引用
    :param api_config: API静态配置
    """
    global page_processor, _page_processor_initialized

    logger.info("检查页面处理器就绪状态...")

    # 如果已经就绪，直接返回
    if is_page_processor_ready():
        logger.info("页面处理器已就绪")
        return True

    logger.warning("页面处理器未就绪，尝试重新初始化...")

    try:
        # 尝试重新初始化
        await init_page_processor(runtime_config, api_config)

        # 再次检查
        if is_page_processor_ready():
            logger.info("页面处理器重新初始化成功")
            return True
        else:
            logger.error("页面处理器重新初始化后仍不可用")
            return False

    except Exception as e:
        logger.error(f"页面处理器重新初始化失败: {e}")
        return False
