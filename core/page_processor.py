import asyncio
import uuid
import logging
import time
import fitz  # PyMuPDF
from typing import Dict, Any, Optional, List, AsyncGenerator, Tuple
from datetime import datetime, timezone
from io import BytesIO
from PIL import Image
import aiohttp
import json
import base64
from enum import Enum

from models.database import (
    TaskStatus, TaskType, task_model, page_result_model,
    task_progress_model, page_batch_model
)
from core.content_moderator import ContentModerator, ModerationFailedException, get_moderation_config

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

        # 初始化内容审查器
        self.moderator = ContentModerator(get_moderation_config())

        # 固定配置(不会变化的)
        self.max_concurrent_pages = self.api_config.get('concurrency', 5)
        self.default_batch_size = self.api_config.get('batch_size', 4)
        self.retry_delay = self.api_config.get('retry_delay', 0.5)
        self.health_check_interval = self.api_config.get('health_check_interval', 300)

        # 会话管理
        self._session = None  # 延迟创建 aiohttp.ClientSession
        self._session_lock = asyncio.Lock()
        self._last_health_check_ts = 0.0
        self._last_health_check_ok = True

        logger.info(f"分页处理器初始化完成 - 并发: {self.max_concurrent_pages}, 批次: {self.default_batch_size}")

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
        if self._runtime_config:
            timeout = getattr(self._runtime_config, 'api_timeout', 120)
            logger.debug(f"从runtime_config读取api_timeout: {timeout}")
            return timeout
        return 120

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
                pdf_document = fitz.open(file_path)
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
                    pdf_document.close()

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
                pdf_document = fitz.open(stream=file_data, filetype="pdf")
                
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
                    pdf_document.close()
            
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
            
            # 转换页面为图片
            page = pdf_document.load_page(page_number - 1)
            
            # 根据策略优化DPI
            dpi = self._calculate_optimal_dpi(page, strategy)
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
            
            # 调用OCR API，支持重试（传递task_id用于审查）
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

        except ModerationFailedException as moderation_exc:
            # 审查失败 - 特殊处理
            processing_time = time.time() - page_start_time
            logger.warning(f"页面内容审查未通过 {task_id}-{page_number}: {moderation_exc.details}")

            # 保存原始内容(仅后端可见)
            await page_result_model.update_page_result(
                task_id, page_number, 'cancelled',
                content=moderation_exc.original_content,
                error_message='内容道德审查未通过',
                processing_time=processing_time,
                batch_id=batch_id
            )

            # 保存审查日志
            await self._save_moderation_log(
                task_id, page_number, 'block',
                moderation_exc.details,
                moderation_exc.original_content
            )

            return {
                'success': False,
                'moderation_failed': True,  # 特殊标记
                'page_number': page_number,
                'error': '内容道德审查未通过',
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
    
    def _calculate_optimal_dpi(self, page, strategy: Dict[str, Any]) -> int:
        """根据页面大小和策略计算最优DPI"""
        rect = page.rect
        width, height = rect.width, rect.height
        page_area = width * height
        
        # 基础DPI
        base_dpi = self.api_config.get('pdf_dpi', 200)
        
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
    
    async def _call_ocr_api_with_retry(self, image_data: bytes, semaphore: asyncio.Semaphore, task_id: str = None) -> str:
        """调用OCR API，包含增强的重试与诊断信息"""
        last_exception: Optional[Exception] = None
        retry_count = 0
        attempt_errors = []

        for attempt in range(self.max_retries):
            retry_count = attempt + 1
            try:
                if attempt > 0:
                    should_continue = await self._should_continue_retry(last_exception, attempt)
                    if not should_continue:
                        logger.info('根据错误类型停止继续重试')
                        break

                async with semaphore:
                    content = await self._make_ocr_request(image_data, task_id=task_id)
                    return content
            except ModerationFailedException:
                # 审查失败异常不重试，直接抛出
                raise
            except Exception as exc:
                last_exception = exc
                attempt_errors.append({
                    'attempt': retry_count,
                    'error': str(exc),
                    'type': type(exc).__name__
                })
                logger.warning(f'OCR API调用失败 (尝试 {retry_count}/{self.max_retries}): {exc}')
                await self._log_error_details(exc, attempt, image_data)

                if attempt < self.max_retries - 1:
                    delay = await self._calculate_retry_delay(attempt, exc)
                    logger.info(f'等待 {delay:.2f} 秒后重试...')
                    await asyncio.sleep(delay)
                else:
                    logger.warning(f'已达到最大重试次数 {self.max_retries}')

        error_message = f'OCR API调用最终失败，已尝试 {retry_count} 次: {last_exception}'
        logger.warning(error_message)

        fallback_context = {
            'retries': retry_count,
            'last_error': str(last_exception) if last_exception else None,
            'attempt_errors': attempt_errors
        }
        fallback_content = await self._generate_fallback_content(fallback_context)
        if fallback_content:
            logger.info('使用备用内容继续处理')
            return fallback_content

        raise Exception(error_message)
    async def _should_continue_retry(self, exception: Exception, attempt: int) -> bool:
        """根据错误类型决定是否应该继续重试"""
        error_str = str(exception).lower()
        
        # 不重试的错误类型
        no_retry_errors = [
            "api密钥未配置",
            "api基础url未配置",
            "api模型未配置",
            "图片数据为空",
            "认证失败",
            "invalid api key",
            "unauthorized",
            "forbidden",
            "401",
            "403"
        ]
        
        for error in no_retry_errors:
            if error in error_str:
                logger.warning(f"错误类型不支持重试: {error}")
                return False
        
        # 重试次数过多时，对于某些错误停止重试
        if attempt >= 3:
            # 对于网络错误，可以继续重试
            if any(net_error in error_str for net_error in ["网络错误", "连接超时", "timeout", "connection"]):
                logger.info("网络错误，继续重试")
                return True
            else:
                logger.warning("重试次数过多，停止重试")
                return False
        
        return True
    
    async def _calculate_retry_delay(self, attempt: int, exception: Exception) -> float:
        """计算重试延迟时间"""
        import random
        
        # 基础延迟
        base_delay = self.retry_delay
        
        # 根据错误类型调整延迟
        error_str = str(exception).lower()
        
        # 网络错误使用较短的延迟
        if any(net_error in error_str for net_error in ["网络错误", "连接超时", "timeout", "connection"]):
            base_delay = base_delay * 0.5
        
        # 服务器错误使用较长的延迟
        if any(server_error in error_str for server_error in ["服务器错误", "server error", "502", "503", "504"]):
            base_delay = base_delay * 2.0
        
        # 指数退避
        delay = base_delay * (2 ** attempt)
        
        # 添加随机抖动
        jitter = random.uniform(0, base_delay)
        delay = delay + jitter
        
        # 最大延迟限制
        max_delay = min(60, self.retry_delay * 10)  # 最大60秒或基础延迟的10倍
        delay = min(delay, max_delay)
        
        return delay
    
    async def _log_error_details(self, exception: Exception, attempt: int, image_data: bytes):
        """记录详细的错误信息"""
        try:
            error_details = {
                "attempt": attempt + 1,
                "error_type": type(exception).__name__,
                "error_message": str(exception),
                "image_size_mb": len(image_data) / (1024 * 1024),
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "max_retries": self.max_retries,
                "retry_delay": self.retry_delay
            }
            
            logger.warning(f"OCR API调用错误详情: {error_details}")
            
        except Exception as log_error:
            logger.debug(f"记录错误详情失败: {log_error}")
    
    async def _generate_fallback_content(self, context: Optional[Dict[str, Any]] = None) -> Optional[str]:
        """生成备用内容，在API完全失败时使用"""
        logger.info("尝试生成备用内容...")

        try:
            diagnostics = "无更多诊断信息"
            if context:
                try:
                    diagnostics = json.dumps(context, ensure_ascii=False, indent=2)
                except Exception:
                    diagnostics = str(context)
            fallback_text = (
                "[系统通知] OCR 服务暂时不可用。\n\n"
                "由于外部识别服务暂时无法返回有效结果，当前页面未能完成识别，请稍后重试或联系系统管理员协助排查。\n\n"
                "诊断信息:\n"
                f"{diagnostics}"
            )
            logger.warning("使用备用内容")
            return fallback_text
        except Exception as e:
            logger.error(f"生成备用内容失败: {e}")
            return None

    async def _get_session(self) -> aiohttp.ClientSession:
        """获取或创建共享的 aiohttp 会话"""
        if self._session and not self._session.closed:
            return self._session

        async with self._session_lock:
            if self._session and not self._session.closed:
                return self._session
            timeout = aiohttp.ClientTimeout(total=self.api_timeout)
            self._session = aiohttp.ClientSession(timeout=timeout)
            return self._session

    async def _get_user_is_admin_from_task(self, task_id: str) -> bool:
        """通过task_id查询用户是否为管理员"""
        try:
            task_data = await task_model.get_task(task_id)
            if not task_data or not task_data.get('user_id'):
                return False

            # 直接查询数据库获取用户信息
            from models.database import db_manager
            async with db_manager.get_connection() as db:
                async with db.execute(
                    "SELECT is_admin FROM users WHERE id = ?",
                    (task_data['user_id'],)
                ) as cursor:
                    user_row = await cursor.fetchone()
                    return bool(user_row[0]) if user_row else False
        except Exception as e:
            logger.warning(f"查询用户管理员权限失败 {task_id}: {e}")
            return False

    async def _make_ocr_request(self, image_data: bytes, task_id: str = None) -> str:
        """Send OCR request with shared session, response validation and content moderation"""
        api_base_url = self.api_config.get('api_base_url', 'https://api.openai.com')
        api_key = self.api_config.get('api_key')
        model = self.api_config.get('model', 'gpt-4o')

        if not api_key:
            raise Exception('API密钥未配置')

        if not api_base_url:
            raise Exception('API基础URL未配置')

        if not model:
            raise Exception('API模型未配置')

        if not image_data:
            raise Exception('图片数据为空')

        precheck_ok = await self._precheck_api_connection(api_base_url, api_key)
        if not precheck_ok:
            logger.warning('API precheck did not succeed; continuing with request')

        system_prompt = (
            'You are an expert OCR assistant. Extract all textual information from the image and '
            'return it as clean Markdown. The final answer MUST be valid JSON with the structure '
            '{"content": "..."}. Do not wrap the JSON in code fences.'
        )

        user_prompt = (
            'Analyze the provided image and output a JSON object with a single string field named "content". '
            'The value should contain the recognized Markdown-formatted text. Avoid any additional commentary.'
        )

        encoded_image = base64.b64encode(image_data).decode("utf-8")

        payload = {
            'messages': [
                {'role': 'system', 'content': system_prompt},
                {
                    'role': 'user',
                    'content': [
                        {'type': 'text', 'text': user_prompt},
                        {'type': 'image_url', 'image_url': {'url': f'data:image/png;base64,{encoded_image}'}}
                    ],
                },
            ],
            'stream': False,
            'model': model,
            'temperature': 0.0,
            'top_p': 1,
        }

        session = await self._get_session()

        try:
            response = await session.post(
                f"{api_base_url}/v1/chat/completions",
                headers={'Authorization': f'Bearer {api_key}'},
                json=payload,
                timeout=aiohttp.ClientTimeout(total=self.api_timeout)
            )

            if response.status == 200:
                result = await response.json()

                if not self._validate_api_response(result):
                    raise Exception('API响应格式验证失败')

                content = result['choices'][0]['message']['content']
                parsed_content = self._parse_api_response(content)

                if not parsed_content:
                    logger.warning('OCR返回内容为空')
                    raise Exception('OCR返回内容为空')

                # ========== 内容道德审查 ==========
                # 查询用户是否为管理员
                user_is_admin = False
                if task_id:
                    user_is_admin = await self._get_user_is_admin_from_task(task_id)

                try:
                    action, moderated_content, details = await self.moderator.moderate_content(
                        content=parsed_content,
                        task_id=task_id,
                        user_is_admin=user_is_admin
                    )
                    # 审查通过，返回内容
                    return moderated_content
                except ModerationFailedException as e:
                    # 审查未通过，直接向上抛出异常（包含原始内容）
                    raise
                # ==================================

            error_text = (await response.text())[:500]
            if response.status in (401, 403):
                raise Exception(f'认证失败, 状态码: {response.status}, 响应: {error_text}')
            raise Exception(f'API请求失败, 状态码: {response.status}, 响应: {error_text}')
        except ModerationFailedException:
            # 审查失败异常直接向上抛出
            raise
        except asyncio.TimeoutError as exc:
            logger.warning('API请求超时: %s', exc)
            raise Exception('API请求超时') from exc
        except aiohttp.ClientError as exc:
            logger.warning('API请求网络错误: %s', exc)
            raise Exception(f'API请求网络错误: {exc}') from exc

    async def _save_moderation_log(self, task_id: str, page_number: int,
                                   action: str, details: Dict[str, Any],
                                   original_content: str = None):
        """保存审查日志到数据库"""
        try:
            from models.database import db_manager
            from datetime import datetime, timezone
            import json

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

    async def close(self) -> None:
        """关闭内部持有的 HTTP 会话"""
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

    def _validate_api_response(self, response: Dict[str, Any]) -> bool:
        """验证API响应格式"""
        try:
            if 'choices' not in response:
                logger.error('API响应缺少choices字段')
                return False

            choices = response['choices']
            if not isinstance(choices, list) or len(choices) == 0:
                logger.error('API响应choices字段格式错误')
                return False

            choice = choices[0]
            if 'message' not in choice:
                logger.error('API响应缺少message字段')
                return False

            message = choice['message']
            if 'content' not in message:
                logger.error('API响应缺少content字段')
                return False

            content = message['content']
            if not isinstance(content, str):
                logger.error('API响应content字段格式错误')
                return False

            return True
        except Exception as e:
            logger.error(f'API响应验证异常: {e}')
            return False

    def _parse_api_response(self, content: str) -> str:
        """Parse LLM response, preferring JSON payloads"""
        if not content:
            logger.warning('API响应内容为空字符串')
            return ''

        cleaned = content.strip()

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
            logger.debug('API响应JSON解析失败: %s', exc)

        return self._parse_api_response_fallback(cleaned)
    def _parse_api_response_fallback(self, content: str) -> str:
        """Fallback parsing strategy when structured formats fail"""
        logger.info('Fallback OCR response parsing engaged')

        try:
            possible_markers = ['This is the content:', 'this is the content:', 'Content:', 'content:', 'OCR result:', 'Recognized text:', 'Text:', 'text:', 'Result:', 'result:', 'Output:', 'output:']
            for marker in possible_markers:
                if marker in content:
                    start_index = content.find(marker) + len(marker)
                    extracted = content[start_index:].strip()

                    end_markers = ['this is the end of the content', 'end of content', 'End']
                    for end_marker in end_markers:
                        if end_marker in extracted:
                            extracted = extracted[:extracted.find(end_marker)].strip()
                            break

                    if extracted:
                        return extracted

            lines = [line.strip() for line in content.split('\n') if line.strip()]
            skip_patterns = ["I'm sorry", 'I cannot', 'As an AI', 'Here is', 'The content', 'I understand', 'I can see']
            filtered_lines = [line for line in lines if not any(pat in line for pat in skip_patterns)]
            fallback_content = '\n'.join(filtered_lines).strip()
            if fallback_content:
                return fallback_content

            cleaned = content.replace('```markdown', '').replace('```', '').strip()
            cleaned_lines = [line.strip() for line in cleaned.split('\n') if line.strip()]
            if cleaned_lines:
                return '\n'.join(cleaned_lines)

            logger.warning('Fallback解析失败，返回空字符串')
            logger.debug('响应内容片段: %s', content[:300])
            return ''
        except Exception as exc:
            logger.error('Fallback解析异常: %s', exc)
            logger.debug('异常响应内容: %s', content[:300])
            try:
                return content.replace('```markdown', '').replace('```', '').strip()
            except Exception:
                return ''
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
