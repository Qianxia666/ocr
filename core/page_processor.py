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
import base64
from enum import Enum

from models.database import (
    TaskStatus, TaskType, task_model, page_result_model, 
    task_progress_model, page_batch_model
)

logger = logging.getLogger(__name__)

class PageStrategy(Enum):
    """分页策略枚举"""
    AUTO = "auto"        # 自动策略
    FIXED = "fixed"      # 固定大小
    ADAPTIVE = "adaptive"  # 自适应策略

class PageProcessor:
    """分页处理核心类 - 智能分页策略和并行处理"""
    
    def __init__(self, api_config: Optional[Dict[str, Any]] = None):
        """
        初始化分页处理器
        :param api_config: API配置
        """
        self.api_config = api_config or {}
        self.max_concurrent_pages = self.api_config.get('concurrency', 5)
        self.default_batch_size = self.api_config.get('batch_size', 4)
        self.max_retries = self.api_config.get('max_retries', 3)
        self.retry_delay = self.api_config.get('retry_delay', 0.5)
        
        logger.info(f"分页处理器初始化完成 - 并发: {self.max_concurrent_pages}, 批次: {self.default_batch_size}")
    
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
                                    websocket_manager=None) -> Dict[str, Any]:
        """
        从临时文件分页处理PDF文档 - 节省内存
        :param task_id: 任务ID
        :param file_path: PDF临时文件路径
        :param total_pages: 总页数
        :param websocket_manager: WebSocket管理器
        :return: 处理结果摘要
        """
        import os
        start_time = time.time()

        try:
            # 获取文件大小
            file_size = os.path.getsize(file_path)

            # 计算分页策略
            strategy = self.calculate_page_strategy(task_id, total_pages, file_size)
            logger.info(f"任务 {task_id} 分页策略(临时文件模式): {strategy}")

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

            logger.info(f"分页处理完成(临时文件模式) {task_id}: {result_summary}")
            return result_summary

        except Exception as e:
            logger.error(f"分页处理失败(临时文件模式) {task_id}: {e}")
            raise

    async def process_pdf_with_pagination(self,
                                        task_id: str,
                                        file_data: bytes,
                                        total_pages: int,
                                        websocket_manager=None) -> Dict[str, Any]:
        """
        分页处理PDF文档(内存模式-兼容旧代码)
        :param task_id: 任务ID
        :param file_data: PDF文件数据
        :param total_pages: 总页数
        :param websocket_manager: WebSocket管理器
        :return: 处理结果摘要
        """
        start_time = time.time()

        try:
            # 计算分页策略
            strategy = self.calculate_page_strategy(task_id, total_pages, len(file_data))
            logger.info(f"任务 {task_id} 分页策略: {strategy}")

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
            
            logger.info(f"分页处理完成 {task_id}: {result_summary}")
            return result_summary
            
        except Exception as e:
            logger.error(f"分页处理失败 {task_id}: {e}")
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
                
                # 顺序处理批次内的页面（改为一个接一个处理，不并发）
                page_results = []
                for page_number in range(page_start, page_end + 1):
                    try:
                        result = await self._process_single_page(
                            task_id, page_number, pdf_document,
                            page_semaphore, batch_id, strategy,
                            websocket_manager
                        )
                        page_results.append(result)
                    except Exception as e:
                        logger.error(f"页面处理异常 {task_id}-{page_number}: {e}")
                        page_results.append(e)
                
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
                
                logger.info(f"批次处理完成 {batch_id}: 成功 {successful_pages}, 失败 {failed_pages}, 耗时 {batch_time:.2f}s")
                
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
            
            # 调用OCR API，支持重试
            content = await self._call_ocr_api_with_retry(image_data, semaphore)
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
    
    async def _call_ocr_api_with_retry(self, image_data: bytes, semaphore: asyncio.Semaphore) -> str:
        """调用OCR API，支持增强的重试机制和错误处理"""
        last_exception = None
        retry_count = 0

        for attempt in range(self.max_retries):
            retry_count = attempt + 1
            try:
                
                # 检查是否应该继续重试
                if attempt > 0:
                    should_continue = await self._should_continue_retry(last_exception, attempt)
                    if not should_continue:
                        logger.info(f"根据错误类型，停止重试")
                        break
                
                async with semaphore:
                    content = await self._make_ocr_request(image_data)
                    # 静默成功,不输出日志
                    return content
                    
            except Exception as e:
                last_exception = e
                logger.warning(f"OCR API调用失败 (尝试 {retry_count}/{self.max_retries}): {e}")
                
                # 记录详细的错误信息
                await self._log_error_details(e, attempt, image_data)
                
                if attempt < self.max_retries - 1:
                    # 计算延迟时间（指数退避 + 随机抖动）
                    delay = await self._calculate_retry_delay(attempt, e)
                    logger.info(f"等待 {delay:.2f} 秒后重试...")
                    await asyncio.sleep(delay)
                else:
                    logger.error(f"已达到最大重试次数 {self.max_retries}")
        
        # 所有重试都失败
        error_msg = f"OCR API调用最终失败，已尝试 {retry_count} 次: {str(last_exception)}"
        logger.error(error_msg)
        
        # 尝试生成备用内容
        fallback_content = await self._generate_fallback_content()
        if fallback_content:
            logger.info("使用备用内容继续处理")
            return fallback_content
        
        raise Exception(error_msg)
    
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
            
            logger.error(f"OCR API调用错误详情: {error_details}")
            
        except Exception as log_error:
            logger.error(f"记录错误详情失败: {log_error}")
    
    async def _generate_fallback_content(self) -> Optional[str]:
        """生成备用内容，在API完全失败时使用"""
        logger.info("尝试生成备用内容...")
        
        try:
            # 这里可以添加简单的本地OCR逻辑
            # 或者返回一个标记内容，表明API处理失败
            
            fallback_text = """
            [系统通知] OCR API处理失败
            
            由于API服务暂时不可用，无法完成OCR识别。请稍后重试或联系系统管理员。
            
            错误信息：API调用失败，已达到最大重试次数
            """
            
            logger.warning("使用备用内容")
            return fallback_text
            
        except Exception as e:
            logger.error(f"生成备用内容失败: {e}")
            return None
    
    async def _make_ocr_request(self, image_data: bytes) -> str:
        """发起OCR请求 - 增强版，包含连接测试和响应验证"""
        api_base_url = self.api_config.get('api_base_url', 'https://api.openai.com')
        api_key = self.api_config.get('api_key')
        model = self.api_config.get('model', 'gpt-4o')

        # 1. 验证API配置
        if not api_key:
            raise Exception("API密钥未配置")

        if not api_base_url:
            raise Exception("API基础URL未配置")

        if not model:
            raise Exception("API模型未配置")

        # 2. 验证图片数据
        if not image_data:
            raise Exception("图片数据为空")

        # 3. 预检查API连接
        if not await self._precheck_api_connection(api_base_url, api_key):
            logger.warning("API连接预检查失败，但将继续尝试请求")

        # 4. 准备请求内容
        system_prompt = """
       You are an expert OCR assistant. Your job is to extract all text from the provided image and convert it into a well-structured, easy-to-read Markdown document that mirrors the intended structure of the original. Follow these precise guidelines:

- Use Markdown headings, paragraphs, lists, and tables to match the document's hierarchy and flow.
- For tables, use standard Markdown table syntax and merge cells if needed. If a table has a title, include it as plain text above the table.
- Render mathematical formulas with LaTeX syntax: use $...$ for inline and $$...$$ for display equations.
- For images, use the syntax ![descriptive alt text](link) with a clear, descriptive alt text.
- Remove unnecessary line breaks so that the text flows naturally without awkward breaks.
- Your final Markdown output must be direct text (do not wrap it in code blocks).

Ensure your output is clear, accurate, and faithfully reflects the original image's content and structure.
        """

        user_prompt = "Analyze the image and extract all text content in the specified format. Start your response with 'This is the content:' followed by the extracted text, and end with 'this is the end of the content'."

        encoded_image = base64.b64encode(image_data).decode('utf-8')

        # 5. 发起API请求
        timeout = aiohttp.ClientTimeout(total=120)  # 2分钟超时

        try:
            async with aiohttp.ClientSession(timeout=timeout) as session:
                response = await session.post(
                    f"{api_base_url}/v1/chat/completions",
                    headers={"Authorization": f"Bearer {api_key}"},
                    json={
                        "messages": [
                            {"role": "system", "content": system_prompt},
                            {
                                "role": "user",
                                "content": [
                                    {
                                        "type": "text",
                                        "text": user_prompt
                                    },
                                    {
                                        "type": "image_url",
                                        "image_url": {"url": f"data:image/png;base64,{encoded_image}"}
                                    }
                                ]
                            }
                        ],
                        "stream": False,
                        "model": model,
                        "temperature": 0.5,
                        "top_p": 1
                    }
                )
                
                # 6. 处理响应
                if response.status == 200:
                    result = await response.json()

                    # 验证响应格式
                    if not self._validate_api_response(result):
                        raise Exception("API响应格式验证失败")

                    content = result['choices'][0]['message']['content']

                    # 解析返回内容
                    parsed_content = self._parse_api_response(content)

                    if not parsed_content:
                        logger.error("OCR返回内容为空 - 这可能是问题的根源！")
                        raise Exception("OCR返回内容为空")

                    return parsed_content
                    
                else:
                    error_text = await response.text()
                    logger.error(f"API请求失败: 状态码={response.status}, 错误={error_text}")
                    raise Exception(f"API请求失败, 状态码: {response.status}, 错误: {error_text}")
                    
        except aiohttp.ClientError as e:
            logger.error(f"API请求网络错误: {e}")
            raise Exception(f"API请求网络错误: {e}")
        except asyncio.TimeoutError:
            logger.error("API请求超时")
            raise Exception("API请求超时")
        except Exception as e:
            logger.error(f"API请求未知错误: {e}")
            logger.exception("API请求详细错误信息:")
            raise
    
    async def _precheck_api_connection(self, api_base_url: str, api_key: str) -> bool:
        """预检查API连接"""
        try:
            timeout = aiohttp.ClientTimeout(total=10)  # 10秒超时

            async with aiohttp.ClientSession(timeout=timeout) as session:
                # 使用模型列表API作为连接测试
                test_url = f"{api_base_url}/v1/models"
                headers = {"Authorization": f"Bearer {api_key}"}

                async with session.get(test_url, headers=headers) as response:
                    if response.status == 200:
                        return True
                    else:
                        logger.warning(f"API连接预检查失败，状态码: {response.status}")
                        return False

        except Exception as e:
            logger.warning(f"API连接预检查异常: {e}")
            return False
    
    def _validate_api_response(self, response: Dict[str, Any]) -> bool:
        """验证API响应格式"""
        try:
            # 检查必要字段
            if 'choices' not in response:
                logger.error("API响应缺少choices字段")
                return False
                
            choices = response['choices']
            if not isinstance(choices, list) or len(choices) == 0:
                logger.error("API响应choices字段格式错误")
                return False
                
            choice = choices[0]
            if 'message' not in choice:
                logger.error("API响应缺少message字段")
                return False
                
            message = choice['message']
            if 'content' not in message:
                logger.error("API响应缺少content字段")
                return False
                
            content = message['content']
            if not isinstance(content, str):
                logger.error("API响应content字段格式错误")
                return False

            return True
            
        except Exception as e:
            logger.error(f"API响应验证异常: {e}")
            return False
    
    def _parse_api_response(self, content: str) -> str:
        """解析API响应内容"""
        try:
            # 检查内容格式
            if "This is the content:" not in content:
                logger.error("API响应格式不正确，缺少开始标记 'This is the content:'")
                # 添加诊断：尝试使用备用解析策略
                return self._parse_api_response_fallback(content)

            # 提取内容
            start_index = content.find("This is the content:") + len("This is the content:")
            end_index = content.find("this is the end of the content")

            if end_index == -1:
                end_index = len(content)

            parsed_content = content[start_index:end_index].strip()

            # 清理内容
            parsed_content = parsed_content.replace("```markdown", "").replace("```", "").strip()

            # 验证内容
            if not parsed_content:
                logger.error("解析后的内容为空")
                return self._parse_api_response_fallback(content)

            return parsed_content

        except Exception as e:
            logger.error(f"API响应内容解析异常: {e}")
            # 添加诊断：尝试使用备用解析策略
            return self._parse_api_response_fallback(content)
    
    def _parse_api_response_fallback(self, content: str) -> str:
        """备用API响应解析策略 - 当标准解析失败时使用"""
        logger.info("===  开始备用API响应解析 ===")
        
        try:
            # 策略1: 查找任何可能的内容标记
            logger.info("策略1: 查找可能的内容标记...")
            possible_markers = [
                "This is the content:",
                "this is the content:",
                "Content:",
                "content:",
                "OCR结果:",
                "识别结果:",
                "识别内容:",
                "Text:",
                "text:",
                "Result:",
                "result:",
                "Output:",
                "output:"
            ]
            
            for marker in possible_markers:
                if marker in content:
                    logger.info(f"找到备用标记: {marker}")
                    start_index = content.find(marker) + len(marker)
                    extracted_content = content[start_index:].strip()
                    
                    # 尝试找到结束标记
                    end_markers = ["this is the end of the content", "end of content", "结束", "End"]
                    end_index = -1
                    for end_marker in end_markers:
                        if end_marker in extracted_content:
                            end_index = extracted_content.find(end_marker)
                            break
                    
                    if end_index != -1:
                        extracted_content = extracted_content[:end_index].strip()
                        logger.info(f"找到结束标记，截取内容到位置: {end_index}")
                    
                    if extracted_content:
                        logger.info(f"使用备用标记解析成功，长度: {len(extracted_content)}字符")
                        logger.info(f"备用解析内容预览: {extracted_content[:200]}...")
                        return extracted_content
            
            # 策略2: 如果没有找到任何标记，尝试去除常见的前后缀
            logger.info("策略2: 清理原始内容，去除系统消息...")
            
            # 移除可能的系统消息
            lines = content.split('\n')
            content_lines = []
            
            skip_patterns = [
                "I'm sorry",
                "抱歉",
                "I cannot",
                "我无法",
                "As an AI",
                "作为一个AI",
                "Here is",
                "以下是",
                "The content",
                "内容",
                "I understand",
                "我理解",
                "I can see",
                "我可以看到"
            ]
            
            for line in lines:
                line = line.strip()
                if line and not any(pattern in line for pattern in skip_patterns):
                    content_lines.append(line)
            
            fallback_content = '\n'.join(content_lines).strip()
            
            if fallback_content:
                logger.info(f"使用内容清理策略成功，长度: {len(fallback_content)}字符")
                logger.info(f"清理后内容预览: {fallback_content[:200]}...")
                return fallback_content
            
            # 策略3: 如果所有策略都失败，返回原始内容（但要进行基本清理）
            logger.info("策略3: 使用原始内容进行基本清理...")
            
            # 移除markdown代码块标记
            cleaned_content = content.replace("```markdown", "").replace("```", "").strip()
            
            # 移除多余的空行
            cleaned_lines = [line.strip() for line in cleaned_content.split('\n') if line.strip()]
            final_content = '\n'.join(cleaned_lines)
            
            if final_content:
                logger.info(f"使用基本清理策略成功，长度: {len(final_content)}字符")
                logger.info(f"基本清理内容预览: {final_content[:200]}...")
                return final_content
            else:
                logger.error("所有备用解析策略都失败")
                logger.error(f"最终处理的原始内容: {content[:500]}...")
                return ""
                
        except Exception as e:
            logger.error(f"备用API响应解析异常: {e}")
            logger.exception("备用解析详细异常信息:")
            # 在异常情况下，尝试返回原始内容的基本清理版本
            try:
                return content.replace("```markdown", "").replace("```", "").strip()
            except:
                return ""

# 创建全局页面处理器实例
def create_page_processor(api_config: Dict[str, Any]) -> PageProcessor:
    """创建页面处理器实例"""
    return PageProcessor(api_config)

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

async def init_page_processor(api_config: Dict[str, Any]):
    """初始化全局页面处理器"""
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
        page_processor = create_page_processor(api_config)
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
    """关闭全局页面处理器"""
    global page_processor, _page_processor_initialized
    if page_processor:
        page_processor = None
        _page_processor_initialized = False
        logger.info("全局页面处理器已关闭")

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

async def ensure_page_processor_ready(api_config: Dict[str, Any]) -> bool:
    """确保页面处理器已就绪，如果未就绪则尝试重新初始化"""
    global page_processor, _page_processor_initialized
    
    logger.info("检查页面处理器就绪状态...")
    
    # 如果已经就绪，直接返回
    if is_page_processor_ready():
        logger.info("页面处理器已就绪")
        return True
    
    logger.warning("页面处理器未就绪，尝试重新初始化...")
    
    try:
        # 尝试重新初始化
        await init_page_processor(api_config)
        
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