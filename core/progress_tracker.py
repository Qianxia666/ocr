import asyncio
import logging
import time
from typing import Dict, Any, Optional, List
from datetime import datetime, timezone

from models.database import (
    task_model, task_progress_model, page_result_model, page_batch_model
)
from models.websocket_messages import MessageFactory

logger = logging.getLogger(__name__)

class ProgressTracker:
    """进度跟踪系统 - 实时计算和更新任务进度"""
    
    def __init__(self, websocket_manager=None):
        """
        初始化进度跟踪器
        :param websocket_manager: WebSocket管理器
        """
        self.websocket_manager = websocket_manager
        self._tracking_tasks = {}  # 正在跟踪的任务
        self._tracker_running = False
        self._tracker_task = None
        
        logger.info("进度跟踪系统初始化完成")
    
    async def start_tracking(self, task_id: str, total_pages: int, estimated_time_per_page: float = 3.0):
        """
        开始跟踪任务进度
        :param task_id: 任务ID
        :param total_pages: 总页数
        :param estimated_time_per_page: 每页预估时间
        """
        try:
            self._tracking_tasks[task_id] = {
                'total_pages': total_pages,
                'estimated_time_per_page': estimated_time_per_page,
                'start_time': time.time(),
                'last_update_time': time.time(),
                'last_processed_pages': 0,
                'processing_rates': []  # 存储最近的处理速度
            }
            
            if not self._tracker_running:
                await self._start_progress_tracker()
            
            logger.info(f"开始跟踪任务进度 {task_id}")
            
        except Exception as e:
            logger.error(f"开始跟踪任务进度失败 {task_id}: {e}")
    
    async def stop_tracking(self, task_id: str):
        """
        停止跟踪任务进度
        :param task_id: 任务ID
        """
        try:
            if task_id in self._tracking_tasks:
                del self._tracking_tasks[task_id]
                logger.info(f"停止跟踪任务进度 {task_id}")
            
            # 如果没有正在跟踪的任务，停止跟踪器
            if not self._tracking_tasks and self._tracker_running:
                await self._stop_progress_tracker()
                
        except Exception as e:
            logger.error(f"停止跟踪任务进度失败 {task_id}: {e}")
    
    async def update_page_progress(self, task_id: str, page_number: int, status: str, 
                                  processing_time: Optional[float] = None):
        """
        更新页面进度
        :param task_id: 任务ID
        :param page_number: 页面号
        :param status: 页面状态
        :param processing_time: 处理时间
        """
        try:
            if task_id not in self._tracking_tasks:
                return
            
            # 立即更新任务进度
            await self._calculate_and_update_progress(task_id)
            
            # 发送页面完成消息
            if self.websocket_manager and status in ['completed', 'failed']:
                page_result = await page_result_model.get_page_result(task_id, page_number)
                if page_result:
                    page_message = MessageFactory.create_page_completed(
                        task_id, page_number, status,
                        content_length=page_result.get('content_length', 0) if status == 'completed' else None,
                        processing_time=processing_time,
                        error_message=page_result.get('error_message') if status == 'failed' else None
                    )
                    await self.websocket_manager.send_to_task_subscribers(
                        task_id, page_message.to_dict()
                    )
            
        except Exception as e:
            logger.error(f"更新页面进度失败 {task_id}-{page_number}: {e}")
    
    async def _start_progress_tracker(self):
        """启动进度跟踪器"""
        if self._tracker_running:
            return
        
        self._tracker_running = True
        self._tracker_task = asyncio.create_task(self._progress_tracking_loop())
        logger.info("进度跟踪器已启动")
    
    async def _stop_progress_tracker(self):
        """停止进度跟踪器"""
        if not self._tracker_running:
            return
        
        self._tracker_running = False
        if self._tracker_task:
            self._tracker_task.cancel()
            try:
                await self._tracker_task
            except asyncio.CancelledError:
                pass
            self._tracker_task = None
        
        logger.info("进度跟踪器已停止")
    
    async def _progress_tracking_loop(self):
        """进度跟踪循环"""
        try:
            while self._tracker_running:
                # 更新所有正在跟踪的任务
                for task_id in list(self._tracking_tasks.keys()):
                    try:
                        await self._calculate_and_update_progress(task_id)
                    except Exception as e:
                        logger.error(f"更新任务进度失败 {task_id}: {e}")
                
                # 等待2秒后再次更新
                await asyncio.sleep(2)
                
        except asyncio.CancelledError:
            logger.info("进度跟踪循环被取消")
        except Exception as e:
            logger.error(f"进度跟踪循环异常: {e}")
    
    async def _calculate_and_update_progress(self, task_id: str):
        """计算并更新任务进度"""
        try:
            if task_id not in self._tracking_tasks:
                return
            
            tracking_info = self._tracking_tasks[task_id]
            
            # 获取页面结果统计
            page_results = await page_result_model.get_task_page_results(task_id)
            
            completed_pages = sum(1 for p in page_results if p.get('status') == 'completed')
            failed_pages = sum(1 for p in page_results if p.get('status') == 'failed')
            processing_pages = sum(1 for p in page_results if p.get('status') == 'processing')
            processed_pages = completed_pages + failed_pages
            
            total_pages = tracking_info['total_pages']
            progress = (processed_pages / total_pages * 100) if total_pages > 0 else 0
            
            # 计算处理速度
            current_time = time.time()
            time_elapsed = current_time - tracking_info['start_time']
            
            processing_rate = 0
            estimated_time_remaining = 0
            
            if time_elapsed > 0 and processed_pages > 0:
                # 计算当前处理速度 (页/秒)
                processing_rate = processed_pages / time_elapsed
                
                # 存储最近的处理速度（用于平滑计算）
                if current_time - tracking_info['last_update_time'] >= 5:  # 每5秒更新一次速度
                    if processed_pages > tracking_info['last_processed_pages']:
                        recent_rate = (processed_pages - tracking_info['last_processed_pages']) / (current_time - tracking_info['last_update_time'])
                        tracking_info['processing_rates'].append(recent_rate)
                        
                        # 只保留最近10个速度记录
                        if len(tracking_info['processing_rates']) > 10:
                            tracking_info['processing_rates'].pop(0)
                        
                        tracking_info['last_update_time'] = current_time
                        tracking_info['last_processed_pages'] = processed_pages
                
                # 使用平均速度计算剩余时间
                if tracking_info['processing_rates']:
                    avg_rate = sum(tracking_info['processing_rates']) / len(tracking_info['processing_rates'])
                    remaining_pages = total_pages - processed_pages
                    if avg_rate > 0:
                        estimated_time_remaining = int(remaining_pages / avg_rate)
                elif processing_rate > 0:
                    remaining_pages = total_pages - processed_pages
                    estimated_time_remaining = int(remaining_pages / processing_rate)
            
            # 更新任务状态
            await task_model.update_task_status(
                task_id=task_id,
                status=self._get_task_status(progress, failed_pages, total_pages),
                progress=progress,
                processed_pages=processed_pages,
                failed_pages=failed_pages,
                processing_rate=processing_rate,
                estimated_time_remaining=estimated_time_remaining
            )
            
            # 记录进度历史
            await task_progress_model.record_progress(
                task_id=task_id,
                progress=progress,
                processed_pages=processed_pages,
                failed_pages=failed_pages,
                processing_rate=processing_rate,
                estimated_time_remaining=estimated_time_remaining,
                message=f"Processing: {processed_pages}/{total_pages} pages"
            )
            
            # 发送WebSocket进度消息（静默更新，不触发前端跳转）
            if self.websocket_manager:
                progress_message = MessageFactory.create_task_progress(
                    task_id=task_id,
                    progress=progress,
                    current_page=processed_pages,
                    total_pages=total_pages,
                    status="processing",
                    message=f"Processing page {processed_pages} of {total_pages}",
                    processed_pages=processed_pages,
                    failed_pages=failed_pages,
                    estimated_time_remaining=estimated_time_remaining,
                    processing_rate=processing_rate,
                    silent=True  # 轮询更新标记为静默，避免跳转
                )
                await self.websocket_manager.send_to_task_subscribers(
                    task_id, progress_message.to_dict()
                )
            
            logger.debug(f"任务进度更新 {task_id}: {progress:.1f}% ({processed_pages}/{total_pages})")
            
        except Exception as e:
            logger.error(f"计算任务进度失败 {task_id}: {e}")
    
    def _get_task_status(self, progress: float, failed_pages: int, total_pages: int):
        """根据进度确定任务状态"""
        from models.database import TaskStatus
        
        if progress >= 100:
            return TaskStatus.COMPLETED if failed_pages == 0 else TaskStatus.COMPLETED
        elif progress > 0:
            return TaskStatus.PROCESSING
        else:
            return TaskStatus.PROCESSING
    
    async def get_task_progress_stats(self, task_id: str) -> Dict[str, Any]:
        """获取任务进度统计"""
        try:
            # 获取进度历史
            progress_history = await task_progress_model.get_task_progress_history(task_id, limit=50)
            
            # 获取页面统计
            page_results = await page_result_model.get_task_page_results(task_id)
            
            # 计算统计信息
            stats = {
                'progress_history': progress_history,
                'page_count': len(page_results),
                'completed_pages': sum(1 for p in page_results if p.get('status') == 'completed'),
                'failed_pages': sum(1 for p in page_results if p.get('status') == 'failed'),
                'processing_pages': sum(1 for p in page_results if p.get('status') == 'processing'),
                'pending_pages': sum(1 for p in page_results if p.get('status') == 'pending'),
                'average_processing_time': 0,
                'total_content_length': 0
            }
            
            # 计算平均处理时间和内容长度
            processing_times = [p.get('processing_time', 0) for p in page_results if p.get('processing_time')]
            if processing_times:
                stats['average_processing_time'] = sum(processing_times) / len(processing_times)
            
            stats['total_content_length'] = sum(p.get('content_length', 0) for p in page_results if p.get('content_length'))
            
            return stats
            
        except Exception as e:
            logger.error(f"获取任务进度统计失败 {task_id}: {e}")
            return {}
    
    async def cleanup(self):
        """清理进度跟踪器"""
        try:
            await self._stop_progress_tracker()
            self._tracking_tasks.clear()
            logger.info("进度跟踪器清理完成")
        except Exception as e:
            logger.error(f"进度跟踪器清理失败: {e}")

# 创建全局进度跟踪器实例
def create_progress_tracker(websocket_manager=None) -> ProgressTracker:
    """创建进度跟踪器实例"""
    return ProgressTracker(websocket_manager)

# 全局进度跟踪器实例（需要在应用启动时初始化）
progress_tracker: Optional[ProgressTracker] = None

async def init_progress_tracker(websocket_manager=None):
    """初始化全局进度跟踪器"""
    global progress_tracker
    if progress_tracker is None:
        progress_tracker = create_progress_tracker(websocket_manager)
        logger.info("全局进度跟踪器初始化完成")

async def shutdown_progress_tracker():
    """关闭全局进度跟踪器"""
    global progress_tracker
    if progress_tracker:
        await progress_tracker.cleanup()
        progress_tracker = None
        logger.info("全局进度跟踪器已关闭")