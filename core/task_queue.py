import asyncio
import uuid
import logging
from typing import Dict, Any, Optional, Callable, List
from dataclasses import dataclass
from datetime import datetime, timezone
import weakref
import time

from models.database import TaskStatus, TaskType, task_model, page_result_model

logger = logging.getLogger(__name__)

@dataclass
class TaskItem:
    """任务项数据类"""
    task_id: str
    task_type: TaskType
    file_data: bytes
    file_name: str
    file_size: int
    total_pages: int = 1
    metadata: Optional[Dict[str, Any]] = None
    priority: int = 0  # 优先级，数字越小优先级越高
    created_at: datetime = None
    
    def __post_init__(self):
        if self.created_at is None:
            self.created_at = datetime.now(timezone.utc)

class TaskQueue:
    """基于asyncio.Queue的任务队列系统"""
    
    def __init__(self, maxsize: int = 0):
        """
        初始化任务队列
        :param maxsize: 队列最大大小，0表示无限制
        """
        self._queue = asyncio.PriorityQueue(maxsize=maxsize)
        self._pending_tasks: Dict[str, TaskItem] = {}
        self._processing_tasks: Dict[str, TaskItem] = {}
        self._completed_tasks: Dict[str, TaskItem] = {}
        self._failed_tasks: Dict[str, TaskItem] = {}
        self._lock = asyncio.Lock()
        self._task_counter = 0

        # 统计信息
        self._stats = {
            'total_added': 0,
            'total_processed': 0,
            'total_failed': 0,
            'queue_start_time': datetime.now(timezone.utc)
        }

        logger.info("任务队列初始化完成")
    
    async def put(self, task_item: TaskItem) -> bool:
        """
        添加任务到队列
        :param task_item: 任务项
        :return: 是否添加成功
        """
        try:
            async with self._lock:
                # 检查任务是否已存在
                if task_item.task_id in self._pending_tasks:
                    logger.warning(f"任务 {task_item.task_id} 已在队列中")
                    return False
                
                # 将任务添加到数据库
                success = await task_model.create_task(
                    task_id=task_item.task_id,
                    task_type=task_item.task_type,
                    file_name=task_item.file_name,
                    file_size=task_item.file_size,
                    total_pages=task_item.total_pages,
                    metadata=task_item.metadata
                )
                
                if not success:
                    logger.error(f"任务 {task_item.task_id} 数据库创建失败")
                    return False
                
                # 为PDF任务创建页面结果记录
                if task_item.task_type == TaskType.PDF_OCR:
                    for page_num in range(1, task_item.total_pages + 1):
                        await page_result_model.create_page_result(
                            task_item.task_id, page_num, "pending"
                        )
                
                # 添加到队列和内存跟踪
                priority_tuple = (task_item.priority, self._task_counter, task_item)
                await self._queue.put(priority_tuple)
                self._pending_tasks[task_item.task_id] = task_item
                self._task_counter += 1
                self._stats['total_added'] += 1
                
                logger.info(f"任务 {task_item.task_id} 已添加到队列 (优先级: {task_item.priority})")
                return True
                
        except Exception as e:
            logger.error(f"添加任务到队列失败 {task_item.task_id}: {e}")
            return False
    
    async def get(self, timeout: Optional[float] = None) -> Optional[TaskItem]:
        """
        从队列获取任务
        :param timeout: 超时时间（秒）
        :return: 任务项或None
        """
        try:
            if timeout:
                priority_tuple = await asyncio.wait_for(self._queue.get(), timeout=timeout)
            else:
                priority_tuple = await self._queue.get()

            task_item = priority_tuple[2]  # 获取TaskItem

            async with self._lock:
                # 从pending移动到processing
                if task_item.task_id in self._pending_tasks:
                    del self._pending_tasks[task_item.task_id]
                    self._processing_tasks[task_item.task_id] = task_item

                    # 更新数据库状态
                    await task_model.update_task_status(
                        task_item.task_id,
                        TaskStatus.PROCESSING
                    )

                    logger.info(f"任务 {task_item.task_id} 开始处理")
                    return task_item
                else:
                    logger.warning(f"任务 {task_item.task_id} 不在pending状态")
                    return None

        except asyncio.TimeoutError:
            return None
        except Exception as e:
            logger.error(f"从队列获取任务失败: {e}")
            return None
    
    async def task_done(self, task_id: str, success: bool = True, 
                       error_message: Optional[str] = None,
                       result_summary: Optional[Dict[str, Any]] = None) -> bool:
        """
        标记任务完成
        :param task_id: 任务ID
        :param success: 是否成功
        :param error_message: 错误消息
        :param result_summary: 结果摘要
        :return: 是否标记成功
        """
        try:
            async with self._lock:
                # 首先检查任务是否在 processing 队列中
                if task_id not in self._processing_tasks:
                    logger.warning(f"任务 {task_id} 不在processing状态")
                    return False

                task_item = self._processing_tasks[task_id]
                del self._processing_tasks[task_id]

                if success:
                    self._completed_tasks[task_id] = task_item
                    status = TaskStatus.COMPLETED
                    self._stats['total_processed'] += 1
                    logger.info(f"任务 {task_id} 处理完成")
                else:
                    self._failed_tasks[task_id] = task_item
                    status = TaskStatus.FAILED
                    self._stats['total_failed'] += 1
                    logger.error(f"任务 {task_id} 处理失败: {error_message}")

                # 更新数据库状态
                await task_model.update_task_status(
                    task_id, status, error_message,
                    progress=100.0 if success else None
                )

                # 更新结果摘要
                if result_summary:
                    await task_model.update_task_result_summary(task_id, result_summary)

                # 清理临时文件（如果存在）
                if task_item.metadata and 'temp_file_path' in task_item.metadata:
                    temp_file_path = task_item.metadata['temp_file_path']
                    try:
                        import os
                        if os.path.exists(temp_file_path):
                            os.unlink(temp_file_path)
                            logger.info(f"已清理临时文件: {temp_file_path} (任务{'成功' if success else '失败'})")
                        else:
                            logger.debug(f"临时文件已不存在: {temp_file_path}")
                    except Exception as e:
                        logger.warning(f"清理临时文件失败 {temp_file_path}: {e}")

                return True

        except Exception as e:
            logger.error(f"标记任务完成失败 {task_id}: {e}")
            return False
    

    async def get_task_status(self, task_id: str) -> Optional[str]:
        """获取任务状态"""
        async with self._lock:
            if task_id in self._pending_tasks:
                return "pending"
            elif task_id in self._processing_tasks:
                return "processing"
            elif task_id in self._completed_tasks:
                return "completed"
            elif task_id in self._failed_tasks:
                return "failed"
            else:
                # 从数据库查询
                task = await task_model.get_task(task_id)
                return task['status'] if task else None
    
    async def get_queue_stats(self) -> Dict[str, Any]:
        """获取队列统计信息"""
        async with self._lock:
            current_time = datetime.now(timezone.utc)
            uptime = (current_time - self._stats['queue_start_time']).total_seconds()

            return {
                'pending_count': len(self._pending_tasks),
                'processing_count': len(self._processing_tasks),
                'completed_count': len(self._completed_tasks),
                'failed_count': len(self._failed_tasks),
                'queue_size': self._queue.qsize(),
                'total_added': self._stats['total_added'],
                'total_processed': self._stats['total_processed'],
                'total_failed': self._stats['total_failed'],
                'uptime_seconds': uptime,
                'processing_rate': self._stats['total_processed'] / uptime if uptime > 0 else 0
            }
    
    async def clear_completed_tasks(self, max_age_hours: int = 24) -> int:
        """
        清理已完成的旧任务
        :param max_age_hours: 最大保留时间（小时）
        :return: 清理的任务数量
        """
        try:
            async with self._lock:
                current_time = datetime.now(timezone.utc)
                cleared_count = 0
                
                # 清理completed_tasks
                to_remove = []
                for task_id, task_item in self._completed_tasks.items():
                    age_hours = (current_time - task_item.created_at).total_seconds() / 3600
                    if age_hours > max_age_hours:
                        to_remove.append(task_id)
                
                for task_id in to_remove:
                    del self._completed_tasks[task_id]
                    cleared_count += 1
                
                # 清理failed_tasks
                to_remove = []
                for task_id, task_item in self._failed_tasks.items():
                    age_hours = (current_time - task_item.created_at).total_seconds() / 3600
                    if age_hours > max_age_hours:
                        to_remove.append(task_id)
                
                for task_id in to_remove:
                    del self._failed_tasks[task_id]
                    cleared_count += 1
                
                logger.info(f"清理了 {cleared_count} 个旧任务")
                return cleared_count
                
        except Exception as e:
            logger.error(f"清理旧任务失败: {e}")
            return 0
    
    async def restore_from_database(self) -> int:
        """
        从数据库恢复未完成的任务到队列
        :return: 恢复的任务数量
        """
        try:
            # 获取所有pending状态的任务
            pending_tasks = await task_model.get_tasks_by_status(TaskStatus.PENDING)
            
            # 获取所有processing状态的任务（可能是程序异常退出时留下的）
            processing_tasks = await task_model.get_tasks_by_status(TaskStatus.PROCESSING)
            
            restored_count = 0
            
            for task_data in pending_tasks + processing_tasks:
                # 注意：这里无法恢复file_data，需要重新设计或者存储文件路径
                # 暂时跳过数据恢复，只更新状态
                if task_data['status'] == 'processing':
                    # 将processing状态重置为pending
                    await task_model.update_task_status(
                        task_data['id'], TaskStatus.PENDING
                    )
                    logger.info(f"重置任务状态: {task_data['id']} processing -> pending")
                    restored_count += 1
            
            logger.info(f"从数据库恢复了 {restored_count} 个任务状态")
            return restored_count
            
        except Exception as e:
            logger.error(f"从数据库恢复任务失败: {e}")
            return 0

class WorkerPool:
    """异步工作者池"""
    
    def __init__(self, 
                 task_queue: TaskQueue,
                 min_workers: int = 1,
                 max_workers: int = 5,
                 worker_timeout: float = 300.0):
        """
        初始化工作者池
        :param task_queue: 任务队列
        :param min_workers: 最小工作者数量
        :param max_workers: 最大工作者数量
        :param worker_timeout: 工作者超时时间（秒）
        """
        self.task_queue = task_queue
        self.min_workers = min_workers
        self.max_workers = max_workers
        self.worker_timeout = worker_timeout
        
        self._workers: Dict[str, asyncio.Task] = {}
        self._worker_stats: Dict[str, Dict[str, Any]] = {}
        self._running = False
        self._lock = asyncio.Lock()
        self._task_processor: Optional[Callable] = None
        
        logger.info(f"工作者池初始化完成 (min: {min_workers}, max: {max_workers})")
    
    def set_task_processor(self, processor: Callable):
        """设置任务处理器函数"""
        self._task_processor = processor
        logger.info("任务处理器已设置")
    
    async def start(self):
        """启动工作者池"""
        if self._running:
            logger.warning("工作者池已经在运行")
            return
        
        if not self._task_processor:
            raise ValueError("必须先设置任务处理器")
        
        self._running = True
        
        # 启动最小数量的工作者
        for i in range(self.min_workers):
            await self._start_worker(f"worker-{i}")
        
        logger.info(f"工作者池已启动，初始工作者数量: {self.min_workers}")
    
    async def stop(self):
        """停止工作者池"""
        if not self._running:
            return
        
        self._running = False
        
        # 停止所有工作者
        async with self._lock:
            workers_to_stop = list(self._workers.keys())
        
        for worker_id in workers_to_stop:
            await self._stop_worker(worker_id)
        
        logger.info("工作者池已停止")
    
    async def _start_worker(self, worker_id: str):
        """启动单个工作者"""
        if worker_id in self._workers:
            return
        
        async with self._lock:
            task = asyncio.create_task(self._worker_loop(worker_id))
            self._workers[worker_id] = task
            self._worker_stats[worker_id] = {
                'started_at': datetime.now(timezone.utc),
                'tasks_processed': 0,
                'last_task_at': None,
                'status': 'idle'
            }
        
        logger.info(f"工作者 {worker_id} 已启动")
    
    async def _stop_worker(self, worker_id: str):
        """停止单个工作者"""
        async with self._lock:
            if worker_id in self._workers:
                worker_task = self._workers[worker_id]
                worker_task.cancel()
                try:
                    await worker_task
                except asyncio.CancelledError:
                    pass
                
                del self._workers[worker_id]
                del self._worker_stats[worker_id]
                logger.info(f"工作者 {worker_id} 已停止")
    
    async def _worker_loop(self, worker_id: str):
        """工作者主循环"""
        logger.info(f"工作者 {worker_id} 开始工作循环")
        
        while self._running:
            try:
                # 获取任务
                task_item = await self.task_queue.get(timeout=5.0)
                
                if task_item is None:
                    continue
                
                # 更新工作者状态
                async with self._lock:
                    if worker_id in self._worker_stats:
                        self._worker_stats[worker_id]['status'] = 'processing'
                        self._worker_stats[worker_id]['last_task_at'] = datetime.now(timezone.utc)
                
                # 处理任务
                start_time = time.time()
                try:
                    result = await asyncio.wait_for(
                        self._task_processor(task_item),
                        timeout=self.worker_timeout
                    )
                    
                    processing_time = time.time() - start_time
                    
                    # 标记任务完成
                    await self.task_queue.task_done(
                        task_item.task_id, 
                        success=True,
                        result_summary=result if isinstance(result, dict) else None
                    )
                    
                    # 更新工作者统计
                    async with self._lock:
                        if worker_id in self._worker_stats:
                            self._worker_stats[worker_id]['tasks_processed'] += 1
                            self._worker_stats[worker_id]['status'] = 'idle'
                    
                    logger.info(f"工作者 {worker_id} 完成任务 {task_item.task_id} "
                              f"(耗时: {processing_time:.2f}s)")
                
                except asyncio.TimeoutError:
                    error_msg = f"任务处理超时 (>{self.worker_timeout}s)"
                    await self.task_queue.task_done(
                        task_item.task_id, 
                        success=False,
                        error_message=error_msg
                    )
                    logger.error(f"工作者 {worker_id} 任务 {task_item.task_id} 超时")
                
                except Exception as e:
                    error_msg = f"任务处理异常: {str(e)}"
                    await self.task_queue.task_done(
                        task_item.task_id, 
                        success=False,
                        error_message=error_msg
                    )
                    logger.error(f"工作者 {worker_id} 任务 {task_item.task_id} 异常: {e}")
                
                finally:
                    # 重置工作者状态
                    async with self._lock:
                        if worker_id in self._worker_stats:
                            self._worker_stats[worker_id]['status'] = 'idle'
            
            except asyncio.CancelledError:
                logger.info(f"工作者 {worker_id} 被取消")
                break
            except Exception as e:
                logger.error(f"工作者 {worker_id} 循环异常: {e}")
                await asyncio.sleep(1)  # 避免快速循环
    
    async def scale_workers(self, target_count: int) -> bool:
        """
        动态调整工作者数量
        :param target_count: 目标工作者数量
        :return: 是否调整成功
        """
        if not self._running:
            logger.warning("工作者池未运行，无法调整")
            return False
        
        target_count = max(self.min_workers, min(target_count, self.max_workers))
        
        async with self._lock:
            current_count = len(self._workers)
        
        if target_count == current_count:
            return True
        
        try:
            if target_count > current_count:
                # 增加工作者
                for i in range(current_count, target_count):
                    await self._start_worker(f"worker-{i}")
                logger.info(f"工作者数量增加到 {target_count}")
            else:
                # 减少工作者
                async with self._lock:
                    workers_to_stop = list(self._workers.keys())[target_count:]
                
                for worker_id in workers_to_stop:
                    await self._stop_worker(worker_id)
                logger.info(f"工作者数量减少到 {target_count}")
            
            return True
            
        except Exception as e:
            logger.error(f"调整工作者数量失败: {e}")
            return False
    
    async def get_worker_stats(self) -> Dict[str, Any]:
        """获取工作者统计信息"""
        async with self._lock:
            stats = {
                'total_workers': len(self._workers),
                'min_workers': self.min_workers,
                'max_workers': self.max_workers,
                'running': self._running,
                'workers': dict(self._worker_stats)
            }
            
            # 计算汇总统计
            total_processed = sum(w.get('tasks_processed', 0) for w in self._worker_stats.values())
            active_workers = sum(1 for w in self._worker_stats.values() if w.get('status') == 'processing')
            
            stats['summary'] = {
                'total_tasks_processed': total_processed,
                'active_workers': active_workers,
                'idle_workers': len(self._workers) - active_workers
            }
            
            return stats

# 全局实例 - 修改为单工作者，确保顺序处理
task_queue = TaskQueue(maxsize=1000)  # 最大1000个任务
worker_pool = WorkerPool(task_queue, min_workers=1, max_workers=1, worker_timeout=float('inf'))  # 仅使用1个工作者，确保顺序处理，无超时限制