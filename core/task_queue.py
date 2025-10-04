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
        self._cancelled_tasks: Dict[str, TaskItem] = {}
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

    def _get_log_prefix(self, task_id: str = None, task_item: TaskItem = None) -> str:
        """获取日志前缀（包含文件名）"""
        if task_item and task_item.file_name:
            return f"[{task_item.file_name}] "
        if task_id:
            # 从各个任务字典中查找
            for tasks_dict in [self._pending_tasks, self._processing_tasks,
                              self._completed_tasks, self._failed_tasks, self._cancelled_tasks]:
                if task_id in tasks_dict and tasks_dict[task_id].file_name:
                    return f"[{tasks_dict[task_id].file_name}] "
        return ''

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
                # 从metadata中提取user_id和client_ip
                user_id = task_item.metadata.get('user_id') if task_item.metadata else None
                client_ip = task_item.metadata.get('client_ip') if task_item.metadata else None

                success = await task_model.create_task(
                    task_id=task_item.task_id,
                    task_type=task_item.task_type,
                    file_name=task_item.file_name,
                    file_size=task_item.file_size,
                    total_pages=task_item.total_pages,
                    user_id=user_id,
                    client_ip=client_ip,
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

                    logger.info(f"[{task_item.file_name}] 任务 {task_item.task_id} 开始处理")
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
                       result_summary: Optional[Dict[str, Any]] = None,
                       final_status: Optional[TaskStatus] = None) -> bool:
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

                if final_status is not None:
                    status = final_status
                else:
                    status = TaskStatus.COMPLETED if success else TaskStatus.FAILED

                if status == TaskStatus.COMPLETED:
                    self._completed_tasks[task_id] = task_item
                    self._stats['total_processed'] += 1
                    logger.info(f"任务 {task_id} 处理完成")
                elif status == TaskStatus.CANCELLED:
                    self._cancelled_tasks[task_id] = task_item
                    logger.info(f"任务 {task_id} 已取消: {error_message or '无详情'}")
                else:
                    self._failed_tasks[task_id] = task_item
                    self._stats['total_failed'] += 1
                    logger.error(f"任务 {task_id} 处理失败: {error_message}")

                # 更新数据库状态
                await task_model.update_task_status(
                    task_id, status, error_message,
                    progress=100.0 if status == TaskStatus.COMPLETED else None
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
                            status_label = '成功' if status == TaskStatus.COMPLETED else ('取消' if status == TaskStatus.CANCELLED else '失败')
                            logger.info(f"已清理临时文件 {temp_file_path} (任务{status_label})")
                        else:
                            logger.debug(f"临时文件已不存在: {temp_file_path}")
                    except Exception as e:
                        logger.warning(f"清理临时文件失败 {temp_file_path}: {e}")

                return True

        except Exception as e:
            logger.error(f"标记任务完成失败 {task_id}: {e}")
            return False
    


    async def cancel_task(self, task_id: str, reason: str = "任务已取消") -> bool:
        """取消队列中的等待任务"""
        task_item = None
        cleanup_file = None
        try:
            async with self._lock:
                if task_id in self._pending_tasks:
                    task_item = self._pending_tasks.pop(task_id)
                    new_queue = asyncio.PriorityQueue(maxsize=self._queue.maxsize)
                    while True:
                        try:
                            priority_item = self._queue.get_nowait()
                        except asyncio.QueueEmpty:
                            break
                        if priority_item[2].task_id == task_id:
                            continue
                        new_queue.put_nowait(priority_item)
                    self._queue = new_queue
                    self._cancelled_tasks[task_id] = task_item
                    if task_item.metadata and 'temp_file_path' in task_item.metadata:
                        cleanup_file = task_item.metadata['temp_file_path']
                elif task_id in self._processing_tasks:
                    logger.info(f"任务 {task_id} 正在处理，等待工作线程取消")
                    return False
                elif task_id in self._cancelled_tasks or task_id in self._failed_tasks or task_id in self._completed_tasks:
                    return False
                else:
                    return False

            if not task_item:
                return False

            await task_model.update_task_status(
                task_id, TaskStatus.CANCELLED, reason
            )

            if task_item.task_type == TaskType.PDF_OCR:
                for page_number in range(1, task_item.total_pages + 1):
                    await page_result_model.update_page_result(
                        task_id, page_number, "cancelled",
                        error_message=reason
                    )

            if cleanup_file:
                try:
                    import os
                    if os.path.exists(cleanup_file):
                        os.unlink(cleanup_file)
                        logger.info(f"取消pending任务清理临时文件: {cleanup_file} (任务: {task_id})")
                except Exception as e:
                    logger.warning(f"取消任务清理临时文件失败 {cleanup_file}: {e}")

            logger.info(f"任务 {task_id} 已从等待队列取消")
            return True

        except Exception as e:
            logger.error(f"取消任务失败 {task_id}: {e}")
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
            elif task_id in self._cancelled_tasks:
                return "cancelled"
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
        程序异常终止后恢复时，删除临时文件并标记任务为失败
        :return: 恢复的任务数量
        """
        try:
            import os

            # 获取所有pending状态的任务
            pending_tasks = await task_model.get_tasks_by_status(TaskStatus.PENDING)

            # 获取所有processing状态的任务（可能是程序异常退出时留下的）
            processing_tasks = await task_model.get_tasks_by_status(TaskStatus.PROCESSING)

            restored_count = 0

            for task_data in pending_tasks + processing_tasks:
                task_id = task_data['id']

                # 检查是否有临时文件需要清理
                metadata = task_data.get('metadata')
                if metadata and isinstance(metadata, str):
                    try:
                        import json
                        metadata = json.loads(metadata)
                    except:
                        metadata = {}
                elif not metadata:
                    metadata = {}

                temp_file_path = metadata.get('temp_file_path')

                # 如果任务处于processing状态（程序异常终止时留下的）
                if task_data['status'] == 'processing':
                    # 清理临时文件
                    if temp_file_path and os.path.exists(temp_file_path):
                        try:
                            os.unlink(temp_file_path)
                            logger.info(f"已清理异常终止任务的临时文件: {temp_file_path} (任务: {task_id})")
                        except Exception as e:
                            logger.warning(f"清理临时文件失败 {temp_file_path}: {e}")

                    # 将任务标记为失败
                    await task_model.update_task_status(
                        task_id, TaskStatus.FAILED,
                        error_message="程序异常终止，任务已取消"
                    )
                    logger.info(f"标记异常终止任务为失败: {task_id}")
                    restored_count += 1

                # 如果任务处于pending状态
                elif task_data['status'] == 'pending':
                    # 也清理pending任务的临时文件，因为文件数据无法恢复
                    if temp_file_path and os.path.exists(temp_file_path):
                        try:
                            os.unlink(temp_file_path)
                            logger.info(f"已清理pending任务的临时文件: {temp_file_path} (任务: {task_id})")
                        except Exception as e:
                            logger.warning(f"清理临时文件失败 {temp_file_path}: {e}")

                    # 标记为失败（因为文件已丢失）
                    await task_model.update_task_status(
                        task_id, TaskStatus.FAILED,
                        error_message="程序重启后源文件已清理，任务无法继续"
                    )
                    logger.info(f"标记pending任务为失败: {task_id}")
                    restored_count += 1

            logger.info(f"处理了 {restored_count} 个未完成任务（已清理临时文件并标记为失败）")
            return restored_count

        except Exception as e:
            logger.error(f"从数据库恢复任务失败: {e}")
            return 0



class WorkerPool:
    """Async worker pool with queue affinity"""

    def __init__(self,
                 task_queue: TaskQueue,
                 min_workers: int = 1,
                 max_workers: int = 5,
                 worker_timeout: float = 300.0,
                 name: str = 'default'):
        """Configure worker pool"""
        self.task_queue = task_queue
        self.min_workers = min_workers
        self.max_workers = max_workers
        self.worker_timeout = worker_timeout
        self.name = name
        self._log_prefix = f"[{self.name}] "

        self._workers: Dict[str, asyncio.Task] = {}
        self._worker_stats: Dict[str, Dict[str, Any]] = {}
        self._active_tasks: Dict[str, asyncio.Task] = {}
        self._cancellation_reasons: Dict[str, str] = {}
        self._running = False
        self._lock = asyncio.Lock()
        self._task_processor: Optional[Callable] = None

        self._log('info', f'worker pool ready (min={min_workers}, max={max_workers})')

    def _log(self, level: str, message: str):
        getattr(logger, level)(f"{self._log_prefix}{message}")

    def set_task_processor(self, processor: Callable):
        """Bind task processor"""
        self._task_processor = processor
        self._log('info', 'task processor registered')

    async def start(self):
        """Start pool"""
        if self._running:
            self._log('warning', 'worker pool already running')
            return

        if not self._task_processor:
            raise ValueError('task processor must be set before starting the pool')

        self._running = True

        for i in range(self.min_workers):
            await self._start_worker(f"worker-{i}")

        self._log('info', f'started with {self.min_workers} worker(s)')

    async def stop(self):
        """Stop pool"""
        if not self._running:
            return

        self._running = False

        async with self._lock:
            workers_to_stop = list(self._workers.keys())

        for worker_id in workers_to_stop:
            await self._stop_worker(worker_id)

        self._log('info', 'worker pool stopped')

    async def _start_worker(self, worker_id: str):
        """Spawn worker task"""
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

        self._log('info', f'worker {worker_id} started')

    async def _stop_worker(self, worker_id: str):
        """Cancel worker task"""
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

        self._log('info', f'worker {worker_id} stopped')

    async def _worker_loop(self, worker_id: str):
        """Worker loop"""
        self._log('info', f'worker {worker_id} loop started')

        while self._running:
            try:
                task_item = await self.task_queue.get(timeout=5.0)

                if task_item is None:
                    continue

                async with self._lock:
                    if worker_id in self._worker_stats:
                        self._worker_stats[worker_id]['status'] = 'processing'
                        self._worker_stats[worker_id]['last_task_at'] = datetime.now(timezone.utc)

                start_time = time.time()
                processing_task = asyncio.create_task(self._task_processor(task_item))
                async with self._lock:
                    self._active_tasks[task_item.task_id] = processing_task
                try:
                    result = await asyncio.wait_for(
                        processing_task,
                        timeout=self.worker_timeout
                    )

                    processing_time = time.time() - start_time

                    await self.task_queue.task_done(
                        task_item.task_id,
                        success=True,
                        result_summary=result if isinstance(result, dict) else None
                    )

                    async with self._lock:
                        if worker_id in self._worker_stats:
                            self._worker_stats[worker_id]['tasks_processed'] += 1
                            self._worker_stats[worker_id]['status'] = 'idle'

                    self._log('info', f'worker {worker_id} completed {task_item.task_id} in {processing_time:.2f}s')

                except asyncio.CancelledError:
                    reason = self._cancellation_reasons.pop(task_item.task_id, "任务已取消")
                    await self.task_queue.task_done(
                        task_item.task_id,
                        success=False,
                        error_message=reason,
                        final_status=TaskStatus.CANCELLED
                    )
                    self._log('info', f'worker {worker_id} cancelled task {task_item.task_id}: {reason}')
                except asyncio.TimeoutError:
                    error_msg = f'task timeout (>{self.worker_timeout}s)'
                    processing_task.cancel()
                    try:
                        await processing_task
                    except Exception:
                        pass
                    await self.task_queue.task_done(
                        task_item.task_id,
                        success=False,
                        error_message=error_msg
                    )
                    self._log('error', f'worker {worker_id} timeout on {task_item.task_id}')

                except Exception as e:
                    error_msg = f'task error: {e}'
                    await self.task_queue.task_done(
                        task_item.task_id,
                        success=False,
                        error_message=error_msg
                    )
                    self._log('error', f'worker {worker_id} error on {task_item.task_id}: {e}')

                finally:
                    async with self._lock:
                        self._active_tasks.pop(task_item.task_id, None)
                        if worker_id in self._worker_stats:
                            self._worker_stats[worker_id]['status'] = 'idle'
                    self._cancellation_reasons.pop(task_item.task_id, None)

            except asyncio.CancelledError:
                self._log('info', f'worker {worker_id} cancelled')
                break
            except Exception as e:
                self._log('error', f'worker {worker_id} loop exception: {e}')
                await asyncio.sleep(1)

    async def cancel_task(self, task_id: str, reason: str = "任务已取消") -> bool:
        """取消正在处理的任务"""
        async with self._lock:
            processing_task = self._active_tasks.get(task_id)
            if not processing_task:
                return False
            self._cancellation_reasons[task_id] = reason
            processing_task.cancel()
        try:
            await processing_task
        except asyncio.CancelledError:
            pass
        except Exception:
            pass
        return True

    async def scale_workers(self, target_count: int) -> bool:
        """Scale worker pool"""
        if not self._running:
            self._log('warning', 'worker pool not running, skip scaling')
            return False

        target_count = max(self.min_workers, min(target_count, self.max_workers))

        async with self._lock:
            current_count = len(self._workers)

        if target_count == current_count:
            return True

        try:
            if target_count > current_count:
                for i in range(current_count, target_count):
                    await self._start_worker(f"worker-{i}")
                self._log('info', f'scaled up to {target_count} worker(s)')
            else:
                async with self._lock:
                    workers_to_stop = list(self._workers.keys())[target_count:]

                for worker_id in workers_to_stop:
                    await self._stop_worker(worker_id)
                self._log('info', f'scaled down to {target_count} worker(s)')

            return True

        except Exception as e:
            self._log('error', f'failed to scale workers: {e}')
            return False

    async def get_worker_stats(self) -> Dict[str, Any]:
        """Return worker stats"""
        async with self._lock:
            stats = {
                'total_workers': len(self._workers),
                'min_workers': self.min_workers,
                'max_workers': self.max_workers,
                'running': self._running,
                'workers': dict(self._worker_stats)
            }

            total_processed = sum(w.get('tasks_processed', 0) for w in self._worker_stats.values())
            active_workers = sum(1 for w in self._worker_stats.values() if w.get('status') == 'processing')

            stats['summary'] = {
                'total_tasks_processed': total_processed,
                'active_workers': active_workers,
                'idle_workers': len(self._workers) - active_workers
            }

            return stats

# 全局实例 - 双队列系统：图片队列和PDF队列分别处理
image_task_queue = TaskQueue(maxsize=1000)  # 图片任务队列
pdf_task_queue = TaskQueue(maxsize=1000)  # PDF任务队列

# 工作者池将在初始化时根据环境变量配置创建
image_worker_pool = None
pdf_worker_pool = None

# 保留旧的变量名以兼容现有代码（将根据任务类型路由）
task_queue = None  # 将在运行时根据任务类型选择对应队列
worker_pool = None  # 将在运行时根据任务类型选择对应工作者池
