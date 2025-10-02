import asyncio
import time
import logging
from typing import Dict, Any, Optional, Callable, Set, List
from datetime import datetime, timezone, timedelta
from dataclasses import dataclass, field
from enum import Enum
import weakref

logger = logging.getLogger(__name__)

class TimeoutLevel(Enum):
    """超时级别枚举"""
    API_REQUEST = "api_request"      # API请求级超时 (30秒)
    PAGE_PROCESSING = "page_processing"  # 页面处理级超时 (300秒)
    TASK_EXECUTION = "task_execution"    # 任务级超时 (3600秒)
    CONNECTION = "connection"        # 连接级超时 (WebSocket心跳)

class TimeoutAction(Enum):
    """超时后的动作"""
    CANCEL = "cancel"                # 取消操作
    RETRY = "retry"                  # 重试操作
    WARNING = "warning"              # 发送警告
    ESCALATE = "escalate"            # 升级处理

@dataclass
class TimeoutConfig:
    """超时配置"""
    timeout_seconds: float
    warning_threshold: float = 0.8   # 达到80%时发送预警
    max_retries: int = 3
    retry_delay: float = 1.0
    action: TimeoutAction = TimeoutAction.CANCEL
    callback: Optional[Callable] = None

@dataclass
class TimeoutContext:
    """超时上下文"""
    timeout_id: str
    level: TimeoutLevel
    config: TimeoutConfig
    start_time: float
    task_id: Optional[str] = None
    page_number: Optional[int] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    warning_sent: bool = False
    retry_count: int = 0
    
    @property
    def elapsed_time(self) -> float:
        """已经过的时间"""
        return time.time() - self.start_time
    
    @property
    def remaining_time(self) -> float:
        """剩余时间"""
        return max(0, self.config.timeout_seconds - self.elapsed_time)
    
    @property
    def progress_ratio(self) -> float:
        """进度比例 (0-1)"""
        return min(1.0, self.elapsed_time / self.config.timeout_seconds)
    
    @property
    def is_warning_threshold_reached(self) -> bool:
        """是否达到预警阈值"""
        return self.progress_ratio >= self.config.warning_threshold
    
    @property
    def is_timeout(self) -> bool:
        """是否已超时"""
        return self.elapsed_time >= self.config.timeout_seconds

class TimeoutManager:
    """多层超时管理器"""
    
    def __init__(self, websocket_manager=None):
        """
        初始化超时管理器
        :param websocket_manager: WebSocket管理器，用于发送预警通知
        """
        self.websocket_manager = websocket_manager
        self._active_timeouts: Dict[str, TimeoutContext] = {}
        self._timeout_tasks: Dict[str, asyncio.Task] = {}
        self._lock = asyncio.Lock()
        self._running = False
        self._monitor_task: Optional[asyncio.Task] = None
        
        # 默认超时配置
        self._default_configs = {
            TimeoutLevel.API_REQUEST: TimeoutConfig(
                timeout_seconds=30.0,
                warning_threshold=0.8,
                max_retries=3,
                retry_delay=1.0,
                action=TimeoutAction.RETRY
            ),
            TimeoutLevel.PAGE_PROCESSING: TimeoutConfig(
                timeout_seconds=300.0,
                warning_threshold=0.8,
                max_retries=2,
                retry_delay=5.0,
                action=TimeoutAction.RETRY
            ),
            TimeoutLevel.TASK_EXECUTION: TimeoutConfig(
                timeout_seconds=3600.0,
                warning_threshold=0.9,
                max_retries=1,
                retry_delay=10.0,
                action=TimeoutAction.WARNING
            ),
            TimeoutLevel.CONNECTION: TimeoutConfig(
                timeout_seconds=300.0,
                warning_threshold=0.8,
                max_retries=0,
                retry_delay=0.0,
                action=TimeoutAction.CANCEL
            )
        }
        
        # 统计信息
        self._stats = {
            'total_timeouts': 0,
            'active_timeouts': 0,
            'warnings_sent': 0,
            'timeouts_by_level': {level.value: 0 for level in TimeoutLevel},
            'retries_performed': 0,
            'cancellations': 0
        }
        
        logger.info("超时管理器初始化完成")
    
    async def start(self):
        """启动超时管理器"""
        if self._running:
            return
        
        self._running = True
        # 启动监控任务
        self._monitor_task = asyncio.create_task(self._monitor_timeouts())
        logger.info("超时管理器已启动")
    
    async def stop(self):
        """停止超时管理器"""
        if not self._running:
            return
        
        self._running = False
        
        # 停止监控任务
        if self._monitor_task:
            self._monitor_task.cancel()
            try:
                await self._monitor_task
            except asyncio.CancelledError:
                pass
        
        # 取消所有活跃的超时任务
        async with self._lock:
            for timeout_task in self._timeout_tasks.values():
                timeout_task.cancel()
            
            # 等待所有任务完成
            if self._timeout_tasks:
                await asyncio.gather(*self._timeout_tasks.values(), return_exceptions=True)
            
            self._timeout_tasks.clear()
            self._active_timeouts.clear()
        
        logger.info("超时管理器已停止")
    
    async def start_timeout(self, 
                          timeout_id: str,
                          level: TimeoutLevel,
                          task_id: Optional[str] = None,
                          page_number: Optional[int] = None,
                          custom_config: Optional[TimeoutConfig] = None,
                          metadata: Optional[Dict[str, Any]] = None) -> TimeoutContext:
        """
        开始一个超时监控
        :param timeout_id: 超时ID
        :param level: 超时级别
        :param task_id: 关联的任务ID
        :param page_number: 关联的页面号
        :param custom_config: 自定义超时配置
        :param metadata: 元数据
        :return: 超时上下文
        """
        config = custom_config or self._default_configs[level]
        
        context = TimeoutContext(
            timeout_id=timeout_id,
            level=level,
            config=config,
            start_time=time.time(),
            task_id=task_id,
            page_number=page_number,
            metadata=metadata or {}
        )
        
        async with self._lock:
            # 如果已存在相同ID的超时，先取消它
            if timeout_id in self._active_timeouts:
                await self._cancel_timeout_internal(timeout_id)
            
            self._active_timeouts[timeout_id] = context
            self._stats['active_timeouts'] += 1
            self._stats['total_timeouts'] += 1
            
            # 创建超时监控任务
            timeout_task = asyncio.create_task(
                self._handle_timeout(context)
            )
            self._timeout_tasks[timeout_id] = timeout_task
        
        return context
    
    async def cancel_timeout(self, timeout_id: str) -> bool:
        """
        取消超时监控
        :param timeout_id: 超时ID
        :return: 是否成功取消
        """
        async with self._lock:
            return await self._cancel_timeout_internal(timeout_id)
    
    async def stop_timeout(self, timeout_id: str) -> bool:
        """
        停止超时监控（别名方法，与cancel_timeout相同）
        :param timeout_id: 超时ID
        :return: 是否成功停止
        """
        return await self.cancel_timeout(timeout_id)
    
    async def _cancel_timeout_internal(self, timeout_id: str) -> bool:
        """内部取消超时方法（需要持有锁）"""
        if timeout_id not in self._active_timeouts:
            return False
        
        # 取消超时任务
        if timeout_id in self._timeout_tasks:
            self._timeout_tasks[timeout_id].cancel()
            del self._timeout_tasks[timeout_id]
        
        # 移除超时上下文
        del self._active_timeouts[timeout_id]
        self._stats['active_timeouts'] -= 1
        
        return True
    
    async def extend_timeout(self, timeout_id: str, additional_seconds: float) -> bool:
        """
        延长超时时间
        :param timeout_id: 超时ID
        :param additional_seconds: 额外的秒数
        :return: 是否成功延长
        """
        async with self._lock:
            if timeout_id not in self._active_timeouts:
                return False
            
            context = self._active_timeouts[timeout_id]
            context.config.timeout_seconds += additional_seconds
            
            logger.info(f"延长超时时间: {timeout_id} (+{additional_seconds}s)")
            return True
    
    async def get_timeout_status(self, timeout_id: str) -> Optional[Dict[str, Any]]:
        """
        获取超时状态
        :param timeout_id: 超时ID
        :return: 超时状态信息
        """
        async with self._lock:
            if timeout_id not in self._active_timeouts:
                return None
            
            context = self._active_timeouts[timeout_id]
            return {
                'timeout_id': timeout_id,
                'level': context.level.value,
                'elapsed_time': context.elapsed_time,
                'remaining_time': context.remaining_time,
                'progress_ratio': context.progress_ratio,
                'timeout_seconds': context.config.timeout_seconds,
                'warning_threshold': context.config.warning_threshold,
                'warning_sent': context.warning_sent,
                'retry_count': context.retry_count,
                'max_retries': context.config.max_retries,
                'task_id': context.task_id,
                'page_number': context.page_number,
                'metadata': context.metadata
            }
    
    async def get_all_timeouts(self) -> List[Dict[str, Any]]:
        """获取所有活跃超时的状态"""
        async with self._lock:
            timeouts = []
            for timeout_id in self._active_timeouts:
                status = await self.get_timeout_status(timeout_id)
                if status:
                    timeouts.append(status)
            return timeouts
    
    async def _handle_timeout(self, context: TimeoutContext):
        """处理单个超时监控"""
        try:
            while True:
                # 检查间隔
                check_interval = min(5.0, context.config.timeout_seconds * 0.1)
                await asyncio.sleep(check_interval)
                
                # 检查是否需要发送预警
                if (not context.warning_sent and 
                    context.is_warning_threshold_reached and 
                    not context.is_timeout):
                    await self._send_timeout_warning(context)
                    context.warning_sent = True
                
                # 检查是否超时
                if context.is_timeout:
                    await self._handle_timeout_action(context)
                    break

        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.error(f"超时监控异常 {context.timeout_id}: {e}")
        finally:
            # 清理
            async with self._lock:
                if context.timeout_id in self._active_timeouts:
                    del self._active_timeouts[context.timeout_id]
                    self._stats['active_timeouts'] -= 1
                if context.timeout_id in self._timeout_tasks:
                    del self._timeout_tasks[context.timeout_id]
    
    async def _send_timeout_warning(self, context: TimeoutContext):
        """发送超时预警"""
        try:
            warning_message = {
                'type': 'timeout_warning',
                'timeout_id': context.timeout_id,
                'level': context.level.value,
                'task_id': context.task_id,
                'page_number': context.page_number,
                'elapsed_time': context.elapsed_time,
                'remaining_time': context.remaining_time,
                'progress_ratio': context.progress_ratio,
                'message': f"{context.level.value}即将超时",
                'timestamp': datetime.now(timezone.utc).isoformat()
            }
            
            # 通过WebSocket发送预警
            if self.websocket_manager and context.task_id:
                await self.websocket_manager.send_to_task_subscribers(
                    context.task_id, warning_message
                )
            
            # 调用自定义回调
            if context.config.callback:
                try:
                    await context.config.callback(context, 'warning')
                except Exception as e:
                    logger.error(f"超时预警回调异常: {e}")
            
            self._stats['warnings_sent'] += 1
            logger.warning(f"发送超时预警: {context.timeout_id} ({context.level.value})")
            
        except Exception as e:
            logger.error(f"发送超时预警失败 {context.timeout_id}: {e}")
    
    async def _handle_timeout_action(self, context: TimeoutContext):
        """处理超时动作"""
        try:
            self._stats['timeouts_by_level'][context.level.value] += 1
            
            if context.config.action == TimeoutAction.RETRY:
                await self._handle_timeout_retry(context)
            elif context.config.action == TimeoutAction.WARNING:
                await self._handle_timeout_warning_only(context)
            elif context.config.action == TimeoutAction.CANCEL:
                await self._handle_timeout_cancellation(context)
            elif context.config.action == TimeoutAction.ESCALATE:
                await self._handle_timeout_escalation(context)
            
            # 调用自定义回调
            if context.config.callback:
                try:
                    await context.config.callback(context, 'timeout')
                except Exception as e:
                    logger.error(f"超时处理回调异常: {e}")
                    
        except Exception as e:
            logger.error(f"处理超时动作失败 {context.timeout_id}: {e}")
    
    async def _handle_timeout_retry(self, context: TimeoutContext):
        """处理超时重试"""
        if context.retry_count >= context.config.max_retries:
            logger.error(f"超时重试次数已达上限: {context.timeout_id}")
            await self._handle_timeout_cancellation(context)
            return
        
        context.retry_count += 1
        self._stats['retries_performed'] += 1
        
        # 发送重试通知
        retry_message = {
            'type': 'timeout_retry',
            'timeout_id': context.timeout_id,
            'level': context.level.value,
            'task_id': context.task_id,
            'page_number': context.page_number,
            'retry_count': context.retry_count,
            'max_retries': context.config.max_retries,
            'message': f"{context.level.value}超时，正在重试",
            'timestamp': datetime.now(timezone.utc).isoformat()
        }
        
        if self.websocket_manager and context.task_id:
            await self.websocket_manager.send_to_task_subscribers(
                context.task_id, retry_message
            )
        
        # 等待重试延迟
        if context.config.retry_delay > 0:
            await asyncio.sleep(context.config.retry_delay)
        
        # 重置超时时间
        context.start_time = time.time()
        context.warning_sent = False
        
        logger.info(f"执行超时重试: {context.timeout_id} (第{context.retry_count}次)")
    
    async def _handle_timeout_warning_only(self, context: TimeoutContext):
        """处理仅警告的超时"""
        warning_message = {
            'type': 'timeout_exceeded',
            'timeout_id': context.timeout_id,
            'level': context.level.value,
            'task_id': context.task_id,
            'page_number': context.page_number,
            'elapsed_time': context.elapsed_time,
            'timeout_seconds': context.config.timeout_seconds,
            'message': f"{context.level.value}已超时，但继续执行",
            'timestamp': datetime.now(timezone.utc).isoformat()
        }
        
        if self.websocket_manager and context.task_id:
            await self.websocket_manager.send_to_task_subscribers(
                context.task_id, warning_message
            )
        
        logger.warning(f"超时但继续执行: {context.timeout_id} ({context.level.value})")
    
    async def _handle_timeout_cancellation(self, context: TimeoutContext):
        """处理超时取消"""
        self._stats['cancellations'] += 1
        
        cancellation_message = {
            'type': 'timeout_cancellation',
            'timeout_id': context.timeout_id,
            'level': context.level.value,
            'task_id': context.task_id,
            'page_number': context.page_number,
            'elapsed_time': context.elapsed_time,
            'timeout_seconds': context.config.timeout_seconds,
            'message': f"{context.level.value}超时，操作已取消",
            'timestamp': datetime.now(timezone.utc).isoformat()
        }
        
        if self.websocket_manager and context.task_id:
            await self.websocket_manager.send_to_task_subscribers(
                context.task_id, cancellation_message
            )
        
        logger.error(f"超时取消操作: {context.timeout_id} ({context.level.value})")
    
    async def _handle_timeout_escalation(self, context: TimeoutContext):
        """处理超时升级"""
        escalation_message = {
            'type': 'timeout_escalation',
            'timeout_id': context.timeout_id,
            'level': context.level.value,
            'task_id': context.task_id,
            'page_number': context.page_number,
            'elapsed_time': context.elapsed_time,
            'timeout_seconds': context.config.timeout_seconds,
            'message': f"{context.level.value}超时，需要人工干预",
            'timestamp': datetime.now(timezone.utc).isoformat()
        }
        
        if self.websocket_manager and context.task_id:
            await self.websocket_manager.send_to_task_subscribers(
                context.task_id, escalation_message
            )
        
        logger.critical(f"超时升级处理: {context.timeout_id} ({context.level.value})")
    
    async def _monitor_timeouts(self):
        """监控所有超时的后台任务"""
        logger.info("超时监控循环已启动")
        
        while self._running:
            try:
                await asyncio.sleep(10)  # 每10秒检查一次
                
                # 清理已完成的超时任务
                async with self._lock:
                    completed_timeouts = []
                    for timeout_id, task in self._timeout_tasks.items():
                        if task.done():
                            completed_timeouts.append(timeout_id)
                    
                    for timeout_id in completed_timeouts:
                        if timeout_id in self._timeout_tasks:
                            del self._timeout_tasks[timeout_id]
                        if timeout_id in self._active_timeouts:
                            del self._active_timeouts[timeout_id]
                            self._stats['active_timeouts'] -= 1
                
                # 记录监控状态
                active_count = len(self._active_timeouts)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"超时监控循环异常: {e}")
    
    def get_stats(self) -> Dict[str, Any]:
        """获取超时管理器统计信息"""
        return {
            'running': self._running,
            'active_timeouts': self._stats['active_timeouts'],
            'total_timeouts': self._stats['total_timeouts'],
            'warnings_sent': self._stats['warnings_sent'],
            'retries_performed': self._stats['retries_performed'],
            'cancellations': self._stats['cancellations'],
            'timeouts_by_level': dict(self._stats['timeouts_by_level']),
            'default_configs': {
                level.value: {
                    'timeout_seconds': config.timeout_seconds,
                    'warning_threshold': config.warning_threshold,
                    'max_retries': config.max_retries,
                    'action': config.action.value
                }
                for level, config in self._default_configs.items()
            }
        }
    
    async def get_statistics(self) -> Dict[str, Any]:
        """获取超时管理器统计信息（别名方法）"""
        return self.get_stats()
    
    def update_default_config(self, level: TimeoutLevel, config: TimeoutConfig):
        """更新默认超时配置"""
        self._default_configs[level] = config
        logger.info(f"更新默认超时配置: {level.value}")

# 全局超时管理器实例
timeout_manager: Optional[TimeoutManager] = None

async def init_timeout_manager(websocket_manager=None):
    """初始化全局超时管理器"""
    global timeout_manager
    if timeout_manager is None:
        timeout_manager = TimeoutManager(websocket_manager)
        await timeout_manager.start()
        logger.info("全局超时管理器初始化完成")

async def shutdown_timeout_manager():
    """关闭全局超时管理器"""
    global timeout_manager
    if timeout_manager:
        await timeout_manager.stop()
        timeout_manager = None
        logger.info("全局超时管理器已关闭")