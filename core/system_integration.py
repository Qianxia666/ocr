"""
系统集成模块 - 协调超时管理、错误处理、监控和恢复机制
"""
import asyncio
import logging
from typing import Dict, Any, Optional, Callable
from datetime import datetime

from .timeout_manager import TimeoutManager, TimeoutLevel
from .error_handler import ErrorHandler, ErrorType
from .system_monitor import SystemMonitor
from .recovery_manager import RecoveryManager
from models.websocket_messages import MessageFactory

logger = logging.getLogger(__name__)

class SystemIntegration:
    """系统集成器 - 统一管理所有超时、错误处理、监控和恢复功能"""
    
    def __init__(self, runtime_config=None, websocket_manager=None):
        """
        初始化系统集成器
        :param runtime_config: 运行时配置对象引用
        :param websocket_manager: WebSocket管理器，用于发送通知
        """
        self.websocket_manager = websocket_manager
        self.timeout_manager = TimeoutManager(runtime_config, websocket_manager)
        self.error_handler = ErrorHandler()
        self.system_monitor = SystemMonitor()
        self.recovery_manager = RecoveryManager()

        # 初始化状态
        self._initialized = False
        self._shutdown_event = asyncio.Event()

        # 设置组件间的回调
        self._setup_component_callbacks()

        logger.info("系统集成器创建完成")
    
    def _setup_component_callbacks(self):
        """设置组件间的回调函数"""
        # 注释掉不存在的回调方法，避免启动错误
        # TODO: 根据实际组件接口实现回调机制
        # 超时管理器回调
        # self.timeout_manager.set_warning_callback(self._on_timeout_warning)
        # self.timeout_manager.set_timeout_callback(self._on_timeout_occurred)
        
        # 错误处理器回调
        # self.error_handler.set_recovery_callback(self._on_error_recovery)
        
        # 系统监控器回调
        # self.system_monitor.set_alert_callback(self._on_system_alert)
        
        # 恢复管理器回调
        # self.recovery_manager.set_notification_callback(self._on_recovery_notification)
        
        logger.warning("组件回调机制暂时禁用，需要根据实际组件接口实现")
    
    async def initialize(self):
        """初始化所有组件"""
        if self._initialized:
            return
        
        try:
            # 初始化各个组件
            await self.timeout_manager.start()
            # 尝试初始化其他组件，如果失败则记录警告但不阻止启动
            try:
                await self.error_handler.initialize()
            except Exception as e:
                logger.warning(f"错误处理器初始化失败: {e}")
            
            try:
                await self.system_monitor.initialize()
            except Exception as e:
                logger.warning(f"系统监控器初始化失败: {e}")
            
            try:
                await self.recovery_manager.initialize()
            except Exception as e:
                logger.warning(f"恢复管理器初始化失败: {e}")
            
            # 启动监控任务
            asyncio.create_task(self._monitoring_loop())
            
            self._initialized = True
            logger.info("系统集成器初始化完成")
            
        except Exception as e:
            logger.error(f"系统集成器初始化失败: {e}")
            raise
    
    async def shutdown(self):
        """关闭所有组件"""
        if not self._initialized:
            return
        
        try:
            # 设置关闭事件
            self._shutdown_event.set()
            
            # 关闭各个组件
            await self.timeout_manager.stop()
            # await self.error_handler.shutdown()
            # await self.system_monitor.shutdown()
            # await self.recovery_manager.shutdown()
            
            self._initialized = False
            logger.info("系统集成器关闭完成")
            
        except Exception as e:
            logger.error(f"系统集成器关闭失败: {e}")
    
    # ========== 任务处理相关方法 ==========
    
    async def execute_with_protection(self,
                                    task_id: str,
                                    operation: Callable,
                                    timeout_level: TimeoutLevel = TimeoutLevel.API_REQUEST,
                                    recovery_strategy: str = "auto",
                                    max_retries: int = 3,
                                    **kwargs) -> Any:
        """
        执行受保护的操作 - 包含超时、错误处理和恢复机制
        :param task_id: 任务ID
        :param operation: 要执行的操作
        :param timeout_level: 超时级别
        :param recovery_strategy: 恢复策略
        :param max_retries: 最大重试次数
        :param kwargs: 操作参数
        :return: 操作结果
        """
        checkpoint_id = None
        last_error = None
        
        try:
            # 使用非递归的重试机制
            for retry_count in range(max_retries + 1):  # +1 包括第一次尝试
                try:
                    # 创建检查点（仅在第一次尝试时创建）
                    if retry_count == 0:
                        from .recovery_manager import CheckpointType
                        checkpoint_id = await self.recovery_manager.create_checkpoint(
                            task_id=task_id,
                            checkpoint_type=CheckpointType.TASK_START,
                            progress=0.0,
                            state_data={"operation": operation.__name__, **kwargs}
                        )
                    
                    # 启动超时保护
                    timeout_context = await self.timeout_manager.start_timeout(
                        task_id, timeout_level, operation.__name__
                    )
                    
                    try:
                        # 执行操作
                        result = await operation(**kwargs)
                        
                        # 停止超时保护
                        await self.timeout_manager.stop_timeout(timeout_context.timeout_id)
                        
                        # 清理检查点
                        if checkpoint_id:
                            await self.recovery_manager.cleanup_checkpoint(checkpoint_id)
                        
                        return result
                        
                    except asyncio.TimeoutError as e:
                        # 处理超时错误
                        await self._handle_timeout_error(task_id, timeout_context, str(e))
                        raise
                        
                    except Exception as e:
                        # 审查失败异常不进入重试逻辑，直接传播
                        from core.content_moderator import ModerationFailedException
                        if isinstance(e, ModerationFailedException):
                            raise

                        last_error = e
                        # 处理其他错误
                        if retry_count < max_retries:
                            recovery_result = await self.error_handler.handle_error(
                                e, task_id, recovery_strategy
                            )

                            if recovery_result.should_retry:
                                # 计算退避延迟
                                retry_delay = min(1.0 * (2 ** retry_count), 30.0)  # 指数退避，最大30秒
                                logger.info(f"任务 {task_id} 准备第 {retry_count + 1} 次重试，延迟 {retry_delay:.1f} 秒")
                                await asyncio.sleep(retry_delay)
                                continue  # 继续下一次重试

                        # 如果是最后一次重试或者不需要重试，抛出异常
                        raise
                
                except Exception as e:
                    # 审查失败异常直接传播，不判断重试次数
                    from core.content_moderator import ModerationFailedException
                    if isinstance(e, ModerationFailedException):
                        raise

                    # 如果所有重试都失败了，抛出最后的错误
                    if retry_count == max_retries:
                        raise
            
        except Exception as e:
            # 最终异常处理
            if checkpoint_id:
                await self.recovery_manager.mark_checkpoint_failed(
                    checkpoint_id, str(e)
                )
            
            # 发送错误通知
            await self._send_error_notification(task_id, str(e))
            raise
    
    async def monitor_task_execution(self, task_id: str, total_pages: int = 1):
        """
        监控任务执行过程
        :param task_id: 任务ID
        :param total_pages: 总页数
        """
        try:
            # 记录任务开始
            await self.system_monitor.record_task_start(task_id, total_pages)
            
            # 启动任务级超时保护
            timeout_context = await self.timeout_manager.start_timeout(
                task_id, TimeoutLevel.TASK_EXECUTION, f"task_{task_id}"
            )
            
            return timeout_context
            
        except Exception as e:
            logger.error(f"任务监控启动失败 {task_id}: {e}")
            raise
    
    async def complete_task_monitoring(self, task_id: str, timeout_context, success: bool = True):
        """
        完成任务监控
        :param task_id: 任务ID
        :param timeout_context: 超时上下文
        :param success: 是否成功
        """
        try:
            # 停止超时保护
            if timeout_context:
                await self.timeout_manager.stop_timeout(timeout_context.timeout_id)
            
            # 记录任务完成
            await self.system_monitor.record_task_completion(task_id, success)
            
        except Exception as e:
            logger.error(f"任务监控完成失败 {task_id}: {e}")
    
    # ========== API调用保护 ==========
    
    async def protected_api_call(self, task_id: str, api_func: Callable, *args, **kwargs):
        """
        受保护的API调用 - 包含超时和重试机制
        :param task_id: 任务ID
        :param api_func: API函数
        :param args: 位置参数
        :param kwargs: 关键字参数
        :return: API调用结果
        """
        return await self.execute_with_protection(
            task_id=task_id,
            operation=api_func,
            timeout_level=TimeoutLevel.API_REQUEST,
            recovery_strategy="auto",
            *args,
            **kwargs
        )
    
    # ========== 系统状态查询 ==========
    
    async def get_system_health(self) -> Dict[str, Any]:
        """获取系统健康状态"""
        try:
            timeout_stats = self.timeout_manager.get_statistics()
            error_stats = self.error_handler.get_statistics()
            system_metrics = self.system_monitor.get_current_metrics()
            recovery_stats = self.recovery_manager.get_statistics()
            
            return {
                "timestamp": datetime.now().isoformat(),
                "overall_status": "healthy",  # 可以根据各组件状态计算
                "timeout_manager": timeout_stats,
                "error_handler": error_stats,
                "system_monitor": system_metrics,
                "recovery_manager": recovery_stats
            }
            
        except Exception as e:
            logger.error(f"获取系统健康状态失败: {e}")
            return {
                "timestamp": datetime.now().isoformat(),
                "overall_status": "error",
                "error": str(e)
            }
    
    # ========== 回调处理方法 ==========
    
    async def _on_timeout_warning(self, timeout_id: str, task_id: str, 
                                elapsed_time: float, total_timeout: float):
        """处理超时预警"""
        try:
            percentage = (elapsed_time / total_timeout) * 100
            
            # 发送超时预警消息
            if self.websocket_manager:
                message = MessageFactory.create_timeout_warning(
                    task_id, timeout_id, elapsed_time, total_timeout, percentage
                )
                await self.websocket_manager.send_to_task_subscribers(
                    task_id, message.to_dict()
                )
            
            logger.warning(f"任务 {task_id} 超时预警: {percentage:.1f}% ({elapsed_time:.1f}s/{total_timeout}s)")
            
        except Exception as e:
            logger.error(f"处理超时预警失败: {e}")
    
    async def _on_timeout_occurred(self, timeout_id: str, task_id: str, elapsed_time: float):
        """处理超时发生"""
        try:
            # 发送超时发生消息
            if self.websocket_manager:
                message = MessageFactory.create_timeout_occurred(
                    task_id, timeout_id, elapsed_time
                )
                await self.websocket_manager.send_to_task_subscribers(
                    task_id, message.to_dict()
                )
            
            logger.error(f"任务 {task_id} 发生超时: {elapsed_time:.1f}s")
            
        except Exception as e:
            logger.error(f"处理超时发生失败: {e}")
    
    async def _on_error_recovery(self, task_id: str, error_type: str, 
                               recovery_action: str, success: bool):
        """处理错误恢复"""
        try:
            # 发送错误恢复消息
            if self.websocket_manager:
                message = MessageFactory.create_error_recovery(
                    task_id, error_type, recovery_action, success
                )
                await self.websocket_manager.send_to_task_subscribers(
                    task_id, message.to_dict()
                )
            
            status = "成功" if success else "失败"
            logger.info(f"任务 {task_id} 错误恢复{status}: {error_type} -> {recovery_action}")
            
        except Exception as e:
            logger.error(f"处理错误恢复失败: {e}")
    
    async def _on_system_alert(self, alert_type: str, message: str, 
                             severity: str, metrics: Dict[str, Any]):
        """处理系统告警"""
        try:
            # 发送系统告警消息
            if self.websocket_manager:
                alert_message = MessageFactory.create_system_alert(
                    alert_type, message, severity, metrics
                )
                await self.websocket_manager.broadcast_message(alert_message.to_dict())
            
            logger.warning(f"系统告警 ({severity}): {alert_type} - {message}")
            
        except Exception as e:
            logger.error(f"处理系统告警失败: {e}")
    
    async def _on_recovery_notification(self, event_type: str, task_id: str, 
                                      message: str, data: Dict[str, Any]):
        """处理恢复通知"""
        try:
            # 发送恢复通知消息
            if self.websocket_manager:
                recovery_message = MessageFactory.create_recovery_notification(
                    event_type, task_id, message, data
                )
                await self.websocket_manager.send_to_task_subscribers(
                    task_id, recovery_message.to_dict()
                )
            
            logger.info(f"恢复通知 {task_id}: {event_type} - {message}")
            
        except Exception as e:
            logger.error(f"处理恢复通知失败: {e}")
    
    # ========== 内部方法 ==========
    
    async def _handle_timeout_error(self, task_id: str, timeout_context, error_msg: str):
        """处理超时错误"""
        try:
            # 记录超时事件
            await self.error_handler.handle_error(
                asyncio.TimeoutError(error_msg), 
                task_id, 
                "auto"
            )
            
            # 发送超时错误通知
            if self.websocket_manager:
                message = MessageFactory.create_timeout_occurred(
                    task_id, timeout_context.timeout_id, timeout_context.elapsed_time
                )
                await self.websocket_manager.send_to_task_subscribers(
                    task_id, message.to_dict()
                )
            
        except Exception as e:
            logger.error(f"处理超时错误失败: {e}")
    
    async def _send_error_notification(self, task_id: str, error_msg: str):
        """发送错误通知"""
        try:
            if self.websocket_manager:
                message = MessageFactory.create_error_notification(
                    "task_error", error_msg, task_id, "error"
                )
                await self.websocket_manager.send_to_task_subscribers(
                    task_id, message.to_dict()
                )
        except Exception as e:
            logger.error(f"发送错误通知失败: {e}")
    
    async def _monitoring_loop(self):
        """监控循环 - 定期检查系统状态"""
        while not self._shutdown_event.is_set():
            try:
                # 检查系统资源
                metrics = await self.system_monitor._collect_system_metrics()
                await self.system_monitor._check_system_alerts(metrics)
                
                # 清理过期数据
                # await self.timeout_manager.cleanup_expired_timeouts()
                # await self.error_handler.cleanup_old_records()
                # await self.recovery_manager.cleanup_expired_checkpoints()
                
                # 等待下一次检查
                await asyncio.sleep(30)  # 每30秒检查一次
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"监控循环异常: {e}")
                await asyncio.sleep(60)  # 发生异常时等待更长时间

# 全局系统集成器实例
system_integration: Optional[SystemIntegration] = None

async def init_system_integration(runtime_config=None, websocket_manager=None):
    """
    初始化全局系统集成器
    :param runtime_config: 运行时配置对象引用
    :param websocket_manager: WebSocket管理器
    """
    global system_integration
    if system_integration is None:
        system_integration = SystemIntegration(runtime_config, websocket_manager)
        await system_integration.initialize()
        logger.info("全局系统集成器初始化完成")

async def shutdown_system_integration():
    """关闭全局系统集成器"""
    global system_integration
    if system_integration:
        await system_integration.shutdown()
        system_integration = None
        logger.info("全局系统集成器已关闭")

def get_system_integration() -> Optional[SystemIntegration]:
    """获取全局系统集成器实例"""
    return system_integration