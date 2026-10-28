"""
绯荤粺闆嗘垚妯″潡 - 鍗忚皟瓒呮椂绠＄悊銆侀敊璇鐞嗐€佺洃鎺у拰鎭㈠鏈哄埗
"""
import asyncio
import logging
from typing import Dict, Any, Optional, Callable
from datetime import datetime

from .timeout_manager import TimeoutManager, TimeoutLevel
from .error_handler import ErrorHandler
from .system_monitor import SystemMonitor
from .recovery_manager import RecoveryManager
from models.websocket_messages import MessageFactory

logger = logging.getLogger(__name__)

class SystemIntegration:
    """绯荤粺闆嗘垚鍣?- 缁熶竴绠＄悊鎵€鏈夎秴鏃躲€侀敊璇鐞嗐€佺洃鎺у拰鎭㈠鍔熻兘"""
    
    def __init__(self, runtime_config=None, websocket_manager=None):
        """
        鍒濆鍖栫郴缁熼泦鎴愬櫒
        :param runtime_config: 杩愯鏃堕厤缃璞″紩鐢?
        :param websocket_manager: WebSocket绠＄悊鍣紝鐢ㄤ簬鍙戦€侀€氱煡
        """
        self._runtime_config = runtime_config
        self.websocket_manager = websocket_manager
        self.timeout_manager = TimeoutManager(runtime_config, websocket_manager)
        self.error_handler = ErrorHandler()
        self.system_monitor = SystemMonitor(
            websocket_manager=websocket_manager,
            error_handler=self.error_handler,
        )
        self.recovery_manager = RecoveryManager(
            websocket_manager=websocket_manager,
            error_handler=self.error_handler,
        )

        # 鍒濆鍖栫姸鎬?
        self._initialized = False
        self._shutdown_event = asyncio.Event()

        # 设置组件间的回调
        self._setup_component_callbacks()

        logger.info("system integration created")
    
    def _setup_component_callbacks(self):
        """设置组件间的回调函数"""
        # 娉ㄩ噴鎺変笉瀛樺湪鐨勫洖璋冩柟娉曪紝閬垮厤鍚姩閿欒
        # TODO: 鏍规嵁瀹為檯缁勪欢鎺ュ彛瀹炵幇鍥炶皟鏈哄埗
        # 瓒呮椂绠＄悊鍣ㄥ洖璋?
        # self.timeout_manager.set_warning_callback(self._on_timeout_warning)
        # self.timeout_manager.set_timeout_callback(self._on_timeout_occurred)
        
        # 閿欒澶勭悊鍣ㄥ洖璋?
        # self.error_handler.set_recovery_callback(self._on_error_recovery)
        
        # 绯荤粺鐩戞帶鍣ㄥ洖璋?
        # self.system_monitor.set_alert_callback(self._on_system_alert)
        
        # 鎭㈠绠＄悊鍣ㄥ洖璋?
        # self.recovery_manager.set_notification_callback(self._on_recovery_notification)
        
        logger.warning("component callbacks are temporarily disabled")
    
    def _resolve_max_retries(self, max_retries: Optional[int]) -> int:
        """瑙ｆ瀽鏈€澶ч噸璇曟鏁伴厤缃紝纭繚杩斿洖鏈夋晥鍊?"""
        if max_retries is not None:
            candidate = max_retries
        elif self._runtime_config is not None and hasattr(self._runtime_config, 'max_retries'):
            candidate = getattr(self._runtime_config, 'max_retries')
        else:
            candidate = 3

        try:
            resolved = int(candidate)
        except (TypeError, ValueError):
            resolved = 3

        return max(1, resolved)
    
    async def initialize(self):
        """鍒濆鍖栨墍鏈夌粍浠?"""
        if self._initialized:
            return
        
        try:
            # 鍒濆鍖栧悇涓粍浠?
            await self.timeout_manager.start()
            # 灏濊瘯鍒濆鍖栧叾浠栫粍浠讹紝濡傛灉澶辫触鍒欒褰曡鍛婁絾涓嶉樆姝㈠惎鍔?
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
            
            # 鍚姩鐩戞帶浠诲姟
            asyncio.create_task(self._monitoring_loop())
            
            self._initialized = True
            logger.info("系统集成器初始化完成")
            
        except Exception as e:
            logger.error(f"系统集成器初始化失败: {e}")
            raise
    
    async def shutdown(self):
        """鍏抽棴鎵€鏈夌粍浠?"""
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
            logger.info("system integration shutdown complete")
            
        except Exception as e:
            logger.error(f"系统集成器关闭失败: {e}")
    
    # ========== 任务处理相关方法 ==========
    
    async def execute_with_protection(self,
                                    task_id: str,
                                    operation: Callable,
                                    timeout_level: TimeoutLevel = TimeoutLevel.API_REQUEST,
                                    recovery_strategy: str = "auto",
                                    max_retries: Optional[int] = None,
                                    **kwargs) -> Any:
        """
        鎵ц鍙椾繚鎶ょ殑鎿嶄綔 - 鍖呭惈瓒呮椂銆侀敊璇鐞嗗拰鎭㈠鏈哄埗
        :param task_id: 任务ID
        :param operation: 瑕佹墽琛岀殑鎿嶄綔
        :param timeout_level: 超时级别
        :param recovery_strategy: 鎭㈠绛栫暐
        :param max_retries: 鏈€澶ч噸璇曟鏁帮紙鎬诲皾璇曟鏁帮級
        :param kwargs: 操作参数
        :return: 鎿嶄綔缁撴灉
        """
        checkpoint_id = None
        last_error = None
        attempts_allowed = self._resolve_max_retries(max_retries)
        
        try:
            # 浣跨敤闈為€掑綊鐨勯噸璇曟満鍒?
            for attempt in range(attempts_allowed):
                timeout_context = None
                timeout_stopped = False
                try:
                    # 鍒涘缓妫€鏌ョ偣锛堜粎鍦ㄧ涓€娆″皾璇曟椂鍒涘缓锛?
                    if attempt == 0:
                        from .recovery_manager import CheckpointType
                        checkpoint_id = await self.recovery_manager.create_checkpoint(
                            task_id=task_id,
                            checkpoint_type=CheckpointType.TASK_START,
                            progress=0.0,
                            state_data={"operation": operation.__name__, **kwargs}
                        )
                    
                    # 鍚姩瓒呮椂淇濇姢
                    timeout_context = await self.timeout_manager.start_timeout(
                        timeout_id=f"{task_id}:{operation.__name__}:{attempt}",
                        level=timeout_level,
                        task_id=task_id,
                        metadata={"operation": operation.__name__, "attempt": attempt}
                    )
                    
                    try:
                        # 鎵ц鎿嶄綔
                        result = await operation(**kwargs)
                        
                        # 鍋滄瓒呮椂淇濇姢
                        await self.timeout_manager.stop_timeout(timeout_context.timeout_id)
                        timeout_stopped = True
                        
                        # 娓呯悊妫€鏌ョ偣
                        if checkpoint_id:
                            await self.recovery_manager.cleanup_checkpoint(checkpoint_id)
                        
                        return result
                        
                    except asyncio.TimeoutError as e:
                        # 澶勭悊瓒呮椂閿欒
                        await self._handle_timeout_error(task_id, timeout_context, str(e))
                        raise
                        
                    except Exception as e:

                        last_error = e
                        # 澶勭悊鍏朵粬閿欒
                        if attempt < attempts_allowed - 1:
                            recovery_result = await self.error_handler.handle_error(
                                e, task_id, recovery_strategy
                            )

                            if recovery_result.should_retry:
                                logger.info(
                                    "task %s scheduled for retry attempt %s/%s",
                                    task_id,
                                    attempt + 2,
                                    attempts_allowed,
                                )
                                continue  # 缁х画涓嬩竴娆￠噸璇?
                        # 濡傛灉鏄渶鍚庝竴娆￠噸璇曟垨鑰呬笉闇€瑕侀噸璇曪紝鎶涘嚭寮傚父
                        raise

                except Exception as e:

                    # 濡傛灉鎵€鏈夐噸璇曢兘澶辫触浜嗭紝鎶涘嚭鏈€鍚庣殑閿欒
                    if attempt == attempts_allowed - 1:
                        raise
                finally:
                    if timeout_context and not timeout_stopped:
                        try:
                            await self.timeout_manager.stop_timeout(timeout_context.timeout_id)
                        except Exception as stop_error:
                            logger.warning("stop_timeout_failed timeout_id=%s error=%s", timeout_context.timeout_id, stop_error)
            
        except Exception as e:
            # 鏈€缁堝紓甯稿鐞?
            if checkpoint_id:
                await self.recovery_manager.mark_checkpoint_failed(
                    checkpoint_id, str(e)
                )
            
            # 鍙戦€侀敊璇€氱煡
            await self._send_error_notification(task_id, str(e))
            raise
    
    async def monitor_task_execution(self, task_id: str, total_pages: int = 1):
        """
        鐩戞帶浠诲姟鎵ц杩囩▼
        :param task_id: 任务ID
        :param total_pages: 鎬婚〉鏁?
        """
        try:
            # 璁板綍浠诲姟寮€濮?
            await self.system_monitor.record_task_start(task_id, total_pages)
            
            # 鍚姩浠诲姟绾ц秴鏃朵繚鎶?
            timeout_context = await self.timeout_manager.start_timeout(
                timeout_id=f"task_execution:{task_id}",
                level=TimeoutLevel.TASK_EXECUTION,
                task_id=task_id,
                metadata={"total_pages": total_pages}
            )
            
            return timeout_context
            
        except Exception as e:
            logger.error(f"任务监控启动失败 {task_id}: {e}")
            raise
    
    async def complete_task_monitoring(self, task_id: str, timeout_context, success: bool = True):
        """
        完成任务监控
        :param task_id: 任务ID
        :param timeout_context: 瓒呮椂涓婁笅鏂?
        :param success: 鏄惁鎴愬姛
        """
        try:
            # 鍋滄瓒呮椂淇濇姢
            if timeout_context:
                await self.timeout_manager.stop_timeout(timeout_context.timeout_id)
            
            # 记录任务完成
            await self.system_monitor.record_task_completion(task_id, success)
            
        except Exception as e:
            logger.error(f"任务监控完成失败 {task_id}: {e}")
    
    async def finalize_task_monitoring(self, task_id: str, timeout_context):
        """Stop a monitoring context without recording a second completion."""
        if not timeout_context:
            return
        try:
            await self.timeout_manager.stop_timeout(timeout_context.timeout_id)
        except Exception as exc:
            logger.warning("任务监控清理失败 %s: %s", task_id, exc)

    # ========== API璋冪敤淇濇姢 ==========
    
    async def protected_api_call(self,
                                task_id: str,
                                api_func: Callable,
                                *args,
                                max_retries: Optional[int] = None,
                                **kwargs):
        """
        鍙椾繚鎶ょ殑API璋冪敤 - 鍖呭惈瓒呮椂鍜岄噸璇曟満鍒?
        :param task_id: 任务ID
        :param api_func: API鍑芥暟
        :param args: 位置参数
        :param kwargs: 鍏抽敭瀛楀弬鏁?
        :return: API璋冪敤缁撴灉
        """
        resolved_retries = self._resolve_max_retries(
            1 if max_retries is None else max_retries
        )
        return await self.execute_with_protection(
            task_id=task_id,
            operation=api_func,
            timeout_level=TimeoutLevel.API_REQUEST,
            recovery_strategy="auto",
            max_retries=resolved_retries,
            *args,
            **kwargs
        )
    
    # ========== 绯荤粺鐘舵€佹煡璇?==========
    
    async def get_system_health(self) -> Dict[str, Any]:
        """鑾峰彇绯荤粺鍋ュ悍鐘舵€?"""
        try:
            timeout_stats = await self.timeout_manager.get_statistics()
            error_stats = self.error_handler.get_statistics()
            system_metrics = self.system_monitor.get_current_metrics() or {}
            recovery_stats = self.recovery_manager.get_statistics()
            
            return {
                "timestamp": datetime.now().isoformat(),
                "overall_status": "healthy",  # 鍙互鏍规嵁鍚勭粍浠剁姸鎬佽绠?
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
        """澶勭悊瓒呮椂棰勮"""
        try:
            percentage = (elapsed_time / total_timeout) * 100
            
            # 鍙戦€佽秴鏃堕璀︽秷鎭?
            if self.websocket_manager:
                message = MessageFactory.create_timeout_warning(
                    timeout_type=timeout_id,
                    current_duration=elapsed_time,
                    timeout_threshold=total_timeout,
                    warning_threshold=total_timeout * 0.8,
                    progress_percentage=percentage,
                    task_id=task_id,
                    operation_id=timeout_id,
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
            # 鍙戦€佽秴鏃跺彂鐢熸秷鎭?
            if self.websocket_manager:
                message = MessageFactory.create_timeout_exceeded(
                    timeout_type=timeout_id,
                    duration=elapsed_time,
                    timeout_threshold=elapsed_time,
                    action_taken="timeout_occurred",
                    task_id=task_id,
                    operation_id=timeout_id,
                )
                await self.websocket_manager.send_to_task_subscribers(
                    task_id, message.to_dict()
                )
            
            logger.error(f"任务 {task_id} 发生超时: {elapsed_time:.1f}s")
            
        except Exception as e:
            logger.error(f"处理超时发生失败: {e}")
    
    async def _on_error_recovery(self, task_id: str, error_type: str, 
                               recovery_action: str, success: bool):
        """澶勭悊閿欒鎭㈠"""
        try:
            # 鍙戦€侀敊璇仮澶嶆秷鎭?
            if self.websocket_manager:
                if success:
                    message = MessageFactory.create_error_recovery_completed(
                        error_type=error_type,
                        error_message=recovery_action,
                        recovery_strategy=recovery_action,
                        attempt_count=1,
                        max_attempts=1,
                        task_id=task_id,
                    )
                else:
                    message = MessageFactory.create_error_recovery_failed(
                        error_type=error_type,
                        error_message=recovery_action,
                        recovery_strategy=recovery_action,
                        attempt_count=1,
                        max_attempts=1,
                        task_id=task_id,
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
        """澶勭悊绯荤粺鍛婅"""
        try:
            # 鍙戦€佺郴缁熷憡璀︽秷鎭?
            if self.websocket_manager:
                alert_message = MessageFactory.create_system_alert(
                    alert_id=alert_type,
                    metric_name=alert_type,
                    level=severity,
                    current_value=metrics.get("current_value", 0.0),
                    threshold=metrics.get("threshold", 0.0),
                    message=message,
                    triggered_at=datetime.now().isoformat(),
                    context=metrics,
                )
                await self.websocket_manager.broadcast(
                    alert_message.to_dict(),
                    require_admin=True,
                )
            
            logger.warning(f"系统告警 ({severity}): {alert_type} - {message}")
            
        except Exception as e:
            logger.error(f"处理系统告警失败: {e}")
    
    async def _on_recovery_notification(self, event_type: str, task_id: str, 
                                      message: str, data: Dict[str, Any]):
        """澶勭悊鎭㈠閫氱煡"""
        try:
            # 鍙戦€佹仮澶嶉€氱煡娑堟伅
            if self.websocket_manager:
                recovery_message = MessageFactory.create_recovery_job_update(
                    task_id=task_id,
                    recovery_state=event_type,
                    recovery_attempts=data.get("recovery_attempts", 0),
                    max_recovery_attempts=data.get("max_recovery_attempts", 0),
                    recovery_strategy=data.get("recovery_strategy", "unknown"),
                    error_messages=[message],
                )
                await self.websocket_manager.send_to_task_subscribers(
                    task_id, recovery_message.to_dict()
                )
            
            logger.info(f"恢复通知 {task_id}: {event_type} - {message}")
            
        except Exception as e:
            logger.error(f"处理恢复通知失败: {e}")
    
    # ========== 鍐呴儴鏂规硶 ==========
    
    async def _handle_timeout_error(self, task_id: str, timeout_context, error_msg: str):
        """澶勭悊瓒呮椂閿欒"""
        try:
            # 记录超时事件
            await self.error_handler.handle_error(
                asyncio.TimeoutError(error_msg), 
                task_id, 
                "auto"
            )
            
            # 鍙戦€佽秴鏃堕敊璇€氱煡
            if self.websocket_manager:
                message = MessageFactory.create_timeout_exceeded(
                    timeout_type=timeout_context.timeout_id,
                    duration=timeout_context.elapsed_time,
                    timeout_threshold=timeout_context.elapsed_time,
                    action_taken="timeout_occurred",
                    task_id=task_id,
                    operation_id=timeout_context.timeout_id,
                )
                await self.websocket_manager.send_to_task_subscribers(
                    task_id, message.to_dict()
                )
            
        except Exception as e:
            logger.error(f"处理超时错误失败: {e}")
    
    async def _send_error_notification(self, task_id: str, error_msg: str):
        """鍙戦€侀敊璇€氱煡"""
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
        """鐩戞帶寰幆 - 瀹氭湡妫€鏌ョ郴缁熺姸鎬?"""
        while not self._shutdown_event.is_set():
            try:
                # 妫€鏌ョ郴缁熻祫婧?
                metrics = await self.system_monitor._collect_system_metrics()
                await self.system_monitor._check_system_alerts(metrics)
                
                # 娓呯悊杩囨湡鏁版嵁
                # await self.timeout_manager.cleanup_expired_timeouts()
                # await self.error_handler.cleanup_old_records()
                # await self.recovery_manager.cleanup_expired_checkpoints()
                
                # 绛夊緟涓嬩竴娆℃鏌?
                await asyncio.sleep(30)  # 姣?0绉掓鏌ヤ竴娆?
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"监控循环异常: {e}")
                await asyncio.sleep(60)  # 鍙戠敓寮傚父鏃剁瓑寰呮洿闀挎椂闂?

# 鍏ㄥ眬绯荤粺闆嗘垚鍣ㄥ疄渚?
system_integration: Optional[SystemIntegration] = None

async def init_system_integration(runtime_config=None, websocket_manager=None):
    """
    鍒濆鍖栧叏灞€绯荤粺闆嗘垚鍣?
    :param runtime_config: 杩愯鏃堕厤缃璞″紩鐢?
    :param websocket_manager: WebSocket绠＄悊鍣?
    """
    global system_integration
    if system_integration is None:
        system_integration = SystemIntegration(runtime_config, websocket_manager)
        await system_integration.initialize()
        logger.info("全局系统集成器初始化完成")

async def shutdown_system_integration():
    """鍏抽棴鍏ㄥ眬绯荤粺闆嗘垚鍣?"""
    global system_integration
    if system_integration:
        await system_integration.shutdown()
        system_integration = None
        logger.info("全局系统集成器已关闭")

def get_system_integration() -> Optional[SystemIntegration]:
    """鑾峰彇鍏ㄥ眬绯荤粺闆嗘垚鍣ㄥ疄渚?"""
    return system_integration
