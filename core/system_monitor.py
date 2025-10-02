
import asyncio
import psutil
import time
import logging
from typing import Dict, Any, Optional, List, Callable
from datetime import datetime, timezone, timedelta
from dataclasses import dataclass, field
from collections import deque, defaultdict
import json
import os
import gc
import threading

logger = logging.getLogger(__name__)

@dataclass
class SystemMetrics:
    """系统指标数据类"""
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    
    # CPU指标
    cpu_percent: float = 0.0
    cpu_count: int = 0
    load_average: List[float] = field(default_factory=list)
    
    # 内存指标
    memory_total: int = 0
    memory_available: int = 0
    memory_percent: float = 0.0
    memory_used: int = 0
    
    # 磁盘指标
    disk_total: int = 0
    disk_used: int = 0
    disk_free: int = 0
    disk_percent: float = 0.0
    
    # 网络指标
    network_bytes_sent: int = 0
    network_bytes_recv: int = 0
    network_packets_sent: int = 0
    network_packets_recv: int = 0
    
    # 进程指标
    process_count: int = 0
    thread_count: int = 0
    file_descriptors: int = 0
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return {
            'timestamp': self.timestamp.isoformat(),
            'cpu': {
                'percent': self.cpu_percent,
                'count': self.cpu_count,
                'load_average': self.load_average
            },
            'memory': {
                'total_mb': round(self.memory_total / 1024 / 1024, 2),
                'available_mb': round(self.memory_available / 1024 / 1024, 2),
                'used_mb': round(self.memory_used / 1024 / 1024, 2),
                'percent': self.memory_percent
            },
            'disk': {
                'total_gb': round(self.disk_total / 1024 / 1024 / 1024, 2),
                'used_gb': round(self.disk_used / 1024 / 1024 / 1024, 2),
                'free_gb': round(self.disk_free / 1024 / 1024 / 1024, 2),
                'percent': self.disk_percent
            },
            'network': {
                'bytes_sent': self.network_bytes_sent,
                'bytes_recv': self.network_bytes_recv,
                'packets_sent': self.network_packets_sent,
                'packets_recv': self.network_packets_recv
            },
            'process': {
                'count': self.process_count,
                'thread_count': self.thread_count,
                'file_descriptors': self.file_descriptors
            }
        }

@dataclass
class APIMetrics:
    """API指标数据类"""
    endpoint: str
    method: str = "GET"
    response_time: float = 0.0
    status_code: int = 200
    success: bool = True
    error_message: Optional[str] = None
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return {
            'endpoint': self.endpoint,
            'method': self.method,
            'response_time': self.response_time,
            'status_code': self.status_code,
            'success': self.success,
            'error_message': self.error_message,
            'timestamp': self.timestamp.isoformat()
        }

@dataclass
class AlertThreshold:
    """告警阈值配置"""
    metric_name: str
    warning_threshold: float
    critical_threshold: float
    enabled: bool = True
    callback: Optional[Callable] = None

@dataclass
class AlertEvent:
    """告警事件"""
    alert_id: str
    metric_name: str
    level: str  # warning, critical
    current_value: float
    threshold: float
    message: str
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    resolved: bool = False
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return {
            'alert_id': self.alert_id,
            'metric_name': self.metric_name,
            'level': self.level,
            'current_value': self.current_value,
            'threshold': self.threshold,
            'message': self.message,
            'timestamp': self.timestamp.isoformat(),
            'resolved': self.resolved
        }

class SystemMonitor:
    """系统监控器"""
    
    def __init__(self, 
                 websocket_manager=None,
                 error_handler=None,
                 task_manager=None,
                 monitor_interval: int = 10,
                 history_size: int = 1000):
        """
        初始化系统监控器
        :param websocket_manager: WebSocket管理器
        :param error_handler: 错误处理器
        :param task_manager: 任务管理器
        :param monitor_interval: 监控间隔（秒）
        :param history_size: 历史数据大小
        """
        self.websocket_manager = websocket_manager
        self.error_handler = error_handler
        self.task_manager = task_manager
        self.monitor_interval = monitor_interval
        self.history_size = history_size
        
        # 监控状态
        self._running = False
        self._monitor_task: Optional[asyncio.Task] = None
        
        # 历史数据存储
        self._system_metrics_history: deque = deque(maxlen=history_size)
        self._api_metrics_history: deque = deque(maxlen=history_size)
        
        # API响应时间统计
        self._api_stats: Dict[str, Dict[str, Any]] = defaultdict(lambda: {
            'total_requests': 0,
            'successful_requests': 0,
            'failed_requests': 0,
            'total_response_time': 0.0,
            'min_response_time': float('inf'),
            'max_response_time': 0.0,
            'last_request_time': None
        })
        
        # 告警配置
        self._alert_thresholds = {
            'cpu_percent': AlertThreshold('cpu_percent', 70.0, 90.0),
            'memory_percent': AlertThreshold('memory_percent', 80.0, 95.0),
            'disk_percent': AlertThreshold('disk_percent', 85.0, 95.0),
            'api_response_time': AlertThreshold('api_response_time', 5000.0, 10000.0),  # 毫秒
            'api_error_rate': AlertThreshold('api_error_rate', 10.0, 25.0),  # 百分比
        }
        
        # 活跃告警
        self._active_alerts: Dict[str, AlertEvent] = {}
        
        # 性能统计
        self._performance_stats = {
            'monitoring_start_time': None,
            'total_metrics_collected': 0,
            'total_alerts_triggered': 0,
            'system_uptime': 0,
            'gc_collections': 0,
            'memory_leaks_detected': 0
        }
        
        logger.info("系统监控器初始化完成")
    
    async def initialize(self):
        """初始化系统监控器的异步组件"""
        logger.info("开始初始化系统监控器异步组件...")
        # 这里可以添加异步初始化逻辑
        logger.info("系统监控器异步组件初始化完成")
    
    def get_statistics(self):
        """获取系统监控器统计信息"""
        return {
            'monitoring_stats': dict(self._performance_stats),
            'active_alerts_count': len(self._active_alerts),
            'system_metrics_history_size': len(self._system_metrics_history),
            'api_metrics_history_size': len(self._api_metrics_history),
            'alert_thresholds_count': len(self._alert_thresholds)
        }
    
    async def record_task_start(self, task_id: str, total_pages: int = 1):
        """记录任务开始"""
        try:
            logger.info(f"记录任务开始: {task_id}, 总页数: {total_pages}")
            # 这里可以添加任务开始记录的逻辑
            return True
        except Exception as e:
            logger.error(f"记录任务开始失败: {e}")
            return False
    
    async def record_task_completion(self, task_id: str, success: bool = True, error_message: Optional[str] = None):
        """记录任务完成"""
        try:
            logger.info(f"记录任务完成: {task_id}, 成功: {success}")
            if error_message:
                logger.warning(f"任务错误: {error_message}")
            # 这里可以添加任务完成记录的逻辑
            return True
        except Exception as e:
            logger.error(f"记录任务完成失败: {e}")
            return False
    
    async def start(self):
        """启动系统监控器"""
        if self._running:
            return
        
        self._running = True
        self._performance_stats['monitoring_start_time'] = datetime.now(timezone.utc)
        
        # 启动监控任务
        self._monitor_task = asyncio.create_task(self._monitoring_loop())
        
        logger.info("系统监控器已启动")
    
    async def stop(self):
        """停止系统监控器"""
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
        
        logger.info("系统监控器已停止")
    
    async def _monitoring_loop(self):
        """监控主循环"""
        logger.info("系统监控循环已启动")
        
        while self._running:
            try:
                # 收集系统指标
                system_metrics = await self._collect_system_metrics()
                self._system_metrics_history.append(system_metrics)
                
                # 检查告警
                await self._check_system_alerts(system_metrics)
                
                # 收集API统计信息
                await self._check_api_alerts()
                
                # 发送监控数据
                await self._send_monitoring_update(system_metrics)
                
                # 清理过期数据
                await self._cleanup_expired_data()
                
                # 更新统计
                self._performance_stats['total_metrics_collected'] += 1
                
                await asyncio.sleep(self.monitor_interval)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"监控循环异常: {e}")
                if self.error_handler:
                    await self.error_handler.handle_error(
                        exception=e,
                        operation="system_monitoring"
                    )
                await asyncio.sleep(5)  # 错误恢复延迟
    
    async def _collect_system_metrics(self) -> SystemMetrics:
        """收集系统指标"""
        try:
            # CPU指标
            cpu_percent = psutil.cpu_percent(interval=1)
            cpu_count = psutil.cpu_count()
            
            # 负载平均值（仅在Unix系统上可用）
            load_average = []
            try:
                if hasattr(os, 'getloadavg'):
                    load_average = list(os.getloadavg())
            except (OSError, AttributeError):
                pass
            
            # 内存指标
            memory = psutil.virtual_memory()
            
            # 磁盘指标
            disk = psutil.disk_usage('/')
            
            # 网络指标
            network = psutil.net_io_counters()
            
            # 进程指标
            process_count = len(psutil.pids())
            
            # 当前进程的线程数和文件描述符
            current_process = psutil.Process()
            thread_count = current_process.num_threads()
            
            file_descriptors = 0
            try:
                file_descriptors = current_process.num_fds()
            except (AttributeError, psutil.AccessDenied):
                pass
            
            metrics = SystemMetrics(
                cpu_percent=cpu_percent,
                cpu_count=cpu_count,
                load_average=load_average,
                memory_total=memory.total,
                memory_available=memory.available,
                memory_percent=memory.percent,
                memory_used=memory.used,
                disk_total=disk.total,
                disk_used=disk.used,
                disk_free=disk.free,
                disk_percent=disk.percent,
                network_bytes_sent=network.bytes_sent,
                network_bytes_recv=network.bytes_recv,
                network_packets_sent=network.packets_sent,
                network_packets_recv=network.packets_recv,
                process_count=process_count,
                thread_count=thread_count,
                file_descriptors=file_descriptors
            )
            
            return metrics
            
        except Exception as e:
            logger.error(f"收集系统指标失败: {e}")
            return SystemMetrics()  # 返回空指标
    
    async def record_api_metrics(self, 
                                endpoint: str,
                                method: str = "GET",
                                response_time: float = 0.0,
                                status_code: int = 200,
                                success: bool = True,
                                error_message: Optional[str] = None):
        """记录API指标"""
        try:
            # 创建API指标
            api_metrics = APIMetrics(
                endpoint=endpoint,
                method=method,
                response_time=response_time,
                status_code=status_code,
                success=success,
                error_message=error_message
            )
            
            # 添加到历史记录
            self._api_metrics_history.append(api_metrics)
            
            # 更新统计信息
            stats = self._api_stats[endpoint]
            stats['total_requests'] += 1
            stats['total_response_time'] += response_time
            stats['last_request_time'] = datetime.now(timezone.utc)
            
            if success:
                stats['successful_requests'] += 1
            else:
                stats['failed_requests'] += 1
            
            # 更新响应时间统计
            if response_time < stats['min_response_time']:
                stats['min_response_time'] = response_time
            if response_time > stats['max_response_time']:
                stats['max_response_time'] = response_time
                
        except Exception as e:
            logger.error(f"记录API指标失败: {e}")
    
    async def _check_system_alerts(self, metrics: SystemMetrics):
        """检查系统告警"""
        try:
            # 检查CPU使用率
            await self._check_metric_threshold(
                'cpu_percent',
                metrics.cpu_percent,
                f"CPU使用率: {metrics.cpu_percent:.1f}%"
            )
            
            # 检查内存使用率
            await self._check_metric_threshold(
                'memory_percent',
                metrics.memory_percent,
                f"内存使用率: {metrics.memory_percent:.1f}%"
            )
            
            # 检查磁盘使用率
            await self._check_metric_threshold(
                'disk_percent',
                metrics.disk_percent,
                f"磁盘使用率: {metrics.disk_percent:.1f}%"
            )
            
        except Exception as e:
            logger.error(f"检查系统告警失败: {e}")
    
    async def _check_api_alerts(self):
        """检查API告警"""
        try:
            current_time = datetime.now(timezone.utc)
            
            for endpoint, stats in self._api_stats.items():
                if stats['total_requests'] == 0:
                    continue
                
                # 检查API响应时间
                avg_response_time = stats['total_response_time'] / stats['total_requests']
                await self._check_metric_threshold(
                    'api_response_time',
                    avg_response_time,
                    f"API {endpoint} 平均响应时间: {avg_response_time:.2f}ms"
                )
                
                # 检查API错误率
                error_rate = (stats['failed_requests'] / stats['total_requests']) * 100
                await self._check_metric_threshold(
                    'api_error_rate',
                    error_rate,
                    f"API {endpoint} 错误率: {error_rate:.1f}%"
                )
                
        except Exception as e:
            logger.error(f"检查API告警失败: {e}")
    
    async def _check_metric_threshold(self, metric_name: str, current_value: float, message: str):
        """检查指标阈值"""
        if metric_name not in self._alert_thresholds:
            return
        
        threshold_config = self._alert_thresholds[metric_name]
        if not threshold_config.enabled:
            return
        
        alert_key = f"{metric_name}_{hash(message)}"
        
        # 检查是否需要触发告警
        if current_value >= threshold_config.critical_threshold:
            await self._trigger_alert(
                alert_key, metric_name, 'critical',
                current_value, threshold_config.critical_threshold, message
            )
        elif current_value >= threshold_config.warning_threshold:
            await self._trigger_alert(
                alert_key, metric_name, 'warning',
                current_value, threshold_config.warning_threshold, message
            )
        else:
            # 检查是否需要解决告警
            await self._resolve_alert(alert_key)
    
    async def _trigger_alert(self, alert_key: str, metric_name: str, level: str,
                           current_value: float, threshold: float, message: str):
        """触发告警"""
        try:
            # 检查是否已存在相同告警
            if alert_key in self._active_alerts:
                return
            
            # 创建告警事件
            alert_event = AlertEvent(
                alert_id=alert_key,
                metric_name=metric_name,
                level=level,
                current_value=current_value,
                threshold=threshold,
                message=message
            )
            
            self._active_alerts[alert_key] = alert_event
            self._performance_stats['total_alerts_triggered'] += 1
            
            # 发送告警通知
            await self._send_alert_notification(alert_event)
            
            # 调用自定义回调
            threshold_config = self._alert_thresholds[metric_name]
            if threshold_config.callback:
                try:
                    await threshold_config.callback(alert_event)
                except Exception as e:
                    logger.error(f"告警回调执行失败: {e}")
            
            logger.warning(f"触发{level}告警: {message}")
            
        except Exception as e:
            logger.error(f"触发告警失败: {e}")
    
    async def _resolve_alert(self, alert_key: str):
        """解决告警"""
        try:
            if alert_key not in self._active_alerts:
                return
            
            alert_event = self._active_alerts[alert_key]
            alert_event.resolved = True
            
            # 发送告警解决通知
            await self._send_alert_resolved_notification(alert_event)
            
            # 移除活跃告警
            del self._active_alerts[alert_key]
            
            logger.info(f"告警已解决: {alert_event.message}")
            
        except Exception as e:
            logger.error(f"解决告警失败: {e}")
    
    async def _send_monitoring_update(self, metrics: SystemMetrics):
        """发送监控更新"""
        try:
            if not self.websocket_manager:
                return
            
            # 构建监控消息
            monitoring_message = {
                'type': 'system_monitoring_update',
                'system_metrics': metrics.to_dict(),
                'api_statistics': self._get_api_statistics(),
                'task_queue_status': await self._get_task_queue_status(),
                'active_alerts': [alert.to_dict() for alert in self._active_alerts.values()],
                'performance_stats': self._get_performance_stats(),
                'timestamp': datetime.now(timezone.utc).isoformat()
            }
            
            # 广播监控数据
            await self.websocket_manager.broadcast(monitoring_message)
            
        except Exception as e:
            logger.error(f"发送监控更新失败: {e}")
    
    async def _send_alert_notification(self, alert_event: AlertEvent):
        """发送告警通知"""
        try:
            if not self.websocket_manager:
                return
            
            alert_message = {
                'type': 'system_alert',
                'alert': alert_event.to_dict(),
                'timestamp': datetime.now(timezone.utc).isoformat()
            }
            
            await self.websocket_manager.broadcast(alert_message)
            
        except Exception as e:
            logger.error(f"发送告警通知失败: {e}")
    
    async def _send_alert_resolved_notification(self, alert_event: AlertEvent):
        """发送告警解决通知"""
        try:
            if not self.websocket_manager:
                return
            
            resolved_message = {
                'type': 'system_alert_resolved',
                'alert': alert_event.to_dict(),
                'timestamp': datetime.now(timezone.utc).isoformat()
            }
            
            await self.websocket_manager.broadcast(resolved_message)
            
        except Exception as e:
            logger.error(f"发送告警解决通知失败: {e}")
    
    async def _get_task_queue_status(self) -> Dict[str, Any]:
        """获取任务队列状态"""
        try:
            if not self.task_manager:
                return {}
            
            system_stats = await self.task_manager.get_system_stats()
            return system_stats
            
        except Exception as e:
            logger.error(f"获取任务队列状态失败: {e}")
            return {}
    
    def _get_api_statistics(self) -> Dict[str, Any]:
        """获取API统计信息"""
        api_stats = {}
        
        for endpoint, stats in self._api_stats.items():
            if stats['total_requests'] == 0:
                continue
            
            avg_response_time = stats['total_response_time'] / stats['total_requests']
            success_rate = (stats['successful_requests'] / stats['total_requests']) * 100
            
            api_stats[endpoint] = {
                'total_requests': stats['total_requests'],
                'successful_requests': stats['successful_requests'],
                'failed_requests': stats['failed_requests'],
                'success_rate': round(success_rate, 2),
                'avg_response_time': round(avg_response_time, 2),
                'min_response_time': stats['min_response_time'] if stats['min_response_time'] != float('inf') else 0,
                'max_response_time': stats['max_response_time'],
                'last_request_time': stats['last_request_time'].isoformat() if stats['last_request_time'] else None
            }
        
        return api_stats
    
    def _get_performance_stats(self) -> Dict[str, Any]:
        """获取性能统计信息"""
        current_time = datetime.now(timezone.utc)
        
        # 计算系统运行时间
        if self._performance_stats['monitoring_start_time']:
            uptime = (current_time - self._performance_stats['monitoring_start_time']).total_seconds()
            self._performance_stats['system_uptime'] = uptime
        
        # 获取垃圾回收统计
        gc_stats = gc.get_stats()
        self._performance_stats['gc_collections'] = sum(stat['collections'] for stat in gc_stats)
        
        return dict(self._performance_stats)
    
    async def _cleanup_expired_data(self):
        """清理过期数据"""
        try:
            current_time = datetime.now(timezone.utc)
            cleanup_threshold = current_time - timedelta(hours=24)
            
            # 清理过期的API指标
            while (self._api_metrics_history and
                   self._api_metrics_history[0].timestamp < cleanup_threshold):
                self._api_metrics_history.popleft()
            
            # 清理过期的系统指标
            while (self._system_metrics_history and
                   self._system_metrics_history[0].timestamp < cleanup_threshold):
                self._system_metrics_history.popleft()
            
            # 重置API统计（保留最近1小时的数据）
            reset_threshold = current_time - timedelta(hours=1)
            for endpoint, stats in list(self._api_stats.items()):
                if (stats['last_request_time'] and
                    stats['last_request_time'] < reset_threshold):
                    # 重置统计但保留结构
                    stats.update({
                        'total_requests': 0,
                        'successful_requests': 0,
                        'failed_requests': 0,
                        'total_response_time': 0.0,
                        'min_response_time': float('inf'),
                        'max_response_time': 0.0,
                        'last_request_time': None
                    })
            
        except Exception as e:
            logger.error(f"清理过期数据失败: {e}")
    
    def get_current_metrics(self) -> Optional[Dict[str, Any]]:
        """获取当前指标"""
        if not self._system_metrics_history:
            return None
        
        latest_metrics = self._system_metrics_history[-1]
        return {
            'system_metrics': latest_metrics.to_dict(),
            'api_statistics': self._get_api_statistics(),
            'active_alerts': [alert.to_dict() for alert in self._active_alerts.values()],
            'performance_stats': self._get_performance_stats()
        }
    
    def get_metrics_history(self, hours: int = 1) -> List[Dict[str, Any]]:
        """获取指标历史"""
        cutoff_time = datetime.now(timezone.utc) - timedelta(hours=hours)
        
        history = []
        for metrics in self._system_metrics_history:
            if metrics.timestamp >= cutoff_time:
                history.append(metrics.to_dict())
        
        return history
    
    def update_alert_threshold(self, metric_name: str, threshold: AlertThreshold):
        """更新告警阈值"""
        self._alert_thresholds[metric_name] = threshold
        logger.info(f"更新告警阈值: {metric_name}")
    
    def disable_alert(self, metric_name: str):
        """禁用告警"""
        if metric_name in self._alert_thresholds:
            self._alert_thresholds[metric_name].enabled = False
            logger.info(f"禁用告警: {metric_name}")
    
    def enable_alert(self, metric_name: str):
        """启用告警"""
        if metric_name in self._alert_thresholds:
            self._alert_thresholds[metric_name].enabled = True
            logger.info(f"启用告警: {metric_name}")
    
    def get_alert_status(self) -> Dict[str, Any]:
        """获取告警状态"""
        return {
            'active_alerts': [alert.to_dict() for alert in self._active_alerts.values()],
            'alert_thresholds': {
                name: {
                    'warning_threshold': threshold.warning_threshold,
                    'critical_threshold': threshold.critical_threshold,
                    'enabled': threshold.enabled
                }
                for name, threshold in self._alert_thresholds.items()
            },
            'total_alerts_triggered': self._performance_stats['total_alerts_triggered']
        }
    
    async def force_gc(self):
        """强制垃圾回收"""
        try:
            collected = gc.collect()
            logger.info(f"强制垃圾回收完成，回收对象数: {collected}")
            return collected
        except Exception as e:
            logger.error(f"强制垃圾回收失败: {e}")
            return 0

# 全局系统监控器实例
system_monitor: Optional[SystemMonitor] = None

async def init_system_monitor(websocket_manager=None, error_handler=None, task_manager=None):
    """初始化全局系统监控器"""
    global system_monitor
    if system_monitor is None:
        system_monitor = SystemMonitor(websocket_manager, error_handler, task_manager)
        await system_monitor.start()
        logger.info("全局系统监控器初始化完成")

async def shutdown_system_monitor():
    """关闭全局系统监控器"""
    global system_monitor
    if system_monitor:
        await system_monitor.stop()
        system_monitor = None
        logger.info("全局系统监控器已关闭")