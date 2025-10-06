import asyncio
import logging
import time
import traceback
import random
from typing import Dict, Any, Optional, Callable, List, Union, Type
from datetime import datetime, timezone, timedelta
from dataclasses import dataclass, field
from enum import Enum
import aiohttp
import json
import re

logger = logging.getLogger(__name__)

class ErrorType(Enum):
    """错误类型枚举"""
    NETWORK_ERROR = "network_error"          # 网络错误
    API_ERROR = "api_error"                  # API错误
    SYSTEM_ERROR = "system_error"            # 系统错误
    BUSINESS_ERROR = "business_error"        # 业务错误
    TIMEOUT_ERROR = "timeout_error"          # 超时错误
    RESOURCE_ERROR = "resource_error"        # 资源错误
    AUTHENTICATION_ERROR = "auth_error"      # 认证错误
    RATE_LIMIT_ERROR = "rate_limit_error"    # 限流错误
    UNKNOWN_ERROR = "unknown_error"          # 未知错误

class ErrorSeverity(Enum):
    """错误严重程度"""
    LOW = "low"          # 低级错误，可以忽略或简单重试
    MEDIUM = "medium"    # 中级错误，需要处理但不影响整个系统
    HIGH = "high"        # 高级错误，需要立即处理
    CRITICAL = "critical" # 严重错误，可能导致系统不可用

class RecoveryStrategy(Enum):
    """恢复策略"""
    RETRY = "retry"                    # 重试
    EXPONENTIAL_BACKOFF = "exp_backoff" # 指数退避重试
    CIRCUIT_BREAKER = "circuit_breaker" # 熔断器
    FALLBACK = "fallback"              # 降级处理
    ESCALATE = "escalate"              # 升级处理
    IGNORE = "ignore"                  # 忽略错误
    ABORT = "abort"                    # 中止操作

@dataclass
class ErrorContext:
    """错误上下文"""
    error_id: str
    error_type: ErrorType
    severity: ErrorSeverity
    exception: Exception
    task_id: Optional[str] = None
    page_number: Optional[int] = None
    operation: Optional[str] = None
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    retry_count: int = 0
    max_retries: int = 3
    should_retry: bool = True
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    @property
    def error_message(self) -> str:
        """获取错误消息"""
        return str(self.exception)
    
    @property
    def error_traceback(self) -> str:
        """获取错误堆栈"""
        return traceback.format_exception(
            type(self.exception), self.exception, self.exception.__traceback__
        )

@dataclass
class RecoveryConfig:
    """恢复配置"""
    strategy: RecoveryStrategy
    max_retries: int = 3
    base_delay: float = 1.0
    max_delay: float = 60.0
    backoff_multiplier: float = 2.0
    jitter: bool = True
    circuit_breaker_threshold: int = 5
    circuit_breaker_timeout: float = 60.0
    fallback_handler: Optional[Callable] = None
    custom_handler: Optional[Callable] = None

class ErrorClassifier:
    """错误分类器"""
    
    def __init__(self):
        # 错误模式匹配规则
        self._error_patterns = {
            ErrorType.NETWORK_ERROR: [
                r'connection.*error',
                r'timeout.*error',
                r'dns.*error',
                r'socket.*error',
                r'network.*unreachable',
                r'connection.*refused',
                r'connection.*reset',
                ConnectionResetError,
                ConnectionError,
                BrokenPipeError,
                aiohttp.ClientConnectionError,
                aiohttp.ClientConnectorError,
                aiohttp.ServerTimeoutError,
                asyncio.TimeoutError,
            ],
            ErrorType.API_ERROR: [
                r'api.*error',
                r'http.*error',
                r'status.*code.*[45]\d\d',
                aiohttp.ClientResponseError,
                aiohttp.ClientPayloadError,
            ],
            ErrorType.RATE_LIMIT_ERROR: [
                r'rate.*limit',
                r'too.*many.*requests',
                r'quota.*exceeded',
                r'throttl',
                lambda e: hasattr(e, 'status') and e.status == 429,
            ],
            ErrorType.AUTHENTICATION_ERROR: [
                r'auth.*error',
                r'unauthorized',
                r'forbidden',
                r'invalid.*token',
                r'access.*denied',
                lambda e: hasattr(e, 'status') and e.status in [401, 403],
            ],
            ErrorType.SYSTEM_ERROR: [
                r'memory.*error',
                r'disk.*space',
                r'permission.*denied',
                r'file.*not.*found',
                MemoryError,
                OSError,
                IOError,
                PermissionError,
                FileNotFoundError,
            ],
            ErrorType.BUSINESS_ERROR: [
                r'file.*format.*error',
                r'unsupported.*format',
                r'invalid.*input',
                r'content.*empty',
                r'parsing.*error',
                ValueError,
                TypeError,
            ],
            ErrorType.RESOURCE_ERROR: [
                r'resource.*not.*available',
                r'service.*unavailable',
                r'temporary.*unavailable',
                lambda e: hasattr(e, 'status') and e.status == 503,
            ]
        }
    
    def classify_error(self, exception: Exception, 
                      operation: Optional[str] = None) -> tuple[ErrorType, ErrorSeverity]:
        """
        分类错误
        :param exception: 异常对象
        :param operation: 操作类型
        :return: (错误类型, 严重程度)
        """
        error_message = str(exception).lower()
        exception_type = type(exception)
        
        # 匹配错误类型
        classified_type = ErrorType.UNKNOWN_ERROR
        for error_type, patterns in self._error_patterns.items():
            for pattern in patterns:
                if isinstance(pattern, type) and isinstance(exception, pattern):
                    classified_type = error_type
                    break
                elif isinstance(pattern, str) and re.search(pattern, error_message):
                    classified_type = error_type
                    break
                elif callable(pattern):
                    try:
                        if pattern(exception):
                            classified_type = error_type
                            break
                    except:
                        continue
            if classified_type != ErrorType.UNKNOWN_ERROR:
                break
        
        # 确定严重程度
        severity = self._determine_severity(classified_type, exception, operation)
        
        return classified_type, severity
    
    def _determine_severity(self, error_type: ErrorType, 
                          exception: Exception, 
                          operation: Optional[str]) -> ErrorSeverity:
        """确定错误严重程度"""
        # 基于错误类型的基础严重程度
        base_severity_map = {
            ErrorType.NETWORK_ERROR: ErrorSeverity.MEDIUM,
            ErrorType.API_ERROR: ErrorSeverity.MEDIUM,
            ErrorType.RATE_LIMIT_ERROR: ErrorSeverity.LOW,
            ErrorType.AUTHENTICATION_ERROR: ErrorSeverity.HIGH,
            ErrorType.SYSTEM_ERROR: ErrorSeverity.HIGH,
            ErrorType.BUSINESS_ERROR: ErrorSeverity.MEDIUM,
            ErrorType.RESOURCE_ERROR: ErrorSeverity.MEDIUM,
            ErrorType.TIMEOUT_ERROR: ErrorSeverity.LOW,
            ErrorType.UNKNOWN_ERROR: ErrorSeverity.MEDIUM,
        }
        
        severity = base_severity_map.get(error_type, ErrorSeverity.MEDIUM)
        
        # 基于操作类型调整严重程度
        if operation:
            if operation in ['task_initialization', 'system_startup']:
                # 提升系统关键操作的严重程度
                if severity == ErrorSeverity.LOW:
                    severity = ErrorSeverity.MEDIUM
                elif severity == ErrorSeverity.MEDIUM:
                    severity = ErrorSeverity.HIGH
        
        # 基于异常属性调整严重程度
        if hasattr(exception, 'status'):
            status_code = exception.status
            if status_code >= 500:
                severity = ErrorSeverity.HIGH
            elif status_code == 401:
                severity = ErrorSeverity.CRITICAL
        
        return severity

class CircuitBreaker:
    """熔断器"""
    
    def __init__(self, threshold: int = 5, timeout: float = 60.0):
        self.threshold = threshold
        self.timeout = timeout
        self.failure_count = 0
        self.last_failure_time = 0
        self.state = 'closed'  # closed, open, half_open
    
    def can_execute(self) -> bool:
        """检查是否可以执行操作"""
        current_time = time.time()
        
        if self.state == 'open':
            if current_time - self.last_failure_time > self.timeout:
                self.state = 'half_open'
                return True
            return False
        
        return True
    
    def record_success(self):
        """记录成功"""
        self.failure_count = 0
        self.state = 'closed'
    
    def record_failure(self):
        """记录失败"""
        self.failure_count += 1
        self.last_failure_time = time.time()
        
        if self.failure_count >= self.threshold:
            self.state = 'open'

class ErrorHandler:
    """智能错误处理器"""
    
    def __init__(self, websocket_manager=None, timeout_manager=None):
        """
        初始化错误处理器
        :param websocket_manager: WebSocket管理器
        :param timeout_manager: 超时管理器
        """
        self.websocket_manager = websocket_manager
        self.timeout_manager = timeout_manager
        self.classifier = ErrorClassifier()
        
        # 错误统计
        self._error_stats = {
            'total_errors': 0,
            'errors_by_type': {error_type.value: 0 for error_type in ErrorType},
            'errors_by_severity': {severity.value: 0 for severity in ErrorSeverity},
            'recovery_attempts': 0,
            'successful_recoveries': 0,
            'failed_recoveries': 0,
        }
        
        # 熔断器实例
        self._circuit_breakers: Dict[str, CircuitBreaker] = {}
        
        # 默认恢复配置
        self._default_recovery_configs = {
            ErrorType.NETWORK_ERROR: RecoveryConfig(
                strategy=RecoveryStrategy.EXPONENTIAL_BACKOFF,
                max_retries=5,
                base_delay=1.0,
                max_delay=30.0
            ),
            ErrorType.API_ERROR: RecoveryConfig(
                strategy=RecoveryStrategy.EXPONENTIAL_BACKOFF,
                max_retries=3,
                base_delay=2.0,
                max_delay=60.0
            ),
            ErrorType.RATE_LIMIT_ERROR: RecoveryConfig(
                strategy=RecoveryStrategy.EXPONENTIAL_BACKOFF,
                max_retries=10,
                base_delay=5.0,
                max_delay=300.0,
                backoff_multiplier=1.5
            ),
            ErrorType.AUTHENTICATION_ERROR: RecoveryConfig(
                strategy=RecoveryStrategy.ESCALATE,
                max_retries=0
            ),
            ErrorType.SYSTEM_ERROR: RecoveryConfig(
                strategy=RecoveryStrategy.CIRCUIT_BREAKER,
                max_retries=2,
                circuit_breaker_threshold=3,
                circuit_breaker_timeout=120.0
            ),
            ErrorType.BUSINESS_ERROR: RecoveryConfig(
                strategy=RecoveryStrategy.FALLBACK,
                max_retries=1
            ),
            ErrorType.RESOURCE_ERROR: RecoveryConfig(
                strategy=RecoveryStrategy.EXPONENTIAL_BACKOFF,
                max_retries=3,
                base_delay=10.0,
                max_delay=120.0
            ),
            ErrorType.TIMEOUT_ERROR: RecoveryConfig(
                strategy=RecoveryStrategy.RETRY,
                max_retries=2,
                base_delay=5.0
            ),
            ErrorType.UNKNOWN_ERROR: RecoveryConfig(
                strategy=RecoveryStrategy.RETRY,
                max_retries=2,
                base_delay=1.0
            )
        }
        
        logger.info("错误处理器初始化完成")
    
    async def initialize(self):
        """初始化错误处理器的异步组件"""
        logger.info("开始初始化错误处理器异步组件...")
        # 这里可以添加异步初始化逻辑
        logger.info("错误处理器异步组件初始化完成")
    
    async def handle_error(self, 
                          exception: Exception,
                          task_id: Optional[str] = None,
                          page_number: Optional[int] = None,
                          operation: Optional[str] = None,
                          custom_config: Optional[RecoveryConfig] = None,
                          metadata: Optional[Dict[str, Any]] = None) -> ErrorContext:
        """
        处理错误
        :param exception: 异常对象
        :param task_id: 任务ID
        :param page_number: 页面编号
        :param operation: 操作名称
        :param custom_config: 自定义恢复配置
        :param metadata: 元数据
        :return: 错误上下文
        """
        # 分类错误
        error_type, severity = self.classifier.classify_error(exception, operation)
        
        # 创建错误上下文
        error_context = ErrorContext(
            error_id=f"err_{int(time.time() * 1000)}_{random.randint(1000, 9999)}",
            error_type=error_type,
            severity=severity,
            exception=exception,
            task_id=task_id,
            page_number=page_number,
            operation=operation,
            metadata=metadata or {}
        )
        
        # 更新统计
        self._update_error_stats(error_type, severity)
        
        # 记录错误
        await self._log_error(error_context)
        
        # 发送错误通知
        await self._send_error_notification(error_context)
        
        # 执行恢复策略
        recovery_config = custom_config or self._default_recovery_configs.get(
            error_type, self._default_recovery_configs[ErrorType.UNKNOWN_ERROR]
        )
        
        await self._execute_recovery_strategy(error_context, recovery_config)
        
        return error_context
    
    async def _execute_recovery_strategy(self, 
                                       error_context: ErrorContext, 
                                       config: RecoveryConfig):
        """执行恢复策略"""
        try:
            self._error_stats['recovery_attempts'] += 1
            
            if config.strategy == RecoveryStrategy.RETRY:
                await self._handle_retry(error_context, config)
            elif config.strategy == RecoveryStrategy.EXPONENTIAL_BACKOFF:
                await self._handle_exponential_backoff(error_context, config)
            elif config.strategy == RecoveryStrategy.CIRCUIT_BREAKER:
                await self._handle_circuit_breaker(error_context, config)
            elif config.strategy == RecoveryStrategy.FALLBACK:
                await self._handle_fallback(error_context, config)
            elif config.strategy == RecoveryStrategy.ESCALATE:
                await self._handle_escalation(error_context, config)
            elif config.strategy == RecoveryStrategy.IGNORE:
                await self._handle_ignore(error_context, config)
            elif config.strategy == RecoveryStrategy.ABORT:
                await self._handle_abort(error_context, config)
            
            # 调用自定义处理器
            if config.custom_handler:
                await config.custom_handler(error_context, config)
                
        except Exception as e:
            logger.error(f"执行恢复策略失败: {e}")
            self._error_stats['failed_recoveries'] += 1
    
    async def _handle_retry(self, error_context: ErrorContext, config: RecoveryConfig):
        """处理简单重试"""
        if error_context.retry_count < config.max_retries:
            error_context.retry_count += 1
            
            # 计算延迟时间
            delay = config.base_delay
            if config.jitter:
                delay += random.uniform(0, config.base_delay * 0.1)
            
            await self._send_retry_notification(error_context, delay)
            
            if delay > 0:
                await asyncio.sleep(delay)
            
            logger.info(f"执行重试: {error_context.error_id} (第{error_context.retry_count}次)")
            self._error_stats['successful_recoveries'] += 1
        else:
            await self._handle_max_retries_exceeded(error_context)
    
    async def _handle_exponential_backoff(self, error_context: ErrorContext, config: RecoveryConfig):
        """处理指数退避重试"""
        if error_context.retry_count < config.max_retries:
            error_context.retry_count += 1
            
            # 计算指数退避延迟
            delay = min(
                config.base_delay * (config.backoff_multiplier ** (error_context.retry_count - 1)),
                config.max_delay
            )
            
            # 添加随机抖动
            if config.jitter:
                jitter_range = delay * 0.1
                delay += random.uniform(-jitter_range, jitter_range)
                delay = max(0, delay)
            
            await self._send_retry_notification(error_context, delay)
            
            if delay > 0:
                await asyncio.sleep(delay)
            
            logger.info(f"执行指数退避重试: {error_context.error_id} "
                       f"(第{error_context.retry_count}次, 延迟{delay:.2f}s)")
            self._error_stats['successful_recoveries'] += 1
        else:
            await self._handle_max_retries_exceeded(error_context)
    
    async def _handle_circuit_breaker(self, error_context: ErrorContext, config: RecoveryConfig):
        """处理熔断器策略"""
        breaker_key = f"{error_context.operation or 'default'}"
        
        if breaker_key not in self._circuit_breakers:
            self._circuit_breakers[breaker_key] = CircuitBreaker(
                threshold=config.circuit_breaker_threshold,
                timeout=config.circuit_breaker_timeout
            )
        
        circuit_breaker = self._circuit_breakers[breaker_key]
        
        if not circuit_breaker.can_execute():
            await self._send_circuit_breaker_notification(error_context, 'open')
            logger.warning(f"熔断器开启，跳过操作: {error_context.error_id}")
            return
        
        # 记录失败
        circuit_breaker.record_failure()
        
        if circuit_breaker.state == 'open':
            await self._send_circuit_breaker_notification(error_context, 'triggered')
            logger.error(f"熔断器触发: {error_context.error_id}")
        else:
            # 尝试重试
            await self._handle_retry(error_context, config)
    
    async def _handle_fallback(self, error_context: ErrorContext, config: RecoveryConfig):
        """处理降级策略"""
        fallback_message = {
            'type': 'fallback_activated',
            'error_id': error_context.error_id,
            'task_id': error_context.task_id,
            'page_number': error_context.page_number,
            'operation': error_context.operation,
            'message': '启用降级处理模式',
            'timestamp': datetime.now(timezone.utc).isoformat()
        }
        
        if self.websocket_manager and error_context.task_id:
            await self.websocket_manager.send_to_task_subscribers(
                error_context.task_id, fallback_message
            )
        
        # 执行降级处理器
        if config.fallback_handler:
            try:
                await config.fallback_handler(error_context)
                self._error_stats['successful_recoveries'] += 1
            except Exception as e:
                logger.error(f"降级处理失败: {e}")
                self._error_stats['failed_recoveries'] += 1
        
        logger.info(f"执行降级处理: {error_context.error_id}")
    
    async def _handle_escalation(self, error_context: ErrorContext, config: RecoveryConfig):
        """处理升级策略"""
        escalation_message = {
            'type': 'error_escalation',
            'error_id': error_context.error_id,
            'task_id': error_context.task_id,
            'page_number': error_context.page_number,
            'operation': error_context.operation,
            'error_type': error_context.error_type.value,
            'severity': error_context.severity.value,
            'error_message': error_context.error_message,
            'message': '错误需要人工干预',
            'timestamp': datetime.now(timezone.utc).isoformat()
        }
        
        if self.websocket_manager and error_context.task_id:
            await self.websocket_manager.send_to_task_subscribers(
                error_context.task_id, escalation_message
            )
        
        logger.critical(f"错误升级处理: {error_context.error_id} - {error_context.error_message}")
    
    async def _handle_ignore(self, error_context: ErrorContext, config: RecoveryConfig):
        """处理忽略策略"""
        logger.info(f"忽略错误: {error_context.error_id}")
        self._error_stats['successful_recoveries'] += 1
    
    async def _handle_abort(self, error_context: ErrorContext, config: RecoveryConfig):
        """处理中止策略"""
        abort_message = {
            'type': 'operation_aborted',
            'error_id': error_context.error_id,
            'task_id': error_context.task_id,
            'page_number': error_context.page_number,
            'operation': error_context.operation,
            'error_type': error_context.error_type.value,
            'message': '操作已中止',
            'timestamp': datetime.now(timezone.utc).isoformat()
        }
        
        if self.websocket_manager and error_context.task_id:
            await self.websocket_manager.send_to_task_subscribers(
                error_context.task_id, abort_message
            )
        
        logger.error(f"中止操作: {error_context.error_id}")
        self._error_stats['failed_recoveries'] += 1
    
    async def _handle_max_retries_exceeded(self, error_context: ErrorContext):
        """处理重试次数超限"""
        max_retries_message = {
            'type': 'max_retries_exceeded',
            'error_id': error_context.error_id,
            'task_id': error_context.task_id,
            'page_number': error_context.page_number,
            'operation': error_context.operation,
            'retry_count': error_context.retry_count,
            'message': '重试次数已达上限',
            'timestamp': datetime.now(timezone.utc).isoformat()
        }
        
        if self.websocket_manager and error_context.task_id:
            await self.websocket_manager.send_to_task_subscribers(
                error_context.task_id, max_retries_message
            )
        
        logger.error(f"重试次数已达上限: {error_context.error_id}")
        self._error_stats['failed_recoveries'] += 1
    
    async def _log_error(self, error_context: ErrorContext):
        """记录错误日志"""
        log_message = (
            f"错误处理 - ID: {error_context.error_id}, "
            f"类型: {error_context.error_type.value}, "
            f"严重程度: {error_context.severity.value}, "
            f"任务: {error_context.task_id}, "
            f"页面: {error_context.page_number}, "
            f"操作: {error_context.operation}, "
            f"消息: {error_context.error_message}"
        )
        
        if error_context.severity == ErrorSeverity.CRITICAL:
            logger.critical(log_message)
        elif error_context.severity == ErrorSeverity.HIGH:
            logger.error(log_message)
        elif error_context.severity == ErrorSeverity.MEDIUM:
            logger.warning(log_message)
        else:
            logger.info(log_message)
    
    async def _send_error_notification(self, error_context: ErrorContext):
        """发送错误通知"""
        try:
            error_message = {
                'type': 'error_occurred',
                'error_id': error_context.error_id,
                'task_id': error_context.task_id,
                'page_number': error_context.page_number,
                'operation': error_context.operation,
                'error_type': error_context.error_type.value,
                'severity': error_context.severity.value,
                'error_message': error_context.error_message,
                'timestamp': error_context.timestamp.isoformat(),
                'metadata': error_context.metadata
            }
            
            if self.websocket_manager and error_context.task_id:
                await self.websocket_manager.send_to_task_subscribers(
                    error_context.task_id, error_message
                )
                
        except Exception as e:
            logger.error(f"发送错误通知失败: {e}")
    
    async def _send_retry_notification(self, error_context: ErrorContext, delay: float):
        """发送重试通知"""
        try:
            retry_message = {
                'type': 'error_retry',
                'error_id': error_context.error_id,
                'task_id': error_context.task_id,
                'page_number': error_context.page_number,
                'operation': error_context.operation,
                'retry_count': error_context.retry_count,
                'max_retries': error_context.max_retries,
                'delay_seconds': delay,
                'message': f'第{error_context.retry_count}次重试',
                'timestamp': datetime.now(timezone.utc).isoformat()
            }
            
            if self.websocket_manager and error_context.task_id:
                await self.websocket_manager.send_to_task_subscribers(
                    error_context.task_id, retry_message
                )
                
        except Exception as e:
            logger.error(f"发送重试通知失败: {e}")
    
    async def _send_circuit_breaker_notification(self, error_context: ErrorContext, state: str):
        """发送熔断器通知"""
        try:
            cb_message = {
                'type': 'circuit_breaker_state',
                'error_id': error_context.error_id,
                'task_id': error_context.task_id,
                'operation': error_context.operation,
                'state': state,
                'message': f'熔断器状态: {state}',
                'timestamp': datetime.now(timezone.utc).isoformat()
            }
            
            if self.websocket_manager and error_context.task_id:
                await self.websocket_manager.send_to_task_subscribers(
                    error_context.task_id, cb_message
                )
                
        except Exception as e:
            logger.error(f"发送熔断器通知失败: {e}")
    
    def _update_error_stats(self, error_type: ErrorType, severity: ErrorSeverity):
        """更新错误统计"""
        self._error_stats['total_errors'] += 1
        self._error_stats['errors_by_type'][error_type.value] += 1
        self._error_stats['errors_by_severity'][severity.value] += 1
    
    def get_error_stats(self) -> Dict[str, Any]:
        """获取错误统计信息"""
        return {
            'error_statistics': dict(self._error_stats),
            'circuit_breakers': {
                key: {
                    'state': breaker.state,
                    'failure_count': breaker.failure_count,
                    'threshold': breaker.threshold,
                    'last_failure_time': breaker.last_failure_time
                }
                for key, breaker in self._circuit_breakers.items()
            },
            'recovery_configs': {
                error_type.value: {
                    'strategy': config.strategy.value,
                    'max_retries': config.max_retries,
                    'base_delay': config.base_delay,
                    'max_delay': config.max_delay
                }
                for error_type, config in self._default_recovery_configs.items()
            }
        }
    
    def get_statistics(self) -> Dict[str, Any]:
        """获取错误处理器统计信息（get_stats的别名）"""
        return self.get_error_stats()

    def update_recovery_config(self, error_type: ErrorType, config: RecoveryConfig):
        """更新恢复配置"""
        self._default_recovery_configs[error_type] = config
        logger.info(f"更新恢复配置: {error_type.value}")
    
    def reset_circuit_breaker(self, operation: str):
        """重置熔断器"""
        if operation in self._circuit_breakers:
            self._circuit_breakers[operation].failure_count = 0
            self._circuit_breakers[operation].state = 'closed'
            logger.info(f"重置熔断器: {operation}")

# 全局错误处理器实例
error_handler: Optional[ErrorHandler] = None

async def init_error_handler(websocket_manager=None, timeout_manager=None):
    """初始化全局错误处理器"""
    global error_handler
    if error_handler is None:
        error_handler = ErrorHandler(websocket_manager, timeout_manager)
        logger.info("全局错误处理器初始化完成")

async def shutdown_error_handler():
    """关闭全局错误处理器"""
    global error_handler
    if error_handler:
        error_handler = None
        logger.info("全局错误处理器已关闭")