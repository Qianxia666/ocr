import asyncio
import logging
import time
import traceback
import random
from typing import Dict, Any, Optional, Callable
from datetime import datetime, timezone
from dataclasses import dataclass, field
from enum import Enum
import aiohttp
import re

logger = logging.getLogger(__name__)

class ErrorType(Enum):
    """閿欒绫诲瀷鏋氫妇"""
    NETWORK_ERROR = "network_error"          # 缃戠粶閿欒
    API_ERROR = "api_error"                  # API閿欒
    SYSTEM_ERROR = "system_error"            # 绯荤粺閿欒
    BUSINESS_ERROR = "business_error"        # 涓氬姟閿欒
    TIMEOUT_ERROR = "timeout_error"          # 瓒呮椂閿欒
    RESOURCE_ERROR = "resource_error"        # 璧勬簮閿欒
    AUTHENTICATION_ERROR = "auth_error"      # 璁よ瘉閿欒
    RATE_LIMIT_ERROR = "rate_limit_error"    # 闄愭祦閿欒
    UNKNOWN_ERROR = "unknown_error"          # 鏈煡閿欒

class ErrorSeverity(Enum):
    """閿欒涓ラ噸绋嬪害"""
    LOW = "low"          # 浣庣骇閿欒锛屽彲浠ュ拷鐣ユ垨绠€鍗曢噸璇?
    MEDIUM = "medium"    # 涓骇閿欒锛岄渶瑕佸鐞嗕絾涓嶅奖鍝嶆暣涓郴缁?
    HIGH = "high"        # 楂樼骇閿欒锛岄渶瑕佺珛鍗冲鐞?
    CRITICAL = "critical" # 涓ラ噸閿欒锛屽彲鑳藉鑷寸郴缁熶笉鍙敤

class RecoveryStrategy(Enum):
    """鎭㈠绛栫暐"""
    RETRY = "retry"                    # 閲嶈瘯
    EXPONENTIAL_BACKOFF = "exp_backoff" # 鎸囨暟閫€閬块噸璇?
    CIRCUIT_BREAKER = "circuit_breaker" # 鐔旀柇鍣?
    FALLBACK = "fallback"              # 降级处理
    ESCALATE = "escalate"              # 升级处理
    IGNORE = "ignore"                  # 蹇界暐閿欒
    ABORT = "abort"                    # 涓鎿嶄綔

@dataclass
class ErrorContext:
    """閿欒涓婁笅鏂?"""
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
    should_retry: bool = False
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    @property
    def error_message(self) -> str:
        """鑾峰彇閿欒娑堟伅"""
        return str(self.exception)
    
    @property
    def error_traceback(self) -> str:
        """鑾峰彇閿欒鍫嗘爤"""
        return traceback.format_exception(
            type(self.exception), self.exception, self.exception.__traceback__
        )

@dataclass
class RecoveryConfig:
    """鎭㈠閰嶇疆"""
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
    """閿欒鍒嗙被鍣?"""
    
    def __init__(self):
        # 閿欒妯″紡鍖归厤瑙勫垯
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
        鍒嗙被閿欒
        :param exception: 异常对象
        :param operation: 操作类型
        :return: (閿欒绫诲瀷, 涓ラ噸绋嬪害)
        """
        error_message = str(exception).lower()
        exception_type = type(exception)
        
        # 鍖归厤閿欒绫诲瀷
        classified_type = ErrorType.UNKNOWN_ERROR
        for error_type, patterns in self._error_patterns.items():
            for pattern in patterns:
                if isinstance(pattern, type) and isinstance(exception, pattern):
                    classified_type = error_type
                    break
                elif isinstance(pattern, str) and re.search(pattern, error_message):
                    classified_type = error_type
                    break
                elif callable(pattern) and not isinstance(pattern, type):
                    try:
                        if pattern(exception):
                            classified_type = error_type
                            break
                    except Exception:
                        continue
            if classified_type != ErrorType.UNKNOWN_ERROR:
                break
        
        # 纭畾涓ラ噸绋嬪害
        severity = self._determine_severity(classified_type, exception, operation)
        
        return classified_type, severity
    
    def _determine_severity(self, error_type: ErrorType, 
                          exception: Exception, 
                          operation: Optional[str]) -> ErrorSeverity:
        """纭畾閿欒涓ラ噸绋嬪害"""
        # 鍩轰簬閿欒绫诲瀷鐨勫熀纭€涓ラ噸绋嬪害
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
                # 鎻愬崌绯荤粺鍏抽敭鎿嶄綔鐨勪弗閲嶇▼搴?
                if severity == ErrorSeverity.LOW:
                    severity = ErrorSeverity.MEDIUM
                elif severity == ErrorSeverity.MEDIUM:
                    severity = ErrorSeverity.HIGH
        
        # 鍩轰簬寮傚父灞炴€ц皟鏁翠弗閲嶇▼搴?
        if hasattr(exception, 'status'):
            status_code = exception.status
            if status_code >= 500:
                severity = ErrorSeverity.HIGH
            elif status_code == 401:
                severity = ErrorSeverity.CRITICAL
        
        return severity

class CircuitBreaker:
    """鐔旀柇鍣?"""
    
    def __init__(self, threshold: int = 5, timeout: float = 60.0):
        self.threshold = threshold
        self.timeout = timeout
        self.failure_count = 0
        self.last_failure_time = 0
        self.state = 'closed'  # closed, open, half_open
    
    def can_execute(self) -> bool:
        """妫€鏌ユ槸鍚﹀彲浠ユ墽琛屾搷浣?"""
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
    """鏅鸿兘閿欒澶勭悊鍣?"""
    
    def __init__(self, websocket_manager=None, timeout_manager=None):
        """
        鍒濆鍖栭敊璇鐞嗗櫒
        :param websocket_manager: WebSocket绠＄悊鍣?
        :param timeout_manager: 瓒呮椂绠＄悊鍣?
        """
        self.websocket_manager = websocket_manager
        self.timeout_manager = timeout_manager
        self.classifier = ErrorClassifier()
        
        # 閿欒缁熻
        self._error_stats = {
            'total_errors': 0,
            'errors_by_type': {error_type.value: 0 for error_type in ErrorType},
            'errors_by_severity': {severity.value: 0 for severity in ErrorSeverity},
            'recovery_attempts': 0,
            'successful_recoveries': 0,
            'failed_recoveries': 0,
        }
        
        # 鐔旀柇鍣ㄥ疄渚?
        self._circuit_breakers: Dict[str, CircuitBreaker] = {}
        
        # 榛樿鎭㈠閰嶇疆
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
        """鍒濆鍖栭敊璇鐞嗗櫒鐨勫紓姝ョ粍浠?"""
        logger.info("开始初始化错误处理器异步组件...")
        # 杩欓噷鍙互娣诲姞寮傛鍒濆鍖栭€昏緫
        logger.info("错误处理器异步组件初始化完成")
    
    async def handle_error(self, 
                          exception: Exception,
                          task_id: Optional[str] = None,
                          page_number: Optional[int] = None,
                          operation: Optional[str] = None,
                          custom_config: Optional[RecoveryConfig] = None,
                          metadata: Optional[Dict[str, Any]] = None) -> ErrorContext:
        """
        澶勭悊閿欒
        :param exception: 异常对象
        :param task_id: 任务ID
        :param page_number: 椤甸潰缂栧彿
        :param operation: 鎿嶄綔鍚嶇О
        :param custom_config: 鑷畾涔夋仮澶嶉厤缃?
        :param metadata: 鍏冩暟鎹?
        :return: 閿欒涓婁笅鏂?
        """
        # 鍒嗙被閿欒
        error_type, severity = self.classifier.classify_error(exception, operation)
        
        # 鍒涘缓閿欒涓婁笅鏂?
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
        
        # 鏇存柊缁熻
        self._update_error_stats(error_type, severity)
        
        # 璁板綍閿欒
        await self._log_error(error_context)
        
        # 鍙戦€侀敊璇€氱煡
        await self._send_error_notification(error_context)
        
        # 鎵ц鎭㈠绛栫暐
        recovery_config = custom_config or self._default_recovery_configs.get(
            error_type, self._default_recovery_configs[ErrorType.UNKNOWN_ERROR]
        )

        error_context.max_retries = recovery_config.max_retries
        error_context.should_retry = False

        await self._execute_recovery_strategy(error_context, recovery_config)
        
        return error_context
    
    async def _execute_recovery_strategy(self, 
                                       error_context: ErrorContext, 
                                       config: RecoveryConfig):
        """鎵ц鎭㈠绛栫暐"""
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
            
            # 璋冪敤鑷畾涔夊鐞嗗櫒
            if config.custom_handler:
                await config.custom_handler(error_context, config)
                
        except Exception as e:
            logger.error(f"执行恢复策略失败: {e}")
            self._error_stats['failed_recoveries'] += 1
    
    async def _handle_retry(self, error_context: ErrorContext, config: RecoveryConfig):
        """澶勭悊绠€鍗曢噸璇?"""
        if error_context.retry_count < config.max_retries:
            error_context.retry_count += 1
            error_context.should_retry = True

            # 计算延迟时间
            delay = config.base_delay
            if config.jitter:
                delay += random.uniform(0, config.base_delay * 0.1)
            
            await self._send_retry_notification(error_context, delay)
            
            if delay > 0:
                await asyncio.sleep(delay)
            
            logger.info("retrying error %s attempt %s", error_context.error_id, error_context.retry_count)
            self._error_stats['successful_recoveries'] += 1
        else:
            error_context.should_retry = False
            await self._handle_max_retries_exceeded(error_context)

    async def _handle_exponential_backoff(self, error_context: ErrorContext, config: RecoveryConfig):
        """处理指数退避重试"""
        if error_context.retry_count < config.max_retries:
            error_context.retry_count += 1
            error_context.should_retry = True

            delay = min(
                config.base_delay * (config.backoff_multiplier ** (error_context.retry_count - 1)),
                config.max_delay,
            )

            if config.jitter:
                jitter_range = delay * 0.1
                delay += random.uniform(-jitter_range, jitter_range)
                delay = max(0, delay)

            await self._send_retry_notification(error_context, delay)

            if delay > 0:
                await asyncio.sleep(delay)

            logger.info(
                "执行指数退避重试: %s (第%s次, 延迟%.2fs)",
                error_context.error_id,
                error_context.retry_count,
                delay,
            )
            self._error_stats['successful_recoveries'] += 1
        else:
            error_context.should_retry = False
            await self._handle_max_retries_exceeded(error_context)
    
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
        
        # 鎵ц闄嶇骇澶勭悊鍣?
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
            'message': 'error requires escalation',
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
        """澶勭悊涓绛栫暐"""
        abort_message = {
            'type': 'operation_aborted',
            'error_id': error_context.error_id,
            'task_id': error_context.task_id,
            'page_number': error_context.page_number,
            'operation': error_context.operation,
            'error_type': error_context.error_type.value,
            'message': 'operation aborted',
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
        error_context.should_retry = False
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
        """璁板綍閿欒鏃ュ織"""
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
        """鍙戦€侀敊璇€氱煡"""
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
        """鍙戦€侀噸璇曢€氱煡"""
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
                'message': f'retry attempt {error_context.retry_count}',
                'timestamp': datetime.now(timezone.utc).isoformat()
            }
            
            if self.websocket_manager and error_context.task_id:
                await self.websocket_manager.send_to_task_subscribers(
                    error_context.task_id, retry_message
                )
                
        except Exception as e:
            logger.error(f"发送重试通知失败: {e}")
    
    async def _send_circuit_breaker_notification(self, error_context: ErrorContext, state: str):
        """鍙戦€佺啍鏂櫒閫氱煡"""
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
        """鏇存柊閿欒缁熻"""
        self._error_stats['total_errors'] += 1
        self._error_stats['errors_by_type'][error_type.value] += 1
        self._error_stats['errors_by_severity'][severity.value] += 1
    
    def get_error_stats(self) -> Dict[str, Any]:
        """鑾峰彇閿欒缁熻淇℃伅"""
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
        """鑾峰彇閿欒澶勭悊鍣ㄧ粺璁′俊鎭紙get_stats鐨勫埆鍚嶏級"""
        return self.get_error_stats()

    def update_recovery_config(self, error_type: ErrorType, config: RecoveryConfig):
        """鏇存柊鎭㈠閰嶇疆"""
        self._default_recovery_configs[error_type] = config
        logger.info(f"更新恢复配置: {error_type.value}")
    
    def reset_circuit_breaker(self, operation: str):
        """閲嶇疆鐔旀柇鍣?"""
        if operation in self._circuit_breakers:
            self._circuit_breakers[operation].failure_count = 0
            self._circuit_breakers[operation].state = 'closed'
            logger.info(f"重置熔断器: {operation}")

# 鍏ㄥ眬閿欒澶勭悊鍣ㄥ疄渚?
error_handler: Optional[ErrorHandler] = None

async def init_error_handler(websocket_manager=None, timeout_manager=None):
    """鍒濆鍖栧叏灞€閿欒澶勭悊鍣?"""
    global error_handler
    if error_handler is None:
        error_handler = ErrorHandler(websocket_manager, timeout_manager)
        logger.info("全局错误处理器初始化完成")

async def shutdown_error_handler():
    """鍏抽棴鍏ㄥ眬閿欒澶勭悊鍣?"""
    global error_handler
    if error_handler:
        error_handler = None
        logger.info("全局错误处理器已关闭")
