from datetime import datetime, timezone
from typing import Dict, Any, Optional, Union, List
from dataclasses import dataclass, asdict
from enum import Enum
import json

class MessageType(Enum):
    """WebSocket消息类型枚举"""
    # 连接管理
    CONNECTION_ESTABLISHED = "connection_established"
    PING = "ping"
    PONG = "pong"
    ERROR = "error"
    
    # 任务订阅
    SUBSCRIBE_TASK = "subscribe_task"
    UNSUBSCRIBE_TASK = "unsubscribe_task"
    TASK_SUBSCRIBED = "task_subscribed"
    TASK_UNSUBSCRIBED = "task_unsubscribed"
    
    # 任务状态查询
    GET_TASK_STATUS = "get_task_status"
    TASK_STATUS_RESPONSE = "task_status_response"
    
    # 任务生命周期事件
    TASK_STARTED = "task_started"
    TASK_PROGRESS = "task_progress"
    PAGE_COMPLETED = "page_completed"
    TASK_COMPLETED = "task_completed"
    TASK_FAILED = "task_failed"
    TASK_CANCELLED = "task_cancelled"
    
    # 分页相关事件
    BATCH_STARTED = "batch_started"
    BATCH_COMPLETED = "batch_completed"
    BATCH_FAILED = "batch_failed"
    PAGE_RETRY = "page_retry"
    PAGINATION_STRATEGY_CHANGED = "pagination_strategy_changed"
    
    # 详细进度事件
    DETAILED_PROGRESS = "detailed_progress"
    PROCESSING_RATE_UPDATE = "processing_rate_update"
    TIME_ESTIMATION_UPDATE = "time_estimation_update"
    
    # 错误通知
    ERROR_NOTIFICATION = "error_notification"
    
    # 任务控制
    PAUSE_TASK = "pause_task"
    RESUME_TASK = "resume_task"
    CANCEL_TASK = "cancel_task"
    RETRY_PAGE = "retry_page"
    
    # 系统状态
    SYSTEM_STATUS = "system_status"
    
    # 超时预警相关
    TIMEOUT_WARNING = "timeout_warning"
    TIMEOUT_EXCEEDED = "timeout_exceeded"
    TIMEOUT_RECOVERY = "timeout_recovery"
    
    # 错误恢复相关
    ERROR_RECOVERY_STARTED = "error_recovery_started"
    ERROR_RECOVERY_PROGRESS = "error_recovery_progress"
    ERROR_RECOVERY_COMPLETED = "error_recovery_completed"
    ERROR_RECOVERY_FAILED = "error_recovery_failed"
    
    # 系统监控相关
    SYSTEM_MONITORING_UPDATE = "system_monitoring_update"
    SYSTEM_ALERT = "system_alert"
    SYSTEM_ALERT_RESOLVED = "system_alert_resolved"
    PERFORMANCE_WARNING = "performance_warning"
    RESOURCE_WARNING = "resource_warning"
    
    # 检查点和恢复相关
    CHECKPOINT_CREATED = "checkpoint_created"
    CHECKPOINT_RESTORED = "checkpoint_restored"
    TASK_RECOVERED = "task_recovered"
    RECOVERY_JOB_UPDATE = "recovery_job_update"
    
    # 降级处理相关
    SYSTEM_DEGRADED = "system_degraded"
    SYSTEM_RECOVERED = "system_recovered"
    FALLBACK_ACTIVATED = "fallback_activated"

@dataclass
class BaseMessage:
    """基础消息类"""
    type: str = None
    timestamp: str = None
    
    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now(timezone.utc).isoformat()
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return asdict(self)
    
    def to_json(self) -> str:
        """转换为JSON字符串"""
        return json.dumps(self.to_dict(), ensure_ascii=False)

@dataclass
class ConnectionMessage(BaseMessage):
    """连接相关消息"""
    client_id: str = None
    message: str = None
    
    def __post_init__(self):
        super().__post_init__()
        self.type = MessageType.CONNECTION_ESTABLISHED.value

@dataclass
class ErrorMessage(BaseMessage):
    """错误消息"""
    message: str = None
    error_code: str = None
    details: Dict[str, Any] = None
    
    def __post_init__(self):
        super().__post_init__()
        self.type = MessageType.ERROR.value

@dataclass
class TaskSubscriptionMessage(BaseMessage):
    """任务订阅消息"""
    task_id: str = None
    action: str = None  # subscribe/unsubscribe
    
    def __post_init__(self):
        super().__post_init__()
        self.type = MessageType.SUBSCRIBE_TASK.value

@dataclass
class TaskProgressData:
    """任务进度数据"""
    progress: float  # 0-100
    current_page: int = None
    total_pages: int = None
    status: str = None
    message: str = None
    processed_pages: int = None
    failed_pages: int = None
    estimated_time_remaining: int = None  # 秒
    processing_rate: float = None  # 页/秒

@dataclass
class TaskStartedMessage(BaseMessage):
    """任务开始消息"""
    task_id: str = None
    task_type: str = None
    file_name: str = None
    total_pages: int = None
    estimated_time: int = None  # 预估处理时间（秒）
    
    def __post_init__(self):
        super().__post_init__()
        self.type = MessageType.TASK_STARTED.value

@dataclass
class TaskProgressMessage(BaseMessage):
    """任务进度消息"""
    task_id: str = None
    data: TaskProgressData = None
    
    def __post_init__(self):
        super().__post_init__()
        self.type = MessageType.TASK_PROGRESS.value

@dataclass
class PageCompletedData:
    """页面完成数据"""
    page_number: int
    status: str  # completed/failed
    content_length: int = None
    processing_time: float = None
    error_message: str = None

@dataclass
class PageCompletedMessage(BaseMessage):
    """页面完成消息"""
    task_id: str = None
    data: PageCompletedData = None
    
    def __post_init__(self):
        super().__post_init__()
        self.type = MessageType.PAGE_COMPLETED.value

@dataclass
class TaskResultSummary:
    """任务结果摘要"""
    total_pages: int
    successful_pages: int
    failed_pages: int
    success_rate: str
    processing_time: float
    content_length: int = None
    file_size_mb: float = None

@dataclass
class TaskCompletedMessage(BaseMessage):
    """任务完成消息"""
    task_id: str = None
    result_summary: TaskResultSummary = None
    content_preview: str = None  # 内容预览（前500字符）
    
    def __post_init__(self):
        super().__post_init__()
        self.type = MessageType.TASK_COMPLETED.value

@dataclass
class TaskFailedMessage(BaseMessage):
    """任务失败消息"""
    task_id: str = None
    error_message: str = None
    failed_page: int = None
    partial_results: TaskResultSummary = None
    
    def __post_init__(self):
        super().__post_init__()
        self.type = MessageType.TASK_FAILED.value

@dataclass
class TaskCancelledMessage(BaseMessage):
    """任务取消消息"""
    task_id: str = None
    reason: str = "用户取消"
    partial_results: TaskResultSummary = None
    
    def __post_init__(self):
        super().__post_init__()
        self.type = MessageType.TASK_CANCELLED.value

@dataclass
class ErrorNotificationMessage(BaseMessage):
    """错误通知消息"""
    error_type: str = None
    error_message: str = None
    task_id: str = None
    severity: str = "error"  # info/warning/error/critical
    
    def __post_init__(self):
        super().__post_init__()
        self.type = MessageType.ERROR_NOTIFICATION.value

@dataclass
class TaskControlMessage(BaseMessage):
    """任务控制消息"""
    task_id: str = None
    action: str = None  # pause/resume/cancel
    reason: str = None
    
    def __post_init__(self):
        super().__post_init__()
        self.type = "task_control"

@dataclass
class SystemStatusData:
    """系统状态数据"""
    active_connections: int
    queue_stats: Dict[str, Any]
    worker_stats: Dict[str, Any]
    memory_usage: Dict[str, Any] = None
    uptime: float = None

@dataclass
class SystemStatusMessage(BaseMessage):
    """系统状态消息"""
    data: SystemStatusData = None
    
    def __post_init__(self):
        super().__post_init__()
        self.type = MessageType.SYSTEM_STATUS.value

@dataclass
class BatchData:
    """批次数据"""
    batch_id: str
    batch_number: int
    total_batches: int
    page_range: str  # e.g., "1-5"
    pages_in_batch: int
    estimated_time: int = None
    processing_strategy: str = None

@dataclass
class BatchStartedMessage(BaseMessage):
    """批次开始消息"""
    task_id: str = None
    data: BatchData = None
    
    def __post_init__(self):
        super().__post_init__()
        self.type = MessageType.BATCH_STARTED.value

@dataclass
class BatchCompletedMessage(BaseMessage):
    """批次完成消息"""
    task_id: str = None
    data: BatchData = None
    successful_pages: int = None
    failed_pages: int = None
    processing_time: float = None
    
    def __post_init__(self):
        super().__post_init__()
        self.type = MessageType.BATCH_COMPLETED.value

@dataclass
class BatchFailedMessage(BaseMessage):
    """批次失败消息"""
    task_id: str = None
    data: BatchData = None
    error_message: str = None
    failed_pages: List[int] = None
    
    def __post_init__(self):
        super().__post_init__()
        self.type = MessageType.BATCH_FAILED.value

@dataclass
class PageRetryData:
    """页面重试数据"""
    page_number: int
    retry_count: int
    max_retries: int
    retry_reason: str
    previous_error: str = None

@dataclass
class PageRetryMessage(BaseMessage):
    """页面重试消息"""
    task_id: str = None
    data: PageRetryData = None
    
    def __post_init__(self):
        super().__post_init__()
        self.type = MessageType.PAGE_RETRY.value

@dataclass
class DetailedProgressData:
    """详细进度数据"""
    progress_percentage: float
    processed_pages: int
    failed_pages: int
    pending_pages: int
    total_pages: int
    processing_rate: float  # 页/秒
    average_page_time: float  # 秒/页
    estimated_time_remaining: int  # 秒
    current_batch: int = None
    total_batches: int = None
    memory_usage_mb: float = None
    success_rate: float = None

@dataclass
class DetailedProgressMessage(BaseMessage):
    """详细进度消息"""
    task_id: str = None
    data: DetailedProgressData = None
    
    def __post_init__(self):
        super().__post_init__()
        self.type = MessageType.DETAILED_PROGRESS.value

@dataclass
class ProcessingRateData:
    """处理速率数据"""
    current_rate: float  # 页/秒
    average_rate: float  # 页/秒
    peak_rate: float  # 页/秒
    rate_trend: str  # increasing/decreasing/stable
    efficiency_score: float = None  # 0-100

@dataclass
class ProcessingRateMessage(BaseMessage):
    """处理速率更新消息"""
    task_id: str = None
    data: ProcessingRateData = None
    
    def __post_init__(self):
        super().__post_init__()
        self.type = MessageType.PROCESSING_RATE_UPDATE.value

@dataclass
class TimeEstimationData:
    """时间估算数据"""
    estimated_total_time: int  # 秒
    estimated_remaining_time: int  # 秒
    elapsed_time: int  # 秒
    confidence_level: float  # 0-1
    estimation_method: str  # linear/exponential/adaptive

@dataclass
class TimeEstimationMessage(BaseMessage):
    """时间估算更新消息"""
    task_id: str = None
    data: TimeEstimationData = None
    
    def __post_init__(self):
        super().__post_init__()
        self.type = MessageType.TIME_ESTIMATION_UPDATE.value

@dataclass
class PaginationStrategyData:
    """分页策略数据"""
    old_strategy: str
    new_strategy: str
    reason: str
    batch_size_change: Dict[str, int] = None  # {"old": 5, "new": 3}
    expected_improvement: str = None

@dataclass
class PaginationStrategyMessage(BaseMessage):
    """分页策略变更消息"""
    task_id: str = None
    data: PaginationStrategyData = None
    
    def __post_init__(self):
        super().__post_init__()
        self.type = MessageType.PAGINATION_STRATEGY_CHANGED.value

# 新增的超时预警和错误恢复相关数据类

@dataclass
class TimeoutWarningData:
    """超时预警数据"""
    timeout_type: str  # api_request/page_processing/task_execution/connection
    current_duration: float  # 当前已用时间（秒）
    timeout_threshold: float  # 超时阈值（秒）
    warning_threshold: float  # 预警阈值（秒）
    progress_percentage: float  # 当前进度百分比
    estimated_completion_time: float = None  # 预估完成时间（秒）
    context: Dict[str, Any] = None  # 上下文信息

@dataclass
class TimeoutWarningMessage(BaseMessage):
    """超时预警消息"""
    task_id: str = None
    operation_id: str = None
    data: TimeoutWarningData = None
    
    def __post_init__(self):
        super().__post_init__()
        self.type = MessageType.TIMEOUT_WARNING.value

@dataclass
class TimeoutExceededData:
    """超时超出数据"""
    timeout_type: str
    duration: float  # 实际用时（秒）
    timeout_threshold: float  # 超时阈值（秒）
    action_taken: str  # 采取的措施
    recovery_strategy: str = None  # 恢复策略
    context: Dict[str, Any] = None

@dataclass
class TimeoutExceededMessage(BaseMessage):
    """超时超出消息"""
    task_id: str = None
    operation_id: str = None
    data: TimeoutExceededData = None
    
    def __post_init__(self):
        super().__post_init__()
        self.type = MessageType.TIMEOUT_EXCEEDED.value

@dataclass
class TimeoutRecoveryData:
    """超时恢复数据"""
    timeout_type: str
    recovery_action: str  # retry/fallback/abort
    recovery_result: str  # success/failed/in_progress
    elapsed_time: float  # 恢复用时（秒）
    context: Dict[str, Any] = None

@dataclass
class TimeoutRecoveryMessage(BaseMessage):
    """超时恢复消息"""
    task_id: str = None
    operation_id: str = None
    data: TimeoutRecoveryData = None
    
    def __post_init__(self):
        super().__post_init__()
        self.type = MessageType.TIMEOUT_RECOVERY.value

@dataclass
class ErrorRecoveryData:
    """错误恢复数据"""
    error_type: str
    error_message: str
    recovery_strategy: str  # retry/fallback/escalate/abort
    recovery_state: str  # started/in_progress/completed/failed
    attempt_count: int
    max_attempts: int
    elapsed_time: float = None
    success_rate: float = None
    context: Dict[str, Any] = None

@dataclass
class ErrorRecoveryStartedMessage(BaseMessage):
    """错误恢复开始消息"""
    task_id: str = None
    operation_id: str = None
    data: ErrorRecoveryData = None
    
    def __post_init__(self):
        super().__post_init__()
        self.type = MessageType.ERROR_RECOVERY_STARTED.value

@dataclass
class ErrorRecoveryProgressMessage(BaseMessage):
    """错误恢复进度消息"""
    task_id: str = None
    operation_id: str = None
    data: ErrorRecoveryData = None
    
    def __post_init__(self):
        super().__post_init__()
        self.type = MessageType.ERROR_RECOVERY_PROGRESS.value

@dataclass
class ErrorRecoveryCompletedMessage(BaseMessage):
    """错误恢复完成消息"""
    task_id: str = None
    operation_id: str = None
    data: ErrorRecoveryData = None
    
    def __post_init__(self):
        super().__post_init__()
        self.type = MessageType.ERROR_RECOVERY_COMPLETED.value

@dataclass
class ErrorRecoveryFailedMessage(BaseMessage):
    """错误恢复失败消息"""
    task_id: str = None
    operation_id: str = None
    data: ErrorRecoveryData = None
    
    def __post_init__(self):
        super().__post_init__()
        self.type = MessageType.ERROR_RECOVERY_FAILED.value

@dataclass
class SystemMonitoringData:
    """系统监控数据"""
    system_metrics: Dict[str, Any]  # CPU, 内存, 磁盘等
    api_statistics: Dict[str, Any]  # API统计信息
    task_queue_status: Dict[str, Any]  # 任务队列状态
    active_alerts: List[Dict[str, Any]]  # 活跃告警
    performance_stats: Dict[str, Any]  # 性能统计

@dataclass
class SystemMonitoringUpdateMessage(BaseMessage):
    """系统监控更新消息"""
    data: SystemMonitoringData = None
    
    def __post_init__(self):
        super().__post_init__()
        self.type = MessageType.SYSTEM_MONITORING_UPDATE.value

@dataclass
class SystemAlertData:
    """系统告警数据"""
    alert_id: str
    metric_name: str
    level: str  # warning/critical
    current_value: float
    threshold: float
    message: str
    triggered_at: str
    context: Dict[str, Any] = None

@dataclass
class SystemAlertMessage(BaseMessage):
    """系统告警消息"""
    alert: SystemAlertData = None
    
    def __post_init__(self):
        super().__post_init__()
        self.type = MessageType.SYSTEM_ALERT.value

@dataclass
class SystemAlertResolvedMessage(BaseMessage):
    """系统告警解决消息"""
    alert: SystemAlertData = None
    
    def __post_init__(self):
        super().__post_init__()
        self.type = MessageType.SYSTEM_ALERT_RESOLVED.value

@dataclass
class PerformanceWarningData:
    """性能预警数据"""
    metric_name: str  # response_time/throughput/error_rate
    current_value: float
    baseline_value: float
    degradation_percentage: float
    trend: str  # increasing/decreasing/stable
    impact_assessment: str  # low/medium/high
    recommended_action: str = None

@dataclass
class PerformanceWarningMessage(BaseMessage):
    """性能预警消息"""
    task_id: str = None
    data: PerformanceWarningData = None
    
    def __post_init__(self):
        super().__post_init__()
        self.type = MessageType.PERFORMANCE_WARNING.value

@dataclass
class ResourceWarningData:
    """资源预警数据"""
    resource_type: str  # memory/disk/cpu/network
    current_usage: float  # 当前使用量
    total_capacity: float  # 总容量
    usage_percentage: float  # 使用百分比
    threshold_percentage: float  # 阈值百分比
    projected_exhaustion_time: int = None  # 预计耗尽时间（秒）
    recommended_action: str = None

@dataclass
class ResourceWarningMessage(BaseMessage):
    """资源预警消息"""
    data: ResourceWarningData = None
    
    def __post_init__(self):
        super().__post_init__()
        self.type = MessageType.RESOURCE_WARNING.value

@dataclass
class CheckpointData:
    """检查点数据"""
    task_id: str
    checkpoint_id: str
    checkpoint_type: str
    progress: float
    timestamp: str
    metadata: Dict[str, Any] = None

@dataclass
class CheckpointCreatedMessage(BaseMessage):
    """检查点创建消息"""
    checkpoint: CheckpointData = None
    
    def __post_init__(self):
        super().__post_init__()
        self.type = MessageType.CHECKPOINT_CREATED.value

@dataclass
class CheckpointRestoredMessage(BaseMessage):
    """检查点恢复消息"""
    checkpoint: CheckpointData = None
    
    def __post_init__(self):
        super().__post_init__()
        self.type = MessageType.CHECKPOINT_RESTORED.value

@dataclass
class TaskRecoveredMessage(BaseMessage):
    """任务恢复消息"""
    checkpoint: CheckpointData = None
    
    def __post_init__(self):
        super().__post_init__()
        self.type = MessageType.TASK_RECOVERED.value

@dataclass
class RecoveryJobData:
    """恢复任务数据"""
    task_id: str
    recovery_state: str
    recovery_attempts: int
    max_recovery_attempts: int
    recovery_strategy: str
    last_checkpoint: CheckpointData = None
    error_messages: List[str] = None

@dataclass
class RecoveryJobUpdateMessage(BaseMessage):
    """恢复任务更新消息"""
    recovery_job: RecoveryJobData = None
    
    def __post_init__(self):
        super().__post_init__()
        self.type = MessageType.RECOVERY_JOB_UPDATE.value

@dataclass
class SystemDegradationData:
    """系统降级数据"""
    degradation_level: str  # low/medium/high
    affected_services: List[str]
    cause: str
    mitigation_actions: List[str]
    estimated_recovery_time: int = None  # 秒
    performance_impact: Dict[str, float] = None

@dataclass
class SystemDegradedMessage(BaseMessage):
    """系统降级消息"""
    data: SystemDegradationData = None
    
    def __post_init__(self):
        super().__post_init__()
        self.type = MessageType.SYSTEM_DEGRADED.value

@dataclass
class SystemRecoveredMessage(BaseMessage):
    """系统恢复消息"""
    data: SystemDegradationData = None
    recovery_time: float = None  # 恢复用时（秒）
    
    def __post_init__(self):
        super().__post_init__()
        self.type = MessageType.SYSTEM_RECOVERED.value

@dataclass
class FallbackData:
    """降级处理数据"""
    fallback_type: str  # service/feature/quality
    original_service: str
    fallback_service: str
    reason: str
    performance_impact: str  # minimal/moderate/significant
    estimated_duration: int = None  # 秒

@dataclass
class FallbackActivatedMessage(BaseMessage):
    """降级处理激活消息"""
    task_id: str = None
    data: FallbackData = None
    
    def __post_init__(self):
        super().__post_init__()
        self.type = MessageType.FALLBACK_ACTIVATED.value

class MessageFactory:
    """消息工厂类，用于创建各种类型的消息"""
    
    @staticmethod
    def create_task_started(task_id: str, task_type: str, file_name: str,
                          total_pages: int, estimated_time: int = None) -> TaskStartedMessage:
        """创建任务开始消息"""
        return TaskStartedMessage(
            task_id=task_id,
            task_type=task_type,
            file_name=file_name,
            total_pages=total_pages,
            estimated_time=estimated_time
        )
    
    @staticmethod
    def create_task_progress(task_id: str, progress: float, current_page: int = None,
                           total_pages: int = None, status: str = None,
                           message: str = None, **kwargs) -> TaskProgressMessage:
        """创建任务进度消息"""
        progress_data = TaskProgressData(
            progress=progress,
            current_page=current_page,
            total_pages=total_pages,
            status=status,
            message=message,
            **kwargs
        )
        return TaskProgressMessage(task_id=task_id, data=progress_data)
    
    @staticmethod
    def create_page_completed(task_id: str, page_number: int, status: str,
                            content_length: int = None, processing_time: float = None,
                            error_message: str = None) -> PageCompletedMessage:
        """创建页面完成消息"""
        page_data = PageCompletedData(
            page_number=page_number,
            status=status,
            content_length=content_length,
            processing_time=processing_time,
            error_message=error_message
        )
        return PageCompletedMessage(task_id=task_id, data=page_data)
    
    @staticmethod
    def create_task_completed(task_id: str, result_summary: Dict[str, Any],
                            content_preview: str = None) -> TaskCompletedMessage:
        """创建任务完成消息"""
        summary = TaskResultSummary(**result_summary)
        return TaskCompletedMessage(
            task_id=task_id,
            result_summary=summary,
            content_preview=content_preview
        )
    
    @staticmethod
    def create_task_failed(task_id: str, error_message: str, failed_page: int = None,
                         partial_results: Dict[str, Any] = None) -> TaskFailedMessage:
        """创建任务失败消息"""
        partial_summary = None
        if partial_results:
            partial_summary = TaskResultSummary(**partial_results)
        
        return TaskFailedMessage(
            task_id=task_id,
            error_message=error_message,
            failed_page=failed_page,
            partial_results=partial_summary
        )
    
    @staticmethod
    def create_task_cancelled(task_id: str, reason: str = "用户取消",
                            partial_results: Dict[str, Any] = None) -> TaskCancelledMessage:
        """创建任务取消消息"""
        partial_summary = None
        if partial_results:
            partial_summary = TaskResultSummary(**partial_results)
        
        return TaskCancelledMessage(
            task_id=task_id,
            reason=reason,
            partial_results=partial_summary
        )
    
    @staticmethod
    def create_error_notification(error_type: str, error_message: str,
                                task_id: str = None, severity: str = "error") -> ErrorNotificationMessage:
        """创建错误通知消息"""
        return ErrorNotificationMessage(
            error_type=error_type,
            error_message=error_message,
            task_id=task_id,
            severity=severity
        )
    
    @staticmethod
    def create_system_status(active_connections: int, queue_stats: Dict[str, Any],
                           worker_stats: Dict[str, Any], **kwargs) -> SystemStatusMessage:
        """创建系统状态消息"""
        status_data = SystemStatusData(
            active_connections=active_connections,
            queue_stats=queue_stats,
            worker_stats=worker_stats,
            **kwargs
        )
        return SystemStatusMessage(data=status_data)
    
    @staticmethod
    def create_error(message: str, error_code: str = None,
                    details: Dict[str, Any] = None) -> ErrorMessage:
        """创建错误消息"""
        return ErrorMessage(
            type=MessageType.ERROR.value,
            message=message,
            error_code=error_code,
            details=details
        )
    
    @staticmethod
    def create_connection_established(client_id: str) -> ConnectionMessage:
        """创建连接建立消息"""
        return ConnectionMessage(
            type=MessageType.CONNECTION_ESTABLISHED.value,
            client_id=client_id,
            message="WebSocket连接已建立"
        )
    
    @staticmethod
    def create_batch_started(task_id: str, batch_id: str, batch_number: int,
                           total_batches: int, page_range: str, pages_in_batch: int,
                           estimated_time: int = None, processing_strategy: str = None) -> BatchStartedMessage:
        """创建批次开始消息"""
        batch_data = BatchData(
            batch_id=batch_id,
            batch_number=batch_number,
            total_batches=total_batches,
            page_range=page_range,
            pages_in_batch=pages_in_batch,
            estimated_time=estimated_time,
            processing_strategy=processing_strategy
        )
        return BatchStartedMessage(task_id=task_id, data=batch_data)
    
    @staticmethod
    def create_batch_completed(task_id: str, batch_id: str, batch_number: int,
                             total_batches: int, page_range: str, pages_in_batch: int,
                             successful_pages: int = None, failed_pages: int = None,
                             processing_time: float = None) -> BatchCompletedMessage:
        """创建批次完成消息"""
        batch_data = BatchData(
            batch_id=batch_id,
            batch_number=batch_number,
            total_batches=total_batches,
            page_range=page_range,
            pages_in_batch=pages_in_batch
        )
        return BatchCompletedMessage(
            task_id=task_id,
            data=batch_data,
            successful_pages=successful_pages,
            failed_pages=failed_pages,
            processing_time=processing_time
        )
    
    @staticmethod
    def create_batch_failed(task_id: str, batch_id: str, batch_number: int,
                          total_batches: int, page_range: str, pages_in_batch: int,
                          error_message: str, failed_pages: List[int] = None) -> BatchFailedMessage:
        """创建批次失败消息"""
        batch_data = BatchData(
            batch_id=batch_id,
            batch_number=batch_number,
            total_batches=total_batches,
            page_range=page_range,
            pages_in_batch=pages_in_batch
        )
        return BatchFailedMessage(
            task_id=task_id,
            data=batch_data,
            error_message=error_message,
            failed_pages=failed_pages
        )
    
    @staticmethod
    def create_page_retry(task_id: str, page_number: int, retry_count: int,
                        max_retries: int, retry_reason: str,
                        previous_error: str = None) -> PageRetryMessage:
        """创建页面重试消息"""
        retry_data = PageRetryData(
            page_number=page_number,
            retry_count=retry_count,
            max_retries=max_retries,
            retry_reason=retry_reason,
            previous_error=previous_error
        )
        return PageRetryMessage(task_id=task_id, data=retry_data)
    
    @staticmethod
    def create_detailed_progress(task_id: str, progress_percentage: float,
                               processed_pages: int, failed_pages: int, pending_pages: int,
                               total_pages: int, processing_rate: float, average_page_time: float,
                               estimated_time_remaining: int, **kwargs) -> DetailedProgressMessage:
        """创建详细进度消息"""
        progress_data = DetailedProgressData(
            progress_percentage=progress_percentage,
            processed_pages=processed_pages,
            failed_pages=failed_pages,
            pending_pages=pending_pages,
            total_pages=total_pages,
            processing_rate=processing_rate,
            average_page_time=average_page_time,
            estimated_time_remaining=estimated_time_remaining,
            **kwargs
        )
        return DetailedProgressMessage(task_id=task_id, data=progress_data)
    
    @staticmethod
    def create_processing_rate_update(task_id: str, current_rate: float, average_rate: float,
                                    peak_rate: float, rate_trend: str,
                                    efficiency_score: float = None) -> ProcessingRateMessage:
        """创建处理速率更新消息"""
        rate_data = ProcessingRateData(
            current_rate=current_rate,
            average_rate=average_rate,
            peak_rate=peak_rate,
            rate_trend=rate_trend,
            efficiency_score=efficiency_score
        )
        return ProcessingRateMessage(task_id=task_id, data=rate_data)
    
    @staticmethod
    def create_time_estimation_update(task_id: str, estimated_total_time: int,
                                    estimated_remaining_time: int, elapsed_time: int,
                                    confidence_level: float, estimation_method: str) -> TimeEstimationMessage:
        """创建时间估算更新消息"""
        estimation_data = TimeEstimationData(
            estimated_total_time=estimated_total_time,
            estimated_remaining_time=estimated_remaining_time,
            elapsed_time=elapsed_time,
            confidence_level=confidence_level,
            estimation_method=estimation_method
        )
        return TimeEstimationMessage(task_id=task_id, data=estimation_data)
    
    @staticmethod
    def create_pagination_strategy_changed(task_id: str, old_strategy: str, new_strategy: str,
                                         reason: str, batch_size_change: Dict[str, int] = None,
                                         expected_improvement: str = None) -> PaginationStrategyMessage:
        """创建分页策略变更消息"""
        strategy_data = PaginationStrategyData(
            old_strategy=old_strategy,
            new_strategy=new_strategy,
            reason=reason,
            batch_size_change=batch_size_change,
            expected_improvement=expected_improvement
        )
        return PaginationStrategyMessage(task_id=task_id, data=strategy_data)
    
    # 新增的消息创建方法
    
    @staticmethod
    def create_timeout_warning(timeout_type: str, current_duration: float,
                             timeout_threshold: float, warning_threshold: float,
                             progress_percentage: float, task_id: str = None,
                             operation_id: str = None, **kwargs) -> TimeoutWarningMessage:
        """创建超时预警消息"""
        warning_data = TimeoutWarningData(
            timeout_type=timeout_type,
            current_duration=current_duration,
            timeout_threshold=timeout_threshold,
            warning_threshold=warning_threshold,
            progress_percentage=progress_percentage,
            **kwargs
        )
        return TimeoutWarningMessage(
            task_id=task_id,
            operation_id=operation_id,
            data=warning_data
        )
    
    @staticmethod
    def create_timeout_exceeded(timeout_type: str, duration: float,
                              timeout_threshold: float, action_taken: str,
                              task_id: str = None, operation_id: str = None,
                              **kwargs) -> TimeoutExceededMessage:
        """创建超时超出消息"""
        exceeded_data = TimeoutExceededData(
            timeout_type=timeout_type,
            duration=duration,
            timeout_threshold=timeout_threshold,
            action_taken=action_taken,
            **kwargs
        )
        return TimeoutExceededMessage(
            task_id=task_id,
            operation_id=operation_id,
            data=exceeded_data
        )
    
    @staticmethod
    def create_timeout_recovery(timeout_type: str, recovery_action: str,
                              recovery_result: str, elapsed_time: float,
                              task_id: str = None, operation_id: str = None,
                              **kwargs) -> TimeoutRecoveryMessage:
        """创建超时恢复消息"""
        recovery_data = TimeoutRecoveryData(
            timeout_type=timeout_type,
            recovery_action=recovery_action,
            recovery_result=recovery_result,
            elapsed_time=elapsed_time,
            **kwargs
        )
        return TimeoutRecoveryMessage(
            task_id=task_id,
            operation_id=operation_id,
            data=recovery_data
        )
    
    @staticmethod
    def create_error_recovery_started(error_type: str, error_message: str,
                                    recovery_strategy: str, attempt_count: int,
                                    max_attempts: int, task_id: str = None,
                                    operation_id: str = None, **kwargs) -> ErrorRecoveryStartedMessage:
        """创建错误恢复开始消息"""
        recovery_data = ErrorRecoveryData(
            error_type=error_type,
            error_message=error_message,
            recovery_strategy=recovery_strategy,
            recovery_state="started",
            attempt_count=attempt_count,
            max_attempts=max_attempts,
            **kwargs
        )
        return ErrorRecoveryStartedMessage(
            task_id=task_id,
            operation_id=operation_id,
            data=recovery_data
        )
    
    @staticmethod
    def create_error_recovery_progress(error_type: str, error_message: str,
                                     recovery_strategy: str, attempt_count: int,
                                     max_attempts: int, task_id: str = None,
                                     operation_id: str = None, **kwargs) -> ErrorRecoveryProgressMessage:
        """创建错误恢复进度消息"""
        recovery_data = ErrorRecoveryData(
            error_type=error_type,
            error_message=error_message,
            recovery_strategy=recovery_strategy,
            recovery_state="in_progress",
            attempt_count=attempt_count,
            max_attempts=max_attempts,
            **kwargs
        )
        return ErrorRecoveryProgressMessage(
            task_id=task_id,
            operation_id=operation_id,
            data=recovery_data
        )
    
    @staticmethod
    def create_error_recovery_completed(error_type: str, error_message: str,
                                      recovery_strategy: str, attempt_count: int,
                                      max_attempts: int, task_id: str = None,
                                      operation_id: str = None, **kwargs) -> ErrorRecoveryCompletedMessage:
        """创建错误恢复完成消息"""
        recovery_data = ErrorRecoveryData(
            error_type=error_type,
            error_message=error_message,
            recovery_strategy=recovery_strategy,
            recovery_state="completed",
            attempt_count=attempt_count,
            max_attempts=max_attempts,
            **kwargs
        )
        return ErrorRecoveryCompletedMessage(
            task_id=task_id,
            operation_id=operation_id,
            data=recovery_data
        )
    
    @staticmethod
    def create_error_recovery_failed(error_type: str, error_message: str,
                                   recovery_strategy: str, attempt_count: int,
                                   max_attempts: int, task_id: str = None,
                                   operation_id: str = None, **kwargs) -> ErrorRecoveryFailedMessage:
        """创建错误恢复失败消息"""
        recovery_data = ErrorRecoveryData(
            error_type=error_type,
            error_message=error_message,
            recovery_strategy=recovery_strategy,
            recovery_state="failed",
            attempt_count=attempt_count,
            max_attempts=max_attempts,
            **kwargs
        )
        return ErrorRecoveryFailedMessage(
            task_id=task_id,
            operation_id=operation_id,
            data=recovery_data
        )
    
    @staticmethod
    def create_system_monitoring_update(system_metrics: Dict[str, Any],
                                      api_statistics: Dict[str, Any],
                                      task_queue_status: Dict[str, Any],
                                      active_alerts: List[Dict[str, Any]],
                                      performance_stats: Dict[str, Any]) -> SystemMonitoringUpdateMessage:
        """创建系统监控更新消息"""
        monitoring_data = SystemMonitoringData(
            system_metrics=system_metrics,
            api_statistics=api_statistics,
            task_queue_status=task_queue_status,
            active_alerts=active_alerts,
            performance_stats=performance_stats
        )
        return SystemMonitoringUpdateMessage(data=monitoring_data)
    
    @staticmethod
    def create_system_alert(alert_id: str, metric_name: str, level: str,
                          current_value: float, threshold: float, message: str,
                          triggered_at: str, **kwargs) -> SystemAlertMessage:
        """创建系统告警消息"""
        alert_data = SystemAlertData(
            alert_id=alert_id,
            metric_name=metric_name,
            level=level,
            current_value=current_value,
            threshold=threshold,
            message=message,
            triggered_at=triggered_at,
            **kwargs
        )
        return SystemAlertMessage(alert=alert_data)
    
    @staticmethod
    def create_system_alert_resolved(alert_id: str, metric_name: str, level: str,
                                   current_value: float, threshold: float, message: str,
                                   triggered_at: str, **kwargs) -> SystemAlertResolvedMessage:
        """创建系统告警解决消息"""
        alert_data = SystemAlertData(
            alert_id=alert_id,
            metric_name=metric_name,
            level=level,
            current_value=current_value,
            threshold=threshold,
            message=message,
            triggered_at=triggered_at,
            **kwargs
        )
        return SystemAlertResolvedMessage(alert=alert_data)
    
    @staticmethod
    def create_performance_warning(metric_name: str, current_value: float,
                                 baseline_value: float, degradation_percentage: float,
                                 trend: str, impact_assessment: str,
                                 task_id: str = None, **kwargs) -> PerformanceWarningMessage:
        """创建性能预警消息"""
        warning_data = PerformanceWarningData(
            metric_name=metric_name,
            current_value=current_value,
            baseline_value=baseline_value,
            degradation_percentage=degradation_percentage,
            trend=trend,
            impact_assessment=impact_assessment,
            **kwargs
        )
        return PerformanceWarningMessage(task_id=task_id, data=warning_data)
    
    @staticmethod
    def create_resource_warning(resource_type: str, current_usage: float,
                              total_capacity: float, usage_percentage: float,
                              threshold_percentage: float, **kwargs) -> ResourceWarningMessage:
        """创建资源预警消息"""
        warning_data = ResourceWarningData(
            resource_type=resource_type,
            current_usage=current_usage,
            total_capacity=total_capacity,
            usage_percentage=usage_percentage,
            threshold_percentage=threshold_percentage,
            **kwargs
        )
        return ResourceWarningMessage(data=warning_data)
    
    @staticmethod
    def create_checkpoint_created(task_id: str, checkpoint_id: str,
                                checkpoint_type: str, progress: float,
                                timestamp: str, **kwargs) -> CheckpointCreatedMessage:
        """创建检查点创建消息"""
        checkpoint_data = CheckpointData(
            task_id=task_id,
            checkpoint_id=checkpoint_id,
            checkpoint_type=checkpoint_type,
            progress=progress,
            timestamp=timestamp,
            **kwargs
        )
        return CheckpointCreatedMessage(checkpoint=checkpoint_data)
    
    @staticmethod
    def create_checkpoint_restored(task_id: str, checkpoint_id: str,
                                 checkpoint_type: str, progress: float,
                                 timestamp: str, **kwargs) -> CheckpointRestoredMessage:
        """创建检查点恢复消息"""
        checkpoint_data = CheckpointData(
            task_id=task_id,
            checkpoint_id=checkpoint_id,
            checkpoint_type=checkpoint_type,
            progress=progress,
            timestamp=timestamp,
            **kwargs
        )
        return CheckpointRestoredMessage(checkpoint=checkpoint_data)
    
    @staticmethod
    def create_task_recovered(task_id: str, checkpoint_id: str,
                            checkpoint_type: str, progress: float,
                            timestamp: str, **kwargs) -> TaskRecoveredMessage:
        """创建任务恢复消息"""
        checkpoint_data = CheckpointData(
            task_id=task_id,
            checkpoint_id=checkpoint_id,
            checkpoint_type=checkpoint_type,
            progress=progress,
            timestamp=timestamp,
            **kwargs
        )
        return TaskRecoveredMessage(checkpoint=checkpoint_data)
    
    @staticmethod
    def create_recovery_job_update(task_id: str, recovery_state: str,
                                 recovery_attempts: int, max_recovery_attempts: int,
                                 recovery_strategy: str, **kwargs) -> RecoveryJobUpdateMessage:
        """创建恢复任务更新消息"""
        recovery_data = RecoveryJobData(
            task_id=task_id,
            recovery_state=recovery_state,
            recovery_attempts=recovery_attempts,
            max_recovery_attempts=max_recovery_attempts,
            recovery_strategy=recovery_strategy,
            **kwargs
        )
        return RecoveryJobUpdateMessage(recovery_job=recovery_data)
    
    @staticmethod
    def create_system_degraded(degradation_level: str, affected_services: List[str],
                             cause: str, mitigation_actions: List[str],
                             **kwargs) -> SystemDegradedMessage:
        """创建系统降级消息"""
        degradation_data = SystemDegradationData(
            degradation_level=degradation_level,
            affected_services=affected_services,
            cause=cause,
            mitigation_actions=mitigation_actions,
            **kwargs
        )
        return SystemDegradedMessage(data=degradation_data)
    
    @staticmethod
    def create_system_recovered(degradation_level: str, affected_services: List[str],
                              cause: str, mitigation_actions: List[str],
                              recovery_time: float = None, **kwargs) -> SystemRecoveredMessage:
        """创建系统恢复消息"""
        degradation_data = SystemDegradationData(
            degradation_level=degradation_level,
            affected_services=affected_services,
            cause=cause,
            mitigation_actions=mitigation_actions,
            **kwargs
        )
        return SystemRecoveredMessage(data=degradation_data, recovery_time=recovery_time)
    
    @staticmethod
    def create_fallback_activated(fallback_type: str, original_service: str,
                                fallback_service: str, reason: str,
                                performance_impact: str, task_id: str = None,
                                **kwargs) -> FallbackActivatedMessage:
        """创建降级处理激活消息"""
        fallback_data = FallbackData(
            fallback_type=fallback_type,
            original_service=original_service,
            fallback_service=fallback_service,
            reason=reason,
            performance_impact=performance_impact,
            **kwargs
        )
        return FallbackActivatedMessage(task_id=task_id, data=fallback_data)


def parse_message(message_str: str) -> Optional[Dict[str, Any]]:
    """
    解析WebSocket消息字符串
    :param message_str: 消息字符串
    :return: 解析后的消息字典，解析失败返回None
    """
    try:
        return json.loads(message_str)
    except json.JSONDecodeError:
        return None

def validate_message(message_dict: Dict[str, Any]) -> bool:
    """
    验证消息格式
    :param message_dict: 消息字典
    :return: 是否有效
    """
    if not isinstance(message_dict, dict):
        return False
    
    # 检查必需字段
    if 'type' not in message_dict:
        return False
    
    message_type = message_dict['type']
    
    # 验证消息类型
    valid_types = [t.value for t in MessageType]
    if message_type not in valid_types:
        return False
    
    # 根据消息类型验证特定字段
    if message_type in [MessageType.SUBSCRIBE_TASK.value, MessageType.UNSUBSCRIBE_TASK.value]:
        return 'task_id' in message_dict
    
    if message_type in [MessageType.PAUSE_TASK.value, MessageType.RESUME_TASK.value, 
                       MessageType.CANCEL_TASK.value]:
        return 'task_id' in message_dict
    
    return True

# 消息类型到类的映射，用于反序列化
MESSAGE_TYPE_MAP = {
    MessageType.TASK_STARTED.value: TaskStartedMessage,
    MessageType.TASK_PROGRESS.value: TaskProgressMessage,
    MessageType.PAGE_COMPLETED.value: PageCompletedMessage,
    MessageType.TASK_COMPLETED.value: TaskCompletedMessage,
    MessageType.TASK_FAILED.value: TaskFailedMessage,
    MessageType.TASK_CANCELLED.value: TaskCancelledMessage,
    MessageType.ERROR_NOTIFICATION.value: ErrorNotificationMessage,
    MessageType.SYSTEM_STATUS.value: SystemStatusMessage,
    MessageType.ERROR.value: ErrorMessage,
    MessageType.CONNECTION_ESTABLISHED.value: ConnectionMessage,
    # 分页相关消息类型
    MessageType.BATCH_STARTED.value: BatchStartedMessage,
    MessageType.BATCH_COMPLETED.value: BatchCompletedMessage,
    MessageType.BATCH_FAILED.value: BatchFailedMessage,
    MessageType.PAGE_RETRY.value: PageRetryMessage,
    MessageType.DETAILED_PROGRESS.value: DetailedProgressMessage,
    MessageType.PROCESSING_RATE_UPDATE.value: ProcessingRateMessage,
    MessageType.TIME_ESTIMATION_UPDATE.value: TimeEstimationMessage,
    MessageType.PAGINATION_STRATEGY_CHANGED.value: PaginationStrategyMessage,
    # 超时预警相关消息类型
    MessageType.TIMEOUT_WARNING.value: TimeoutWarningMessage,
    MessageType.TIMEOUT_EXCEEDED.value: TimeoutExceededMessage,
    MessageType.TIMEOUT_RECOVERY.value: TimeoutRecoveryMessage,
    # 错误恢复相关消息类型
    MessageType.ERROR_RECOVERY_STARTED.value: ErrorRecoveryStartedMessage,
    MessageType.ERROR_RECOVERY_PROGRESS.value: ErrorRecoveryProgressMessage,
    MessageType.ERROR_RECOVERY_COMPLETED.value: ErrorRecoveryCompletedMessage,
    MessageType.ERROR_RECOVERY_FAILED.value: ErrorRecoveryFailedMessage,
    # 系统监控相关消息类型
    MessageType.SYSTEM_MONITORING_UPDATE.value: SystemMonitoringUpdateMessage,
    MessageType.SYSTEM_ALERT.value: SystemAlertMessage,
    MessageType.SYSTEM_ALERT_RESOLVED.value: SystemAlertResolvedMessage,
    MessageType.PERFORMANCE_WARNING.value: PerformanceWarningMessage,
    MessageType.RESOURCE_WARNING.value: ResourceWarningMessage,
    # 检查点和恢复相关消息类型
    MessageType.CHECKPOINT_CREATED.value: CheckpointCreatedMessage,
    MessageType.CHECKPOINT_RESTORED.value: CheckpointRestoredMessage,
    MessageType.TASK_RECOVERED.value: TaskRecoveredMessage,
    MessageType.RECOVERY_JOB_UPDATE.value: RecoveryJobUpdateMessage,
    # 降级处理相关消息类型
    MessageType.SYSTEM_DEGRADED.value: SystemDegradedMessage,
    MessageType.SYSTEM_RECOVERED.value: SystemRecoveredMessage,
    MessageType.FALLBACK_ACTIVATED.value: FallbackActivatedMessage,
}

def deserialize_message(message_dict: Dict[str, Any]) -> Optional[BaseMessage]:
    """
    反序列化消息字典为消息对象
    :param message_dict: 消息字典
    :return: 消息对象，失败返回None
    """
    if not validate_message(message_dict):
        return None
    
    message_type = message_dict['type']
    message_class = MESSAGE_TYPE_MAP.get(message_type)
    
    if message_class:
        try:
            return message_class(**message_dict)
        except Exception:
            return None
    
    # 对于未映射的消息类型，返回基础消息
    return BaseMessage(**message_dict)