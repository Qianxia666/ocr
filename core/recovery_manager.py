"""
恢复管理器模块
实现任务检查点、断点续传和自动恢复机制
"""

import asyncio
import json
import logging
import os
import pickle
import shutil
import sqlite3
import time
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Any, Callable, Set, Tuple
from dataclasses import dataclass, asdict
from pathlib import Path
from enum import Enum
import hashlib

logger = logging.getLogger(__name__)

class RecoveryState(Enum):
    """恢复状态"""
    NONE = "none"
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"

class CheckpointType(Enum):
    """检查点类型"""
    TASK_START = "task_start"
    PROGRESS_UPDATE = "progress_update"
    PARTIAL_RESULT = "partial_result"
    ERROR_OCCURRED = "error_occurred"
    TASK_COMPLETE = "task_complete"
    SYSTEM_SHUTDOWN = "system_shutdown"

@dataclass
class TaskCheckpoint:
    """任务检查点"""
    task_id: str
    checkpoint_id: str
    checkpoint_type: CheckpointType
    timestamp: datetime
    progress: float
    state_data: Dict[str, Any]
    metadata: Dict[str, Any]
    file_paths: List[str]
    hash_value: str
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return {
            'task_id': self.task_id,
            'checkpoint_id': self.checkpoint_id,
            'checkpoint_type': self.checkpoint_type.value,
            'timestamp': self.timestamp.isoformat(),
            'progress': self.progress,
            'state_data': self.state_data,
            'metadata': self.metadata,
            'file_paths': self.file_paths,
            'hash_value': self.hash_value
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'TaskCheckpoint':
        """从字典创建"""
        return cls(
            task_id=data['task_id'],
            checkpoint_id=data['checkpoint_id'],
            checkpoint_type=CheckpointType(data['checkpoint_type']),
            timestamp=datetime.fromisoformat(data['timestamp']),
            progress=data['progress'],
            state_data=data['state_data'],
            metadata=data['metadata'],
            file_paths=data['file_paths'],
            hash_value=data['hash_value']
        )

@dataclass
class RecoveryJob:
    """恢复任务"""
    task_id: str
    recovery_state: RecoveryState
    last_checkpoint: Optional[TaskCheckpoint]
    recovery_attempts: int
    max_recovery_attempts: int
    recovery_strategy: str
    created_at: datetime
    updated_at: datetime
    error_messages: List[str]
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return {
            'task_id': self.task_id,
            'recovery_state': self.recovery_state.value,
            'last_checkpoint': self.last_checkpoint.to_dict() if self.last_checkpoint else None,
            'recovery_attempts': self.recovery_attempts,
            'max_recovery_attempts': self.max_recovery_attempts,
            'recovery_strategy': self.recovery_strategy,
            'created_at': self.created_at.isoformat(),
            'updated_at': self.updated_at.isoformat(),
            'error_messages': self.error_messages
        }

class RecoveryManager:
    """恢复管理器"""
    
    def __init__(self,
                 checkpoint_dir: str = None,
                 recovery_db_path: str = None,
                 websocket_manager=None,
                 error_handler=None):
        import os
        # 优先使用环境变量配置路径
        if checkpoint_dir is None:
            checkpoint_dir = os.getenv("CHECKPOINT_DIR", "data/checkpoints")
        if recovery_db_path is None:
            recovery_db_path = os.getenv("RECOVERY_DB_PATH", "data/recovery.db")

        self.checkpoint_dir = Path(checkpoint_dir)
        self.recovery_db_path = recovery_db_path
        self.websocket_manager = websocket_manager
        self.error_handler = error_handler

        # 确保检查点目录存在
        self.checkpoint_dir.mkdir(parents=True, exist_ok=True)
        # 确保恢复数据库目录存在
        recovery_db_dir = os.path.dirname(recovery_db_path)
        if recovery_db_dir and not os.path.exists(recovery_db_dir):
            os.makedirs(recovery_db_dir, exist_ok=True)
        
        # 内存状态
        self._active_checkpoints: Dict[str, TaskCheckpoint] = {}
        self._recovery_jobs: Dict[str, RecoveryJob] = {}
        self._recovery_callbacks: Dict[str, Callable] = {}
        self._cleanup_tasks: Set[str] = set()
        
        # 配置
        self._max_checkpoints_per_task = 10
        self._checkpoint_retention_hours = 72
        self._auto_cleanup_interval = 3600  # 1小时
        self._recovery_timeout = 300  # 5分钟
        
        # 统计信息
        self._stats = {
            'checkpoints_created': 0,
            'checkpoints_restored': 0,
            'recovery_jobs_created': 0,
            'recovery_jobs_completed': 0,
            'recovery_jobs_failed': 0,
            'files_recovered': 0,
            'data_integrity_checks': 0,
            'cleanup_operations': 0
        }
        
        # 内部状态
        self._running = False
        self._cleanup_task = None
        
        # 初始化数据库
        self._init_database()
    
    async def initialize(self):
        """初始化恢复管理器的异步组件"""
        logger.info("开始初始化恢复管理器异步组件...")
        # 这里可以添加异步初始化逻辑
        logger.info("恢复管理器异步组件初始化完成")
    
    def _init_database(self):
        """初始化恢复数据库"""
        try:
            with sqlite3.connect(self.recovery_db_path) as conn:
                cursor = conn.cursor()
                
                # 创建检查点表
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS checkpoints (
                        task_id TEXT,
                        checkpoint_id TEXT PRIMARY KEY,
                        checkpoint_type TEXT,
                        timestamp TEXT,
                        progress REAL,
                        state_data TEXT,
                        metadata TEXT,
                        file_paths TEXT,
                        hash_value TEXT,
                        created_at TEXT DEFAULT CURRENT_TIMESTAMP
                    )
                ''')
                
                # 创建恢复任务表
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS recovery_jobs (
                        task_id TEXT PRIMARY KEY,
                        recovery_state TEXT,
                        last_checkpoint_id TEXT,
                        recovery_attempts INTEGER,
                        max_recovery_attempts INTEGER,
                        recovery_strategy TEXT,
                        created_at TEXT,
                        updated_at TEXT,
                        error_messages TEXT
                    )
                ''')
                
                # 创建索引
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_checkpoints_task_id ON checkpoints(task_id)')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_checkpoints_timestamp ON checkpoints(timestamp)')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_recovery_jobs_state ON recovery_jobs(recovery_state)')
                
                conn.commit()
                
        except Exception as e:
            logger.error(f"初始化恢复数据库失败: {e}")
            raise
    
    async def start(self):
        """启动恢复管理器"""
        if self._running:
            return
        
        self._running = True
        
        try:
            # 加载现有的恢复任务
            await self._load_recovery_jobs()
            
            # 启动自动清理任务
            self._cleanup_task = asyncio.create_task(self._auto_cleanup_loop())
            
            logger.info("恢复管理器启动完成")
            
        except Exception as e:
            logger.error(f"启动恢复管理器失败: {e}")
            self._running = False
            raise
    
    async def stop(self):
        """停止恢复管理器"""
        if not self._running:
            return
        
        self._running = False
        
        try:
            # 创建系统关闭检查点
            await self._create_shutdown_checkpoints()
            
            # 停止清理任务
            if self._cleanup_task:
                self._cleanup_task.cancel()
                try:
                    await self._cleanup_task
                except asyncio.CancelledError:
                    pass
            
            # 保存恢复任务状态
            await self._save_recovery_jobs()
            
            logger.info("恢复管理器已停止")
            
        except Exception as e:
            logger.error(f"停止恢复管理器失败: {e}")
    
    async def create_checkpoint(self, 
                              task_id: str,
                              checkpoint_type: CheckpointType,
                              progress: float,
                              state_data: Dict[str, Any],
                              metadata: Optional[Dict[str, Any]] = None,
                              file_paths: Optional[List[str]] = None) -> str:
        """创建任务检查点"""
        try:
            # 生成检查点ID
            checkpoint_id = f"{task_id}_{int(time.time() * 1000)}_{checkpoint_type.value}"
            
            # 准备数据
            if metadata is None:
                metadata = {}
            if file_paths is None:
                file_paths = []
            
            # 计算状态数据哈希
            state_json = json.dumps(state_data, sort_keys=True)
            hash_value = hashlib.sha256(state_json.encode()).hexdigest()
            
            # 创建检查点对象
            checkpoint = TaskCheckpoint(
                task_id=task_id,
                checkpoint_id=checkpoint_id,
                checkpoint_type=checkpoint_type,
                timestamp=datetime.now(timezone.utc),
                progress=progress,
                state_data=state_data,
                metadata=metadata,
                file_paths=file_paths,
                hash_value=hash_value
            )
            
            # 保存检查点到文件
            await self._save_checkpoint_to_file(checkpoint)
            
            # 保存检查点到数据库
            await self._save_checkpoint_to_db(checkpoint)
            
            # 更新内存缓存
            self._active_checkpoints[checkpoint_id] = checkpoint
            
            # 清理旧检查点
            await self._cleanup_old_checkpoints(task_id)
            
            # 更新统计
            self._stats['checkpoints_created'] += 1
            
            # 发送通知
            await self._send_checkpoint_notification(checkpoint)
            
            logger.info(f"创建检查点成功: {checkpoint_id}")
            return checkpoint_id
            
        except Exception as e:
            logger.error(f"创建检查点失败: {e}")
            if self.error_handler:
                await self.error_handler.handle_error(e, {
                    'operation': 'create_checkpoint',
                    'task_id': task_id,
                    'checkpoint_type': checkpoint_type.value
                })
            raise
    
    async def restore_from_checkpoint(self, 
                                    task_id: str,
                                    checkpoint_id: Optional[str] = None) -> Optional[TaskCheckpoint]:
        """从检查点恢复任务"""
        try:
            # 查找检查点
            if checkpoint_id:
                checkpoint = await self._load_checkpoint(checkpoint_id)
            else:
                checkpoint = await self._find_latest_checkpoint(task_id)
            
            if not checkpoint:
                logger.warning(f"未找到任务 {task_id} 的检查点")
                return None
            
            # 验证检查点完整性
            if not await self._verify_checkpoint_integrity(checkpoint):
                logger.error(f"检查点 {checkpoint.checkpoint_id} 完整性验证失败")
                return None
            
            # 恢复文件状态
            await self._restore_checkpoint_files(checkpoint)
            
            # 更新统计
            self._stats['checkpoints_restored'] += 1
            
            # 发送恢复通知
            await self._send_recovery_notification(checkpoint)
            
            logger.info(f"从检查点恢复任务成功: {checkpoint.checkpoint_id}")
            return checkpoint
            
        except Exception as e:
            logger.error(f"从检查点恢复任务失败: {e}")
            if self.error_handler:
                await self.error_handler.handle_error(e, {
                    'operation': 'restore_from_checkpoint',
                    'task_id': task_id,
                    'checkpoint_id': checkpoint_id
                })
            return None
    
    async def create_recovery_job(self, 
                                task_id: str,
                                recovery_strategy: str = "auto",
                                max_attempts: int = 3) -> bool:
        """创建恢复任务"""
        try:
            # 查找最新检查点
            latest_checkpoint = await self._find_latest_checkpoint(task_id)
            
            # 创建恢复任务
            recovery_job = RecoveryJob(
                task_id=task_id,
                recovery_state=RecoveryState.PENDING,
                last_checkpoint=latest_checkpoint,
                recovery_attempts=0,
                max_recovery_attempts=max_attempts,
                recovery_strategy=recovery_strategy,
                created_at=datetime.now(timezone.utc),
                updated_at=datetime.now(timezone.utc),
                error_messages=[]
            )
            
            # 保存恢复任务
            self._recovery_jobs[task_id] = recovery_job
            await self._save_recovery_job_to_db(recovery_job)
            
            # 更新统计
            self._stats['recovery_jobs_created'] += 1
            
            # 发送恢复任务通知
            await self._send_recovery_job_notification(recovery_job)
            
            logger.info(f"创建恢复任务成功: {task_id}")
            return True
            
        except Exception as e:
            logger.error(f"创建恢复任务失败: {e}")
            return False
    
    async def execute_recovery(self, task_id: str) -> bool:
        """执行任务恢复"""
        try:
            recovery_job = self._recovery_jobs.get(task_id)
            if not recovery_job:
                logger.warning(f"未找到任务 {task_id} 的恢复任务")
                return False
            
            # 检查恢复尝试次数
            if recovery_job.recovery_attempts >= recovery_job.max_recovery_attempts:
                recovery_job.recovery_state = RecoveryState.FAILED
                recovery_job.error_messages.append("超过最大恢复尝试次数")
                await self._update_recovery_job(recovery_job)
                return False
            
            # 更新恢复状态
            recovery_job.recovery_state = RecoveryState.IN_PROGRESS
            recovery_job.recovery_attempts += 1
            recovery_job.updated_at = datetime.now(timezone.utc)
            await self._update_recovery_job(recovery_job)
            
            # 执行恢复策略
            success = await self._execute_recovery_strategy(recovery_job)
            
            if success:
                recovery_job.recovery_state = RecoveryState.COMPLETED
                self._stats['recovery_jobs_completed'] += 1
                logger.info(f"任务恢复成功: {task_id}")
            else:
                recovery_job.recovery_state = RecoveryState.FAILED
                recovery_job.error_messages.append("恢复策略执行失败")
                self._stats['recovery_jobs_failed'] += 1
                logger.error(f"任务恢复失败: {task_id}")
            
            recovery_job.updated_at = datetime.now(timezone.utc)
            await self._update_recovery_job(recovery_job)
            
            return success
            
        except Exception as e:
            logger.error(f"执行任务恢复失败: {e}")
            return False
    
    async def _execute_recovery_strategy(self, recovery_job: RecoveryJob) -> bool:
        """执行恢复策略"""
        try:
            strategy = recovery_job.recovery_strategy
            
            if strategy == "auto":
                return await self._auto_recovery_strategy(recovery_job)
            elif strategy == "checkpoint_restore":
                return await self._checkpoint_restore_strategy(recovery_job)
            elif strategy == "partial_recovery":
                return await self._partial_recovery_strategy(recovery_job)
            elif strategy == "full_restart":
                return await self._full_restart_strategy(recovery_job)
            else:
                logger.error(f"未知的恢复策略: {strategy}")
                return False
                
        except Exception as e:
            logger.error(f"执行恢复策略失败: {e}")
            return False
    
    async def _auto_recovery_strategy(self, recovery_job: RecoveryJob) -> bool:
        """自动恢复策略"""
        try:
            # 优先尝试从检查点恢复
            if recovery_job.last_checkpoint:
                if await self._checkpoint_restore_strategy(recovery_job):
                    return True
            
            # 如果检查点恢复失败，尝试部分恢复
            if await self._partial_recovery_strategy(recovery_job):
                return True
            
            # 最后尝试完全重启
            return await self._full_restart_strategy(recovery_job)
            
        except Exception as e:
            logger.error(f"自动恢复策略失败: {e}")
            return False
    
    async def _checkpoint_restore_strategy(self, recovery_job: RecoveryJob) -> bool:
        """检查点恢复策略"""
        try:
            if not recovery_job.last_checkpoint:
                return False
            
            # 从检查点恢复
            checkpoint = await self.restore_from_checkpoint(
                recovery_job.task_id,
                recovery_job.last_checkpoint.checkpoint_id
            )
            
            if checkpoint:
                # 调用恢复回调
                if recovery_job.task_id in self._recovery_callbacks:
                    callback = self._recovery_callbacks[recovery_job.task_id]
                    return await callback(checkpoint)
                return True
            
            return False
            
        except Exception as e:
            logger.error(f"检查点恢复策略失败: {e}")
            return False
    
    async def _partial_recovery_strategy(self, recovery_job: RecoveryJob) -> bool:
        """部分恢复策略"""
        try:
            # 查找所有相关检查点
            checkpoints = await self._find_task_checkpoints(recovery_job.task_id)
            
            if not checkpoints:
                return False
            
            # 尝试从最近的有效检查点恢复
            for checkpoint in reversed(checkpoints):
                if await self._verify_checkpoint_integrity(checkpoint):
                    restored_checkpoint = await self.restore_from_checkpoint(
                        recovery_job.task_id,
                        checkpoint.checkpoint_id
                    )
                    
                    if restored_checkpoint:
                        # 调用恢复回调
                        if recovery_job.task_id in self._recovery_callbacks:
                            callback = self._recovery_callbacks[recovery_job.task_id]
                            if await callback(restored_checkpoint):
                                return True
                        else:
                            return True
            
            return False
            
        except Exception as e:
            logger.error(f"部分恢复策略失败: {e}")
            return False
    
    async def _full_restart_strategy(self, recovery_job: RecoveryJob) -> bool:
        """完全重启策略"""
        try:
            # 清理任务相关的检查点和文件
            await self._cleanup_task_data(recovery_job.task_id)
            
            # 调用重启回调
            if recovery_job.task_id in self._recovery_callbacks:
                callback = self._recovery_callbacks[recovery_job.task_id]
                return await callback(None)  # None表示完全重启
            
            return True
            
        except Exception as e:
            logger.error(f"完全重启策略失败: {e}")
            return False
    
    async def _save_checkpoint_to_file(self, checkpoint: TaskCheckpoint):
        """保存检查点到文件"""
        try:
            # 创建任务目录
            task_dir = self.checkpoint_dir / checkpoint.task_id
            task_dir.mkdir(parents=True, exist_ok=True)
            
            # 保存检查点数据
            checkpoint_file = task_dir / f"{checkpoint.checkpoint_id}.json"
            with open(checkpoint_file, 'w', encoding='utf-8') as f:
                json.dump(checkpoint.to_dict(), f, indent=2, ensure_ascii=False)
            
            # 如果有关联文件，复制到检查点目录
            if checkpoint.file_paths:
                files_dir = task_dir / f"{checkpoint.checkpoint_id}_files"
                files_dir.mkdir(parents=True, exist_ok=True)
                
                for file_path in checkpoint.file_paths:
                    if os.path.exists(file_path):
                        dest_path = files_dir / os.path.basename(file_path)
                        shutil.copy2(file_path, dest_path)
            
        except Exception as e:
            logger.error(f"保存检查点到文件失败: {e}")
            raise
    
    async def _save_checkpoint_to_db(self, checkpoint: TaskCheckpoint):
        """保存检查点到数据库"""
        try:
            with sqlite3.connect(self.recovery_db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    INSERT OR REPLACE INTO checkpoints 
                    (task_id, checkpoint_id, checkpoint_type, timestamp, progress, 
                     state_data, metadata, file_paths, hash_value)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    checkpoint.task_id,
                    checkpoint.checkpoint_id,
                    checkpoint.checkpoint_type.value,
                    checkpoint.timestamp.isoformat(),
                    checkpoint.progress,
                    json.dumps(checkpoint.state_data),
                    json.dumps(checkpoint.metadata),
                    json.dumps(checkpoint.file_paths),
                    checkpoint.hash_value
                ))
                conn.commit()
                
        except Exception as e:
            logger.error(f"保存检查点到数据库失败: {e}")
            raise
    
    async def _load_checkpoint(self, checkpoint_id: str) -> Optional[TaskCheckpoint]:
        """加载检查点"""
        try:
            # 先从内存缓存查找
            if checkpoint_id in self._active_checkpoints:
                return self._active_checkpoints[checkpoint_id]
            
            # 从数据库查找
            with sqlite3.connect(self.recovery_db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    SELECT * FROM checkpoints WHERE checkpoint_id = ?
                ''', (checkpoint_id,))
                
                row = cursor.fetchone()
                if row:
                    checkpoint_data = {
                        'task_id': row[0],
                        'checkpoint_id': row[1],
                        'checkpoint_type': row[2],
                        'timestamp': row[3],
                        'progress': row[4],
                        'state_data': json.loads(row[5]),
                        'metadata': json.loads(row[6]),
                        'file_paths': json.loads(row[7]),
                        'hash_value': row[8]
                    }
                    
                    checkpoint = TaskCheckpoint.from_dict(checkpoint_data)
                    self._active_checkpoints[checkpoint_id] = checkpoint
                    return checkpoint
            
            return None
            
        except Exception as e:
            logger.error(f"加载检查点失败: {e}")
            return None
    
    async def _find_latest_checkpoint(self, task_id: str) -> Optional[TaskCheckpoint]:
        """查找最新检查点"""
        try:
            with sqlite3.connect(self.recovery_db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    SELECT * FROM checkpoints 
                    WHERE task_id = ? 
                    ORDER BY timestamp DESC 
                    LIMIT 1
                ''', (task_id,))
                
                row = cursor.fetchone()
                if row:
                    checkpoint_data = {
                        'task_id': row[0],
                        'checkpoint_id': row[1],
                        'checkpoint_type': row[2],
                        'timestamp': row[3],
                        'progress': row[4],
                        'state_data': json.loads(row[5]),
                        'metadata': json.loads(row[6]),
                        'file_paths': json.loads(row[7]),
                        'hash_value': row[8]
                    }
                    
                    return TaskCheckpoint.from_dict(checkpoint_data)
            
            return None
            
        except Exception as e:
            logger.error(f"查找最新检查点失败: {e}")
            return None
    
    async def _find_task_checkpoints(self, task_id: str) -> List[TaskCheckpoint]:
        """查找任务的所有检查点"""
        try:
            checkpoints = []
            
            with sqlite3.connect(self.recovery_db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    SELECT * FROM checkpoints 
                    WHERE task_id = ? 
                    ORDER BY timestamp ASC
                ''', (task_id,))
                
                for row in cursor.fetchall():
                    checkpoint_data = {
                        'task_id': row[0],
                        'checkpoint_id': row[1],
                        'checkpoint_type': row[2],
                        'timestamp': row[3],
                        'progress': row[4],
                        'state_data': json.loads(row[5]),
                        'metadata': json.loads(row[6]),
                        'file_paths': json.loads(row[7]),
                        'hash_value': row[8]
                    }
                    
                    checkpoints.append(TaskCheckpoint.from_dict(checkpoint_data))
            
            return checkpoints
            
        except Exception as e:
            logger.error(f"查找任务检查点失败: {e}")
            return []
    
    async def _verify_checkpoint_integrity(self, checkpoint: TaskCheckpoint) -> bool:
        """验证检查点完整性"""
        try:
            # 验证状态数据哈希
            state_json = json.dumps(checkpoint.state_data, sort_keys=True)
            calculated_hash = hashlib.sha256(state_json.encode()).hexdigest()
            
            if calculated_hash != checkpoint.hash_value:
                logger.error(f"检查点 {checkpoint.checkpoint_id} 数据哈希验证失败")
                return False
            
            # 验证关联文件
            if checkpoint.file_paths:
                task_dir = self.checkpoint_dir / checkpoint.task_id
                files_dir = task_dir / f"{checkpoint.checkpoint_id}_files"
                
                if not files_dir.exists():
                    logger.error(f"检查点 {checkpoint.checkpoint_id} 关联文件目录不存在")
                    return False
                
                for file_path in checkpoint.file_paths:
                    file_name = os.path.basename(file_path)
                    backup_file = files_dir / file_name
                    
                    if not backup_file.exists():
                        logger.error(f"检查点 {checkpoint.checkpoint_id} 关联文件 {file_name} 不存在")
                        return False
            
            self._stats['data_integrity_checks'] += 1
            return True
            
        except Exception as e:
            logger.error(f"验证检查点完整性失败: {e}")
            return False
    
    async def _restore_checkpoint_files(self, checkpoint: TaskCheckpoint):
        """恢复检查点关联文件"""
        try:
            if not checkpoint.file_paths:
                return
            
            task_dir = self.checkpoint_dir / checkpoint.task_id
            files_dir = task_dir / f"{checkpoint.checkpoint_id}_files"
            
            if not files_dir.exists():
                logger.warning(f"检查点 {checkpoint.checkpoint_id} 文件目录不存在")
                return
            
            for file_path in checkpoint.file_paths:
                file_name = os.path.basename(file_path)
                backup_file = files_dir / file_name
                
                if backup_file.exists():
                    # 确保目标目录存在
                    os.makedirs(os.path.dirname(file_path), exist_ok=True)
                    
                    # 恢复文件
                    shutil.copy2(backup_file, file_path)
                    self._stats['files_recovered'] += 1
                    logger.info(f"恢复文件: {file_path}")
                else:
                    logger.warning(f"备份文件不存在: {backup_file}")
            
        except Exception as e:
            logger.error(f"恢复检查点文件失败: {e}")
            raise
    
    async def _cleanup_old_checkpoints(self, task_id: str):
        """清理旧检查点"""
        try:
            # 获取任务的所有检查点
            checkpoints = await self._find_task_checkpoints(task_id)
            
            # 如果检查点数量超过限制，删除最旧的
            if len(checkpoints) > self._max_checkpoints_per_task:
                excess_count = len(checkpoints) - self._max_checkpoints_per_task
                old_checkpoints = checkpoints[:excess_count]
                
                for checkpoint in old_checkpoints:
                    await self._delete_checkpoint(checkpoint.checkpoint_id)
            
            # 删除过期的检查点
            cutoff_time = datetime.now(timezone.utc) - timedelta(hours=self._checkpoint_retention_hours)
            
            for checkpoint in checkpoints:
                if checkpoint.timestamp < cutoff_time:
                    await self._delete_checkpoint(checkpoint.checkpoint_id)
            
        except Exception as e:
            logger.error(f"清理旧检查点失败: {e}")
    
    async def _delete_checkpoint(self, checkpoint_id: str):
        """删除检查点"""
        try:
            # 从数据库删除
            with sqlite3.connect(self.recovery_db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('DELETE FROM checkpoints WHERE checkpoint_id = ?', (checkpoint_id,))
                conn.commit()
            
            # 从内存缓存删除
            if checkpoint_id in self._active_checkpoints:
                checkpoint = self._active_checkpoints[checkpoint_id]
                del self._active_checkpoints[checkpoint_id]
                
                # 删除文件
                task_dir = self.checkpoint_dir / checkpoint.task_id
                checkpoint_file = task_dir / f"{checkpoint_id}.json"
                files_dir = task_dir / f"{checkpoint_id}_files"
                
                if checkpoint_file.exists():
                    checkpoint_file.unlink()
                
                if files_dir.exists():
                    shutil.rmtree(files_dir)
            
            logger.info(f"删除检查点: {checkpoint_id}")
            
        except Exception as e:
            logger.error(f"删除检查点失败: {e}")
    
    async def _cleanup_task_data(self, task_id: str):
        """清理任务相关数据"""
        try:
            # 删除所有检查点
            checkpoints = await self._find_task_checkpoints(task_id)
            for checkpoint in checkpoints:
                await self._delete_checkpoint(checkpoint.checkpoint_id)
            
            # 删除任务目录
            task_dir = self.checkpoint_dir / task_id
            if task_dir.exists():
                shutil.rmtree(task_dir)
            
            # 删除恢复任务
            if task_id in self._recovery_jobs:
                del self._recovery_jobs[task_id]
            
            with sqlite3.connect(self.recovery_db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('DELETE FROM recovery_jobs WHERE task_id = ?', (task_id,))
                conn.commit()
            
            logger.info(f"清理任务数据: {task_id}")
            
        except Exception as e:
            logger.error(f"清理任务数据失败: {e}")
    
    async def _save_recovery_job_to_db(self, recovery_job: RecoveryJob):
        """保存恢复任务到数据库"""
        try:
            with sqlite3.connect(self.recovery_db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    INSERT OR REPLACE INTO recovery_jobs 
                    (task_id, recovery_state, last_checkpoint_id, recovery_attempts,
                     max_recovery_attempts, recovery_strategy, created_at, updated_at, error_messages)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    recovery_job.task_id,
                    recovery_job.recovery_state.value,
                    recovery_job.last_checkpoint.checkpoint_id if recovery_job.last_checkpoint else None,
                    recovery_job.recovery_attempts,
                    recovery_job.max_recovery_attempts,
                    recovery_job.recovery_strategy,
                    recovery_job.created_at.isoformat(),
                    recovery_job.updated_at.isoformat(),
                    json.dumps(recovery_job.error_messages)
                ))
                conn.commit()
                
        except Exception as e:
            logger.error(f"保存恢复任务到数据库失败: {e}")
            raise
    
    async def _update_recovery_job(self, recovery_job: RecoveryJob):
        """更新恢复任务"""
        recovery_job.updated_at = datetime.now(timezone.utc)
        await self._save_recovery_job_to_db(recovery_job)
        await self._send_recovery_job_notification(recovery_job)
    
    async def _load_recovery_jobs(self):
        """加载恢复任务"""
        try:
            with sqlite3.connect(self.recovery_db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('SELECT * FROM recovery_jobs')
                
                for row in cursor.fetchall():
                    task_id = row[0]
                    
                    # 加载最后的检查点
                    last_checkpoint = None
                    if row[2]:  # last_checkpoint_id
                        last_checkpoint = await self._load_checkpoint(row[2])
                    
                    recovery_job = RecoveryJob(
                        task_id=task_id,
                        recovery_state=RecoveryState(row[1]),
                        last_checkpoint=last_checkpoint,
                        recovery_attempts=row[3],
                        max_recovery_attempts=row[4],
                        recovery_strategy=row[5],
                        created_at=datetime.fromisoformat(row[6]),
                        updated_at=datetime.fromisoformat(row[7]),
                        error_messages=json.loads(row[8]) if row[8] else []
                    )
                    
                    self._recovery_jobs[task_id] = recovery_job
            
            logger.info(f"加载了 {len(self._recovery_jobs)} 个恢复任务")
            
        except Exception as e:
            logger.error(f"加载恢复任务失败: {e}")
    
    async def _save_recovery_jobs(self):
        """保存所有恢复任务"""
        try:
            for recovery_job in self._recovery_jobs.values():
                await self._save_recovery_job_to_db(recovery_job)
            
        except Exception as e:
            logger.error(f"保存恢复任务失败: {e}")
    
    async def _create_shutdown_checkpoints(self):
        """创建系统关闭检查点"""
        try:
            # 为所有活跃的任务创建关闭检查点
            for task_id in list(self._active_checkpoints.keys()):
                await self.create_checkpoint(
                    task_id=task_id,
                    checkpoint_type=CheckpointType.SYSTEM_SHUTDOWN,
                    progress=0.0,
                    state_data={'shutdown_time': datetime.now(timezone.utc).isoformat()},
                    metadata={'reason': 'system_shutdown'}
                )
            
        except Exception as e:
            logger.error(f"创建系统关闭检查点失败: {e}")
    
    async def _auto_cleanup_loop(self):
        """自动清理循环"""
        while self._running:
            try:
                await asyncio.sleep(self._auto_cleanup_interval)
                
                if not self._running:
                    break
                
                # 执行清理操作
                await self._perform_cleanup()
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"自动清理失败: {e}")
    
    async def _perform_cleanup(self):
        """执行清理操作"""
        try:
            # 清理过期检查点
            cutoff_time = datetime.now(timezone.utc) - timedelta(hours=self._checkpoint_retention_hours)
            
            with sqlite3.connect(self.recovery_db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    SELECT checkpoint_id FROM checkpoints 
                    WHERE timestamp < ?
                ''', (cutoff_time.isoformat(),))
                
                expired_checkpoints = cursor.fetchall()
                
                for (checkpoint_id,) in expired_checkpoints:
                    await self._delete_checkpoint(checkpoint_id)
            
            # 清理完成的恢复任务
            completed_jobs = [
                job for job in self._recovery_jobs.values()
                if job.recovery_state in [RecoveryState.COMPLETED, RecoveryState.FAILED]
                and job.updated_at < cutoff_time
            ]
            
            for job in completed_jobs:
                del self._recovery_jobs[job.task_id]
                
                with sqlite3.connect(self.recovery_db_path) as conn:
                    cursor = conn.cursor()
                    cursor.execute('DELETE FROM recovery_jobs WHERE task_id = ?', (job.task_id,))
                    conn.commit()
            
            self._stats['cleanup_operations'] += 1
            logger.info(f"清理操作完成，删除了 {len(expired_checkpoints)} 个过期检查点和 {len(completed_jobs)} 个完成的恢复任务")
            
        except Exception as e:
            logger.error(f"执行清理操作失败: {e}")
    
    async def _send_checkpoint_notification(self, checkpoint: TaskCheckpoint):
        """发送检查点通知"""
        try:
            if not self.websocket_manager:
                return
            
            message = {
                'type': 'checkpoint_created',
                'checkpoint': checkpoint.to_dict(),
                'timestamp': datetime.now(timezone.utc).isoformat()
            }
            
            await self.websocket_manager.broadcast(message)
            
        except Exception as e:
            logger.error(f"发送检查点通知失败: {e}")
    
    async def _send_recovery_notification(self, checkpoint: TaskCheckpoint):
        """发送恢复通知"""
        try:
            if not self.websocket_manager:
                return
            
            message = {
                'type': 'task_recovered',
                'checkpoint': checkpoint.to_dict(),
                'timestamp': datetime.now(timezone.utc).isoformat()
            }
            
            await self.websocket_manager.broadcast(message)
            
        except Exception as e:
            logger.error(f"发送恢复通知失败: {e}")
    
    async def _send_recovery_job_notification(self, recovery_job: RecoveryJob):
        """发送恢复任务通知"""
        try:
            if not self.websocket_manager:
                return
            
            message = {
                'type': 'recovery_job_update',
                'recovery_job': recovery_job.to_dict(),
                'timestamp': datetime.now(timezone.utc).isoformat()
            }
            
            await self.websocket_manager.broadcast(message)
            
        except Exception as e:
            logger.error(f"发送恢复任务通知失败: {e}")
    
    def register_recovery_callback(self, task_id: str, callback: Callable):
        """注册恢复回调"""
        self._recovery_callbacks[task_id] = callback
        logger.info(f"注册恢复回调: {task_id}")
    
    def unregister_recovery_callback(self, task_id: str):
        """注销恢复回调"""
        if task_id in self._recovery_callbacks:
            del self._recovery_callbacks[task_id]
            logger.info(f"注销恢复回调: {task_id}")
    
    def get_task_checkpoints(self, task_id: str) -> List[Dict[str, Any]]:
        """获取任务检查点列表"""
        try:
            checkpoints = []
            
            with sqlite3.connect(self.recovery_db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    SELECT checkpoint_id, checkpoint_type, timestamp, progress 
                    FROM checkpoints 
                    WHERE task_id = ? 
                    ORDER BY timestamp DESC
                ''', (task_id,))
                
                for row in cursor.fetchall():
                    checkpoints.append({
                        'checkpoint_id': row[0],
                        'checkpoint_type': row[1],
                        'timestamp': row[2],
                        'progress': row[3]
                    })
            
            return checkpoints
            
        except Exception as e:
            logger.error(f"获取任务检查点列表失败: {e}")
            return []
    
    def get_recovery_status(self, task_id: str) -> Optional[Dict[str, Any]]:
        """获取恢复状态"""
        recovery_job = self._recovery_jobs.get(task_id)
        if recovery_job:
            return recovery_job.to_dict()
        return None
    
    def get_statistics(self) -> Dict[str, Any]:
        """获取统计信息"""
        return {
            'checkpoints': dict(self._stats),
            'active_checkpoints': len(self._active_checkpoints),
            'recovery_jobs': len(self._recovery_jobs),
            'checkpoint_dir_size': self._get_directory_size(self.checkpoint_dir)
        }
    
    def _get_directory_size(self, path: Path) -> int:
        """获取目录大小"""
        try:
            total_size = 0
            for file_path in path.rglob('*'):
                if file_path.is_file():
                    total_size += file_path.stat().st_size
            return total_size
        except Exception:
            return 0
    
    async def mark_checkpoint_failed(self, checkpoint_id: str, error_message: str):
        """标记检查点为失败状态"""
        try:
            # 更新检查点元数据
            with sqlite3.connect(self.recovery_db_path) as conn:
                cursor = conn.cursor()
                # 获取检查点信息
                cursor.execute('SELECT * FROM checkpoints WHERE checkpoint_id = ?', (checkpoint_id,))
                row = cursor.fetchone()
                
                if row:
                    # 更新检查点状态
                    checkpoint_data = {
                        'task_id': row[0],
                        'checkpoint_id': row[1],
                        'checkpoint_type': row[2],
                        'timestamp': row[3],
                        'progress': row[4],
                        'state_data': json.loads(row[5]),
                        'metadata': json.loads(row[6]),
                        'file_paths': json.loads(row[7]),
                        'hash_value': row[8]
                    }
                    
                    # 添加错误信息到元数据
                    checkpoint_data['metadata']['error'] = error_message
                    checkpoint_data['metadata']['failed_at'] = datetime.now(timezone.utc).isoformat()
                    
                    # 更新数据库
                    cursor.execute('''
                        UPDATE checkpoints
                        SET metadata = ?
                        WHERE checkpoint_id = ?
                    ''', (json.dumps(checkpoint_data['metadata']), checkpoint_id))
                    conn.commit()
                    
                    logger.info(f"标记检查点为失败: {checkpoint_id} - {error_message}")
                else:
                    logger.warning(f"未找到检查点: {checkpoint_id}")
                    
        except Exception as e:
            logger.error(f"标记检查点失败失败: {e}")
    
    async def cleanup_checkpoint(self, checkpoint_id: str):
        """清理检查点"""
        try:
            # 删除检查点
            await self._delete_checkpoint(checkpoint_id)
            logger.info(f"清理检查点: {checkpoint_id}")
        except Exception as e:
            logger.error(f"清理检查点失败: {e}")

# 全局恢复管理器实例
recovery_manager: Optional[RecoveryManager] = None

async def init_recovery_manager(websocket_manager=None, error_handler=None):
    """初始化全局恢复管理器"""
    global recovery_manager
    if recovery_manager is None:
        recovery_manager = RecoveryManager(
            websocket_manager=websocket_manager,
            error_handler=error_handler
        )
        await recovery_manager.start()
        logger.info("全局恢复管理器初始化完成")

async def shutdown_recovery_manager():
    """关闭全局恢复管理器"""
    global recovery_manager
    if recovery_manager:
        await recovery_manager.stop()
        recovery_manager = None
        logger.info("全局恢复管理器已关闭")