import sqlite3
import asyncio
import aiosqlite
from datetime import datetime, timezone
from enum import Enum
from typing import Optional, Dict, Any, List
import json
import logging
from contextlib import asynccontextmanager

logger = logging.getLogger(__name__)

class TaskStatus(Enum):
    """任务状态枚举"""
    PENDING = "pending"      # 等待处理
    PROCESSING = "processing"  # 正在处理
    COMPLETED = "completed"   # 已完成
    FAILED = "failed"        # 失败
    CANCELLED = "cancelled"   # 已取消

class TaskType(Enum):
    """任务类型枚举"""
    IMAGE_OCR = "image_ocr"  # 图片OCR
    PDF_OCR = "pdf_ocr"      # PDF OCR

class DatabaseManager:
    """数据库管理器，负责SQLite数据库的初始化和连接管理"""

    def __init__(self, db_path: str = None):
        import os
        # 优先使用环境变量，其次使用 /app/data 目录（Docker），最后使用当前目录
        if db_path is None:
            db_path = os.getenv("DATABASE_PATH", "data/tasks.db")
            # 确保数据目录存在
            db_dir = os.path.dirname(db_path)
            if db_dir and not os.path.exists(db_dir):
                os.makedirs(db_dir, exist_ok=True)
        self.db_path = db_path
        self._connection_pool = {}
        self._lock = asyncio.Lock()
    
    async def initialize(self):
        """初始化数据库表结构"""
        async with aiosqlite.connect(self.db_path) as db:
            # 启用外键约束
            await db.execute("PRAGMA foreign_keys = ON")
            
            # 创建任务表
            await db.execute("""
                CREATE TABLE IF NOT EXISTS tasks (
                    id TEXT PRIMARY KEY,
                    task_type TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'pending',
                    file_name TEXT,
                    file_size INTEGER,
                    total_pages INTEGER DEFAULT 1,
                    processed_pages INTEGER DEFAULT 0,
                    failed_pages INTEGER DEFAULT 0,
                    progress REAL DEFAULT 0.0,
                    processing_rate REAL DEFAULT 0.0,  -- 处理速度 (页/秒)
                    estimated_time_remaining INTEGER DEFAULT 0,  -- 预估剩余时间 (秒)
                    created_at TEXT NOT NULL,
                    started_at TEXT,
                    completed_at TEXT,
                    last_updated_at TEXT,
                    error_message TEXT,
                    metadata TEXT,  -- JSON格式存储额外信息
                    result_summary TEXT,  -- JSON格式存储处理结果摘要
                    page_strategy TEXT DEFAULT 'auto',  -- 分页策略: auto/fixed/adaptive
                    batch_size INTEGER DEFAULT 4,  -- 批处理大小
                    retry_count INTEGER DEFAULT 0,  -- 重试次数
                    max_retries INTEGER DEFAULT 3  -- 最大重试次数
                )
            """)
            
            # 创建页面结果表
            await db.execute("""
                CREATE TABLE IF NOT EXISTS page_results (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    task_id TEXT NOT NULL,
                    page_number INTEGER NOT NULL,
                    status TEXT NOT NULL DEFAULT 'pending',
                    content TEXT,
                    content_length INTEGER DEFAULT 0,
                    error_message TEXT,
                    processing_time REAL,
                    retry_count INTEGER DEFAULT 0,
                    max_retries INTEGER DEFAULT 3,
                    batch_id TEXT,  -- 批次ID，用于批量处理跟踪
                    priority INTEGER DEFAULT 0,  -- 页面优先级
                    created_at TEXT NOT NULL,
                    updated_at TEXT,
                    started_at TEXT,  -- 页面开始处理时间
                    completed_at TEXT,  -- 页面完成时间
                    FOREIGN KEY (task_id) REFERENCES tasks (id) ON DELETE CASCADE,
                    UNIQUE(task_id, page_number)
                )
            """)
            
            # 创建进度跟踪表
            await db.execute("""
                CREATE TABLE IF NOT EXISTS task_progress (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    task_id TEXT NOT NULL,
                    timestamp TEXT NOT NULL,
                    progress REAL NOT NULL,
                    processed_pages INTEGER NOT NULL,
                    failed_pages INTEGER NOT NULL,
                    processing_rate REAL,
                    estimated_time_remaining INTEGER,
                    current_batch_id TEXT,
                    message TEXT,
                    FOREIGN KEY (task_id) REFERENCES tasks (id) ON DELETE CASCADE
                )
            """)
            
            # 创建批次处理表
            await db.execute("""
                CREATE TABLE IF NOT EXISTS page_batches (
                    id TEXT PRIMARY KEY,
                    task_id TEXT NOT NULL,
                    batch_number INTEGER NOT NULL,
                    status TEXT NOT NULL DEFAULT 'pending',
                    page_start INTEGER NOT NULL,
                    page_end INTEGER NOT NULL,
                    pages_count INTEGER NOT NULL,
                    processed_pages INTEGER DEFAULT 0,
                    failed_pages INTEGER DEFAULT 0,
                    created_at TEXT NOT NULL,
                    started_at TEXT,
                    completed_at TEXT,
                    processing_time REAL,
                    FOREIGN KEY (task_id) REFERENCES tasks (id) ON DELETE CASCADE,
                    UNIQUE(task_id, batch_number)
                )
            """)
            
            # 创建索引以提高查询性能
            await db.execute("CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks (status)")
            await db.execute("CREATE INDEX IF NOT EXISTS idx_tasks_created_at ON tasks (created_at)")
            await db.execute("CREATE INDEX IF NOT EXISTS idx_tasks_progress ON tasks (progress)")
            await db.execute("CREATE INDEX IF NOT EXISTS idx_page_results_task_id ON page_results (task_id)")
            await db.execute("CREATE INDEX IF NOT EXISTS idx_page_results_status ON page_results (status)")
            
            # 检查 batch_id 列是否存在，如果存在则创建索引
            try:
                await db.execute("CREATE INDEX IF NOT EXISTS idx_page_results_batch ON page_results (batch_id)")
            except sqlite3.OperationalError as e:
                if "no such column" in str(e):
                    logger.warning("batch_id 列不存在，跳过创建索引")
                else:
                    raise
                    
            await db.execute("CREATE INDEX IF NOT EXISTS idx_task_progress_task_id ON task_progress (task_id)")
            await db.execute("CREATE INDEX IF NOT EXISTS idx_task_progress_timestamp ON task_progress (timestamp)")
            await db.execute("CREATE INDEX IF NOT EXISTS idx_page_batches_task_id ON page_batches (task_id)")
            await db.execute("CREATE INDEX IF NOT EXISTS idx_page_batches_status ON page_batches (status)")
            
            await db.commit()
            logger.info("数据库表结构初始化完成")
    
    @asynccontextmanager
    async def get_connection(self):
        """获取数据库连接的上下文管理器"""
        async with self._lock:
            connection = await aiosqlite.connect(self.db_path)
            await connection.execute("PRAGMA foreign_keys = ON")
            try:
                yield connection
            finally:
                await connection.close()

class TaskModel:
    """任务模型，提供任务相关的数据库操作"""
    
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
    
    async def create_task(self, 
                         task_id: str,
                         task_type: TaskType,
                         file_name: str,
                         file_size: int,
                         total_pages: int = 1,
                         metadata: Optional[Dict[str, Any]] = None) -> bool:
        """创建新任务"""
        try:
            async with self.db_manager.get_connection() as db:
                await db.execute("""
                    INSERT INTO tasks (
                        id, task_type, status, file_name, file_size, 
                        total_pages, created_at, metadata
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    task_id,
                    task_type.value,
                    TaskStatus.PENDING.value,
                    file_name,
                    file_size,
                    total_pages,
                    datetime.now(timezone.utc).isoformat(),
                    json.dumps(metadata) if metadata else None
                ))
                await db.commit()
                logger.info(f"任务 {task_id} 创建成功")
                return True
        except Exception as e:
            logger.error(f"创建任务失败 {task_id}: {e}")
            return False
    
    async def get_task(self, task_id: str) -> Optional[Dict[str, Any]]:
        """获取任务信息"""
        try:
            async with self.db_manager.get_connection() as db:
                db.row_factory = aiosqlite.Row
                async with db.execute("""
                    SELECT * FROM tasks WHERE id = ?
                """, (task_id,)) as cursor:
                    row = await cursor.fetchone()
                    if row:
                        task = dict(row)
                        # 解析JSON字段
                        if task.get('metadata'):
                            task['metadata'] = json.loads(task['metadata'])
                        if task.get('result_summary'):
                            task['result_summary'] = json.loads(task['result_summary'])
                        return task
                    return None
        except Exception as e:
            logger.error(f"获取任务失败 {task_id}: {e}")
            return None
    
    async def update_task_status(self,
                                task_id: str,
                                status: TaskStatus,
                                error_message: Optional[str] = None,
                                progress: Optional[float] = None,
                                processed_pages: Optional[int] = None,
                                failed_pages: Optional[int] = None,
                                processing_rate: Optional[float] = None,
                                estimated_time_remaining: Optional[int] = None) -> bool:
        """更新任务状态"""
        try:
            async with self.db_manager.get_connection() as db:
                # 构建更新字段
                fields = ["status = ?", "last_updated_at = ?"]
                values = [status.value, datetime.now(timezone.utc).isoformat()]
                
                if error_message is not None:
                    fields.append("error_message = ?")
                    values.append(error_message)
                
                if progress is not None:
                    fields.append("progress = ?")
                    values.append(progress)
                
                if processed_pages is not None:
                    fields.append("processed_pages = ?")
                    values.append(processed_pages)
                
                if failed_pages is not None:
                    fields.append("failed_pages = ?")
                    values.append(failed_pages)
                
                if processing_rate is not None:
                    fields.append("processing_rate = ?")
                    values.append(processing_rate)
                
                if estimated_time_remaining is not None:
                    fields.append("estimated_time_remaining = ?")
                    values.append(estimated_time_remaining)
                
                # 根据状态设置时间戳
                if status == TaskStatus.PROCESSING:
                    fields.append("started_at = ?")
                    values.append(datetime.now(timezone.utc).isoformat())
                elif status in [TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.CANCELLED]:
                    fields.append("completed_at = ?")
                    values.append(datetime.now(timezone.utc).isoformat())
                
                values.append(task_id)
                
                await db.execute(f"""
                    UPDATE tasks SET {', '.join(fields)} WHERE id = ?
                """, values)
                await db.commit()
                return True
        except Exception as e:
            logger.error(f"更新任务状态失败 {task_id}: {e}")
            return False
    
    async def update_task_result_summary(self, 
                                       task_id: str, 
                                       result_summary: Dict[str, Any]) -> bool:
        """更新任务结果摘要"""
        try:
            async with self.db_manager.get_connection() as db:
                await db.execute("""
                    UPDATE tasks SET result_summary = ? WHERE id = ?
                """, (json.dumps(result_summary), task_id))
                await db.commit()
                return True
        except Exception as e:
            logger.error(f"更新任务结果摘要失败 {task_id}: {e}")
            return False
    
    async def get_tasks_by_status(self, status: TaskStatus, limit: int = 100) -> List[Dict[str, Any]]:
        """根据状态获取任务列表"""
        try:
            async with self.db_manager.get_connection() as db:
                db.row_factory = aiosqlite.Row
                async with db.execute("""
                    SELECT * FROM tasks WHERE status = ?
                    ORDER BY created_at ASC LIMIT ?
                """, (status.value, limit)) as cursor:
                    rows = await cursor.fetchall()
                    tasks = []
                    for row in rows:
                        task = dict(row)
                        if task.get('metadata'):
                            task['metadata'] = json.loads(task['metadata'])
                        if task.get('result_summary'):
                            task['result_summary'] = json.loads(task['result_summary'])
                        tasks.append(task)
                    return tasks
        except Exception as e:
            logger.error(f"获取任务列表失败 {status.value}: {e}")
            return []
    
    async def get_recent_tasks(self, limit: int = 50) -> List[Dict[str, Any]]:
        """获取最近的任务列表"""
        try:
            async with self.db_manager.get_connection() as db:
                db.row_factory = aiosqlite.Row
                async with db.execute("""
                    SELECT * FROM tasks
                    ORDER BY created_at DESC LIMIT ?
                """, (limit,)) as cursor:
                    rows = await cursor.fetchall()
                    tasks = []
                    for row in rows:
                        task = dict(row)
                        if task.get('metadata'):
                            task['metadata'] = json.loads(task['metadata'])
                        if task.get('result_summary'):
                            task['result_summary'] = json.loads(task['result_summary'])
                        tasks.append(task)
                    return tasks
        except Exception as e:
            logger.error(f"获取最近任务列表失败: {e}")
            return []

class PageResultModel:
    """页面结果模型，提供页面处理结果相关的数据库操作"""
    
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
    
    async def create_page_result(self, 
                               task_id: str,
                               page_number: int,
                               status: str = "pending") -> bool:
        """创建页面处理结果记录"""
        try:
            async with self.db_manager.get_connection() as db:
                await db.execute("""
                    INSERT OR REPLACE INTO page_results (
                        task_id, page_number, status, created_at
                    ) VALUES (?, ?, ?, ?)
                """, (
                    task_id,
                    page_number,
                    status,
                    datetime.now(timezone.utc).isoformat()
                ))
                await db.commit()
                return True
        except Exception as e:
            logger.error(f"创建页面结果失败 {task_id}-{page_number}: {e}")
            return False
    
    async def update_page_result(self,
                               task_id: str,
                               page_number: int,
                               status: str,
                               content: Optional[str] = None,
                               error_message: Optional[str] = None,
                               processing_time: Optional[float] = None,
                               batch_id: Optional[str] = None,
                               retry_count: Optional[int] = None) -> bool:
        """更新页面处理结果"""
        try:
            async with self.db_manager.get_connection() as db:
                # 构建更新字段
                fields = ["status = ?", "updated_at = ?"]
                values = [status, datetime.now(timezone.utc).isoformat()]
                
                if content is not None:
                    fields.append("content = ?")
                    fields.append("content_length = ?")
                    values.append(content)
                    values.append(len(content) if content else 0)
                
                if error_message is not None:
                    fields.append("error_message = ?")
                    values.append(error_message)
                
                if processing_time is not None:
                    fields.append("processing_time = ?")
                    values.append(processing_time)
                
                if batch_id is not None:
                    fields.append("batch_id = ?")
                    values.append(batch_id)
                
                if retry_count is not None:
                    fields.append("retry_count = ?")
                    values.append(retry_count)
                
                # 根据状态设置时间戳
                if status == "processing":
                    fields.append("started_at = ?")
                    values.append(datetime.now(timezone.utc).isoformat())
                elif status in ["completed", "failed"]:
                    fields.append("completed_at = ?")
                    values.append(datetime.now(timezone.utc).isoformat())
                
                values.extend([task_id, page_number])
                
                await db.execute(f"""
                    UPDATE page_results SET {', '.join(fields)}
                    WHERE task_id = ? AND page_number = ?
                """, values)
                await db.commit()
                return True
        except Exception as e:
            logger.error(f"更新页面结果失败 {task_id}-{page_number}: {e}")
            return False
    
    async def get_task_page_results(self, task_id: str) -> List[Dict[str, Any]]:
        """获取任务的所有页面结果"""
        try:
            async with self.db_manager.get_connection() as db:
                db.row_factory = aiosqlite.Row
                async with db.execute("""
                    SELECT * FROM page_results
                    WHERE task_id = ?
                    ORDER BY page_number ASC
                """, (task_id,)) as cursor:
                    rows = await cursor.fetchall()
                    return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"获取页面结果失败 {task_id}: {e}")
            return []
    
    async def get_page_result(self, task_id: str, page_number: int) -> Optional[Dict[str, Any]]:
        """获取特定页面的处理结果"""
        try:
            async with self.db_manager.get_connection() as db:
                db.row_factory = aiosqlite.Row
                async with db.execute("""
                    SELECT * FROM page_results
                    WHERE task_id = ? AND page_number = ?
                """, (task_id, page_number)) as cursor:
                    row = await cursor.fetchone()
                    return dict(row) if row else None
        except Exception as e:
            logger.error(f"获取页面结果失败 {task_id}-{page_number}: {e}")
            return None

class TaskProgressModel:
    """任务进度模型，提供进度跟踪相关的数据库操作"""
    
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
    
    async def record_progress(self, 
                             task_id: str,
                             progress: float,
                             processed_pages: int,
                             failed_pages: int,
                             processing_rate: Optional[float] = None,
                             estimated_time_remaining: Optional[int] = None,
                             current_batch_id: Optional[str] = None,
                             message: Optional[str] = None) -> bool:
        """记录任务进度"""
        try:
            async with self.db_manager.get_connection() as db:
                await db.execute("""
                    INSERT INTO task_progress (
                        task_id, timestamp, progress, processed_pages, failed_pages,
                        processing_rate, estimated_time_remaining, current_batch_id, message
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    task_id,
                    datetime.now(timezone.utc).isoformat(),
                    progress,
                    processed_pages,
                    failed_pages,
                    processing_rate,
                    estimated_time_remaining,
                    current_batch_id,
                    message
                ))
                await db.commit()
                return True
        except Exception as e:
            logger.error(f"记录任务进度失败 {task_id}: {e}")
            return False
    
    async def get_task_progress_history(self, task_id: str, limit: int = 100) -> List[Dict[str, Any]]:
        """获取任务进度历史"""
        try:
            async with self.db_manager.get_connection() as db:
                db.row_factory = aiosqlite.Row
                async with db.execute("""
                    SELECT * FROM task_progress
                    WHERE task_id = ?
                    ORDER BY timestamp DESC LIMIT ?
                """, (task_id, limit)) as cursor:
                    rows = await cursor.fetchall()
                    return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"获取任务进度历史失败 {task_id}: {e}")
            return []
    
    async def get_latest_progress(self, task_id: str) -> Optional[Dict[str, Any]]:
        """获取任务最新进度"""
        try:
            async with self.db_manager.get_connection() as db:
                db.row_factory = aiosqlite.Row
                async with db.execute("""
                    SELECT * FROM task_progress
                    WHERE task_id = ?
                    ORDER BY timestamp DESC LIMIT 1
                """, (task_id,)) as cursor:
                    row = await cursor.fetchone()
                    return dict(row) if row else None
        except Exception as e:
            logger.error(f"获取任务最新进度失败 {task_id}: {e}")
            return None

class PageBatchModel:
    """页面批次模型，提供批次处理相关的数据库操作"""
    
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
    
    async def create_batch(self, 
                          batch_id: str,
                          task_id: str,
                          batch_number: int,
                          page_start: int,
                          page_end: int,
                          pages_count: int) -> bool:
        """创建页面批次"""
        try:
            async with self.db_manager.get_connection() as db:
                await db.execute("""
                    INSERT INTO page_batches (
                        id, task_id, batch_number, page_start, page_end, 
                        pages_count, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (
                    batch_id,
                    task_id,
                    batch_number,
                    page_start,
                    page_end,
                    pages_count,
                    datetime.now(timezone.utc).isoformat()
                ))
                await db.commit()
                return True
        except Exception as e:
            logger.error(f"创建批次失败 {batch_id}: {e}")
            return False
    
    async def update_batch_status(self, 
                                 batch_id: str,
                                 status: str,
                                 processed_pages: Optional[int] = None,
                                 failed_pages: Optional[int] = None,
                                 processing_time: Optional[float] = None) -> bool:
        """更新批次状态"""
        try:
            async with self.db_manager.get_connection() as db:
                fields = ["status = ?"]
                values = [status]
                
                if processed_pages is not None:
                    fields.append("processed_pages = ?")
                    values.append(processed_pages)
                
                if failed_pages is not None:
                    fields.append("failed_pages = ?")
                    values.append(failed_pages)
                
                if processing_time is not None:
                    fields.append("processing_time = ?")
                    values.append(processing_time)
                
                # 根据状态设置时间戳
                if status == "processing":
                    fields.append("started_at = ?")
                    values.append(datetime.now(timezone.utc).isoformat())
                elif status in ["completed", "failed"]:
                    fields.append("completed_at = ?")
                    values.append(datetime.now(timezone.utc).isoformat())
                
                values.append(batch_id)
                
                await db.execute(f"""
                    UPDATE page_batches SET {', '.join(fields)} WHERE id = ?
                """, values)
                await db.commit()
                return True
        except Exception as e:
            logger.error(f"更新批次状态失败 {batch_id}: {e}")
            return False
    
    async def get_task_batches(self, task_id: str) -> List[Dict[str, Any]]:
        """获取任务的所有批次"""
        try:
            async with self.db_manager.get_connection() as db:
                db.row_factory = aiosqlite.Row
                async with db.execute("""
                    SELECT * FROM page_batches
                    WHERE task_id = ?
                    ORDER BY batch_number ASC
                """, (task_id,)) as cursor:
                    rows = await cursor.fetchall()
                    return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"获取任务批次失败 {task_id}: {e}")
            return []
    
    async def get_batch(self, batch_id: str) -> Optional[Dict[str, Any]]:
        """获取特定批次信息"""
        try:
            async with self.db_manager.get_connection() as db:
                db.row_factory = aiosqlite.Row
                async with db.execute("""
                    SELECT * FROM page_batches WHERE id = ?
                """, (batch_id,)) as cursor:
                    row = await cursor.fetchone()
                    return dict(row) if row else None
        except Exception as e:
            logger.error(f"获取批次信息失败 {batch_id}: {e}")
            return None

# 全局数据库管理器实例
db_manager = DatabaseManager()
task_model = TaskModel(db_manager)
page_result_model = PageResultModel(db_manager)
task_progress_model = TaskProgressModel(db_manager)
page_batch_model = PageBatchModel(db_manager)

async def init_database():
    """初始化数据库"""
    await db_manager.initialize()
    logger.info("数据库初始化完成")

async def cleanup_database():
    """清理数据库连接"""
    # 这里可以添加清理逻辑，比如关闭连接池
    logger.info("数据库清理完成")