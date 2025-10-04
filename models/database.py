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

            # 创建用户表
            await db.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    id TEXT PRIMARY KEY,
                    username TEXT UNIQUE NOT NULL,
                    password_hash TEXT NOT NULL,
                    is_admin BOOLEAN DEFAULT 0,
                    is_disabled BOOLEAN DEFAULT 0,
                    total_pages INTEGER DEFAULT 0,
                    used_pages INTEGER DEFAULT 0,
                    created_at TEXT NOT NULL,
                    last_login TEXT,
                    session_token TEXT
                )
            """)

            # 创建兑换码表
            await db.execute("""
                CREATE TABLE IF NOT EXISTS redemption_codes (
                    code TEXT PRIMARY KEY,
                    pages INTEGER NOT NULL,
                    max_uses INTEGER DEFAULT 1,
                    used_count INTEGER DEFAULT 0,
                    created_by TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    expires_at TEXT,
                    is_active BOOLEAN DEFAULT 1,
                    description TEXT,
                    FOREIGN KEY (created_by) REFERENCES users (id)
                )
            """)

            # 创建兑换记录表
            await db.execute("""
                CREATE TABLE IF NOT EXISTS redemption_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id TEXT NOT NULL,
                    code TEXT NOT NULL,
                    pages_granted INTEGER NOT NULL,
                    redeemed_at TEXT NOT NULL,
                    FOREIGN KEY (user_id) REFERENCES users (id),
                    FOREIGN KEY (code) REFERENCES redemption_codes (code)
                )
            """)

            # 创建系统设置表
            await db.execute("""
                CREATE TABLE IF NOT EXISTS system_settings (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL,
                    description TEXT,
                    updated_at TEXT NOT NULL
                )
            """)

            # 创建注册令牌表
            await db.execute("""
                CREATE TABLE IF NOT EXISTS registration_tokens (
                    token TEXT PRIMARY KEY,
                    max_uses INTEGER NOT NULL,
                    used_count INTEGER DEFAULT 0,
                    created_by TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    expires_at TEXT,
                    is_active BOOLEAN DEFAULT 1,
                    description TEXT,
                    FOREIGN KEY (created_by) REFERENCES users (id)
                )
            """)

            # 创建注册令牌使用记录表
            await db.execute("""
                CREATE TABLE IF NOT EXISTS registration_token_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id TEXT NOT NULL,
                    token TEXT NOT NULL,
                    username TEXT NOT NULL,
                    user_uuid TEXT NOT NULL,
                    registered_at TEXT NOT NULL,
                    FOREIGN KEY (user_id) REFERENCES users (id),
                    FOREIGN KEY (token) REFERENCES registration_tokens (token)
                )
            """)

            # 创建OCR IP跟踪表
            await db.execute("""
                CREATE TABLE IF NOT EXISTS ocr_ip_tracking (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id TEXT NOT NULL,
                    client_ip TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    FOREIGN KEY (user_id) REFERENCES users (id)
                )
            """)

            # 初始化系统设置（如果不存在）
            await db.execute("""
                INSERT OR IGNORE INTO system_settings (key, value, description, updated_at)
                VALUES ('registration_enabled', 'true', '是否允许新用户注册', ?)
            """, (datetime.now(timezone.utc).isoformat(),))

            # 初始化IP滥用检测设置（从环境变量读取默认值）
            import os
            ip_abuse_enabled = os.getenv('IP_ABUSE_DETECTION_ENABLED', 'true')
            ip_abuse_threshold = os.getenv('IP_ABUSE_ACCOUNTS_THRESHOLD', '3')
            ip_abuse_days = os.getenv('IP_ABUSE_DAYS_WINDOW', '7')

            await db.execute("""
                INSERT OR IGNORE INTO system_settings (key, value, description, updated_at)
                VALUES ('ip_abuse_detection_enabled', ?, 'IP滥用检测开关', ?)
            """, (ip_abuse_enabled, datetime.now(timezone.utc).isoformat()))

            await db.execute("""
                INSERT OR IGNORE INTO system_settings (key, value, description, updated_at)
                VALUES ('ip_abuse_accounts_threshold', ?, '同一IP账户数阈值', ?)
            """, (ip_abuse_threshold, datetime.now(timezone.utc).isoformat()))

            await db.execute("""
                INSERT OR IGNORE INTO system_settings (key, value, description, updated_at)
                VALUES ('ip_abuse_days_window', ?, 'IP滥用检测天数窗口', ?)
            """, (ip_abuse_days, datetime.now(timezone.utc).isoformat()))

            # 创建任务表
            await db.execute("""
                CREATE TABLE IF NOT EXISTS tasks (
                    id TEXT PRIMARY KEY,
                    user_id TEXT,  -- 所属用户ID
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
                    client_ip TEXT,  -- 客户端IP地址
                    metadata TEXT,  -- JSON格式存储额外信息
                    result_summary TEXT,  -- JSON格式存储处理结果摘要
                    page_strategy TEXT DEFAULT 'auto',  -- 分页策略: auto/fixed/adaptive
                    batch_size INTEGER DEFAULT 4,  -- 批处理大小
                    retry_count INTEGER DEFAULT 0,  -- 重试次数
                    max_retries INTEGER DEFAULT 3,  -- 最大重试次数
                    FOREIGN KEY (user_id) REFERENCES users (id)
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
            # 用户表索引
            await db.execute("CREATE INDEX IF NOT EXISTS idx_users_username ON users (username)")
            await db.execute("CREATE INDEX IF NOT EXISTS idx_users_session_token ON users (session_token)")
            await db.execute("CREATE INDEX IF NOT EXISTS idx_users_is_admin ON users (is_admin)")

            # 兑换码表索引
            await db.execute("CREATE INDEX IF NOT EXISTS idx_redemption_codes_is_active ON redemption_codes (is_active)")
            await db.execute("CREATE INDEX IF NOT EXISTS idx_redemption_codes_created_by ON redemption_codes (created_by)")
            await db.execute("CREATE INDEX IF NOT EXISTS idx_redemption_codes_expires_at ON redemption_codes (expires_at)")

            # 兑换记录表索引
            await db.execute("CREATE INDEX IF NOT EXISTS idx_redemption_history_user_id ON redemption_history (user_id)")
            await db.execute("CREATE INDEX IF NOT EXISTS idx_redemption_history_code ON redemption_history (code)")

            # 注册令牌表索引
            await db.execute("CREATE INDEX IF NOT EXISTS idx_registration_tokens_is_active ON registration_tokens (is_active)")
            await db.execute("CREATE INDEX IF NOT EXISTS idx_registration_tokens_created_by ON registration_tokens (created_by)")
            await db.execute("CREATE INDEX IF NOT EXISTS idx_registration_tokens_expires_at ON registration_tokens (expires_at)")

            # 注册令牌使用记录表索引
            await db.execute("CREATE INDEX IF NOT EXISTS idx_registration_token_history_user_id ON registration_token_history (user_id)")
            await db.execute("CREATE INDEX IF NOT EXISTS idx_registration_token_history_token ON registration_token_history (token)")

            # OCR IP跟踪表索引
            await db.execute("CREATE INDEX IF NOT EXISTS idx_ocr_ip_tracking_user_id ON ocr_ip_tracking (user_id)")
            await db.execute("CREATE INDEX IF NOT EXISTS idx_ocr_ip_tracking_client_ip ON ocr_ip_tracking (client_ip)")
            await db.execute("CREATE INDEX IF NOT EXISTS idx_ocr_ip_tracking_created_at ON ocr_ip_tracking (created_at)")

            # 检查并迁移旧数据：如果tasks表存在但没有user_id列，则添加
            try:
                # 检查user_id列是否存在
                async with db.execute("PRAGMA table_info(tasks)") as cursor:
                    columns = await cursor.fetchall()
                    column_names = [col[1] for col in columns]

                    if 'user_id' not in column_names and len(column_names) > 0:
                        # 旧表存在但没有user_id列，需要迁移
                        logger.warning("检测到旧的tasks表结构，正在添加user_id列...")
                        await db.execute("ALTER TABLE tasks ADD COLUMN user_id TEXT")
                        logger.info("tasks表迁移完成，已添加user_id列")

                    # 检查client_ip列是否存在，如果不存在则添加
                    if 'client_ip' not in column_names and len(column_names) > 0:
                        logger.warning("检测到tasks表缺少client_ip列，正在添加...")
                        await db.execute("ALTER TABLE tasks ADD COLUMN client_ip TEXT")
                        logger.info("tasks表迁移完成，已添加client_ip列")
            except Exception as e:
                logger.error(f"tasks表迁移检查失败: {e}")

            # 任务表索引（在迁移后创建）
            await db.execute("CREATE INDEX IF NOT EXISTS idx_tasks_user_id ON tasks (user_id)")
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
                         user_id: Optional[str] = None,
                         client_ip: Optional[str] = None,
                         metadata: Optional[Dict[str, Any]] = None) -> bool:
        """创建新任务"""
        try:
            async with self.db_manager.get_connection() as db:
                await db.execute("""
                    INSERT INTO tasks (
                        id, user_id, task_type, status, file_name, file_size,
                        total_pages, client_ip, created_at, metadata
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    task_id,
                    user_id,
                    task_type.value,
                    TaskStatus.PENDING.value,
                    file_name,
                    file_size,
                    total_pages,
                    client_ip,
                    datetime.now(timezone.utc).isoformat(),
                    json.dumps(metadata) if metadata else None
                ))
                await db.commit()
                logger.info(f"任务 {task_id} 创建成功 (用户: {user_id}, IP: {client_ip})")
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
    
    async def get_recent_tasks(self, limit: int = 50, user_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """获取最近的任务列表，可选按用户筛选"""
        try:
            async with self.db_manager.get_connection() as db:
                db.row_factory = aiosqlite.Row

                if user_id:
                    # 按用户筛选
                    query = """
                        SELECT * FROM tasks
                        WHERE user_id = ?
                        ORDER BY created_at DESC LIMIT ?
                    """
                    params = (user_id, limit)
                else:
                    # 返回所有任务
                    query = """
                        SELECT * FROM tasks
                        ORDER BY created_at DESC LIMIT ?
                    """
                    params = (limit,)

                async with db.execute(query, params) as cursor:
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

    async def get_all_tasks_with_users(self, limit: int = 100, offset: int = 0, user_id_filter: Optional[str] = None, search_query: Optional[str] = None) -> List[Dict[str, Any]]:
        """获取所有任务列表（包含用户信息），供管理员使用"""
        try:
            async with self.db_manager.get_connection() as db:
                db.row_factory = aiosqlite.Row

                # 构建基础查询
                query_parts = []
                params = []

                # 用户筛选条件
                if user_id_filter:
                    query_parts.append("t.user_id = ?")
                    params.append(user_id_filter)

                # 搜索条件 - 搜索文件名、用户名、任务UUID
                if search_query:
                    search_pattern = f"%{search_query}%"
                    query_parts.append("(t.file_name LIKE ? OR u.username LIKE ? OR t.id LIKE ? OR t.user_id LIKE ?)")
                    params.extend([search_pattern, search_pattern, search_pattern, search_pattern])

                # 构建WHERE子句
                where_clause = ""
                if query_parts:
                    where_clause = "WHERE " + " AND ".join(query_parts)

                # 如果有搜索关键词，还需要搜索文件内容
                if search_query:
                    # 先查询匹配内容的任务ID
                    content_query = """
                        SELECT DISTINCT task_id FROM page_results
                        WHERE content LIKE ?
                    """
                    content_params = [f"%{search_query}%"]

                    async with db.execute(content_query, content_params) as cursor:
                        content_rows = await cursor.fetchall()
                        content_task_ids = [row[0] for row in content_rows]

                    # 合并基础查询和内容查询
                    if content_task_ids:
                        placeholders = ','.join('?' * len(content_task_ids))
                        if query_parts:
                            # 已有其他筛选条件，添加OR条件
                            where_clause += f" OR t.id IN ({placeholders})"
                            params.extend(content_task_ids)
                        else:
                            # 没有其他筛选条件，只有内容搜索
                            where_clause = f"WHERE t.id IN ({placeholders})"
                            params = content_task_ids

                # 完整查询
                query = f"""
                    SELECT
                        t.id, t.user_id, t.task_type, t.status, t.file_name, t.file_size,
                        t.total_pages, t.processed_pages, t.failed_pages, t.progress,
                        t.created_at, t.started_at, t.completed_at, t.error_message, t.client_ip,
                        u.username, u.id as user_uuid
                    FROM tasks t
                    LEFT JOIN users u ON t.user_id = u.id
                    {where_clause}
                    ORDER BY t.created_at DESC
                    LIMIT ? OFFSET ?
                """
                params.extend([limit, offset])

                async with db.execute(query, params) as cursor:
                    rows = await cursor.fetchall()
                    return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"获取任务列表（含用户信息）失败: {e}")
            return []

    async def count_tasks(self, user_id_filter: Optional[str] = None, search_query: Optional[str] = None) -> int:
        """统计任务总数（支持筛选）"""
        try:
            async with self.db_manager.get_connection() as db:
                # 构建查询条件
                query_parts = []
                params = []

                if user_id_filter:
                    query_parts.append("user_id = ?")
                    params.append(user_id_filter)

                if search_query:
                    search_pattern = f"%{search_query}%"
                    query_parts.append("(file_name LIKE ? OR id LIKE ? OR user_id LIKE ?)")
                    params.extend([search_pattern, search_pattern, search_pattern])

                where_clause = ""
                if query_parts:
                    where_clause = "WHERE " + " AND ".join(query_parts)

                query = f"SELECT COUNT(*) FROM tasks {where_clause}"

                async with db.execute(query, params) as cursor:
                    row = await cursor.fetchone()
                    return row[0] if row else 0
        except Exception as e:
            logger.error(f"统计任务数失败: {e}")
            return 0

    async def get_task_stats_by_user(self, user_id: Optional[str] = None) -> Dict[str, Any]:
        """获取任务统计信息，可选按用户筛选"""
        try:
            async with self.db_manager.get_connection() as db:
                if user_id:
                    # 特定用户的统计
                    query = """
                        SELECT
                            COUNT(*) as total_tasks,
                            SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as completed_tasks,
                            SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed_tasks,
                            SUM(CASE WHEN status = 'processing' THEN 1 ELSE 0 END) as processing_tasks,
                            SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END) as pending_tasks,
                            SUM(total_pages) as total_pages,
                            SUM(processed_pages) as processed_pages
                        FROM tasks
                        WHERE user_id = ?
                    """
                    params = (user_id,)
                else:
                    # 全局统计
                    query = """
                        SELECT
                            COUNT(*) as total_tasks,
                            SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as completed_tasks,
                            SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed_tasks,
                            SUM(CASE WHEN status = 'processing' THEN 1 ELSE 0 END) as processing_tasks,
                            SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END) as pending_tasks,
                            SUM(total_pages) as total_pages,
                            SUM(processed_pages) as processed_pages
                        FROM tasks
                    """
                    params = ()

                async with db.execute(query, params) as cursor:
                    row = await cursor.fetchone()
                    if row:
                        return {
                            'total_tasks': row[0] or 0,
                            'completed_tasks': row[1] or 0,
                            'failed_tasks': row[2] or 0,
                            'processing_tasks': row[3] or 0,
                            'pending_tasks': row[4] or 0,
                            'total_pages': row[5] or 0,
                            'processed_pages': row[6] or 0
                        }
                    return {}
        except Exception as e:
            logger.error(f"获取任务统计失败: {e}")
            return {}

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
                elif status in ["completed", "failed", "cancelled"]:
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

class IPTrackingModel:
    """IP跟踪模型，提供OCR IP滥用检测相关的数据库操作"""

    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager

    async def record_ocr_activity(self, user_id: str, client_ip: str) -> bool:
        """记录用户OCR活动的IP地址"""
        try:
            async with self.db_manager.get_connection() as db:
                await db.execute("""
                    INSERT INTO ocr_ip_tracking (user_id, client_ip, created_at)
                    VALUES (?, ?, ?)
                """, (user_id, client_ip, datetime.now(timezone.utc).isoformat()))
                await db.commit()
                logger.debug(f"记录OCR活动: 用户 {user_id}, IP {client_ip}")
                return True
        except Exception as e:
            logger.error(f"记录OCR活动失败 {user_id}, {client_ip}: {e}")
            return False

    async def get_settings(self) -> Dict[str, Any]:
        """获取IP滥用检测配置"""
        try:
            async with self.db_manager.get_connection() as db:
                db.row_factory = aiosqlite.Row
                async with db.execute("""
                    SELECT key, value FROM system_settings
                    WHERE key IN ('ip_abuse_detection_enabled', 'ip_abuse_accounts_threshold', 'ip_abuse_days_window')
                """) as cursor:
                    rows = await cursor.fetchall()
                    settings = {}
                    for row in rows:
                        key = row[0]
                        value = row[1]
                        if key == 'ip_abuse_detection_enabled':
                            settings['enabled'] = value.lower() == 'true'
                        elif key == 'ip_abuse_accounts_threshold':
                            settings['threshold'] = int(value)
                        elif key == 'ip_abuse_days_window':
                            settings['days'] = int(value)
                    return settings
        except Exception as e:
            logger.error(f"获取IP滥用检测配置失败: {e}")
            return {'enabled': True, 'threshold': 3, 'days': 7}

    async def update_settings(self, enabled: Optional[bool] = None,
                            threshold: Optional[int] = None,
                            days: Optional[int] = None) -> bool:
        """更新IP滥用检测配置"""
        try:
            async with self.db_manager.get_connection() as db:
                now = datetime.now(timezone.utc).isoformat()

                if enabled is not None:
                    await db.execute("""
                        UPDATE system_settings
                        SET value = ?, updated_at = ?
                        WHERE key = 'ip_abuse_detection_enabled'
                    """, ('true' if enabled else 'false', now))

                if threshold is not None:
                    await db.execute("""
                        UPDATE system_settings
                        SET value = ?, updated_at = ?
                        WHERE key = 'ip_abuse_accounts_threshold'
                    """, (str(threshold), now))

                if days is not None:
                    await db.execute("""
                        UPDATE system_settings
                        SET value = ?, updated_at = ?
                        WHERE key = 'ip_abuse_days_window'
                    """, (str(days), now))

                await db.commit()
                logger.info(f"更新IP滥用检测配置: enabled={enabled}, threshold={threshold}, days={days}")
                return True
        except Exception as e:
            logger.error(f"更新IP滥用检测配置失败: {e}")
            return False

    async def check_and_suspend_abuse(self, user_model) -> Dict[str, Any]:
        """检测IP滥用并自动封停账户"""
        try:
            settings = await self.get_settings()

            # 如果检测未启用，直接返回
            if not settings.get('enabled', True):
                logger.debug("IP滥用检测已禁用，跳过检查")
                return {'status': 'disabled', 'suspended_count': 0}

            threshold = settings.get('threshold', 3)
            days = settings.get('days', 7)

            # 计算时间窗口
            from datetime import timedelta
            time_window = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()

            async with self.db_manager.get_connection() as db:
                db.row_factory = aiosqlite.Row

                # 查询在时间窗口内，同一IP使用的不同用户数量（仅统计未被禁用的用户）
                async with db.execute("""
                    SELECT t.client_ip,
                           COUNT(DISTINCT t.user_id) as user_count,
                           GROUP_CONCAT(DISTINCT t.user_id) as user_ids
                    FROM ocr_ip_tracking t
                    INNER JOIN users u ON t.user_id = u.id
                    WHERE t.created_at >= ?
                      AND u.is_disabled = 0
                    GROUP BY t.client_ip
                    HAVING user_count >= ?
                """, (time_window, threshold)) as cursor:
                    abuse_ips = await cursor.fetchall()

                suspended_users = []

                for row in abuse_ips:
                    client_ip = row[0]
                    user_count = row[1]
                    user_ids_str = row[2]
                    user_ids = user_ids_str.split(',') if user_ids_str else []

                    logger.warning(f"检测到IP滥用: IP {client_ip} 在 {days} 天内被 {user_count} 个账户使用")

                    # 先检查该IP是否有管理员账户（直接查询，避免调用user_model）
                    has_admin = False
                    for user_id in user_ids:
                        try:
                            async with db.execute(
                                "SELECT is_admin FROM users WHERE id = ?", (user_id,)
                            ) as cursor:
                                user_row = await cursor.fetchone()
                                if user_row and user_row[0]:
                                    has_admin = True
                                    logger.info(f"IP {client_ip} 包含管理员账户 {user_id}，跳过该IP的所有检测")
                                    break
                        except Exception as e:
                            logger.error(f"检查用户是否为管理员失败 {user_id}: {e}")

                    # 如果该IP有管理员，跳过该IP的所有账户
                    if has_admin:
                        continue

                    # 封停所有相关账户（直接操作数据库，避免调用user_model）
                    for user_id in user_ids:
                        try:
                            # 检查用户是否存在且未被禁用
                            async with db.execute(
                                "SELECT username, is_disabled FROM users WHERE id = ?", (user_id,)
                            ) as cursor:
                                user_row = await cursor.fetchone()

                                if not user_row:
                                    continue

                                username = user_row[0]
                                is_disabled = user_row[1]

                                # 跳过已禁用的账户
                                if is_disabled:
                                    continue

                            # 封停账户
                            await db.execute(
                                "UPDATE users SET is_disabled = 1 WHERE id = ?", (user_id,)
                            )

                            suspended_users.append({
                                'user_id': user_id,
                                'username': username,
                                'ip': client_ip,
                                'reason': f'IP滥用检测: 同IP {user_count} 个账户'
                            })
                            logger.info(f"自动封停用户 {username} ({user_id}) (IP: {client_ip})")

                        except Exception as e:
                            logger.error(f"封停用户失败 {user_id}: {e}")

                # 提交所有更改
                await db.commit()

                return {
                    'status': 'success',
                    'suspended_count': len(suspended_users),
                    'suspended_users': suspended_users,
                    'abuse_ips_count': len(abuse_ips)
                }

        except Exception as e:
            logger.error(f"IP滥用检测失败: {e}")
            return {'status': 'error', 'message': str(e), 'suspended_count': 0}

    async def get_suspicious_ips(self, days: int = 7, threshold: int = 3) -> List[Dict[str, Any]]:
        """获取可疑IP列表"""
        try:
            from datetime import timedelta
            time_window = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()

            async with self.db_manager.get_connection() as db:
                db.row_factory = aiosqlite.Row
                async with db.execute("""
                    SELECT
                        client_ip,
                        COUNT(DISTINCT user_id) as user_count,
                        COUNT(*) as activity_count,
                        MIN(created_at) as first_seen,
                        MAX(created_at) as last_seen
                    FROM ocr_ip_tracking
                    WHERE created_at >= ?
                    GROUP BY client_ip
                    HAVING user_count >= ?
                    ORDER BY user_count DESC, activity_count DESC
                """, (time_window, threshold)) as cursor:
                    rows = await cursor.fetchall()
                    return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"获取可疑IP列表失败: {e}")
            return []

    async def reset_to_env_defaults(self) -> bool:
        """重置配置为环境变量中的默认值（应用启动时调用）"""
        try:
            import os
            ip_abuse_enabled = os.getenv('IP_ABUSE_DETECTION_ENABLED', 'true')
            ip_abuse_threshold = os.getenv('IP_ABUSE_ACCOUNTS_THRESHOLD', '3')
            ip_abuse_days = os.getenv('IP_ABUSE_DAYS_WINDOW', '7')

            async with self.db_manager.get_connection() as db:
                now = datetime.now(timezone.utc).isoformat()

                await db.execute("""
                    UPDATE system_settings
                    SET value = ?, updated_at = ?
                    WHERE key = 'ip_abuse_detection_enabled'
                """, (ip_abuse_enabled, now))

                await db.execute("""
                    UPDATE system_settings
                    SET value = ?, updated_at = ?
                    WHERE key = 'ip_abuse_accounts_threshold'
                """, (ip_abuse_threshold, now))

                await db.execute("""
                    UPDATE system_settings
                    SET value = ?, updated_at = ?
                    WHERE key = 'ip_abuse_days_window'
                """, (ip_abuse_days, now))

                await db.commit()
                logger.info(f"IP滥用检测配置已重置为ENV默认值: enabled={ip_abuse_enabled}, threshold={ip_abuse_threshold}, days={ip_abuse_days}")
                return True
        except Exception as e:
            logger.error(f"重置IP滥用检测配置失败: {e}")
            return False

# 全局数据库管理器实例
db_manager = DatabaseManager()
task_model = TaskModel(db_manager)
page_result_model = PageResultModel(db_manager)
task_progress_model = TaskProgressModel(db_manager)
page_batch_model = PageBatchModel(db_manager)
ip_tracking_model = IPTrackingModel(db_manager)

async def init_database():
    """初始化数据库"""
    await db_manager.initialize()
    logger.info("数据库初始化完成")

async def cleanup_database():
    """清理数据库连接"""
    # 这里可以添加清理逻辑，比如关闭连接池
    logger.info("数据库清理完成")