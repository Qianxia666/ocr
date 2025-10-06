import asyncio
import json
import logging
import weakref
from datetime import datetime, timezone
from typing import Dict, Any, Set, Optional, List, Callable
from fastapi import WebSocket, WebSocketDisconnect
import uuid

logger = logging.getLogger(__name__)

class WebSocketConnection:
    """WebSocket连接包装类"""
    
    def __init__(self, websocket: WebSocket, client_id: str):
        self.websocket = websocket
        self.client_id = client_id
        self.connected_at = datetime.now(timezone.utc)
        self.last_heartbeat = datetime.now(timezone.utc)
        self.subscribed_tasks: Set[str] = set()
        self.is_active = True
    
    async def send_message(self, message: Dict[str, Any]) -> bool:
        """发送消息给客户端"""
        try:
            if self.is_active:
                await self.websocket.send_text(json.dumps(message))
                return True
            return False
        except (ConnectionResetError, ConnectionError, RuntimeError,
                WebSocketDisconnect) as e:
            logger.debug(f"客户端连接已断开 {self.client_id}: {e}")
            self.is_active = False
            return False
        except Exception as e:
            logger.error(f"发送消息失败 {self.client_id}: {e}")
            self.is_active = False
            return False
    
    async def close(self, code: int = 1000, reason: str = "Normal closure"):
        """关闭WebSocket连接"""
        try:
            if self.is_active:
                # 检查连接是否仍然有效
                if hasattr(self.websocket, 'client_state') and \
                   self.websocket.client_state.name in ['CONNECTED', 'CONNECTING']:
                    await self.websocket.close(code=code, reason=reason)
        except (ConnectionResetError, ConnectionError, RuntimeError) as e:
            # Windows: ConnectionResetError (WinError 10054)
            # Linux: BrokenPipeError
            # asyncio: RuntimeError (连接已关闭)
            logger.debug(f"连接已断开,忽略关闭错误 {self.client_id}: {e}")
        except Exception as e:
            logger.error(f"关闭连接失败 {self.client_id}: {e}")
        finally:
            self.is_active = False
    
    def update_heartbeat(self):
        """更新心跳时间"""
        self.last_heartbeat = datetime.now(timezone.utc)
    
    def subscribe_task(self, task_id: str):
        """订阅任务更新"""
        self.subscribed_tasks.add(task_id)
    
    def unsubscribe_task(self, task_id: str):
        """取消订阅任务更新"""
        self.subscribed_tasks.discard(task_id)

class WebSocketManager:
    """WebSocket连接管理器"""
    
    def __init__(self, heartbeat_interval: int = 30, connection_timeout: int = 300):
        """
        初始化WebSocket管理器
        :param heartbeat_interval: 心跳间隔（秒）
        :param connection_timeout: 连接超时时间（秒）
        """
        self.connections: Dict[str, WebSocketConnection] = {}
        self.task_subscribers: Dict[str, Set[str]] = {}  # task_id -> client_ids
        self.message_handlers: Dict[str, Callable] = {}
        self.heartbeat_interval = heartbeat_interval
        self.connection_timeout = connection_timeout
        self._lock = asyncio.Lock()
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._running = False
        
        # 注册默认消息处理器
        self._register_default_handlers()
        
        logger.info("WebSocket管理器初始化完成")
    
    def _register_default_handlers(self):
        """注册默认消息处理器"""
        self.message_handlers.update({
            'ping': self._handle_ping,
            'subscribe_task': self._handle_subscribe_task,
            'unsubscribe_task': self._handle_unsubscribe_task,
            'get_task_status': self._handle_get_task_status
        })
    
    async def start(self):
        """启动WebSocket管理器"""
        if self._running:
            return
        
        self._running = True
        # 启动心跳检测任务
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())
        logger.info("WebSocket管理器已启动")
    
    async def stop(self):
        """停止WebSocket管理器"""
        if not self._running:
            return
        
        self._running = False
        
        # 停止心跳检测
        if self._heartbeat_task:
            self._heartbeat_task.cancel()
            try:
                await self._heartbeat_task
            except asyncio.CancelledError:
                pass
        
        # 关闭所有连接
        await self.disconnect_all()
        logger.info("WebSocket管理器已停止")
    
    async def connect(self, websocket: WebSocket, client_id: Optional[str] = None) -> str:
        """
        建立WebSocket连接
        :param websocket: WebSocket实例
        :param client_id: 客户端ID，如果不提供则自动生成
        :return: 客户端ID
        """
        if client_id is None:
            client_id = str(uuid.uuid4())
        
        async with self._lock:
            # 如果客户端已存在，先关闭旧连接
            if client_id in self.connections:
                await self._disconnect_client(client_id)
            
            # 创建新连接
            connection = WebSocketConnection(websocket, client_id)
            self.connections[client_id] = connection
        
        logger.info(f"WebSocket连接已建立: {client_id}")
        
        # 发送连接成功消息
        await self.send_to_client(client_id, {
            'type': 'connection_established',
            'client_id': client_id,
            'timestamp': datetime.now(timezone.utc).isoformat()
        })
        
        return client_id
    
    async def disconnect(self, client_id: str):
        """断开指定客户端连接"""
        async with self._lock:
            await self._disconnect_client(client_id)
    
    async def _disconnect_client(self, client_id: str):
        """内部断开客户端连接方法"""
        if client_id in self.connections:
            connection = self.connections[client_id]
            
            # 清理任务订阅
            for task_id in connection.subscribed_tasks:
                if task_id in self.task_subscribers:
                    self.task_subscribers[task_id].discard(client_id)
                    if not self.task_subscribers[task_id]:
                        del self.task_subscribers[task_id]
            
            # 关闭连接
            await connection.close()
            del self.connections[client_id]
            
            logger.info(f"WebSocket连接已断开: {client_id}")
    
    async def disconnect_all(self):
        """断开所有客户端连接"""
        async with self._lock:
            client_ids = list(self.connections.keys())
            for client_id in client_ids:
                await self._disconnect_client(client_id)
    
    async def send_to_client(self, client_id: str, message: Dict[str, Any]) -> bool:
        """发送消息给指定客户端"""
        async with self._lock:
            if client_id in self.connections:
                connection = self.connections[client_id]
                success = await connection.send_message(message)
                if not success:
                    # 发送失败，移除连接
                    await self._disconnect_client(client_id)
                return success
            return False
    
    async def broadcast(self, message: Dict[str, Any], exclude_clients: Optional[Set[str]] = None) -> int:
        """
        广播消息给所有连接的客户端
        :param message: 要发送的消息
        :param exclude_clients: 排除的客户端ID集合
        :return: 成功发送的客户端数量
        """
        exclude_clients = exclude_clients or set()
        sent_count = 0
        
        async with self._lock:
            client_ids = list(self.connections.keys())
        
        for client_id in client_ids:
            if client_id not in exclude_clients:
                if await self.send_to_client(client_id, message):
                    sent_count += 1
        
        return sent_count
    
    async def send_to_task_subscribers(self, task_id: str, message: Dict[str, Any]) -> int:
        """
        发送消息给订阅了特定任务的客户端
        :param task_id: 任务ID
        :param message: 要发送的消息
        :return: 成功发送的客户端数量
        """
        sent_count = 0
        
        async with self._lock:
            if task_id in self.task_subscribers:
                subscriber_ids = list(self.task_subscribers[task_id])
            else:
                return 0
        
        for client_id in subscriber_ids:
            if await self.send_to_client(client_id, message):
                sent_count += 1
        
        return sent_count
    
    async def handle_client_message(self, client_id: str, message: str):
        """处理客户端消息"""
        try:
            data = json.loads(message)
            message_type = data.get('type')
            
            if message_type in self.message_handlers:
                handler = self.message_handlers[message_type]
                await handler(client_id, data)
            else:
                logger.warning(f"未知消息类型: {message_type} from {client_id}")
                await self.send_to_client(client_id, {
                    'type': 'error',
                    'message': f'未知消息类型: {message_type}',
                    'timestamp': datetime.now(timezone.utc).isoformat()
                })
        
        except json.JSONDecodeError:
            logger.error(f"JSON解析失败: {message} from {client_id}")
            await self.send_to_client(client_id, {
                'type': 'error',
                'message': 'JSON格式错误',
                'timestamp': datetime.now(timezone.utc).isoformat()
            })
        except Exception as e:
            logger.error(f"处理客户端消息失败 {client_id}: {e}")
            await self.send_to_client(client_id, {
                'type': 'error',
                'message': '服务器内部错误',
                'timestamp': datetime.now(timezone.utc).isoformat()
            })
    
    async def _handle_ping(self, client_id: str, data: Dict[str, Any]):
        """处理ping消息"""
        async with self._lock:
            if client_id in self.connections:
                self.connections[client_id].update_heartbeat()
        
        await self.send_to_client(client_id, {
            'type': 'pong',
            'timestamp': datetime.now(timezone.utc).isoformat()
        })
    
    async def _handle_subscribe_task(self, client_id: str, data: Dict[str, Any]):
        """处理任务订阅消息"""
        task_id = data.get('task_id')
        if not task_id:
            await self.send_to_client(client_id, {
                'type': 'error',
                'message': '缺少task_id参数',
                'timestamp': datetime.now(timezone.utc).isoformat()
            })
            return
        
        async with self._lock:
            if client_id in self.connections:
                connection = self.connections[client_id]
                connection.subscribe_task(task_id)
                
                if task_id not in self.task_subscribers:
                    self.task_subscribers[task_id] = set()
                self.task_subscribers[task_id].add(client_id)
        
        await self.send_to_client(client_id, {
            'type': 'task_subscribed',
            'task_id': task_id,
            'timestamp': datetime.now(timezone.utc).isoformat()
        })
        
        logger.info(f"客户端 {client_id} 订阅任务 {task_id}")
    
    async def _handle_unsubscribe_task(self, client_id: str, data: Dict[str, Any]):
        """处理取消任务订阅消息"""
        task_id = data.get('task_id')
        if not task_id:
            await self.send_to_client(client_id, {
                'type': 'error',
                'message': '缺少task_id参数',
                'timestamp': datetime.now(timezone.utc).isoformat()
            })
            return
        
        async with self._lock:
            if client_id in self.connections:
                connection = self.connections[client_id]
                connection.unsubscribe_task(task_id)
                
                if task_id in self.task_subscribers:
                    self.task_subscribers[task_id].discard(client_id)
                    if not self.task_subscribers[task_id]:
                        del self.task_subscribers[task_id]
        
        await self.send_to_client(client_id, {
            'type': 'task_unsubscribed',
            'task_id': task_id,
            'timestamp': datetime.now(timezone.utc).isoformat()
        })
        
        logger.info(f"客户端 {client_id} 取消订阅任务 {task_id}")
    
    async def _handle_get_task_status(self, client_id: str, data: Dict[str, Any]):
        """处理获取任务状态请求"""
        task_id = data.get('task_id')
        if not task_id:
            await self.send_to_client(client_id, {
                'type': 'error',
                'message': '缺少task_id参数',
                'timestamp': datetime.now(timezone.utc).isoformat()
            })
            return
        
        try:
            # 集成任务管理器获取实际任务状态
            from core.task_manager import task_manager
            if task_manager:
                task_info = await task_manager.get_task_info(task_id)
                await self.send_to_client(client_id, {
                    'type': 'task_status_response',
                    'task_id': task_id,
                    'task_info': task_info,
                    'timestamp': datetime.now(timezone.utc).isoformat()
                })
            else:
                await self.send_to_client(client_id, {
                    'type': 'error',
                    'message': '任务管理器未初始化',
                    'timestamp': datetime.now(timezone.utc).isoformat()
                })
        except Exception as e:
            await self.send_to_client(client_id, {
                'type': 'error',
                'message': f'获取任务状态失败: {str(e)}',
                'timestamp': datetime.now(timezone.utc).isoformat()
            })
    
    async def _heartbeat_loop(self):
        """心跳检测循环"""
        logger.info("心跳检测循环已启动")
        
        while self._running:
            try:
                await asyncio.sleep(self.heartbeat_interval)
                await self._check_connections()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"心跳检测异常: {e}")
    
    async def _check_connections(self):
        """检查连接状态并清理超时连接"""
        current_time = datetime.now(timezone.utc)
        to_disconnect = []
        
        async with self._lock:
            for client_id, connection in self.connections.items():
                if not connection.is_active:
                    to_disconnect.append(client_id)
                    continue
                
                # 检查连接超时
                time_since_heartbeat = (current_time - connection.last_heartbeat).total_seconds()
                if time_since_heartbeat > self.connection_timeout:
                    logger.warning(f"客户端 {client_id} 连接超时")
                    to_disconnect.append(client_id)
        
        # 断开超时连接
        for client_id in to_disconnect:
            await self.disconnect(client_id)
    
    def register_message_handler(self, message_type: str, handler: Callable):
        """注册自定义消息处理器"""
        self.message_handlers[message_type] = handler
        logger.info(f"注册消息处理器: {message_type}")
    
    def get_connection_stats(self) -> Dict[str, Any]:
        """获取连接统计信息"""
        current_time = datetime.now(timezone.utc)
        
        active_connections = len(self.connections)
        total_subscriptions = sum(len(subscribers) for subscribers in self.task_subscribers.values())
        
        return {
            'active_connections': active_connections,
            'total_task_subscriptions': total_subscriptions,
            'unique_subscribed_tasks': len(self.task_subscribers),
            'heartbeat_interval': self.heartbeat_interval,
            'connection_timeout': self.connection_timeout,
            'running': self._running,
            'timestamp': current_time.isoformat()
        }
    
    def get_client_info(self, client_id: str) -> Optional[Dict[str, Any]]:
        """获取客户端信息"""
        if client_id in self.connections:
            connection = self.connections[client_id]
            return {
                'client_id': client_id,
                'connected_at': connection.connected_at.isoformat(),
                'last_heartbeat': connection.last_heartbeat.isoformat(),
                'subscribed_tasks': list(connection.subscribed_tasks),
                'is_active': connection.is_active
            }
        return None

# 全局WebSocket管理器实例
websocket_manager = WebSocketManager()

async def init_websocket_manager():
    """初始化WebSocket管理器"""
    await websocket_manager.start()
    logger.info("全局WebSocket管理器初始化完成")

async def shutdown_websocket_manager():
    """关闭WebSocket管理器"""
    await websocket_manager.stop()
    logger.info("全局WebSocket管理器已关闭")