# OCR 项目代码审计与交付验证

日期：2026-09-25

## 审计范围

- Python/配置总代码约 1.85 万行。
- 对全部 Python 文件执行语法编译和 AST 静态检查。
- 对 26 个业务 Python 模块执行逐模块导入验证。
- 重点人工审计：启动生命周期、SQLite 数据库、用户认证与 Session、配额、兑换码、注册令牌并发、任务队列、页面处理器、WebSocket、运行时设置、Docker 启动路径。

## 本次修复

1. 新增 `core/aiosqlite_compat.py`：优先使用官方 `aiosqlite`；依赖暂不可用时使用受限但兼容本项目调用面的异步 SQLite fallback，使离线/受限环境仍可启动和验证。
2. 新增 `core/password_hashing.py`：优先使用 bcrypt；缺少 bcrypt 时使用 `hashlib.scrypt` 安全 fallback。已有 bcrypt 哈希在缺少 bcrypt 时不会被错误接受。
3. `models/database.py`、`models/user.py`、`core/routes/system_routes.py` 接入上述兼容层。
4. 修复静态资源目录依赖当前工作目录的问题：`main.py` 现在使用基于源码文件位置的绝对路径，支持从任意 cwd 启动。
5. 新增 `tests/test_smoke.py`：覆盖密码哈希、数据库初始化、首用户管理员、普通用户认证、配额扣减边界、兑换码单次消费、注册令牌并发原子性。

## 实际运行验证

- `python -m compileall -q .`：通过。
- AST 静态检查：0 个裸 `except`，0 个可变默认参数问题。
- 26 个 Python 模块逐一 import：全部通过。
- FastAPI 路由：95 个；重复 method/path：0。
- Pytest：3 passed。
- 完整 lifespan：数据库、认证、WebSocket、页面处理器、图片/PDF 双工作池启动成功并完整关闭。
- SQLite：启动时成功创建/识别 15 张表。
- HTTP 集成链路：前端配置 200 → 注册 200 → 登录 200 + Session Cookie → 当前用户 200 → 配额 200 → 登出 200 → 登出后访问 401。
- 并发注册令牌：两个并发消费者针对 max_uses=1 的令牌，仅 1 个成功。
- 从非项目目录 `/tmp` 导入应用：成功，验证静态资源绝对路径修复。

## 环境说明

当前执行环境禁止外网访问，因此无法完成真实 OpenAI OCR 请求；页面处理器的 API 连通性探测按设计记录失败但不会阻止服务启动。业务服务生命周期、数据库、认证、队列、HTTP API 均已实际运行验证。

正常部署仍建议执行 `pip install -r requirements.txt`，这样会优先使用官方 `aiosqlite` 和 `bcrypt`。

## 2026-10-28 第二轮修复与验证（本次交付）

### 修复的真实缺陷

1. **管理端任务搜索越权**：`user_id` 筛选与内容搜索原先用 `OR` 拼接，可按内容搜到其他用户任务；改为 `AND` 组合并同步修正分页 `total`（用户名/内容搜索均一致）。
2. **超长密码导致 bcrypt 崩溃**：bcrypt 5.0 对 >72 字节密码直接抛错。新增 `core/password_hashing.py`：超长密码先 SHA-256 再 bcrypt，并以 `$bcrypt-sha256$` 前缀标记；保留旧 bcrypt 与 scrypt 后备兼容。
3. **密码哈希阻塞事件循环**：`create_user` / `authenticate` 改用 `asyncio.to_thread` 执行 CPU 密集型哈希。
4. **启动覆盖管理后台配置**：IP 滥用检测配置原为每次启动 `UPDATE` 覆盖，改为仅 `INSERT OR IGNORE` 补齐缺失项。
5. **重启后任务失败不退配额**：`TaskQueue.restore_from_database` 新增 `on_abandoned` 回调，未完成任务标记失败时按未处理页数退还配额（幂等）。
6. **不存在记录误报成功**：兑换码/注册令牌的启停与删除操作增加 `rowcount` 校验，记录不存在时返回 `False`。
7. **协议接受状态返回 `None`**：`has_user_accepted_current` 统一返回 `bool`。
8. **兑换并发重复写入**：为 `redemption_history(user_id, code)` 增加唯一索引。
9. **配额调整校验**：用户不存在返回 404；新配额低于已使用页数返回 400。
10. **同步 `/process/image` 未校验文件类型**：`text/plain` 等非图片会扣配额并入队；现与异步接口一致，返回 400 且不扣配额。
11. **注册密码上限**：超过 128 字符直接返回 400，避免 bcrypt 边界问题。
12. **代码清理**：修复 `core/task_manager.py` 占位符乱码日志；清理 52 处未使用导入与 2 处被函数参数遮蔽的重复导入。

### 本次验证结果

- `python -m compileall -q main.py core models tests`：通过。
- `ruff check --select F821,F811,F823,F401`：0 遗留（F841 剩余项均为鉴权守卫丢弃返回值或纯计算中间变量，无功能影响）。
- `pytest tests/ -q`：**21 passed**，覆盖：
  - `test_smoke.py`（3）：密码哈希往返、数据库/认证/配额/兑换码、注册令牌并发原子性。
  - `test_bugfixes.py`（7）：超长密码、scrypt 后备、搜索越权与分页、配置重启保留、不存在记录返回 False、协议状态返回 bool、兑换唯一约束。
  - `test_http_api.py`（11）：基础页面/静态资源/404/鉴权、注册登录、密码上限、兑换码全流程、用户管理、搜索隔离、配额下限、注册令牌流程、上传类型校验、WebSocket ping/pong、登出失效。
- 完整 lifespan（数据库、认证、WebSocket、页面处理器、双工作池）随 TestClient 启动/关闭，无异常。

