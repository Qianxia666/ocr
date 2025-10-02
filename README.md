# OCR

利用 OpenAI API 进行图片和 PDF 文档的 OCR 识别，支持异步任务处理、WebSocket 实时通信和完整的任务管理系统。

**项目地址**：[https://github.com/Qianxia666/ocr](https://github.com/Qianxia666/ocr)
**原作者地址**：[https://github.com/gua12345/web_oai_img2text](https://github.com/gua12345/web_oai_img2text)

---

## ✨ 主要特性

### 🎯 核心功能
- **图片 OCR 识别** - 支持多种图片格式的文字识别
- **PDF 文档处理** - 支持大型 PDF 文件的批量处理
- **Markdown/LaTeX 输出** - 自动格式化为 Markdown，支持数学公式（KaTeX）
- **异步任务处理** - 后台处理任务，不阻塞请求
- **WebSocket 实时通信** - 实时获取任务进度更新
- **任务持久化** - SQLite 数据库存储任务状态和结果

### 🚀 增强功能（相比原版）
- **完整的任务管理系统** - 任务队列、工作者池、进度追踪
- **错误恢复机制** - 自动重试、检查点恢复、任务容错
- **系统监控** - 资源监控、性能指标、健康检查
- **批量处理** - 支持批量上传和处理多个文件
- **RESTful API** - 完整的 REST API 接口
- **Docker 优化** - 完善的容器化支持和健康检查

---

## 📦 快速开始

### 方式一：Docker Compose 部署（推荐）

1. **克隆项目**
```bash
git clone https://github.com/Qianxia666/ocr.git
cd ocr
```

2. **配置环境变量**

创建 `.env` 文件：
```bash
# API 配置（必填）
API_BASE_URL=https://api.openai.com
OPENAI_API_KEY=sk-your-api-key-here
MODEL=gpt-4o

# 访问密码（可选，默认: pwd）
PASSWORD=your-custom-password

# 服务端口（默认: 54188）
PORT=54188
HOST_PORT=54188

# 并发和重试配置
CONCURRENCY=5
MAX_RETRIES=5

# PDF 处理配置
PDF_DPI=200
BATCH_SIZE=8

# 文件大小限制（单位: MB）
MAX_IMAGE_SIZE_MB=50
MAX_PDF_SIZE_MB=200
MAX_PDF_PAGES=500

# 前端配置
TITLE=OCR
FAVICON_URL=/static/favicon.ico
BACK_URL=
```

3. **启动服务**
```bash
docker-compose up -d
```

4. **访问服务**
- 主页面: `http://localhost:54188/`
- API 文档: `http://localhost:54188/docs`
- 健康检查: `http://localhost:54188/health`

### 方式二：源码运行

1. **安装依赖**
```bash
pip install -r requirements.txt
```

2. **配置环境变量**

创建 `.env` 文件（参考上面的配置）

3. **运行服务**
```bash
python main.py --port 54188 --host 0.0.0.0
```

---

## 📖 使用教程

### 1️⃣ Web 界面使用

1. **访问页面**
   - 打开浏览器访问 `http://your-server:54188/your-password`

2. **上传文件**
   - 点击上传区域或拖拽文件
   - 支持图片格式：PNG、JPG、JPEG、GIF、BMP、WEBP
   - 支持 PDF 文档（最大 500 页）

3. **查看进度**
   - 实时显示处理进度和状态
   - 支持批次进度追踪（PDF）
   - 自动保存处理结果

4. **导出结果**
   - 复制 Markdown 内容
   - 下载为 TXT/MD/JSON 格式

### 2️⃣ API 使用

#### 提交图片任务
```bash
curl -X POST "http://localhost:54188/pwd/api/async/image" \
  -F "file=@/path/to/image.png"
```

响应：
```json
{
  "status": "success",
  "task_id": "task_xxx",
  "message": "图片任务已提交",
  "estimated_time": 30,
  "endpoints": {
    "status": "/api/tasks/task_xxx",
    "result": "/api/tasks/task_xxx/result",
    "websocket": "/ws"
  }
}
```

#### 提交 PDF 任务
```bash
curl -X POST "http://localhost:54188/pwd/api/async/pdf" \
  -F "file=@/path/to/document.pdf"
```

#### 查询任务状态
```bash
curl "http://localhost:54188/pwd/api/tasks/{task_id}"
```

#### 获取任务结果
```bash
curl "http://localhost:54188/pwd/api/tasks/{task_id}/result"
```

#### 批量提交任务
```bash
curl -X POST "http://localhost:54188/pwd/api/async/batch" \
  -F "files=@image1.png" \
  -F "files=@image2.png" \
  -F "files=@document.pdf"
```

#### 导出结果
```bash
# JSON 格式
curl "http://localhost:54188/pwd/api/tasks/{task_id}/export?format=json" -O

# Markdown 格式
curl "http://localhost:54188/pwd/api/tasks/{task_id}/export?format=md" -O

# 纯文本格式
curl "http://localhost:54188/pwd/api/tasks/{task_id}/export?format=txt" -O
```

### 3️⃣ WebSocket 实时通信

```javascript
const ws = new WebSocket('ws://localhost:54188/pwd/ws?client_id=your_client_id');

ws.onmessage = (event) => {
  const data = JSON.parse(event.data);
  console.log('收到消息:', data);

  // 处理不同类型的消息
  switch(data.type) {
    case 'task_progress':
      console.log('任务进度:', data.progress);
      break;
    case 'task_completed':
      console.log('任务完成:', data.task_id);
      break;
    case 'page_completed':
      console.log('页面完成:', data.page_number);
      break;
  }
};

// 订阅任务更新
ws.send(JSON.stringify({
  type: 'subscribe',
  task_id: 'task_xxx'
}));
```

---

## 🔄 相比原版的主要变化

### 架构升级

| 功能模块 | 原版 | 增强版 |
|---------|------|--------|
| 任务处理 | 同步处理，长时间阻塞 | 异步任务队列，后台处理 |
| 进度反馈 | 无实时进度 | WebSocket 实时进度推送 |
| 错误处理 | 简单重试 | 多级恢复策略、检查点机制 |
| 数据持久化 | 无 | SQLite 数据库持久化 |
| 监控告警 | 无 | 系统资源监控、健康检查 |
| API 接口 | 仅基础上传 | 完整 RESTful API |

### 核心模块说明

#### 1. **任务管理系统** (`core/task_manager.py`)
- 统一的任务生命周期管理
- 优先级队列调度
- 任务状态追踪和查询
- 支持临时文件处理，减少内存占用

#### 2. **WebSocket 通信** (`core/websocket_manager.py`)
- 实时双向通信
- 任务订阅机制
- 心跳检测和自动重连
- 多客户端连接管理

#### 3. **页面处理器** (`core/page_processor.py`)
- PDF 页面批量处理
- 并发限制和资源控制
- 页面级别的错误恢复

#### 4. **进度追踪器** (`core/progress_tracker.py`)
- 细粒度进度计算
- 批次处理进度
- ETA 时间估算

#### 5. **错误处理器** (`core/error_handler.py`)
- 多级重试策略
- 错误分类和统计
- 降级处理

#### 6. **恢复管理器** (`core/recovery_manager.py`)
- 检查点机制
- 任务断点续传
- 状态恢复

#### 7. **系统监控** (`core/system_monitor.py`)
- CPU/内存/磁盘监控
- API 性能指标
- 健康状态评估

#### 8. **超时管理** (`core/timeout_manager.py`)
- 分级超时控制（页面/任务/全局）
- 自动超时检测和处理
- 超时统计

### 新增 API 端点

| 端点 | 方法 | 说明 |
|------|------|------|
| `/api/async/image` | POST | 异步图片任务提交 |
| `/api/async/pdf` | POST | 异步 PDF 任务提交 |
| `/api/async/batch` | POST | 批量任务提交 |
| `/api/tasks/{task_id}` | GET | 获取任务信息 |
| `/api/tasks/{task_id}/result` | GET | 获取任务结果 |
| `/api/tasks/{task_id}/pages` | GET | 获取任务分页列表 |
| `/api/tasks/{task_id}/progress` | GET | 获取详细进度 |
| `/api/tasks/{task_id}/export` | GET | 导出任务结果 |
| `/api/tasks/batch/status` | POST | 批量状态查询 |
| `/api/system/stats` | GET | 系统统计信息 |
| `/api/system/health` | GET | 系统健康状态 |
| `/api/system/metrics` | GET | 系统监控指标 |
| `/health` 或 `/healthz` | GET | 容器健康检查（无需密码） |
| `/ws` | WebSocket | WebSocket 连接端点 |

### 数据库设计

```
tasks                 - 任务主表
├── task_id          - 任务 ID
├── status           - 任务状态
├── task_type        - 任务类型（image/pdf）
├── file_name        - 文件名
├── total_pages      - 总页数
├── created_at       - 创建时间
└── completed_at     - 完成时间

page_results          - 页面结果表
├── page_id          - 页面 ID
├── task_id          - 任务 ID
├── page_number      - 页码
├── status           - 处理状态
├── content          - OCR 内容
└── processing_time  - 处理时长

task_progress         - 任务进度表
├── task_id          - 任务 ID
├── progress         - 进度百分比
├── current_page     - 当前页码
└── eta_seconds      - 预计剩余时间

page_batches          - 批次管理表
├── batch_id         - 批次 ID
├── task_id          - 任务 ID
├── batch_number     - 批次编号
└── status           - 批次状态
```

---

## 🔧 环境变量配置

| 变量名 | 说明 | 默认值 |
|--------|------|--------|
| `API_BASE_URL` | OpenAI API 地址 | `https://api.openai.com` |
| `OPENAI_API_KEY` | OpenAI API 密钥 | `sk-111111111` |
| `MODEL` | 使用的模型 | `gpt-4o` |
| `PASSWORD` | 访问密码 | `pwd` |
| `PORT` | 服务端口 | `54188` |
| `CONCURRENCY` | 最大并发数 | `5` |
| `MAX_RETRIES` | 最大重试次数 | `5` |
| `PDF_DPI` | PDF 渲染 DPI | `200` |
| `BATCH_SIZE` | 批处理大小 | `8` |
| `MAX_IMAGE_SIZE_MB` | 图片最大大小（MB） | `50` |
| `MAX_PDF_SIZE_MB` | PDF 最大大小（MB） | `200` |
| `MAX_PDF_PAGES` | PDF 最大页数 | `500` |
| `TITLE` | 网站标题 | `呱呱的oai图转文` |
| `FAVICON_URL` | 网站图标 URL | `/static/favicon.ico` |
| `BACK_URL` | 后端服务地址（用于绕过 CF 100s 限制） | 空 |

---

## ⚠️ 注意事项

### Cloudflare 100 秒超时限制

如果使用 Cloudflare CDN，会受到 100 秒请求超时限制。解决方案：

1. **不使用 Cloudflare CDN**（推荐）
2. **使用多域名反代**
   - 准备一个不使用 Cloudflare 的域名
   - 反代到内网 54188 端口
   - 设置 `BACK_URL` 环境变量为该域名

```bash
BACK_URL=https://your-non-cf-domain.com
```

### Docker 部署建议

1. **数据持久化**
   - 确保映射 `./data` 卷保存数据库文件
   - 定期备份数据库文件

2. **资源限制**
   - 根据实际情况调整 CPU 和内存限制
   - 建议至少分配 512MB 内存

3. **健康检查**
   - 使用 `/health` 端点进行健康检查
   - 该端点无需密码认证

4. **日志管理**
   - 配置日志轮转避免磁盘占满
   - 默认保留最近 3 个日志文件，每个最大 10MB

---

## 🛠️ 开发和调试

### 开发模式运行
```bash
# 安装开发依赖
pip install -r requirements.txt

# 运行开发服务器
python main.py --port 54188

# 查看日志（仅显示重要日志）
# 日志已配置为 INFO 级别，过滤掉轮询请求
```

### 调试技巧

1. **查看系统状态**
```bash
curl http://localhost:54188/pwd/api/system/stats
```

2. **监控系统指标**
```bash
curl http://localhost:54188/pwd/api/system/metrics
```

3. **查看任务列表**
```bash
curl http://localhost:54188/pwd/api/tasks
```

4. **查看特定任务详情**
```bash
curl http://localhost:54188/pwd/api/tasks/{task_id}/progress
```

---

## 🔐 安全建议

1. **修改默认密码**
   - 不要使用默认的 `pwd` 密码
   - 使用强密码保护服务

2. **API Key 保护**
   - 不要将 API Key 提交到代码仓库
   - 使用 `.env` 文件管理敏感信息
   - `.env` 文件已在 `.gitignore` 中

3. **网络访问控制**
   - 使用防火墙限制访问
   - 考虑使用反向代理（Nginx/Caddy）
   - 启用 HTTPS 加密传输

4. **文件大小限制**
   - 合理设置文件大小限制
   - 防止恶意大文件攻击

---

## 📊 性能优化

### 已实现的优化

1. **内存优化**
   - PDF 流式处理，避免一次性加载
   - 临时文件存储，减少内存占用
   - 页面处理后立即释放资源

2. **并发优化**
   - 可配置的并发数控制
   - 批量处理机制
   - 任务优先级队列

3. **错误恢复**
   - 检查点机制
   - 自动重试
   - 降级处理

4. **数据库优化**
   - 索引优化
   - 连接池管理
   - 批量写入

---

## 🤝 贡献指南

欢迎提交 Issue 和 Pull Request！

1. Fork 本项目
2. 创建特性分支 (`git checkout -b feature/AmazingFeature`)
3. 提交更改 (`git commit -m 'Add some AmazingFeature'`)
4. 推送到分支 (`git push origin feature/AmazingFeature`)
5. 开启 Pull Request

---

## 📄 许可证

本项目基于原项目 [gua12345/oai_img2text](https://github.com/gua12345/oai_img2text) 进行增强开发。

---

## 🙏 致谢

- 原项目作者：[gua12345](https://github.com/gua12345)
- OpenAI API 提供 OCR 能力
- 所有贡献者和使用者

---

## 📮 联系方式

- 问题反馈：[GitHub Issues](https://github.com/你的用户名/web_oai_ocr/issues)
- 功能建议：[GitHub Discussions](https://github.com/你的用户名/web_oai_ocr/discussions)
