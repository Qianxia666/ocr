# OCR

利用 OpenAI API 进行图片和 PDF 文档的 OCR 识别，支持异步任务处理、实时通信和完整用户机制的任务管理系统。

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
- **批量处理** - 支持处理多个文件
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
   - 支持 PDF 文档

3. **查看进度**
   - 实时显示处理进度和状态
   - 支持批次进度追踪（PDF）
   - 自动保存处理结果

4. **导出结果**
   - 复制 Markdown 内容



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
curl http://localhost:54188/your-password/api/system/stats
```

2. **监控系统指标**
```bash
curl http://localhost:54188/your-password/api/system/metrics
```

3. **查看任务列表**
```bash
curl http://localhost:54188/your-password/api/tasks
```

4. **查看特定任务详情**
```bash
curl http://localhost:54188/your-password/api/tasks/{task_id}/progress
```

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

- 问题反馈：[GitHub Issues](https://github.com/Qianxia666/ocr/issues)
- 功能建议：[GitHub Discussions](https://github.com/Qianxia666/ocr/discussions)



