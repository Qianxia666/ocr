# OCR

## 功能特性

### 核心功能
- **智能 OCR 识别**: 基于 AI 模型的高精度文字识别
- **多格式支持**: 支持图片（PNG、JPG 等）和 PDF 文档识别
- **Markdown 输出**: 自动将识别结果格式化为 Markdown 格式，支持数学公式（KaTeX）
- **批量处理**: 支持 PDF 多页文档的批量识别
- **异步处理**: 采用异步任务队列，支持大文件的后台处理

### 用户系统
- **用户注册/登录**: 完整的用户认证系统
- **配额管理**: 基于页数的配额管理系统
- **兑换码系统**: 支持通过兑换码增加配额
- **注册令牌**: 管理员可生成注册令牌控制用户注册
- **IP 防滥用**: 自动检测和防止 IP 地址滥用注册

### 管理功能
- **管理员后台**: 完整的后台管理系统
- **用户管理**: 查看、管理用户账户和配额
- **任务监控**: 实时监控 OCR 任务执行状态
- **系统设置**: 运行时配置调整（并发数、文件大小限制等）
- **统计数据**: 用户统计、任务统计、系统资源监控

### 高级特性
- **WebSocket 实时通信**: 实时推送任务进度和结果
- **内容审查**: 可选的内容道德审查功能
- **Cloudflare Turnstile**: 可选的人机验证
- **Docker 部署**: 完整的 Docker 和 Docker Compose 支持
- **健康检查**: 容器健康检查端点
- **错误恢复**: 自动重试和错误恢复机制
- **批量任务**: 支持一次提交多个文件

## 项目链接

- **当前仓库**: [https://github.com/Qianxia666/ocr](https://github.com/Qianxia666/ocr)
- **原始仓库**: [https://github.com/gua12345/web_oai_ocr](https://github.com/gua12345/web_oai_ocr)

## 部署指南

### 方式一：Docker Compose 部署（推荐）

1. **克隆项目**
```bash
git clone https://github.com/Qianxia666/ocr.git
cd ocr
```

2. **配置环境变量**
```bash
cp .env.example .env
# 编辑 .env 文件，配置必要的参数
```

必须配置的参数：
```env
# OpenAI API 配置
API_BASE_URL=https://api.openai.com
OPENAI_API_KEY=sk-your-api-key-here
MODEL=gpt-4o

# 访问密码（可选）
PASSWORD=your-password-here
```

3. **启动服务**
```bash
docker-compose up -d
```

4. **访问服务**
- 主页: `http://localhost:54188`
- 带密码保护: `http://localhost:54188/your-password-here`
- API 文档: `http://localhost:54188/docs`

### 方式二：Docker 部署

```bash
# 构建镜像
docker build -t ocr-service .

# 运行容器
docker run -d \
  --name ocr-service \
  -p 54188:54188 \
  -v $(pwd)/data:/app/data \
  -e OPENAI_API_KEY=sk-your-api-key-here \
  -e MODEL=gpt-4o \
  -e PASSWORD=your-password \
  ocr-service
```

### 方式三：本地部署

1. **安装依赖**
```bash
pip install -r requirements.txt
```

2. **配置环境变量**
```bash
cp .env.example .env
# 编辑 .env 文件
```

3. **启动服务**
```bash
python main.py
# 或指定端口
python main.py --port 8000 --host 0.0.0.0
```

## 配置说明

### 核心配置

| 参数 | 说明 | 默认值 |
|------|------|--------|
| `API_BASE_URL` | OpenAI API 地址 | `https://api.openai.com` |
| `OPENAI_API_KEY` | OpenAI API 密钥 | - |
| `MODEL` | 使用的模型 | `gpt-4o` |
| `PASSWORD` | 访问密码（可选） | - |
| `PORT` | 服务端口 | `54188` |

### 性能配置

| 参数 | 说明 | 默认值 |
|------|------|--------|
| `IMAGE_CONCURRENCY` | 图片 OCR 并发数 | `2` |
| `PDF_CONCURRENCY` | PDF OCR 并发数 | `2` |
| `MAX_RETRIES` | 最大重试次数 | `5` |
| `PDF_DPI` | PDF 渲染 DPI | `200` |
| `BATCH_SIZE` | 批处理大小 | `8` |

### 限制配置

| 参数 | 说明 | 默认值 |
|------|------|--------|
| `MAX_IMAGE_SIZE_MB` | 图片最大大小 (MB) | `50` |
| `MAX_PDF_SIZE_MB` | PDF 最大大小 (MB) | `200` |
| `MAX_PDF_PAGES` | PDF 最大页数 | `500` |

### 超时配置

| 参数 | 说明 | 默认值 |
|------|------|--------|
| `API_TIMEOUT` | API 调用超时 (秒) | `300` |
| `API_REQUEST_TIMEOUT` | API 请求超时 (秒) | `300` |
| `FRONTEND_POLL_TIMEOUT` | 前端轮询超时 (秒) | `300` |

### 安全配置

| 参数 | 说明 | 默认值 |
|------|------|--------|
| `TURNSTILE_ENABLED` | Turnstile 人机验证开关 | `false` |
| `TURNSTILE_SITE_KEY` | Turnstile Site Key | - |
| `TURNSTILE_SECRET_KEY` | Turnstile Secret Key | - |
| `IP_ABUSE_DETECTION_ENABLED` | IP 滥用检测开关 | `true` |
| `IP_ABUSE_ACCOUNTS_THRESHOLD` | IP 账户数阈值 | `3` |
| `IP_ABUSE_DAYS_WINDOW` | 检测时间窗口 (天) | `7` |

### 审查配置

| 参数 | 说明 | 默认值 |
|------|------|--------|
| `MODERATION_ENABLED` | 内容审查开关 | `true` |
| `MODERATION_API_BASE_URL` | 审查 API 地址 | 与 OCR 相同 |
| `MODERATION_API_KEY` | 审查 API 密钥 | 与 OCR 相同 |
| `MODERATION_MODEL` | 审查模型 | `gpt-4o` |

## 联系方式

如有问题或建议，请通过以下方式联系：

- GitHub Issues: [https://github.com/Qianxia666/ocr/issues](https://github.com/Qianxia666/ocr/issues)

---

**注意**: 本项目仅供学习和研究使用，请遵守当地法律法规和 OpenAI 使用条款。
