import base64
from dotenv import load_dotenv
from fastapi.middleware.cors import CORSMiddleware
import fitz  # PyMuPDF
import logging
from io import BytesIO
from PIL import Image
import aiohttp
import asyncio
from fastapi import FastAPI, Request, UploadFile, Query, HTTPException, WebSocket, WebSocketDisconnect, Response
from fastapi.responses import JSONResponse, HTMLResponse, StreamingResponse, RedirectResponse
from starlette.exceptions import HTTPException as StarletteHTTPException
from typing import Optional
from contextlib import asynccontextmanager
import os
import json
from datetime import datetime
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles

# 配置日志记录 - 支持UTF-8编码
import sys
if sys.platform == 'win32':
    # Windows系统配置UTF-8编码
    try:
        # 设置控制台代码页为UTF-8
        os.system('chcp 65001 > nul')
    except:
        pass

    # 配置日志使用UTF-8编码
    import codecs
    logging.basicConfig(
        level=logging.INFO,  # INFO级别仅显示重要日志
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(
                codecs.getwriter('utf-8')(sys.stdout.buffer, errors='replace')
            )
        ]
    )
else:
    # 非Windows系统使用默认配置
    logging.basicConfig(level=logging.INFO)  # INFO级别仅显示重要日志
logger = logging.getLogger(__name__)

# 加载 .env 文件（如果存在）
load_dotenv()

# 从环境变量中读取配置
API_BASE_URL = os.getenv("API_BASE_URL", "https://api.openai.com")
API_KEY = os.getenv("OPENAI_API_KEY", "sk-111111111")
MODEL = os.getenv("MODEL", "gpt-4o")
PASSWORD = os.getenv("PASSWORD", "pwd")
TURNSTILE_SITE_KEY = os.getenv("TURNSTILE_SITE_KEY", "")
TURNSTILE_SECRET_KEY = os.getenv("TURNSTILE_SECRET_KEY", "")

# 并发限制和重试机制
CONCURRENCY = int(os.getenv("CONCURRENCY", 5))  # 保留用于向后兼容
MAX_RETRIES = int(os.getenv("MAX_RETRIES", 5))
RETRY_DELAY = 0.5  # 重试延迟时间（秒）

# 图片和PDF独立并发配置
IMAGE_CONCURRENCY = int(os.getenv("IMAGE_CONCURRENCY", 2))  # 图片OCR并发数
PDF_CONCURRENCY = int(os.getenv("PDF_CONCURRENCY", 2))  # PDF OCR并发数

# PDF处理优化配置
PDF_DPI = int(os.getenv("PDF_DPI", 200))  # 降低DPI减少内存占用
BATCH_SIZE = int(os.getenv("BATCH_SIZE", 8))  # 批处理大小

# 文件大小限制配置
MAX_IMAGE_SIZE_MB = int(os.getenv("MAX_IMAGE_SIZE_MB", 50))  # 图片文件最大大小(MB)
MAX_PDF_SIZE_MB = int(os.getenv("MAX_PDF_SIZE_MB", 200))  # PDF文件最大大小(MB)
MAX_PDF_PAGES = int(os.getenv("MAX_PDF_PAGES", 500))  # PDF最大页数限制

# 运行时配置管理器
class RuntimeConfig:
    """运行时配置管理器 - 仅保存在内存中，重启后恢复默认值"""
    def __init__(self):
        # 从环境变量初始化默认值
        self.concurrency = int(os.getenv("CONCURRENCY", 5))
        self.max_retries = int(os.getenv("MAX_RETRIES", 5))
        self.image_concurrency = int(os.getenv("IMAGE_CONCURRENCY", 5))
        self.pdf_concurrency = int(os.getenv("PDF_CONCURRENCY", 5))
        self.pdf_dpi = int(os.getenv("PDF_DPI", 200))
        self.batch_size = int(os.getenv("BATCH_SIZE", 8))
        self.max_image_size_mb = int(os.getenv("MAX_IMAGE_SIZE_MB", 50))
        self.max_pdf_size_mb = int(os.getenv("MAX_PDF_SIZE_MB", 50))
        self.max_pdf_pages = int(os.getenv("MAX_PDF_PAGES", 50))
        # 超时配置
        self.api_timeout = int(os.getenv("API_TIMEOUT", 300))
        self.api_request_timeout = int(os.getenv("API_REQUEST_TIMEOUT", 300))
        self.frontend_poll_timeout = int(os.getenv("FRONTEND_POLL_TIMEOUT", 300))

    def get_all(self) -> dict:
        """获取所有配置"""
        return {
            "concurrency": self.concurrency,
            "max_retries": self.max_retries,
            "image_concurrency": self.image_concurrency,
            "pdf_concurrency": self.pdf_concurrency,
            "pdf_dpi": self.pdf_dpi,
            "batch_size": self.batch_size,
            "max_image_size_mb": self.max_image_size_mb,
            "max_pdf_size_mb": self.max_pdf_size_mb,
            "max_pdf_pages": self.max_pdf_pages,
            "api_timeout": self.api_timeout,
            "api_request_timeout": self.api_request_timeout,
            "frontend_poll_timeout": self.frontend_poll_timeout
        }

    def update(self, config: dict):
        """更新配置"""
        if "concurrency" in config:
            self.concurrency = int(config["concurrency"])
        if "max_retries" in config:
            self.max_retries = int(config["max_retries"])
        if "image_concurrency" in config:
            self.image_concurrency = int(config["image_concurrency"])
        if "pdf_concurrency" in config:
            self.pdf_concurrency = int(config["pdf_concurrency"])
        if "pdf_dpi" in config:
            self.pdf_dpi = int(config["pdf_dpi"])
        if "batch_size" in config:
            self.batch_size = int(config["batch_size"])
        if "max_image_size_mb" in config:
            self.max_image_size_mb = int(config["max_image_size_mb"])
        if "max_pdf_size_mb" in config:
            self.max_pdf_size_mb = int(config["max_pdf_size_mb"])
        if "max_pdf_pages" in config:
            self.max_pdf_pages = int(config["max_pdf_pages"])
        if "api_timeout" in config:
            self.api_timeout = int(config["api_timeout"])
            logger.info(f"更新API超时配置: {self.api_timeout}秒")
        if "api_request_timeout" in config:
            self.api_request_timeout = int(config["api_request_timeout"])
            logger.info(f"更新API请求超时配置: {self.api_request_timeout}秒")
        if "frontend_poll_timeout" in config:
            self.frontend_poll_timeout = int(config["frontend_poll_timeout"])
            logger.info(f"更新前端轮询超时配置: {self.frontend_poll_timeout}秒")

# 创建全局运行时配置实例
runtime_config = RuntimeConfig()

# Cloudflare Turnstile 配置读取函数
async def get_turnstile_enabled_status() -> tuple[bool, str]:
    """
    获取 Turnstile 验证开关状态
    返回: (是否启用, 配置来源)

    优先级: 数据库 > .env > 默认值(禁用) - 运行时设置优先
    """
    import os

    # 1. 优先检查数据库配置（运行时设置）
    try:
        from models.database import db_manager
        async with db_manager.get_connection() as db:
            async with db.execute(
                "SELECT value FROM system_settings WHERE key = 'turnstile_enabled'"
            ) as cursor:
                row = await cursor.fetchone()
                if row:
                    enabled = row[0].lower() == 'true'
                    return enabled, 'database'
    except Exception as e:
        logger.error(f"读取 Turnstile 数据库配置失败: {e}")

    # 2. 检查 .env 环境变量（重启后的默认值）
    env_value = os.getenv('TURNSTILE_ENABLED', '').lower()
    if env_value in ('true', 'false'):
        enabled = env_value == 'true'
        return enabled, 'env'

    # 3. 默认值(禁用)
    return False, 'default'

# Cloudflare Turnstile 验证函数
async def verify_turnstile_token(token: str, remote_ip: str = None) -> tuple[bool, str]:
    """
    验证 Cloudflare Turnstile token
    返回: (是否成功, 消息)
    """
    # 检查是否启用 Turnstile 验证
    enabled, source = await get_turnstile_enabled_status()

    if not enabled:
        logger.info(f"Turnstile 验证已禁用 (配置来源: {source})")
        return True, "验证已跳过"

    # 检查必要的密钥配置
    if not TURNSTILE_SECRET_KEY:
        logger.warning("Turnstile Secret Key 未配置,无法验证")
        return False, "人机验证配置错误,请联系管理员"

    if not token:
        return False, "缺少验证令牌"

    url = "https://challenges.cloudflare.com/turnstile/v0/siteverify"
    data = {
        "secret": TURNSTILE_SECRET_KEY,
        "response": token
    }
    if remote_ip:
        data["remoteip"] = remote_ip

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=data) as response:
                result = await response.json()

                if result.get("success"):
                    return True, "验证成功"
                else:
                    error_codes = result.get("error-codes", [])
                    logger.warning(f"Turnstile 验证失败: {error_codes}")
                    return False, "人机验证失败，请刷新页面重试"
    except Exception as e:
        logger.error(f"Turnstile 验证请求失败: {e}")
        return False, "验证服务暂时不可用，请稍后重试"

# 定义生命周期管理器
@asynccontextmanager
async def lifespan(app: FastAPI):
    """应用生命周期管理"""
    # 启动时执行
    try:
        # 初始化数据库
        from models.database import init_database, db_manager
        await init_database()
        logger.info("数据库初始化完成")

        # 初始化用户模型和认证管理器
        from models.user import UserModel, RedemptionCodeModel, RegistrationTokenModel
        from core.auth import init_auth_manager

        global user_model, redemption_code_model, registration_token_model, auth_manager_instance
        user_model = UserModel(db_manager)
        redemption_code_model = RedemptionCodeModel(db_manager)
        registration_token_model = RegistrationTokenModel(db_manager)
        auth_manager_instance = await init_auth_manager(user_model, db_manager)
        logger.info("用户系统初始化完成")

        # 初始化WebSocket管理器
        await init_websocket_manager()
        logger.info("WebSocket管理器初始化完成")

        # 准备API配置
        api_config = {
            'api_base_url': API_BASE_URL,
            'api_key': API_KEY,
            'model': MODEL,
            'concurrency': runtime_config.concurrency,
            'image_concurrency': runtime_config.image_concurrency,
            'pdf_concurrency': runtime_config.pdf_concurrency,
            'max_retries': runtime_config.max_retries,
            'retry_delay': RETRY_DELAY,
            'pdf_dpi': runtime_config.pdf_dpi,
            'api_timeout': runtime_config.api_timeout
        }

        # 初始化页面处理器
        from core.page_processor import init_page_processor, is_page_processor_ready, shutdown_page_processor
        logger.info("开始初始化页面处理器")

        # 准备静态API配置(不会变化的配置)
        static_api_config = {
            'api_base_url': API_BASE_URL,
            'api_key': API_KEY,
            'model': MODEL,
            'concurrency': runtime_config.concurrency,
            'batch_size': 4,
            'retry_delay': RETRY_DELAY,
            'health_check_interval': 300
        }

        # 先检查是否已经初始化，避免重复初始化
        if is_page_processor_ready():
            logger.info("页面处理器已经初始化且就绪，跳过初始化")
        else:
            # 安全地检查现有实例
            try:
                from core.page_processor import page_processor
                if page_processor is not None:
                    logger.info("发现旧的页面处理器实例，进行清理...")
                    await shutdown_page_processor()
                    await asyncio.sleep(0.5)  # 等待清理完成
            except Exception as e:
                logger.error(f"检查现有页面处理器实例时出错: {e}")

            # 进行初始化,传递runtime_config引用
            logger.warning(f"传递给页面处理器: runtime_config={runtime_config}, static_api_config={static_api_config}")
            await init_page_processor(runtime_config, static_api_config)

            # 验证页面处理器状态
            if is_page_processor_ready():
                logger.info("页面处理器初始化完成且就绪")
            else:
                logger.error("页面处理器初始化完成但未就绪")

        # 初始化任务管理器

        # 检查系统内存状况
        try:
            import psutil
            memory = psutil.virtual_memory()
            if memory.percent > 95:
                logger.error(f"系统内存使用率过高: {memory.percent}%")
                import gc
                gc.collect()
        except ImportError:
            logger.info("无法检查内存状况 (psutil未安装)")

        # 强制初始化任务管理器
        logger.info("开始强制初始化任务管理器...")
        init_success = False

        for attempt in range(3):  # 最多尝试3次
            try:
                logger.info(f"任务管理器初始化尝试 {attempt + 1}/3")

                # 在每次尝试前清理内存
                import gc
                gc.collect()

                # 强制重新初始化任务管理器,传递runtime_config引用
                await init_task_manager(runtime_config, api_config, websocket_manager)

                # 直接从模块获取最新的任务管理器实例
                from core.task_manager import task_manager as global_task_manager_instance

                # 更新本地全局变量
                global task_manager
                task_manager = global_task_manager_instance

                # 验证任务管理器状态
                if task_manager and hasattr(task_manager, '_initialized') and task_manager._initialized:
                    logger.info("任务管理器初始化成功（双队列模式）")
                    init_success = True

                    # 验证核心组件
                    if not (task_manager.image_worker_pool and task_manager.image_worker_pool._running):
                        logger.error("图片工作者池未正确启动")
                    if not (task_manager.pdf_worker_pool and task_manager.pdf_worker_pool._running):
                        logger.error("PDF工作者池未正确启动")

                    if not task_manager.image_task_queue:
                        logger.error("图片任务队列未正确初始化")
                    if not task_manager.pdf_task_queue:
                        logger.error("PDF任务队列未正确初始化")

                    break
                else:
                    logger.error("任务管理器初始化状态验证失败")

            except Exception as e:
                logger.error(f"任务管理器初始化失败 (尝试 {attempt + 1}/3): {e}")
                import traceback
                traceback.print_exc()

                # 如果不是最后一次尝试，等待一段时间再重试
                if attempt < 2:
                    logger.info("等待2秒后重试...")
                    await asyncio.sleep(2)

        # 如果所有尝试都失败，记录严重错误
        if not init_success:
            logger.critical("任务管理器初始化失败，API将返回503错误")
            task_manager = None

        # 初始化IP跟踪模型并启动定时检测任务
        from models.database import ip_tracking_model
        global ip_tracking_model_instance, ip_abuse_detection_task
        ip_tracking_model_instance = ip_tracking_model

        # 重置IP滥用检测配置为ENV默认值
        await ip_tracking_model_instance.reset_to_env_defaults()
        logger.info("IP滥用检测配置已从ENV重置")

        # 创建IP滥用检测定时任务（每5分钟执行一次）
        async def ip_abuse_detection_loop():
            """IP滥用检测定时任务"""
            while True:
                try:
                    await asyncio.sleep(300)  # 5分钟
                    logger.info("开始执行IP滥用检测...")

                    # 使用超时保护，避免检测任务卡死
                    try:
                        result = await asyncio.wait_for(
                            ip_tracking_model_instance.check_and_suspend_abuse(user_model),
                            timeout=60.0  # 60秒超时
                        )
                        if result['status'] == 'success' and result['suspended_count'] > 0:
                            logger.warning(f"IP滥用检测: 封停了 {result['suspended_count']} 个账户")
                        elif result['status'] == 'success':
                            logger.debug("IP滥用检测: 未发现滥用行为")
                    except asyncio.TimeoutError:
                        logger.error("IP滥用检测超时（60秒），已跳过本次检测")

                except asyncio.CancelledError:
                    logger.info("IP滥用检测任务已取消")
                    break
                except Exception as e:
                    logger.error(f"IP滥用检测任务执行失败: {e}")
                    import traceback
                    traceback.print_exc()

        ip_abuse_detection_task = asyncio.create_task(ip_abuse_detection_loop())
        logger.info("IP滥用检测定时任务已启动（每5分钟执行一次）")

        logger.info("应用启动完成")

    except Exception as e:
        logger.error(f"应用启动失败: {e}")
        raise

    yield  # 应用运行期间

    # 关闭时执行
    try:
        # 取消IP滥用检测任务
        if 'ip_abuse_detection_task' in globals() and ip_abuse_detection_task:
            ip_abuse_detection_task.cancel()
            try:
                await ip_abuse_detection_task
            except asyncio.CancelledError:
                pass
            logger.info("IP滥用检测任务已停止")

        # 关闭任务管理器
        await shutdown_task_manager()

        # 关闭WebSocket管理器
        await shutdown_websocket_manager()

        logger.info("应用关闭完成")

    except Exception as e:
        logger.error(f"应用关闭失败: {e}")

# 初始化 FastAPI 应用
app = FastAPI(lifespan=lifespan)

# 导入WebSocket和任务管理器
from core.websocket_manager import websocket_manager, init_websocket_manager, shutdown_websocket_manager
from core.task_manager import init_task_manager, shutdown_task_manager
# 任务管理器将在初始化后设置为全局变量
task_manager = None

# 用户系统全局变量
user_model = None
redemption_code_model = None
auth_manager_instance = None
ip_tracking_model_instance = None
ip_abuse_detection_task = None

# 挂载静态文件目录
app.mount("/static", StaticFiles(directory="static"), name="static")

# 环境变量配置
FAVICON_URL = os.getenv("FAVICON_URL", "/static/favicon.ico")
TITLE = os.getenv("TITLE", "智能图文识别服务")
BACK_URL = os.getenv("BACK_URL", "")

# 配置 Jinja2 模板目录
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
templates = Jinja2Templates(directory=os.path.join(BASE_DIR, "templates"))

# 跨域支持
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def get_client_ip(request: Request) -> str:
    """获取客户端真实IP地址"""
    # 优先从代理头获取真实IP
    forwarded = request.headers.get("X-Forwarded-For")
    if forwarded:
        return forwarded.split(",")[0].strip()

    real_ip = request.headers.get("X-Real-IP")
    if real_ip:
        return real_ip

    # 否则从request.client获取
    if request.client:
        return request.client.host

    return "unknown"

async def process_image(session, image_data, semaphore, max_retries=None):
    """使用 OCR 识别图像并进行 Markdown 格式化"""
    if max_retries is None:
        max_retries = runtime_config.max_retries

    system_prompt = """
    OCR识别图片上的内容，给出markdown的katex的格式的内容。
    选择题的序号使用A. B.依次类推。
    支持的主要语法：
    1. 基本语法：
       - 使用 $ 或 $$ 包裹行内或块级数学公式
       - 支持大量数学符号、希腊字母、运算符等
       - 分数：\\frac{分子}{分母}
       - 根号：\\sqrt{被开方数}
       - 上下标：x^2, x_n
    2. 极限使用：\\lim\\limits_x
    3. 参考以下例子格式：
    ### 35. 上3个无穷小量按照从低阶到高阶的排序是( )
    A.$\\alpha_1,\\alpha_2,\\alpha_3$
    B.$\\alpha_2,\\alpha_1,\\alpha_3$
    C.$\\alpha_1,\\alpha_3,\\alpha_2$
    D. $\\alpha_2,\\alpha_3,\\alpha_1$
    36. (I) 求 $\\lim\\limits_{x \\to +\\infty} \\frac{\\arctan 2x - \\arctan x}{\\frac{\\pi}{2} - \\arctan x}$;
        (II) 若 $\\lim\\limits_{x \\to +\\infty} x[1-f(x)]$ 不存在, 而 $l = \\lim\\limits_{x \\to +\\infty} \\frac{\\arctan 2x + [b-1-bf(x)]\\arctan x}{\\frac{\\pi}{2} - \\arctan x}$ 存在,
    试确定 $b$ 的值, 并求 (I)
    """
    for attempt in range(max_retries):
        try:
            async with semaphore:
                encoded_image = base64.b64encode(image_data).decode('utf-8')
                response = await session.post(
                    f"{API_BASE_URL}/v1/chat/completions",
                    headers={"Authorization": f"Bearer {API_KEY}"},
                    json={
                        "messages": [
                            {
                                "role": "system",
                                "content": system_prompt
                            },
                            {
                                "role": "user",
                                "content": [
                                    {
                                        "type": "text",
                                        "text": "Analyze the image and provide the content in the specified format, you only need to return the content, before returning the content you need to say: 'This is the content:', add 'this is the end of the content' at the end of the returned content, do not have any additional text other than these two sentences and the returned content, don't reply to me before I upload the image!"
                                    },
                                    {
                                        "type": "image_url",
                                        "image_url": {
                                            "url": f"data:image/png;base64,{encoded_image}"
                                        }
                                    }
                                ]
                            }
                        ],
                        "stream": False,
                        "model": MODEL,
                        "temperature": 0.5,
                        "presence_penalty": 0,
                        "frequency_penalty": 0,
                        "top_p": 1,
                    },
                )
                if response.status == 200:
                    result = await response.json()
                    return result['choices'][0]['message']['content']
                else:
                    raise Exception(f"请求失败, 状态码: {response.status}")
        except Exception as e:
            if attempt == max_retries - 1:
                return f"识别失败: {str(e)}"
            await asyncio.sleep(2 * attempt)  # 指数退避

def check_pdf_info(pdf_bytes: bytes) -> tuple[int, float]:
    """
    获取PDF文件的基本信息（页数和大小），不进行限制检查
    :param pdf_bytes: PDF 文件的字节数据
    :return: (页数, 文件大小MB)
    """
    try:
        pdf_document = fitz.open(stream=pdf_bytes, filetype="pdf")
        page_count = len(pdf_document)
        file_size_mb = len(pdf_bytes) / (1024 * 1024)
        pdf_document.close()
        
        logger.info(f"PDF 文件信息: {page_count} 页, {file_size_mb:.2f} MB")
        
        return page_count, file_size_mb
    except Exception as e:
        logger.error(f"PDF 文件信息获取失败: {e}")
        raise e

def pdf_to_images_generator(pdf_bytes: bytes, dpi: int = None):
    """
    使用生成器流式处理PDF转图片，减少内存占用
    :param pdf_bytes: PDF 文件的字节数据
    :param dpi: 图像分辨率 (默认200 DPI)
    :yield: (页码, 图片字节数据) 元组
    """
    if dpi is None:
        dpi = runtime_config.pdf_dpi

    pdf_document = None
    try:
        pdf_document = fitz.open(stream=pdf_bytes, filetype="pdf")
        logger.info(f"开始流式处理PDF，共 {len(pdf_document)} 页，DPI: {dpi}")

        for page_number in range(len(pdf_document)):
            try:
                page = pdf_document.load_page(page_number)
                pix = page.get_pixmap(dpi=dpi)
                
                # 转换为PNG字节数据
                with BytesIO() as buffer:
                    image = Image.open(BytesIO(pix.tobytes("png")))
                    image.save(buffer, format="PNG")
                    image_data = buffer.getvalue()
                
                # 释放内存
                pix = None
                page = None
                image = None
                
                logger.debug(f"第 {page_number + 1} 页转换完成")
                yield page_number + 1, image_data
                
            except Exception as e:
                logger.error(f"第 {page_number + 1} 页转换失败: {e}")
                yield page_number + 1, None
                
    except Exception as e:
        logger.error(f"PDF 流式处理失败: {e}")
        raise e
    finally:
        if pdf_document:
            pdf_document.close()


async def process_single_page_ocr(session: aiohttp.ClientSession, image_data: bytes, page_number: int, semaphore: asyncio.Semaphore):
    """
    处理单页OCR识别，改进错误处理和重试机制
    """
    if image_data is None:
        return f"第 {page_number} 页: 图片转换失败，跳过处理"

    for attempt in range(runtime_config.max_retries):
        try:
            async with semaphore:
                logger.info(f"开始OCR处理第 {page_number} 页 (尝试 {attempt + 1}/{runtime_config.max_retries})")
                result = await process_image(session, image_data, semaphore, max_retries=1)

                if result and "This is the content:" in result:
                    start_index = result.find("This is the content:") + len("This is the content:")
                    end_index = result.find("this is the end of the content")
                    if end_index == -1:
                        end_index = len(result)

                    content = result[start_index:end_index].strip()
                    content = content.replace("```markdown", "").replace("```", "").strip()

                    if content:
                        logger.info(f"第 {page_number} 页OCR处理成功")
                        return content
                    else:
                        raise Exception("OCR返回内容为空")
                else:
                    raise Exception("OCR返回格式不正确")

        except Exception as e:
            logger.warning(f"第 {page_number} 页处理失败 (尝试 {attempt + 1}/{runtime_config.max_retries}): {e}")
            if attempt < runtime_config.max_retries - 1:
                await asyncio.sleep(RETRY_DELAY * (attempt + 1))
            else:
                logger.error(f"第 {page_number} 页最终处理失败: {e}")
                return f"第 {page_number} 页处理失败: {str(e)}"

async def process_pages_batch(session: aiohttp.ClientSession, pages_data: list, semaphore: asyncio.Semaphore):
    """
    分批处理页面，避免创建过多并发任务
    :param session: aiohttp会话
    :param pages_data: [(页码, 图片数据)] 列表
    :param semaphore: 并发控制信号量
    :return: 处理结果列表
    """
    logger.info(f"开始处理批次，包含 {len(pages_data)} 页")
    
    tasks = []
    for page_number, image_data in pages_data:
        task = process_single_page_ocr(session, image_data, page_number, semaphore)
        tasks.append(task)
    
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    # 处理异常情况
    processed_results = []
    for i, result in enumerate(results):
        if isinstance(result, Exception):
            page_number = pages_data[i][0]
            error_msg = f"第 {page_number} 页处理异常: {str(result)}"
            logger.error(error_msg)
            processed_results.append(error_msg)
        else:
            processed_results.append(result)
    
    return processed_results

@app.post(f"/{PASSWORD}/process/image" if PASSWORD else "/process/image")
async def process_image_endpoint(file: UploadFile, request: Request, response: Response):
    """同步图片处理端点（保持兼容性）- 立即返回任务ID"""
    import tempfile

    if not task_manager:
        raise HTTPException(status_code=503, detail="任务管理器未初始化")

    if not file:
        raise HTTPException(status_code=400, detail="未收到文件")

    # 获取当前用户
    user = await auth_manager_instance.get_current_user(request)

    # 检查用户配额(图片算1页)
    has_quota, quota_message = await auth_manager_instance.check_quota(user['id'], 1)
    if not has_quota:
        raise HTTPException(status_code=403, detail=f"配额不足: {quota_message}")

    tmp_file_path = None
    try:
        # 创建临时文件保存图片
        with tempfile.NamedTemporaryFile(delete=False, suffix='.img') as tmp_file:
            tmp_file_path = tmp_file.name

            # 分块写入临时文件,避免内存占用
            chunk_size = 1024 * 1024  # 1MB每块
            while chunk := await file.read(chunk_size):
                tmp_file.write(chunk)

        logger.info(f"图片文件已写入临时文件: {tmp_file_path}")

        # 获取文件大小
        file_size_mb = os.path.getsize(tmp_file_path) / (1024 * 1024)

        # 获取客户端IP
        client_ip = get_client_ip(request)

        # 提交异步任务(传递临时文件路径而非数据)
        task_id = await task_manager.submit_image_task_from_file(
            file_path=tmp_file_path,
            file_name=file.filename or "image",
            metadata={
                "source": "sync_endpoint",
                "sync_mode": True,
                "file_size_mb": file_size_mb,
                "user_id": user['id'],  # 添加用户ID用于配额扣除
                "client_ip": client_ip  # 添加客户端IP
            }
        )

        # 扣除用户配额(图片算1页)
        await auth_manager_instance.deduct_quota(user['id'], 1)

        # 立即返回任务ID，避免长时间等待
        return JSONResponse({
            "status": "accepted",
            "task_id": task_id,
            "message": "任务已提交，正在后台处理",
            "estimated_time": 30,
            "websocket_url": f"/ws?client_id=sync_{task_id}",
            "status_url": f"/api/tasks/{task_id}",
            "result_url": f"/api/tasks/{task_id}/result"
        })

    except HTTPException:
        # HTTP异常时清理临时文件
        if tmp_file_path and os.path.exists(tmp_file_path):
            try:
                os.unlink(tmp_file_path)
            except:
                pass
        raise
    except Exception as e:
        # 其他异常时清理临时文件
        if tmp_file_path and os.path.exists(tmp_file_path):
            try:
                os.unlink(tmp_file_path)
            except:
                pass
        logger.error(f"图片任务提交失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post(f"/{PASSWORD}/process/pdf" if PASSWORD else "/process/pdf")
async def process_pdf_endpoint(file: UploadFile, request: Request, response: Response):
    """同步PDF处理端点（保持兼容性）- 立即返回任务ID"""
    import tempfile

    if not task_manager:
        raise HTTPException(status_code=503, detail="任务管理器未初始化")

    if not file:
        raise HTTPException(status_code=400, detail="未收到文件")

    # 获取当前用户
    user = await auth_manager_instance.get_current_user(request)

    tmp_file_path = None
    try:
        # 创建临时文件保存PDF
        with tempfile.NamedTemporaryFile(delete=False, suffix='.pdf') as tmp_file:
            tmp_file_path = tmp_file.name

            # 分块写入临时文件,避免内存占用
            chunk_size = 1024 * 1024  # 1MB每块
            while chunk := await file.read(chunk_size):
                tmp_file.write(chunk)

        logger.info(f"PDF文件已写入临时文件: {tmp_file_path}")

        # 获取PDF文件信息(从临时文件读取)
        try:
            with open(tmp_file_path, 'rb') as f:
                pdf_bytes = f.read()
                page_count, file_size_mb = check_pdf_info(pdf_bytes)
                pdf_bytes = None  # 立即释放
            logger.info(f"PDF文件信息: {page_count} 页, {file_size_mb:.2f} MB")
        except Exception as e:
            logger.error(f"PDF文件信息获取失败: {e}")
            if tmp_file_path and os.path.exists(tmp_file_path):
                os.unlink(tmp_file_path)
            raise HTTPException(status_code=400, detail="PDF文件格式错误或损坏")

        # 页数限制检查（使用环境变量配置）
        if page_count > runtime_config.max_pdf_pages:
            if tmp_file_path and os.path.exists(tmp_file_path):
                os.unlink(tmp_file_path)
            raise HTTPException(status_code=400, detail=f"PDF页数过多，最大支持{runtime_config.max_pdf_pages}页")

        # 检查用户配额
        has_quota, quota_message = await auth_manager_instance.check_quota(user['id'], page_count)
        if not has_quota:
            if tmp_file_path and os.path.exists(tmp_file_path):
                os.unlink(tmp_file_path)
            raise HTTPException(status_code=403, detail=f"配额不足: {quota_message}")

        # 获取客户端IP
        client_ip = get_client_ip(request)

        # 提交异步任务(传递临时文件路径而非数据)
        task_id = await task_manager.submit_pdf_task_from_file(
            file_path=tmp_file_path,
            file_name=file.filename or "document.pdf",
            metadata={
                "source": "sync_endpoint",
                "sync_mode": True,
                "file_size_mb": file_size_mb,
                "total_pages": page_count,
                "user_id": user['id'],  # 添加用户ID用于配额扣除
                "client_ip": client_ip  # 添加客户端IP
            }
        )

        # 扣除用户配额
        await auth_manager_instance.deduct_quota(user['id'], page_count)

        # 估算处理时间
        estimated_time = page_count * 3  # 每页约3秒

        # 立即返回任务ID，避免长时间等待
        return JSONResponse({
            "status": "accepted",
            "task_id": task_id,
            "message": "任务已提交，正在后台处理",
            "total_pages": page_count,
            "file_size_mb": f"{file_size_mb:.2f}",
            "estimated_time": estimated_time,
            "websocket_url": f"/ws?client_id=sync_{task_id}",
            "status_url": f"/api/tasks/{task_id}",
            "result_url": f"/api/tasks/{task_id}/result"
        })

    except HTTPException:
        # HTTP异常时清理临时文件
        if tmp_file_path and os.path.exists(tmp_file_path):
            try:
                os.unlink(tmp_file_path)
            except:
                pass
        raise
    except Exception as e:
        # 其他异常时清理临时文件
        if tmp_file_path and os.path.exists(tmp_file_path):
            try:
                os.unlink(tmp_file_path)
            except:
                pass
        logger.error(f"PDF任务提交失败: {e}")
        raise HTTPException(status_code=500, detail=f"PDF任务提交失败: {str(e)}")


@app.get("/", response_class=HTMLResponse)
async def root_page(request: Request, response: Response):
    """根路径 - 检查登录状态"""
    # 检查用户是否已登录
    user = await auth_manager_instance.get_current_user(request)

    if not user:
        # 未登录，显示开屏页面
        # 获取注册开关状态
        from models.database import db_manager
        try:
            async with db_manager.get_connection() as db:
                async with db.execute(
                    "SELECT value FROM system_settings WHERE key = 'registration_enabled'"
                ) as cursor:
                    row = await cursor.fetchone()
                    registration_enabled = row[0] == 'true' if row else True
        except Exception as e:
            logger.error(f"获取注册状态失败: {e}")
            registration_enabled = False

        return templates.TemplateResponse(
            "landing.html",
            {
                "request": request,
                "favicon_url": FAVICON_URL,
                "title": TITLE,
                "registration_enabled": registration_enabled,
                "model": MODEL,
                "max_image_size_mb": runtime_config.max_image_size_mb,
                "max_pdf_size_mb": runtime_config.max_pdf_size_mb,
                "max_pdf_pages": runtime_config.max_pdf_pages
            }
        )

    if PASSWORD:
        # 已登录但设置了密码，重定向到密码保护的主页
        return RedirectResponse(url=f"/{PASSWORD}", status_code=302)
    else:
        # 没有设置密码，直接显示主页
        return templates.TemplateResponse(
            "web.html",
            {
                "request": request,
                "favicon_url": FAVICON_URL,
                "title": TITLE,
                "backurl": BACK_URL
            }
        )

@app.get(f"/{PASSWORD}" if PASSWORD else "/main", response_class=HTMLResponse)
async def access_with_password(request: Request, response: Response):
    """受密码保护的主页"""
    # 检查用户是否已登录
    user = await auth_manager_instance.get_current_user(request)

    if not user:
        # 未登录，重定向到登录页面
        return RedirectResponse(url="/login", status_code=302)

    return templates.TemplateResponse(
        "web.html",
        {
            "request": request,
            "favicon_url": FAVICON_URL,
            "title": TITLE,
            "backurl": BACK_URL
        }
    )

# 管理员页面路由 - 支持两种路径
@app.get("/admin", response_class=HTMLResponse)
@app.get(f"/{PASSWORD}/admin" if PASSWORD else "/admin-disabled", response_class=HTMLResponse)
async def admin_page(request: Request, response: Response):
    """管理员后台导航页面"""
    try:
        # 检查用户是否已登录
        user = await auth_manager_instance.get_current_user(request)

        if not user:
            # 未登录，重定向到登录页面
            return RedirectResponse(url="/login", status_code=302)

        # 检查是否是管理员
        if not user.get('is_admin'):
            return HTMLResponse(
                content="<h1>403 - 需要管理员权限</h1><p>只有管理员可以访问此页面。<br><a href='/'>返回主页</a></p>",
                status_code=403
            )

        return templates.TemplateResponse(
            "admin.html",
            {
                "request": request,
                "favicon_url": FAVICON_URL,
                "title": f"{TITLE} - 管理后台",
                "backurl": BACK_URL
            }
        )
    except HTTPException as e:
        if e.status_code == 403:
            return HTMLResponse(content="<h1>403 - 需要管理员权限</h1><p>只有管理员可以访问此页面</p>", status_code=403)
        elif e.status_code == 401:
            return RedirectResponse(url="/login", status_code=302)
        raise

# 兑换码管理页面路由
@app.get("/admin/codes", response_class=HTMLResponse)
@app.get(f"/{PASSWORD}/admin/codes" if PASSWORD else "/admin/codes-disabled", response_class=HTMLResponse)
async def admin_codes_page(request: Request, response: Response):
    """兑换码管理页面"""
    try:
        user = await auth_manager_instance.get_current_user(request)
        if not user:
            return RedirectResponse(url="/login", status_code=302)
        if not user.get('is_admin'):
            return HTMLResponse(
                content="<h1>403 - 需要管理员权限</h1><p>只有管理员可以访问此页面。<br><a href='/'>返回主页</a></p>",
                status_code=403
            )
        return templates.TemplateResponse(
            "admin_codes.html",
            {
                "request": request,
                "favicon_url": FAVICON_URL,
                "title": f"{TITLE} - 兑换码管理",
                "password": PASSWORD
            }
        )
    except HTTPException as e:
        if e.status_code == 403:
            return HTMLResponse(content="<h1>403 - 需要管理员权限</h1><p>只有管理员可以访问此页面</p>", status_code=403)
        elif e.status_code == 401:
            return RedirectResponse(url="/login", status_code=302)
        raise

# 用户管理页面路由
@app.get("/admin/users", response_class=HTMLResponse)
@app.get(f"/{PASSWORD}/admin/users" if PASSWORD else "/admin/users-disabled", response_class=HTMLResponse)
async def admin_users_page(request: Request, response: Response):
    """用户管理页面"""
    try:
        user = await auth_manager_instance.get_current_user(request)
        if not user:
            return RedirectResponse(url="/login", status_code=302)
        if not user.get('is_admin'):
            return HTMLResponse(
                content="<h1>403 - 需要管理员权限</h1><p>只有管理员可以访问此页面。<br><a href='/'>返回主页</a></p>",
                status_code=403
            )
        return templates.TemplateResponse(
            "admin_users.html",
            {
                "request": request,
                "favicon_url": FAVICON_URL,
                "title": f"{TITLE} - 用户管理",
                "password_prefix": f"/{PASSWORD}" if PASSWORD else ""
            }
        )
    except HTTPException as e:
        if e.status_code == 403:
            return HTMLResponse(content="<h1>403 - 需要管理员权限</h1><p>只有管理员可以访问此页面</p>", status_code=403)
        elif e.status_code == 401:
            return RedirectResponse(url="/login", status_code=302)
        raise

# 任务管理页面路由
@app.get("/admin/tasks", response_class=HTMLResponse)
@app.get(f"/{PASSWORD}/admin/tasks" if PASSWORD else "/admin/tasks-disabled", response_class=HTMLResponse)
async def admin_tasks_page(request: Request, response: Response):
    """任务管理页面"""
    try:
        user = await auth_manager_instance.get_current_user(request)
        if not user:
            return RedirectResponse(url="/login", status_code=302)
        if not user.get('is_admin'):
            return HTMLResponse(
                content="<h1>403 - 需要管理员权限</h1><p>只有管理员可以访问此页面。<br><a href='/'>返回主页</a></p>",
                status_code=403
            )
        return templates.TemplateResponse(
            "admin_tasks.html",
            {
                "request": request,
                "favicon_url": FAVICON_URL,
                "title": f"{TITLE} - 任务管理",
                "password_prefix": f"/{PASSWORD}" if PASSWORD else ""
            }
        )
    except HTTPException as e:
        if e.status_code == 403:
            return HTMLResponse(content="<h1>403 - 需要管理员权限</h1><p>只有管理员可以访问此页面</p>", status_code=403)
        elif e.status_code == 401:
            return RedirectResponse(url="/login", status_code=302)
        raise

# 系统设置页面路由
@app.get("/admin/settings", response_class=HTMLResponse)
@app.get(f"/{PASSWORD}/admin/settings" if PASSWORD else "/admin/settings-disabled", response_class=HTMLResponse)
async def admin_settings_page(request: Request, response: Response):
    """系统设置页面"""
    try:
        user = await auth_manager_instance.get_current_user(request)
        if not user:
            return RedirectResponse(url="/login", status_code=302)
        if not user.get('is_admin'):
            return HTMLResponse(
                content="<h1>403 - 需要管理员权限</h1><p>只有管理员可以访问此页面。<br><a href='/'>返回主页</a></p>",
                status_code=403
            )
        return templates.TemplateResponse(
            "admin_settings.html",
            {
                "request": request,
                "favicon_url": FAVICON_URL,
                "title": f"{TITLE} - 系统设置",
                "password_prefix": f"/{PASSWORD}" if PASSWORD else ""
            }
        )
    except HTTPException as e:
        if e.status_code == 403:
            return HTMLResponse(content="<h1>403 - 需要管理员权限</h1><p>只有管理员可以访问此页面</p>", status_code=403)
        elif e.status_code == 401:
            return RedirectResponse(url="/login", status_code=302)
        raise

# 令牌注册管理页面路由
@app.get("/admin/registration-tokens", response_class=HTMLResponse)
@app.get(f"/{PASSWORD}/admin/registration-tokens" if PASSWORD else "/admin/registration-tokens-disabled", response_class=HTMLResponse)
async def admin_registration_tokens_page(request: Request, response: Response):
    """令牌注册管理页面"""
    try:
        user = await auth_manager_instance.get_current_user(request)
        if not user:
            return RedirectResponse(url="/login", status_code=302)
        if not user.get('is_admin'):
            return HTMLResponse(
                content="<h1>403 - 需要管理员权限</h1><p>只有管理员可以访问此页面。<br><a href='/'>返回主页</a></p>",
                status_code=403
            )
        return templates.TemplateResponse(
            "admin_registration_tokens.html",
            {
                "request": request,
                "favicon_url": FAVICON_URL,
                "title": f"{TITLE} - 令牌注册管理",
                "password_prefix": f"/{PASSWORD}" if PASSWORD else "",
                "password": PASSWORD
            }
        )
    except HTTPException as e:
        if e.status_code == 403:
            return HTMLResponse(content="<h1>403 - 需要管理员权限</h1><p>只有管理员可以访问此页面</p>", status_code=403)
        elif e.status_code == 401:
            return RedirectResponse(url="/login", status_code=302)
        raise

# WebSocket路由
@app.websocket(f"/{PASSWORD}/ws" if PASSWORD else "/ws")
async def websocket_endpoint(websocket: WebSocket, client_id: str = None):
    """WebSocket连接端点"""
    try:
        await websocket.accept()
        
        # 建立WebSocket连接
        actual_client_id = await websocket_manager.connect(websocket, client_id)
        logger.info(f"WebSocket客户端连接: {actual_client_id}")
        
        try:
            while True:
                # 接收客户端消息
                data = await websocket.receive_text()
                await websocket_manager.handle_client_message(actual_client_id, data)
                
        except WebSocketDisconnect:
            logger.info(f"WebSocket客户端断开连接: {actual_client_id}")

        except (ConnectionResetError, ConnectionError) as e:
            # Windows/Linux 连接重置错误,这是正常的客户端断开情况
            logger.info(f"WebSocket客户端连接断开 {actual_client_id}: {type(e).__name__}")

        except Exception as e:
            logger.error(f"WebSocket连接异常 {actual_client_id}: {e}")
            
    except Exception as e:
        logger.error(f"WebSocket连接失败: {e}")
        
    finally:
        # 清理连接
        if 'actual_client_id' in locals():
            try:
                await websocket_manager.disconnect(actual_client_id)
            except Exception as e:
                logger.debug(f"清理连接时发生错误 {actual_client_id}: {e}")

# 任务管理API路由
@app.post(f"/{PASSWORD}/api/tasks/image" if PASSWORD else "/api/tasks/image")
async def submit_image_task_api(file: UploadFile, request: Request):
    """提交图片OCR任务API"""
    if not task_manager:
        raise HTTPException(status_code=503, detail="任务管理器未初始化")

    if not file:
        raise HTTPException(status_code=400, detail="未收到文件")

    try:
        file_data = await file.read()
        if not file_data:
            raise HTTPException(status_code=400, detail="文件内容为空")

        # 获取客户端IP
        client_ip = get_client_ip(request)

        task_id = await task_manager.submit_image_task(
            file_data=file_data,
            file_name=file.filename or "image",
            metadata={"source": "api", "client_ip": client_ip}
        )
        
        return JSONResponse({
            "status": "success",
            "task_id": task_id,
            "message": "任务提交成功"
        })
        
    except Exception as e:
        logger.error(f"提交图片任务失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post(f"/{PASSWORD}/api/tasks/pdf" if PASSWORD else "/api/tasks/pdf")
async def submit_pdf_task_api(file: UploadFile, request: Request):
    """提交PDF OCR任务API"""
    if not task_manager:
        raise HTTPException(status_code=503, detail="任务管理器未初始化")

    if not file:
        raise HTTPException(status_code=400, detail="未收到文件")

    try:
        file_data = await file.read()
        if not file_data:
            raise HTTPException(status_code=400, detail="文件内容为空")

        # 获取PDF文件信息
        try:
            page_count, file_size_mb = check_pdf_info(file_data)
            logger.info(f"PDF文件信息: {page_count} 页, {file_size_mb:.2f} MB")
        except Exception as e:
            logger.error(f"PDF文件信息获取失败: {e}")
            raise HTTPException(status_code=400, detail="PDF文件格式错误或损坏")

        # 页数限制检查（使用环境变量配置）
        if page_count > runtime_config.max_pdf_pages:
            raise HTTPException(status_code=400, detail=f"PDF页数过多，最大支持{runtime_config.max_pdf_pages}页")

        # 获取客户端IP
        client_ip = get_client_ip(request)

        task_id = await task_manager.submit_pdf_task(
            file_data=file_data,
            file_name=file.filename or "document.pdf",
            metadata={"source": "api", "client_ip": client_ip}
        )
        
        return JSONResponse({
            "status": "success",
            "task_id": task_id,
            "message": "任务提交成功"
        })
        
    except Exception as e:
        logger.error(f"提交PDF任务失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get(f"/{PASSWORD}/api/tasks/{{task_id}}" if PASSWORD else "/api/tasks/{task_id}")
async def get_task_info_api(task_id: str):
    """获取任务信息API"""
    if not task_manager:
        raise HTTPException(status_code=503, detail="任务管理器未初始化")
    
    try:
        task_info = await task_manager.get_task_info(task_id)
        if not task_info:
            raise HTTPException(status_code=404, detail="任务不存在")
        
        return JSONResponse({
            "status": "success",
            "data": task_info
        })
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"获取任务信息失败 {task_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get(f"/{PASSWORD}/api/tasks/{{task_id}}/result" if PASSWORD else "/api/tasks/{task_id}/result")
async def get_task_result_api(task_id: str, request: Request):
    """获取任务结果API"""
    if not task_manager:
        logger.error(f"任务管理器未初始化，无法获取任务结果: {task_id}")
        raise HTTPException(status_code=503, detail="任务管理器未初始化")

    try:
        # 检查用户是否为管理员
        user = await auth_manager_instance.require_user(request)
        is_admin = user.get('is_admin', False)

        task_result = await task_manager.get_task_result(task_id)

        if not task_result:
            logger.warning(f"任务不存在: {task_id}")
            raise HTTPException(status_code=404, detail="任务不存在")

        # 如果是管理员且任务审查失败，返回原始内容
        if is_admin and task_result.get('status') == 'cancelled':
            if task_result.get('error_message') == '内容道德审查未通过' or \
               '所有页面内容道德审查未通过' in str(task_result.get('error_message', '')):
                from models.database import page_result_model, task_model
                task_data = await task_model.get_task(task_id)

                if task_data:
                    page_results = await page_result_model.get_task_page_results(task_id)

                    if task_data['task_type'] == 'image_ocr':
                        # 图片任务：返回原始内容
                        if page_results:
                            task_result['content'] = page_results[0].get('content', '')
                            task_result['admin_view'] = True
                    else:
                        # PDF任务：返回所有页面（包括审查失败的）
                        contents = []
                        for page_result in page_results:
                            content = page_result.get('content', '')
                            if content:
                                contents.append(content)
                        task_result['content'] = '\n\n'.join(contents)
                        task_result['admin_view'] = True

        # 只在异常情况下记录日志
        if task_result.get('status') == 'completed' and not task_result.get('content'):
            logger.error(f"警告: 任务显示已完成但内容为空! 任务ID: {task_id}")
            # 尝试从数据库重新获取
            from models.database import page_result_model
            page_results = await page_result_model.get_task_page_results(task_id)
            logger.info(f"数据库中的页面结果数量: {len(page_results)}")
            for pr in page_results:
                logger.info(f"  页面 {pr.get('page_number')}: 状态={pr.get('status')}, 内容长度={len(pr.get('content', ''))}")

        return JSONResponse({
            "status": "success",
            "data": task_result
        })

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"获取任务结果失败 {task_id}: {e}")
        logger.exception(f"获取任务结果失败详情:")
        raise HTTPException(status_code=500, detail=str(e))

@app.get(f"/{PASSWORD}/api/tasks" if PASSWORD else "/api/tasks")
async def get_tasks_list_api(
    request: Request,
    status: Optional[str] = Query(None, description="任务状态筛选"),
    limit: int = Query(100, description="返回数量限制"),
    offset: int = Query(0, description="偏移量")
):
    """获取任务列表API（用户只能看到自己的任务）"""
    # 从全局模块导入任务管理器，确保获取到最新的实例
    from core.task_manager import task_manager as global_task_manager
    from models.database import task_model

    if not global_task_manager:
        raise HTTPException(status_code=503, detail="任务管理器未初始化")

    try:
        # 获取当前登录用户
        user = await auth_manager_instance.require_user(request)

        # 只返回当前用户的任务
        tasks = await task_model.get_recent_tasks(limit=limit, user_id=user['id'])

        return JSONResponse({
            "status": "success",
            "data": tasks
        })
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"获取任务列表失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get(f"/{PASSWORD}/api/system/stats" if PASSWORD else "/api/system/stats")
async def get_system_stats_api():
    """获取系统统计信息API"""
    if not task_manager:
        raise HTTPException(status_code=503, detail="任务管理器未初始化")
    
    try:
        stats = await task_manager.get_system_stats()
        websocket_stats = websocket_manager.get_connection_stats()
        
        return JSONResponse({
            "status": "success",
            "data": {
                "task_system": stats,
                "websocket": websocket_stats
            }
        })
        
    except Exception as e:
        logger.error(f"获取系统统计失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# ==================== 认证 API 端点 ====================

@app.get("/login", response_class=HTMLResponse)
async def login_page(request: Request):
    """登录页面"""
    # 获取注册开关状态
    from models.database import db_manager
    try:
        async with db_manager.get_connection() as db:
            async with db.execute(
                "SELECT value FROM system_settings WHERE key = 'registration_enabled'"
            ) as cursor:
                row = await cursor.fetchone()
                registration_enabled = row[0] == 'true' if row else True
    except Exception as e:
        logger.error(f"获取注册状态失败: {e}")
        registration_enabled = False

    # 获取 Turnstile 验证开关状态
    turnstile_enabled, _ = await get_turnstile_enabled_status()

    return templates.TemplateResponse("login.html", {
        "request": request,
        "favicon_url": FAVICON_URL,
        "title": TITLE,
        "registration_enabled": registration_enabled,
        "turnstile_site_key": TURNSTILE_SITE_KEY,
        "turnstile_enabled": turnstile_enabled
    })

@app.get("/test-login", response_class=HTMLResponse)
async def test_login_page(request: Request):
    """测试登录页面"""
    return templates.TemplateResponse("test_login.html", {
        "request": request
    })

@app.get("/register", response_class=HTMLResponse)
@app.get(f"/{PASSWORD}/register" if PASSWORD else "/register-disabled", response_class=HTMLResponse)
async def register_page(request: Request, token: str = ""):
    """注册页面"""
    # 获取 Turnstile 验证开关状态
    turnstile_enabled, _ = await get_turnstile_enabled_status()

    return templates.TemplateResponse("register.html", {
        "request": request,
        "token": token,
        "favicon_url": FAVICON_URL,
        "title": TITLE,
        "turnstile_site_key": TURNSTILE_SITE_KEY,
        "turnstile_enabled": turnstile_enabled
    })

@app.post("/api/auth/register")
async def register_api(user_data: dict, request: Request):
    """用户注册（支持开放注册和令牌注册）"""
    try:
        username = user_data.get('username', '').strip()
        password = user_data.get('password', '')
        token = user_data.get('token', '').strip()
        turnstile_token = user_data.get('turnstile_token', '').strip()

        # 验证 Turnstile
        client_ip = request.client.host if request.client else None
        is_valid, message = await verify_turnstile_token(turnstile_token, client_ip)
        if not is_valid:
            raise HTTPException(status_code=400, detail=message)

        # 验证必填字段
        if not username or not password:
            raise HTTPException(status_code=400, detail="用户名和密码不能为空")

        if len(username) < 3 or len(username) > 20:
            raise HTTPException(status_code=400, detail="用户名长度应为3-20个字符")

        if len(password) < 6:
            raise HTTPException(status_code=400, detail="密码长度至少为6个字符")

        # 获取注册开关状态
        from models.database import db_manager
        async with db_manager.get_connection() as db:
            async with db.execute(
                "SELECT value FROM system_settings WHERE key = 'registration_enabled'"
            ) as cursor:
                row = await cursor.fetchone()
                registration_enabled = row[0] == 'true' if row else True

        # 如果注册功能关闭，必须使用有效令牌
        if not registration_enabled:
            if not token:
                raise HTTPException(status_code=400, detail="注册功能已关闭，请使用管理员提供的注册令牌")
            is_valid, message = await registration_token_model.validate_token(token)
            if not is_valid:
                raise HTTPException(status_code=400, detail=message)
        # 如果注册功能开放，但提供了token，也需要验证token
        elif token:
            is_valid, message = await registration_token_model.validate_token(token)
            if not is_valid:
                raise HTTPException(status_code=400, detail=message)

        # 创建用户
        success, register_message, user_id = await auth_manager_instance.register(username, password)

        if not success:
            raise HTTPException(status_code=400, detail=register_message)

        # 如果使用了令牌，标记令牌为已使用并记录使用者信息
        if token:
            token_success, token_message = await registration_token_model.use_token(
                token=token,
                user_id=user_id,
                username=username,
                user_uuid=user_id  # user_id就是UUID
            )
            if not token_success:
                logger.error(f"用户创建成功但令牌使用失败: {token_message}")

        return JSONResponse({
            "status": "success",
            "message": "注册成功",
            "user_id": user_id
        })
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"用户注册失败: {e}")
        raise HTTPException(status_code=500, detail="注册失败")

@app.post("/api/auth/login")
async def login_api(user_data: dict, request: Request):
    """用户登录"""
    try:
        username = user_data.get('username', '').strip()
        password = user_data.get('password', '')
        turnstile_token = user_data.get('turnstile_token', '').strip()

        # 验证 Turnstile
        client_ip = request.client.host if request.client else None
        is_valid, message = await verify_turnstile_token(turnstile_token, client_ip)
        if not is_valid:
            raise HTTPException(status_code=400, detail=message)

        if not username or not password:
            raise HTTPException(status_code=400, detail="用户名和密码不能为空")

        # 验证用户名密码
        user = await user_model.authenticate(username, password)

        if not user:
            raise HTTPException(status_code=401, detail="用户名或密码错误")

        # 生成新的session token
        new_session_token = auth_manager_instance.generate_session_token()
        await user_model.update_session_token(user['id'], new_session_token)

        # 创建响应并设置cookie
        response = JSONResponse({
            "status": "success",
            "message": "登录成功",
            "data": {
                "user_id": user['id'],
                "username": user['username'],
                "is_admin": user['is_admin']
            }
        })

        response.set_cookie(
            key="ocr_session",
            value=new_session_token,
            path='/',
            max_age=30 * 24 * 60 * 60,  # 30天
            httponly=True,
            samesite='lax'
        )

        logger.info(f"用户 {username} 登录成功")
        return response
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"用户登录失败: {e}")
        raise HTTPException(status_code=500, detail="登录失败")

@app.post(f"/{PASSWORD}/api/auth/logout" if PASSWORD else "/api/auth/logout")
async def logout_api(request: Request, response: Response):
    """用户登出"""
    try:
        await auth_manager_instance.logout(response, request)
        return JSONResponse({
            "status": "success",
            "message": "已登出"
        })
    except Exception as e:
        logger.error(f"用户登出失败: {e}")
        raise HTTPException(status_code=500, detail="登出失败")

# ==================== 用户系统 API 端点 ====================

@app.get(f"/{PASSWORD}/api/user/current" if PASSWORD else "/api/user/current")
async def get_current_user_api(request: Request):
    """获取当前用户信息"""
    try:
        user = await auth_manager_instance.get_current_user(request)
        if not user:
            raise HTTPException(status_code=401, detail="未登录")

        quota = await user_model.get_quota(user['id'])

        return JSONResponse({
            "status": "success",
            "data": {
                "user_id": user['id'],
                "username": user['username'],
                "is_admin": user['is_admin'],
                "quota": quota
            }
        })
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"获取当前用户信息失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post(f"/{PASSWORD}/api/user/redeem" if PASSWORD else "/api/user/redeem")
async def redeem_code_api(request: Request, code_data: dict):
    """兑换码兑换"""
    try:
        user = await auth_manager_instance.require_user(request)
        code = code_data.get('code', '').strip().upper()

        if not code:
            raise HTTPException(status_code=400, detail="兑换码不能为空")

        # 使用兑换码
        success, message, pages = await redemption_code_model.use_code(code, user['id'])

        if not success:
            return JSONResponse({
                "status": "error",
                "message": message
            }, status_code=400)

        # 增加用户配额
        await user_model.add_pages(user['id'], pages)

        # 获取更新后的配额
        quota = await user_model.get_quota(user['id'])

        return JSONResponse({
            "status": "success",
            "message": f"兑换成功!获得{pages}页配额",
            "pages_granted": pages,
            "quota": quota
        })

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"兑换码兑换失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get(f"/{PASSWORD}/api/user/quota" if PASSWORD else "/api/user/quota")
async def get_user_quota_api(request: Request):
    """获取用户配额信息"""
    try:
        user = await auth_manager_instance.require_user(request)
        quota = await user_model.get_quota(user['id'])

        if not quota:
            raise HTTPException(status_code=404, detail="配额信息不存在")

        return JSONResponse({
            "status": "success",
            "data": quota
        })
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"获取用户配额失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get(f"/{PASSWORD}/api/user/history" if PASSWORD else "/api/user/history")
async def get_user_redemption_history_api(request: Request):
    """获取用户兑换历史"""
    try:
        user = await auth_manager_instance.require_user(request)
        history = await redemption_code_model.get_redemption_history(user_id=user['id'])

        return JSONResponse({
            "status": "success",
            "data": history
        })
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"获取兑换历史失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# ==================== 管理员 API 端点 ====================

@app.post(f"/{PASSWORD}/api/admin/codes/generate" if PASSWORD else "/api/admin/codes/generate")
async def generate_redemption_code_api(request: Request, code_config: dict):
    """生成兑换码(仅管理员)"""
    try:
        user = await auth_manager_instance.require_admin(request)

        logger.info(f"管理员 {user.get('username')} (ID: {user.get('id')}) 尝试生成兑换码")

        pages = code_config.get('pages', 0)
        max_uses = code_config.get('max_uses', 1)
        expires_days = code_config.get('expires_days')
        description = code_config.get('description')
        batch_count = code_config.get('batch_count', 1)  # 批量生成数量,默认为1

        if pages <= 0:
            raise HTTPException(status_code=400, detail="页数必须大于0")

        if max_uses <= 0:
            raise HTTPException(status_code=400, detail="使用次数必须大于0")

        if batch_count <= 0 or batch_count > 100:
            raise HTTPException(status_code=400, detail="批量生成数量必须在1-100之间")

        logger.info(f"创建兑换码参数: pages={pages}, max_uses={max_uses}, expires_days={expires_days}, batch_count={batch_count}, created_by={user.get('id')}")

        # 批量生成兑换码
        codes = []
        for i in range(batch_count):
            code = await redemption_code_model.create_code(
                created_by=user['id'],
                pages=pages,
                max_uses=max_uses,
                expires_days=expires_days,
                description=description
            )
            if code:
                codes.append(code)
            else:
                logger.error(f"第{i+1}个兑换码创建失败, user_id={user.get('id')}")

        if not codes:
            logger.error(f"兑换码创建全部失败, user_id={user.get('id')}")
            raise HTTPException(status_code=500, detail="兑换码生成失败: 请检查日志获取详细错误信息")

        # 如果是批量生成,返回所有兑换码;如果是单个生成,保持原有格式
        if batch_count == 1:
            return JSONResponse({
                "status": "success",
                "message": "兑换码生成成功",
                "code": codes[0],
                "pages": pages,
                "max_uses": max_uses,
                "expires_days": expires_days
            })
        else:
            return JSONResponse({
                "status": "success",
                "message": f"成功生成{len(codes)}个兑换码",
                "codes": codes,
                "pages": pages,
                "max_uses": max_uses,
                "expires_days": expires_days,
                "total": len(codes),
                "requested": batch_count
            })

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"生成兑换码失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get(f"/{PASSWORD}/api/admin/codes/list" if PASSWORD else "/api/admin/codes/list")
async def list_redemption_codes_api(request: Request, page: int = Query(1, ge=1), page_size: int = Query(100, ge=1, le=1000), search: Optional[str] = Query(None, description="搜索关键词")):
    """查看所有兑换码(仅管理员)"""
    try:
        user = await auth_manager_instance.require_admin(request)

        offset = (page - 1) * page_size
        codes = await redemption_code_model.get_all_codes(limit=page_size, offset=offset, search=search)
        total = await redemption_code_model.count_codes(search=search)

        return JSONResponse({
            "status": "success",
            "data": codes,
            "total": total,
            "page": page,
            "page_size": page_size,
            "total_pages": (total + page_size - 1) // page_size
        })

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"获取兑换码列表失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post(f"/{PASSWORD}/api/admin/codes/{{code}}/deactivate" if PASSWORD else "/api/admin/codes/{code}/deactivate")
async def deactivate_redemption_code_api(request: Request, code: str):
    """禁用兑换码(仅管理员)"""
    try:
        user = await auth_manager_instance.require_admin(request)
        success = await redemption_code_model.deactivate_code(code)

        if not success:
            raise HTTPException(status_code=500, detail="禁用失败")

        return JSONResponse({
            "status": "success",
            "message": f"兑换码 {code} 已禁用"
        })

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"禁用兑换码失败 {code}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post(f"/{PASSWORD}/api/admin/codes/{{code}}/activate" if PASSWORD else "/api/admin/codes/{code}/activate")
async def activate_redemption_code_api(request: Request, code: str):
    """激活兑换码(仅管理员)"""
    try:
        user = await auth_manager_instance.require_admin(request)
        success = await redemption_code_model.activate_code(code)

        if not success:
            raise HTTPException(status_code=500, detail="激活失败")

        return JSONResponse({
            "status": "success",
            "message": f"兑换码 {code} 已激活"
        })

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"激活兑换码失败 {code}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get(f"/{PASSWORD}/api/admin/codes/history" if PASSWORD else "/api/admin/codes/history")
async def get_redemption_history_api(request: Request):
    """获取所有兑换记录(仅管理员)"""
    try:
        user = await auth_manager_instance.require_admin(request)
        history = await redemption_code_model.get_redemption_history_with_users(limit=500)

        return JSONResponse({
            "status": "success",
            "data": history,
            "total": len(history)
        })

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"获取兑换记录失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get(f"/{PASSWORD}/api/admin/users/list" if PASSWORD else "/api/admin/users/list")
async def list_users_api(
    request: Request,
    page: int = Query(1, ge=1),
    page_size: int = Query(100, ge=1, le=1000),
    search: str = Query(None, description="搜索用户名或UUID")
):
    """查看所有用户(仅管理员)，支持搜索"""
    try:
        user = await auth_manager_instance.require_admin(request)

        offset = (page - 1) * page_size
        users = await user_model.get_all_users(limit=page_size, offset=offset, search=search)
        total = await user_model.count_users(search=search)

        return JSONResponse({
            "status": "success",
            "data": users,
            "total": total,
            "page": page,
            "page_size": page_size,
            "total_pages": (total + page_size - 1) // page_size
        })

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"获取用户列表失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post(f"/{PASSWORD}/api/admin/users/{{user_id}}/quota" if PASSWORD else "/api/admin/users/{user_id}/quota")
async def adjust_user_quota_api(request: Request, user_id: str, quota_data: dict):
    """调整用户配额(仅管理员)"""
    try:
        admin = await auth_manager_instance.require_admin(request)

        action = quota_data.get('action')  # 'set' or 'add'
        pages = quota_data.get('pages', 0)

        if pages < 0:
            raise HTTPException(status_code=400, detail="页数不能为负数")

        if action == 'set':
            success = await user_model.set_quota(user_id, pages)
            message = f"用户 {user_id} 配额已设置为 {pages} 页"
        elif action == 'add':
            success = await user_model.add_pages(user_id, pages)
            message = f"用户 {user_id} 配额已增加 {pages} 页"
        else:
            raise HTTPException(status_code=400, detail="action必须是'set'或'add'")

        if not success:
            raise HTTPException(status_code=500, detail="配额调整失败")

        quota = await user_model.get_quota(user_id)

        return JSONResponse({
            "status": "success",
            "message": message,
            "quota": quota
        })

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"调整用户配额失败 {user_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post(f"/{PASSWORD}/api/admin/users/{{user_id}}/disable" if PASSWORD else "/api/admin/users/{user_id}/disable")
async def disable_user_api(request: Request, user_id: str):
    """禁用用户(仅管理员)"""
    try:
        admin = await auth_manager_instance.require_admin(request)

        # 防止管理员禁用自己
        if admin['id'] == user_id:
            raise HTTPException(status_code=400, detail="不能禁用自己的账户")

        success = await user_model.disable_user(user_id)

        if not success:
            raise HTTPException(status_code=500, detail="禁用用户失败")

        return JSONResponse({
            "status": "success",
            "message": "用户已被禁用"
        })

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"禁用用户失败 {user_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post(f"/{PASSWORD}/api/admin/users/{{user_id}}/enable" if PASSWORD else "/api/admin/users/{user_id}/enable")
async def enable_user_api(request: Request, user_id: str):
    """启用用户(仅管理员)"""
    try:
        admin = await auth_manager_instance.require_admin(request)

        success = await user_model.enable_user(user_id)

        if not success:
            raise HTTPException(status_code=500, detail="启用用户失败")

        return JSONResponse({
            "status": "success",
            "message": "用户已被启用"
        })

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"启用用户失败 {user_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get(f"/{PASSWORD}/api/admin/settings/registration" if PASSWORD else "/api/admin/settings/registration")
async def get_registration_status_api(request: Request):
    """获取注册开关状态(仅管理员)"""
    try:
        admin = await auth_manager_instance.require_admin(request)

        from models.database import db_manager
        async with db_manager.get_connection() as db:
            async with db.execute(
                "SELECT value FROM system_settings WHERE key = 'registration_enabled'"
            ) as cursor:
                row = await cursor.fetchone()
                enabled = row[0] == 'true' if row else True

        return JSONResponse({
            "status": "success",
            "registration_enabled": enabled
        })
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"获取注册状态失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post(f"/{PASSWORD}/api/admin/settings/registration" if PASSWORD else "/api/admin/settings/registration")
async def toggle_registration_api(request: Request, settings_data: dict):
    """切换注册开关状态(仅管理员)"""
    try:
        admin = await auth_manager_instance.require_admin(request)

        enabled = settings_data.get('enabled', True)

        from models.database import db_manager
        from datetime import datetime, timezone
        async with db_manager.get_connection() as db:
            await db.execute("""
                UPDATE system_settings
                SET value = ?, updated_at = ?
                WHERE key = 'registration_enabled'
            """, ('true' if enabled else 'false', datetime.now(timezone.utc).isoformat()))
            await db.commit()

        return JSONResponse({
            "status": "success",
            "message": f"注册功能已{'开放' if enabled else '禁止'}",
            "registration_enabled": enabled
        })
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"切换注册状态失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# ==================== Turnstile 验证开关管理 API ====================

@app.get(f"/{PASSWORD}/api/admin/settings/turnstile" if PASSWORD else "/api/admin/settings/turnstile")
async def get_turnstile_settings_api(request: Request):
    """获取 Turnstile 验证配置(仅管理员)"""
    try:
        admin = await auth_manager_instance.require_admin(request)

        import os
        from models.database import db_manager

        # 检查 .env 是否强制控制
        env_value = os.getenv('TURNSTILE_ENABLED', '').lower()
        env_forced = env_value in ('true', 'false')

        # 获取数据库配置
        async with db_manager.get_connection() as db:
            async with db.execute(
                "SELECT value FROM system_settings WHERE key = 'turnstile_enabled'"
            ) as cursor:
                row = await cursor.fetchone()
                db_enabled = row[0].lower() == 'true' if row else False

        # 计算最终生效状态
        enabled, source = await get_turnstile_enabled_status()

        return JSONResponse({
            "status": "success",
            "enabled": enabled,
            "source": source,
            "env_forced": env_forced,
            "env_value": env_value if env_forced else None,
            "db_value": db_enabled,
            "has_secret_key": bool(TURNSTILE_SECRET_KEY),
            "site_key": TURNSTILE_SITE_KEY
        })
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"获取 Turnstile 配置失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post(f"/{PASSWORD}/api/admin/settings/turnstile" if PASSWORD else "/api/admin/settings/turnstile")
async def update_turnstile_settings_api(request: Request, settings_data: dict):
    """更新 Turnstile 验证配置(仅管理员)"""
    try:
        admin = await auth_manager_instance.require_admin(request)

        from models.database import db_manager
        from datetime import datetime, timezone

        enabled = settings_data.get('enabled', False)

        # 更新数据库配置（运行时立即生效）
        async with db_manager.get_connection() as db:
            await db.execute("""
                UPDATE system_settings
                SET value = ?, updated_at = ?
                WHERE key = 'turnstile_enabled'
            """, ('true' if enabled else 'false', datetime.now(timezone.utc).isoformat()))
            await db.commit()

        logger.info(f"管理员 {admin['username']} 更新 Turnstile 验证: {enabled} (运行时生效)")

        return JSONResponse({
            "status": "success",
            "message": f"Turnstile 验证已{'启用' if enabled else '禁用'}",
            "enabled": enabled
        })
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"更新 Turnstile 配置失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get(f"/{PASSWORD}/api/admin/settings/ip-abuse" if PASSWORD else "/api/admin/settings/ip-abuse")
async def get_ip_abuse_settings_api(request: Request):
    """获取IP滥用检测配置(仅管理员)"""
    try:
        admin = await auth_manager_instance.require_admin(request)

        settings = await ip_tracking_model_instance.get_settings()

        return JSONResponse({
            "status": "success",
            "enabled": settings.get('enabled', True),
            "threshold": settings.get('threshold', 3),
            "days": settings.get('days', 7)
        })
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"获取IP滥用检测配置失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post(f"/{PASSWORD}/api/admin/settings/ip-abuse" if PASSWORD else "/api/admin/settings/ip-abuse")
async def update_ip_abuse_settings_api(request: Request, settings_data: dict):
    """更新IP滥用检测配置(仅管理员)"""
    try:
        admin = await auth_manager_instance.require_admin(request)

        enabled = settings_data.get('enabled')
        threshold = settings_data.get('threshold')
        days = settings_data.get('days')

        # 验证参数
        if threshold is not None and (not isinstance(threshold, int) or threshold < 1):
            raise HTTPException(status_code=400, detail="账户数阈值必须是大于0的整数")
        if days is not None and (not isinstance(days, int) or days < 1):
            raise HTTPException(status_code=400, detail="天数窗口必须是大于0的整数")

        success = await ip_tracking_model_instance.update_settings(
            enabled=enabled,
            threshold=threshold,
            days=days
        )

        if not success:
            raise HTTPException(status_code=500, detail="更新配置失败")

        # 获取最新配置返回
        updated_settings = await ip_tracking_model_instance.get_settings()

        return JSONResponse({
            "status": "success",
            "message": "IP滥用检测配置已更新",
            "enabled": updated_settings.get('enabled', True),
            "threshold": updated_settings.get('threshold', 3),
            "days": updated_settings.get('days', 7)
        })
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"更新IP滥用检测配置失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get(f"/{PASSWORD}/api/admin/settings/config" if PASSWORD else "/api/admin/settings/config")
async def get_runtime_config_api(request: Request):
    """获取运行时配置(仅管理员)"""
    try:
        admin = await auth_manager_instance.require_admin(request)

        return JSONResponse({
            "status": "success",
            "config": runtime_config.get_all()
        })
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"获取运行时配置失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post(f"/{PASSWORD}/api/admin/settings/config" if PASSWORD else "/api/admin/settings/config")
async def update_runtime_config_api(request: Request, config_data: dict):
    """更新运行时配置(仅管理员)"""
    try:
        admin = await auth_manager_instance.require_admin(request)

        # 更新配置
        runtime_config.update(config_data)

        return JSONResponse({
            "status": "success",
            "message": "配置已更新(仅在内存中,重启后恢复默认值)",
            "config": runtime_config.get_all()
        })
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"更新运行时配置失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get(f"/{PASSWORD}/api/frontend/config" if PASSWORD else "/api/frontend/config")
async def get_frontend_config_api():
    """获取前端配置"""
    try:
        return JSONResponse({
            "status": "success",
            "config": {
                "frontend_poll_timeout": runtime_config.frontend_poll_timeout,
                "max_image_size_mb": runtime_config.max_image_size_mb,
                "max_pdf_size_mb": runtime_config.max_pdf_size_mb,
                "max_pdf_pages": runtime_config.max_pdf_pages
            }
        })
    except Exception as e:
        logger.error(f"获取前端配置失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# ==================== 注册令牌管理API ====================

@app.post(f"/{PASSWORD}/api/admin/registration-tokens/create" if PASSWORD else "/api/admin/registration-tokens/create")
async def create_registration_token_api(request: Request, token_data: dict):
    """创建注册令牌(仅管理员)"""
    try:
        admin = await auth_manager_instance.require_admin(request)

        max_uses = token_data.get('max_uses', 1)
        expires_days = token_data.get('expires_days')
        description = token_data.get('description')
        batch_count = token_data.get('batch_count', 1)  # 批量生成数量,默认为1

        if max_uses < 1:
            raise HTTPException(status_code=400, detail="最大使用次数必须大于0")

        if batch_count <= 0 or batch_count > 100:
            raise HTTPException(status_code=400, detail="批量生成数量必须在1-100之间")

        logger.info(f"创建注册令牌参数: max_uses={max_uses}, expires_days={expires_days}, batch_count={batch_count}, created_by={admin.get('id')}")

        # 批量生成注册令牌
        tokens = []
        registration_urls = []
        base_url = str(request.base_url).rstrip('/')
        password_prefix = f"/{PASSWORD}" if PASSWORD else ""

        for i in range(batch_count):
            token = await registration_token_model.create_token(
                created_by=admin['id'],
                max_uses=max_uses,
                expires_days=expires_days,
                description=description
            )
            if token:
                tokens.append(token)
                registration_urls.append(f"{base_url}{password_prefix}/register?token={token}")
            else:
                logger.error(f"第{i+1}个注册令牌创建失败, admin_id={admin.get('id')}")

        if not tokens:
            logger.error(f"注册令牌创建全部失败, admin_id={admin.get('id')}")
            raise HTTPException(status_code=500, detail="创建注册令牌失败")

        # 如果是批量生成,返回所有令牌;如果是单个生成,保持原有格式
        if batch_count == 1:
            return JSONResponse({
                "status": "success",
                "message": "注册令牌创建成功",
                "token": tokens[0],
                "registration_url": registration_urls[0],
                "max_uses": max_uses,
                "expires_days": expires_days
            })
        else:
            return JSONResponse({
                "status": "success",
                "message": f"成功创建{len(tokens)}个注册令牌",
                "tokens": tokens,
                "registration_urls": registration_urls,
                "max_uses": max_uses,
                "expires_days": expires_days,
                "total": len(tokens),
                "requested": batch_count
            })

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"创建注册令牌失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get(f"/{PASSWORD}/api/admin/registration-tokens/list" if PASSWORD else "/api/admin/registration-tokens/list")
async def list_registration_tokens_api(request: Request, page: int = Query(1, ge=1), page_size: int = Query(100, ge=1, le=1000), search: Optional[str] = Query(None, description="搜索关键词")):
    """获取所有注册令牌列表(仅管理员)"""
    try:
        admin = await auth_manager_instance.require_admin(request)

        offset = (page - 1) * page_size
        tokens = await registration_token_model.get_all_tokens(limit=page_size, offset=offset, search=search)
        total = await registration_token_model.count_tokens(search=search)

        return JSONResponse({
            "status": "success",
            "data": tokens,
            "total": total,
            "page": page,
            "page_size": page_size,
            "total_pages": (total + page_size - 1) // page_size
        })

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"获取注册令牌列表失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post(f"/{PASSWORD}/api/admin/registration-tokens/{{token}}/toggle" if PASSWORD else "/api/admin/registration-tokens/{token}/toggle")
async def toggle_registration_token_api(request: Request, token: str):
    """启用/禁用注册令牌(仅管理员)"""
    try:
        admin = await auth_manager_instance.require_admin(request)

        token_info = await registration_token_model.get_token(token)
        if not token_info:
            raise HTTPException(status_code=404, detail="注册令牌不存在")

        if token_info['is_active']:
            success = await registration_token_model.deactivate_token(token)
            message = "注册令牌已禁用"
        else:
            success = await registration_token_model.activate_token(token)
            message = "注册令牌已启用"

        if not success:
            raise HTTPException(status_code=500, detail="操作失败")

        return JSONResponse({
            "status": "success",
            "message": message
        })

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"切换注册令牌状态失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.delete(f"/{PASSWORD}/api/admin/registration-tokens/{{token}}" if PASSWORD else "/api/admin/registration-tokens/{token}")
async def delete_registration_token_api(request: Request, token: str):
    """删除注册令牌(仅管理员)"""
    try:
        admin = await auth_manager_instance.require_admin(request)

        success = await registration_token_model.delete_token(token)

        if not success:
            raise HTTPException(status_code=500, detail="删除失败")

        return JSONResponse({
            "status": "success",
            "message": "注册令牌已删除"
        })

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"删除注册令牌失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get(f"/{PASSWORD}/api/admin/registration-tokens/history" if PASSWORD else "/api/admin/registration-tokens/history")
async def get_registration_token_history_api(request: Request):
    """获取所有注册令牌使用历史(仅管理员)"""
    try:
        admin = await auth_manager_instance.require_admin(request)

        history = await registration_token_model.get_token_history()

        return JSONResponse({
            "status": "success",
            "data": history
        })

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"获取令牌使用历史失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/registration/validate-token")
async def validate_registration_token_api(token: str):
    """验证注册令牌是否有效(公开接口,用于注册页面检查)"""
    try:
        if not token:
            raise HTTPException(status_code=400, detail="缺少令牌参数")

        is_valid, message = await registration_token_model.validate_token(token)

        if not is_valid:
            return JSONResponse({
                "status": "error",
                "message": message
            }, status_code=400)

        return JSONResponse({
            "status": "success",
            "message": "令牌有效"
        })

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"验证注册令牌失败: {e}")
        raise HTTPException(status_code=500, detail="验证失败")

@app.get(f"/{PASSWORD}/api/admin/stats" if PASSWORD else "/api/admin/stats")
async def get_admin_stats_api(request: Request):
    """获取系统统计信息(仅管理员)"""
    try:
        admin = await auth_manager_instance.require_admin(request)

        # 用户统计
        all_users = await user_model.get_all_users(limit=10000)
        total_users = len(all_users)
        admin_users = len([u for u in all_users if u['is_admin']])
        total_pages_granted = sum([u['total_pages'] for u in all_users])
        total_pages_used = sum([u['used_pages'] for u in all_users])

        # 兑换码统计
        all_codes = await redemption_code_model.get_all_codes(limit=10000)
        total_codes = len(all_codes)
        active_codes = len([c for c in all_codes if c['is_active']])

        return JSONResponse({
            "status": "success",
            "data": {
                "users": {
                    "total": total_users,
                    "admins": admin_users,
                    "regular": total_users - admin_users
                },
                "pages": {
                    "total_granted": total_pages_granted,
                    "total_used": total_pages_used,
                    "remaining": total_pages_granted - total_pages_used
                },
                "codes": {
                    "total": total_codes,
                    "active": active_codes,
                    "inactive": total_codes - active_codes
                }
            }
        })

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"获取统计信息失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# ==================== 管理员任务管理API ====================

@app.get(f"/{PASSWORD}/api/admin/tasks/list" if PASSWORD else "/api/admin/tasks/list")
async def list_all_tasks_api(request: Request,
                             page: int = Query(1, ge=1),
                             page_size: int = Query(100, ge=1, le=1000),
                             user_id: Optional[str] = Query(None),
                             search: Optional[str] = Query(None)):
    """获取所有任务列表（包含用户信息，仅管理员）"""
    try:
        admin = await auth_manager_instance.require_admin(request)
        from models.database import task_model

        offset = (page - 1) * page_size
        tasks = await task_model.get_all_tasks_with_users(
            limit=page_size,
            offset=offset,
            user_id_filter=user_id,
            search_query=search
        )
        total = await task_model.count_tasks(user_id_filter=user_id, search_query=search)

        return JSONResponse({
            "status": "success",
            "data": tasks,
            "total": total,
            "page": page,
            "page_size": page_size,
            "total_pages": (total + page_size - 1) // page_size
        })

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"获取任务列表失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get(f"/{PASSWORD}/api/admin/tasks/stats" if PASSWORD else "/api/admin/tasks/stats")
async def get_tasks_stats_api(request: Request, user_id: Optional[str] = Query(None)):
    """获取任务统计信息（仅管理员）"""
    try:
        admin = await auth_manager_instance.require_admin(request)
        from models.database import task_model

        stats = await task_model.get_task_stats_by_user(user_id=user_id)

        return JSONResponse({
            "status": "success",
            "data": stats
        })

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"获取任务统计失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post(f"/{PASSWORD}/api/admin/tasks/{{task_id}}/cancel" if PASSWORD else "/api/admin/tasks/{task_id}/cancel")
async def cancel_task_api(request: Request, task_id: str):
    """管理员取消任务"""
    try:
        await auth_manager_instance.require_admin(request)
        if not task_manager:
            raise HTTPException(status_code=503, detail="任务管理器未初始化")

        success, message = await task_manager.cancel_task(task_id, reason="管理员手动停止")
        status = "success" if success else "error"
        return JSONResponse({
            "status": status,
            "message": message
        })

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"管理员取消任务失败 {task_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get(f"/{PASSWORD}/api/admin/tasks/{{task_id}}/content" if PASSWORD else "/api/admin/tasks/{task_id}/content")
async def get_task_content_api(request: Request, task_id: str):
    """获取任务的OCR内容（仅管理员）"""
    try:
        admin = await auth_manager_instance.require_admin(request)
        from models.database import task_model, page_result_model

        # 获取任务信息
        task = await task_model.get_task(task_id)
        if not task:
            raise HTTPException(status_code=404, detail="任务不存在")

        # 获取所有页面结果
        page_results = await page_result_model.get_task_page_results(task_id)

        # 整理内容
        content_data = {
            "task_id": task_id,
            "file_name": task.get("file_name"),
            "status": task.get("status"),
            "total_pages": task.get("total_pages", 0),
            "processed_pages": task.get("processed_pages", 0),
            "created_at": task.get("created_at"),
            "pages": []
        }

        # 添加每页的内容
        for page in page_results:
            content_data["pages"].append({
                "page_number": page.get("page_number"),
                "status": page.get("status"),
                "content": page.get("content", ""),
                "error_message": page.get("error_message")
            })

        return JSONResponse({
            "status": "success",
            "data": content_data
        })

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"获取任务内容失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# Docker/k8s 专用健康检查端点（无密码保护，符合最佳实践）
@app.get("/healthz")
@app.get("/health")
async def health_check():
    """容器健康检查端点（不需要密码认证）"""
    try:
        # 基本检查：应用是否运行
        if not task_manager:
            return JSONResponse(
                status_code=503,
                content={"status": "unhealthy", "reason": "任务管理器未初始化"}
            )

        # 快速检查数据库连接
        try:
            from models.database import db_manager
            import aiosqlite
            async with aiosqlite.connect(db_manager.db_path) as db:
                await db.execute("SELECT 1")
        except Exception as db_error:
            return JSONResponse(
                status_code=503,
                content={"status": "unhealthy", "reason": f"数据库连接失败: {str(db_error)}"}
            )

        return JSONResponse(
            status_code=200,
            content={
                "status": "healthy",
                "timestamp": datetime.now().isoformat()
            }
        )
    except Exception as e:
        return JSONResponse(
            status_code=503,
            content={"status": "unhealthy", "reason": str(e)}
        )

# 新增：系统健康状态API（需要密码保护）
@app.get(f"/{PASSWORD}/api/system/health" if PASSWORD else "/api/system/health")
async def get_system_health_api():
    """获取系统健康状态API"""
    if not task_manager:
        raise HTTPException(status_code=503, detail="任务管理器未初始化")
    
    try:
        # 获取系统健康状态
        stats = await task_manager.get_system_stats()
        system_health = stats.get('system_health', {})
        
        # 计算健康状态
        overall_status = "healthy"
        issues = []
        
        # 检查系统资源
        if 'system_monitor' in system_health:
            metrics = system_health['system_monitor']
            if metrics.get('cpu_percent', 0) > 90:
                issues.append("CPU使用率过高")
            if metrics.get('memory_percent', 0) > 90:
                issues.append("内存使用率过高")
            if metrics.get('disk_percent', 0) > 90:
                issues.append("磁盘使用率过高")
        
        # 检查超时管理器
        if 'timeout_manager' in system_health:
            timeout_stats = system_health['timeout_manager']
            if timeout_stats.get('active_timeouts', 0) > 10:
                issues.append("活跃超时过多")
        
        # 检查错误处理器
        if 'error_handler' in system_health:
            error_stats = system_health['error_handler']
            error_rate = error_stats.get('error_rate', 0)
            if error_rate > 0.1:  # 错误率超过10%
                issues.append("系统错误率过高")
        
        # 确定整体状态
        if issues:
            overall_status = "warning" if len(issues) <= 2 else "critical"
        
        return JSONResponse({
            "status": "success",
            "data": {
                "overall_status": overall_status,
                "timestamp": datetime.now().isoformat(),
                "issues": issues,
                "system_health": system_health
            }
        })
        
    except Exception as e:
        logger.error(f"获取系统健康状态失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# 新增：系统监控指标API
@app.get(f"/{PASSWORD}/api/system/metrics" if PASSWORD else "/api/system/metrics")
async def get_system_metrics_api():
    """获取系统监控指标API"""
    if not task_manager:
        raise HTTPException(status_code=503, detail="任务管理器未初始化")
    
    try:
        stats = await task_manager.get_system_stats()
        system_health = stats.get('system_health', {})
        
        metrics = {
            "timestamp": datetime.now().isoformat(),
            "system_resources": system_health.get('system_monitor', {}),
            "timeout_stats": system_health.get('timeout_manager', {}),
            "error_stats": system_health.get('error_handler', {}),
            "recovery_stats": system_health.get('recovery_manager', {}),
            "api_performance": system_health.get('system_monitor', {}).get('api_stats', {})
        }
        
        return JSONResponse({
            "status": "success",
            "data": metrics
        })
        
    except Exception as e:
        logger.error(f"获取系统监控指标失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# 新增：错误恢复统计API
@app.get(f"/{PASSWORD}/api/system/recovery" if PASSWORD else "/api/system/recovery")
async def get_recovery_stats_api():
    """获取错误恢复统计API"""
    if not task_manager:
        raise HTTPException(status_code=503, detail="任务管理器未初始化")

    try:
        stats = await task_manager.get_system_stats()
        system_health = stats.get('system_health', {})

        recovery_stats = system_health.get('recovery_manager', {})
        error_stats = system_health.get('error_handler', {})

        return JSONResponse({
            "status": "success",
            "data": {
                "recovery_manager": recovery_stats,
                "error_handler": error_stats,
                "summary": {
                    "total_checkpoints": recovery_stats.get('total_checkpoints', 0),
                    "successful_recoveries": recovery_stats.get('successful_recoveries', 0),
                    "failed_recoveries": recovery_stats.get('failed_recoveries', 0),
                    "total_errors_handled": error_stats.get('total_errors_handled', 0),
                    "recovery_success_rate": recovery_stats.get('recovery_success_rate', 0),
                    "error_rate": error_stats.get('error_rate', 0)
                }
            }
        })

    except Exception as e:
        logger.error(f"获取错误恢复统计失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# 审查配置管理接口（仅管理员）
@app.get(f"/{PASSWORD}/api/moderation/config" if PASSWORD else "/api/moderation/config")
async def get_moderation_config_api(request: Request):
    """获取审查配置（仅管理员）"""
    # 验证管理员权限
    user = await auth_manager_instance.get_current_user(request)
    if not user or not user.get('is_admin'):
        raise HTTPException(status_code=403, detail="需要管理员权限")

    from core.content_moderator import get_moderation_config
    config = get_moderation_config()

    return JSONResponse({
        "status": "success",
        "data": config.get_all_config()
    })

@app.post(f"/{PASSWORD}/api/moderation/config" if PASSWORD else "/api/moderation/config")
async def update_moderation_config_api(request: Request):
    """更新审查配置（运行时，重启失效）（仅管理员）"""
    # 验证管理员权限
    user = await auth_manager_instance.get_current_user(request)
    if not user or not user.get('is_admin'):
        raise HTTPException(status_code=403, detail="需要管理员权限")

    try:
        data = await request.json()

        from core.content_moderator import get_moderation_config
        config = get_moderation_config()

        # 更新运行时配置
        config.update_runtime(data)

        logger.info(f"管理员 {user['username']} 更新了审查配置（运行时）: {data}")

        return JSONResponse({
            "status": "success",
            "message": "审查配置已更新（仅本次运行有效，重启后恢复ENV配置）",
            "data": config.get_all_config()
        })

    except Exception as e:
        logger.error(f"更新审查配置失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post(f"/{PASSWORD}/api/moderation/config/reset" if PASSWORD else "/api/moderation/config/reset")
async def reset_moderation_config_api(request: Request):
    """重置审查配置为ENV默认值（仅管理员）"""
    # 验证管理员权限
    user = await auth_manager_instance.get_current_user(request)
    if not user or not user.get('is_admin'):
        raise HTTPException(status_code=403, detail="需要管理员权限")

    try:
        from core.content_moderator import get_moderation_config
        config = get_moderation_config()

        # 重置运行时配置
        config.reset_runtime()

        logger.info(f"管理员 {user['username']} 重置了审查配置为ENV默认值")

        return JSONResponse({
            "status": "success",
            "message": "审查配置已重置为ENV默认值",
            "data": config.get_all_config()
        })

    except Exception as e:
        logger.error(f"重置审查配置失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get(f"/{PASSWORD}/api/moderation/logs" if PASSWORD else "/api/moderation/logs")
async def get_moderation_logs_api(
    request: Request,
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0)
):
    """获取审查日志（仅管理员）"""
    # 验证管理员权限
    user = await auth_manager_instance.get_current_user(request)
    if not user or not user.get('is_admin'):
        raise HTTPException(status_code=403, detail="需要管理员权限")

    try:
        from models.database import db_manager
        async with db_manager.get_connection() as db:
            # 查询审查日志
            async with db.execute("""
                SELECT
                    ml.*,
                    t.file_name,
                    t.status as task_status,
                    u.username
                FROM moderation_logs ml
                LEFT JOIN tasks t ON ml.task_id = t.id
                LEFT JOIN users u ON t.user_id = u.id
                ORDER BY ml.created_at DESC
                LIMIT ? OFFSET ?
            """, (limit, offset)) as cursor:
                rows = await cursor.fetchall()

                logs = []
                for row in rows:
                    logs.append({
                        "id": row[0],
                        "task_id": row[1],
                        "page_number": row[2],
                        "action": row[3],
                        "risk_level": row[4],
                        "violation_types": json.loads(row[5]) if row[5] else [],
                        "reason": row[6],
                        "original_content": row[7][:200] if row[7] else None,  # 只返回前200字符
                        "created_at": row[8],
                        "file_name": row[9] if len(row) > 9 else None,
                        "task_status": row[10] if len(row) > 10 else None,
                        "username": row[11] if len(row) > 11 else None
                    })

            # 获取总数
            async with db.execute("SELECT COUNT(*) FROM moderation_logs") as cursor:
                total_row = await cursor.fetchone()
                total = total_row[0] if total_row else 0

        return JSONResponse({
            "status": "success",
            "data": {
                "logs": logs,
                "total": total,
                "limit": limit,
                "offset": offset
            }
        })

    except Exception as e:
        logger.error(f"获取审查日志失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get(f"/{PASSWORD}/api/moderation/stats" if PASSWORD else "/api/moderation/stats")
async def get_moderation_stats_api(request: Request):
    """获取审查统计（仅管理员）"""
    # 验证管理员权限
    user = await auth_manager_instance.get_current_user(request)
    if not user or not user.get('is_admin'):
        raise HTTPException(status_code=403, detail="需要管理员权限")

    try:
        from models.database import db_manager
        async with db_manager.get_connection() as db:
            # 总审查次数
            async with db.execute("SELECT COUNT(*) FROM moderation_logs") as cursor:
                total_row = await cursor.fetchone()
                total = total_row[0] if total_row else 0

            # 动作分布
            async with db.execute("""
                SELECT action, COUNT(*) as count
                FROM moderation_logs
                GROUP BY action
            """) as cursor:
                action_rows = await cursor.fetchall()
                action_dist = {row[0]: row[1] for row in action_rows}

            # 风险等级分布
            async with db.execute("""
                SELECT risk_level, COUNT(*) as count
                FROM moderation_logs
                GROUP BY risk_level
            """) as cursor:
                risk_rows = await cursor.fetchall()
                risk_dist = {row[0]: row[1] for row in risk_rows}

            # 最近24小时审查次数
            async with db.execute("""
                SELECT COUNT(*) FROM moderation_logs
                WHERE datetime(created_at) >= datetime('now', '-1 day')
            """) as cursor:
                recent_row = await cursor.fetchone()
                recent_24h = recent_row[0] if recent_row else 0

        return JSONResponse({
            "status": "success",
            "data": {
                "total": total,
                "action_distribution": action_dist,
                "risk_distribution": risk_dist,
                "recent_24h": recent_24h
            }
        })

    except Exception as e:
        logger.error(f"获取审查统计失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))
# 新增：异步任务提交端点
@app.post(f"/{PASSWORD}/api/async/image" if PASSWORD else "/api/async/image")
async def submit_async_image_task(file: UploadFile, request: Request):
    """提交异步图片OCR任务"""
    import tempfile

    if not task_manager:
        raise HTTPException(status_code=503, detail="任务管理器未初始化")

    if not file:
        raise HTTPException(status_code=400, detail="未收到文件")

    tmp_file_path = None
    try:
        # 验证文件类型
        if not file.content_type or not file.content_type.startswith('image/'):
            raise HTTPException(status_code=400, detail="不支持的文件类型，请上传图片文件")

        # 创建临时文件保存图片
        with tempfile.NamedTemporaryFile(delete=False, suffix='.img') as tmp_file:
            tmp_file_path = tmp_file.name

            # 分块写入临时文件,避免内存占用
            chunk_size = 1024 * 1024  # 1MB每块
            total_size = 0
            max_file_size = runtime_config.max_image_size_mb * 1024 * 1024

            while chunk := await file.read(chunk_size):
                total_size += len(chunk)
                # 检查文件大小
                if total_size > max_file_size:
                    tmp_file.close()
                    os.unlink(tmp_file_path)
                    raise HTTPException(status_code=400, detail=f"文件过大，最大支持{runtime_config.max_image_size_mb}MB")
                tmp_file.write(chunk)

        logger.info(f"图片文件已写入临时文件: {tmp_file_path}")

        # 获取文件大小
        file_size_mb = os.path.getsize(tmp_file_path) / (1024 * 1024)

        # 获取客户端IP
        client_ip = get_client_ip(request)

        task_id = await task_manager.submit_image_task_from_file(
            file_path=tmp_file_path,
            file_name=file.filename or "image",
            metadata={
                "source": "async_api",
                "file_type": file.content_type,
                "original_filename": file.filename,
                "file_size_mb": file_size_mb,
                "client_ip": client_ip
            }
        )

        return JSONResponse({
            "status": "success",
            "task_id": task_id,
            "message": "图片任务已提交",
            "estimated_time": 30,
            "endpoints": {
                "status": f"/api/tasks/{task_id}",
                "result": f"/api/tasks/{task_id}/result",
                "cancel": f"/api/tasks/{task_id}",
                "websocket": f"/ws"
            }
        })

    except HTTPException:
        # HTTP异常时清理临时文件
        if tmp_file_path and os.path.exists(tmp_file_path):
            try:
                os.unlink(tmp_file_path)
            except:
                pass
        raise
    except Exception as e:
        # 其他异常时清理临时文件
        if tmp_file_path and os.path.exists(tmp_file_path):
            try:
                os.unlink(tmp_file_path)
            except:
                pass
        logger.error(f"提交异步图片任务失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post(f"/{PASSWORD}/api/async/pdf" if PASSWORD else "/api/async/pdf")
async def submit_async_pdf_task(file: UploadFile, request: Request):
    """提交异步PDF OCR任务"""
    if not task_manager:
        raise HTTPException(status_code=503, detail="任务管理器未初始化")

    if not file:
        raise HTTPException(status_code=400, detail="未收到文件")

    try:
        # 流式读取文件，避免一次性加载到内存
        file_data = await file.read()
        if not file_data:
            raise HTTPException(status_code=400, detail="文件内容为空")

        # 验证文件类型
        if not file.content_type or file.content_type != 'application/pdf':
            raise HTTPException(status_code=400, detail="不支持的文件类型，请上传PDF文件")

        # 文件大小检查（使用环境变量配置）
        max_file_size = runtime_config.max_pdf_size_mb * 1024 * 1024
        if len(file_data) > max_file_size:
            raise HTTPException(status_code=400, detail=f"文件过大，最大支持{runtime_config.max_pdf_size_mb}MB")

        # 获取PDF文件信息
        try:
            page_count, file_size_mb = check_pdf_info(file_data)
            logger.info(f"PDF文件信息: {page_count} 页, {file_size_mb:.2f} MB")
        except Exception as e:
            logger.error(f"PDF文件信息获取失败: {e}")
            raise HTTPException(status_code=400, detail="PDF文件格式错误或损坏")

        # 页数限制检查（使用环境变量配置）
        if page_count > runtime_config.max_pdf_pages:
            raise HTTPException(status_code=400, detail=f"PDF页数过多，最大支持{runtime_config.max_pdf_pages}页")

        # 获取客户端IP
        client_ip = get_client_ip(request)

        task_id = await task_manager.submit_pdf_task(
            file_data=file_data,
            file_name=file.filename or "document.pdf",
            metadata={
                "source": "async_api",
                "file_type": file.content_type,
                "original_filename": file.filename,
                "file_size_mb": file_size_mb,
                "client_ip": client_ip
            }
        )
        
        # 估算处理时间
        estimated_time = page_count * 3  # 每页约3秒
        
        return JSONResponse({
            "status": "success",
            "task_id": task_id,
            "message": "PDF任务已提交",
            "total_pages": page_count,
            "file_size_mb": f"{file_size_mb:.2f}",
            "estimated_time": estimated_time,
            "endpoints": {
                "status": f"/api/tasks/{task_id}",
                "result": f"/api/tasks/{task_id}/result",
                "cancel": f"/api/tasks/{task_id}",
                "websocket": f"/ws"
            }
        })
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"提交异步PDF任务失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# 新增：批量任务提交端点
@app.post(f"/{PASSWORD}/api/async/batch" if PASSWORD else "/api/async/batch")
async def submit_batch_tasks(files: list[UploadFile], request: Request):
    """提交批量异步任务"""
    if not task_manager:
        raise HTTPException(status_code=503, detail="任务管理器未初始化")

    if not files or len(files) == 0:
        raise HTTPException(status_code=400, detail="未收到文件")

    if len(files) > 20:  # 限制批量上传数量
        raise HTTPException(status_code=400, detail="批量上传最多支持20个文件")

    try:
        # 获取客户端IP（对整个批次只获取一次）
        client_ip = get_client_ip(request)

        submitted_tasks = []
        failed_files = []

        for file in files:
            tmp_file_path = None
            try:
                # 根据文件类型选择处理方式
                if file.content_type and file.content_type.startswith('image/'):
                    # 图片任务 - 使用临时文件
                    import tempfile
                    with tempfile.NamedTemporaryFile(delete=False, suffix='.img') as tmp_file:
                        tmp_file_path = tmp_file.name

                        chunk_size = 1024 * 1024  # 1MB每块
                        total_size = 0
                        max_file_size = runtime_config.max_image_size_mb * 1024 * 1024

                        while chunk := await file.read(chunk_size):
                            total_size += len(chunk)
                            if total_size > max_file_size:
                                tmp_file.close()
                                os.unlink(tmp_file_path)
                                failed_files.append({"filename": file.filename, "error": f"图片文件过大(最大{runtime_config.max_image_size_mb}MB)"})
                                tmp_file_path = None
                                break
                            tmp_file.write(chunk)

                    if tmp_file_path:
                        file_size_mb = os.path.getsize(tmp_file_path) / (1024 * 1024)

                        task_id = await task_manager.submit_image_task_from_file(
                            file_path=tmp_file_path,
                            file_name=file.filename or "image",
                            priority=1,  # 批量任务优先级稍低
                            metadata={
                                "source": "batch_api",
                                "file_type": file.content_type,
                                "original_filename": file.filename,
                                "file_size_mb": file_size_mb,
                                "client_ip": client_ip
                            }
                        )

                        submitted_tasks.append({
                            "task_id": task_id,
                            "filename": file.filename,
                            "type": "image",
                            "estimated_time": 30
                        })
                        tmp_file_path = None  # 已提交,不需要清理
                    
                elif file.content_type == 'application/pdf':
                    # PDF任务 - 使用临时文件
                    import tempfile
                    with tempfile.NamedTemporaryFile(delete=False, suffix='.pdf') as tmp_file:
                        tmp_file_path = tmp_file.name

                        chunk_size = 1024 * 1024  # 1MB每块
                        total_size = 0
                        max_file_size = runtime_config.max_pdf_size_mb * 1024 * 1024

                        while chunk := await file.read(chunk_size):
                            total_size += len(chunk)
                            if total_size > max_file_size:
                                tmp_file.close()
                                os.unlink(tmp_file_path)
                                failed_files.append({"filename": file.filename, "error": f"PDF文件过大(最大{runtime_config.max_pdf_size_mb}MB)"})
                                tmp_file_path = None
                                break
                            tmp_file.write(chunk)

                    if tmp_file_path:
                        # 获取PDF信息
                        try:
                            with open(tmp_file_path, 'rb') as f:
                                pdf_bytes = f.read()
                                page_count, file_size_mb = check_pdf_info(pdf_bytes)
                                pdf_bytes = None  # 立即释放

                            if page_count > runtime_config.max_pdf_pages:
                                os.unlink(tmp_file_path)
                                failed_files.append({"filename": file.filename, "error": f"PDF页数过多(最大{runtime_config.max_pdf_pages}页)"})
                                tmp_file_path = None
                            else:
                                task_id = await task_manager.submit_pdf_task_from_file(
                                    file_path=tmp_file_path,
                                    file_name=file.filename or "document.pdf",
                                    priority=1,  # 批量任务优先级稍低
                                    metadata={
                                        "source": "batch_api",
                                        "file_type": file.content_type,
                                        "original_filename": file.filename,
                                        "file_size_mb": file_size_mb,
                                        "total_pages": page_count,
                                        "client_ip": client_ip
                                    }
                                )

                                submitted_tasks.append({
                                    "task_id": task_id,
                                    "filename": file.filename,
                                    "type": "pdf",
                                    "total_pages": page_count,
                                    "estimated_time": page_count * 3
                                })
                                tmp_file_path = None  # 已提交,不需要清理
                        except Exception as e:
                            if tmp_file_path and os.path.exists(tmp_file_path):
                                os.unlink(tmp_file_path)
                            failed_files.append({"filename": file.filename, "error": f"PDF文件格式错误: {str(e)}"})
                            tmp_file_path = None

                else:
                    failed_files.append({"filename": file.filename, "error": "不支持的文件类型"})

            except Exception as e:
                # 清理临时文件
                if tmp_file_path and os.path.exists(tmp_file_path):
                    try:
                        os.unlink(tmp_file_path)
                    except:
                        pass
                failed_files.append({"filename": file.filename, "error": str(e)})
        
        total_estimated_time = sum(task.get("estimated_time", 0) for task in submitted_tasks)
        
        return JSONResponse({
            "status": "success",
            "message": f"批量任务提交完成，成功: {len(submitted_tasks)}, 失败: {len(failed_files)}",
            "submitted_tasks": submitted_tasks,
            "failed_files": failed_files,
            "total_tasks": len(submitted_tasks),
            "total_estimated_time": total_estimated_time,
            "endpoints": {
                "batch_status": "/api/tasks/batch/status",
                "websocket": "/ws"
            }
        })
        
    except Exception as e:
        logger.error(f"批量任务提交失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# 新增：批量任务状态查询
@app.post(f"/{PASSWORD}/api/tasks/batch/status" if PASSWORD else "/api/tasks/batch/status")
async def get_batch_tasks_status(task_ids: dict):
    """查询批量任务状态"""
    if not task_manager:
        raise HTTPException(status_code=503, detail="任务管理器未初始化")
    
    task_id_list = task_ids.get("task_ids", [])
    if not task_id_list or len(task_id_list) == 0:
        raise HTTPException(status_code=400, detail="未提供任务ID")
    
    if len(task_id_list) > 50:  # 限制查询数量
        raise HTTPException(status_code=400, detail="批量查询最多支持50个任务")
    
    try:
        tasks_status = []
        
        for task_id in task_id_list:
            try:
                task_info = await task_manager.get_task_info(task_id)
                if task_info:
                    tasks_status.append({
                        "task_id": task_id,
                        "status": task_info["status"],
                        "progress": task_info.get("progress", 0),
                        "created_at": task_info["created_at"],
                        "completed_at": task_info.get("completed_at"),
                        "error_message": task_info.get("error_message")
                    })
                else:
                    tasks_status.append({
                        "task_id": task_id,
                        "status": "not_found",
                        "error_message": "任务不存在"
                    })
            except Exception as e:
                tasks_status.append({
                    "task_id": task_id,
                    "status": "error",
                    "error_message": str(e)
                })
        
        # 统计信息
        status_counts = {}
        for task in tasks_status:
            status = task["status"]
            status_counts[status] = status_counts.get(status, 0) + 1
        
        return JSONResponse({
            "status": "success",
            "tasks": tasks_status,
            "summary": {
                "total_tasks": len(tasks_status),
                "status_counts": status_counts
            }
        })
        
    except Exception as e:
        logger.error(f"批量任务状态查询失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# 新增：分页相关API端点
@app.get(f"/{PASSWORD}/api/tasks/{{task_id}}/pages" if PASSWORD else "/api/tasks/{task_id}/pages")
async def get_task_pages_api(task_id: str):
    """获取任务的分页列表和状态"""
    if not task_manager:
        raise HTTPException(status_code=503, detail="任务管理器未初始化")
    
    try:
        pages = await task_manager.get_task_pages(task_id)
        if not pages and not await task_manager.get_task_info(task_id):
            raise HTTPException(status_code=404, detail="任务不存在")
        
        return JSONResponse({
            "status": "success",
            "data": {
                "task_id": task_id,
                "pages": pages,
                "total_pages": len(pages),
                "page_statistics": _calculate_page_statistics(pages)
            }
        })
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"获取任务分页失败 {task_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get(f"/{PASSWORD}/api/tasks/{{task_id}}/pages/{{page_num}}" if PASSWORD else "/api/tasks/{task_id}/pages/{page_num}")
async def get_task_page_api(task_id: str, page_num: int):
    """获取特定页面结果"""
    if not task_manager:
        raise HTTPException(status_code=503, detail="任务管理器未初始化")
    
    if page_num < 1:
        raise HTTPException(status_code=400, detail="页码必须大于0")
    
    try:
        page_result = await task_manager.get_task_page(task_id, page_num)
        if not page_result:
            # 检查任务是否存在
            task_info = await task_manager.get_task_info(task_id)
            if not task_info:
                raise HTTPException(status_code=404, detail="任务不存在")
            elif page_num > task_info.get('total_pages', 0):
                raise HTTPException(status_code=404, detail="页码超出范围")
            else:
                raise HTTPException(status_code=404, detail="页面结果不存在")
        
        return JSONResponse({
            "status": "success",
            "data": {
                "task_id": task_id,
                "page_number": page_num,
                "page_result": page_result
            }
        })
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"获取任务页面失败 {task_id}-{page_num}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get(f"/{PASSWORD}/api/tasks/{{task_id}}/progress" if PASSWORD else "/api/tasks/{task_id}/progress")
async def get_task_progress_detail_api(task_id: str):
    """获取详细进度信息"""
    if not task_manager:
        raise HTTPException(status_code=503, detail="任务管理器未初始化")
    
    try:
        progress_detail = await task_manager.get_task_progress_detail(task_id)
        if not progress_detail:
            raise HTTPException(status_code=404, detail="任务不存在")
        
        return JSONResponse({
            "status": "success",
            "data": progress_detail
        })
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"获取任务详细进度失败 {task_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))



@app.get(f"/{PASSWORD}/api/tasks/{{task_id}}/batches" if PASSWORD else "/api/tasks/{task_id}/batches")
async def get_task_batches_api(task_id: str):
    """获取任务的批次信息"""
    if not task_manager:
        raise HTTPException(status_code=503, detail="任务管理器未初始化")
    
    try:
        # 获取详细进度信息（包含批次信息）
        progress_detail = await task_manager.get_task_progress_detail(task_id)
        if not progress_detail:
            raise HTTPException(status_code=404, detail="任务不存在")
        
        batches = progress_detail.get('batches', [])
        
        return JSONResponse({
            "status": "success",
            "data": {
                "task_id": task_id,
                "batches": batches,
                "total_batches": len(batches)
            }
        })
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"获取任务批次信息失败 {task_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get(f"/{PASSWORD}/api/tasks/{{task_id}}/export" if PASSWORD else "/api/tasks/{task_id}/export")
async def export_task_result_api(task_id: str, format: str = Query("json", description="导出格式: json, txt, md")):
    """导出任务结果"""
    if not task_manager:
        raise HTTPException(status_code=503, detail="任务管理器未初始化")
    
    if format not in ["json", "txt", "md"]:
        raise HTTPException(status_code=400, detail="不支持的导出格式，支持: json, txt, md")
    
    try:
        task_result = await task_manager.get_task_result(task_id)
        if not task_result:
            raise HTTPException(status_code=404, detail="任务不存在")
        
        if task_result["status"] != "completed":
            raise HTTPException(status_code=400, detail="任务未完成，无法导出")
        
        content = task_result.get("content", "")
        filename = f"task_{task_id}_result"
        
        if format == "json":
            # JSON格式导出
            export_data = {
                "task_id": task_id,
                "export_time": datetime.now().isoformat(),
                "task_info": task_result,
                "content": content
            }
            
            response_content = json.dumps(export_data, ensure_ascii=False, indent=2)
            media_type = "application/json"
            filename += ".json"
            
        elif format == "txt":
            # 纯文本格式导出
            response_content = content
            media_type = "text/plain"
            filename += ".txt"
            
        elif format == "md":
            # Markdown格式导出
            response_content = f"# Task {task_id} Result\n\n{content}"
            media_type = "text/markdown"
            filename += ".md"
        
        return Response(
            content=response_content,
            media_type=media_type,
            headers={
                "Content-Disposition": f"attachment; filename={filename}",
                "Content-Type": f"{media_type}; charset=utf-8"
            }
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"导出任务结果失败 {task_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

def _calculate_page_statistics(pages: list) -> dict:
    """计算页面统计信息"""
    stats = {
        "total_pages": len(pages),
        "completed_pages": 0,
        "failed_pages": 0,
        "pending_pages": 0,
        "processing_pages": 0,
        "average_processing_time": 0,
        "total_content_length": 0,
        "success_rate": 0
    }
    
    if not pages:
        return stats
    
    total_processing_time = 0
    processing_time_count = 0
    
    for page in pages:
        status = page.get('status', 'pending')
        
        if status == 'completed':
            stats['completed_pages'] += 1
            stats['total_content_length'] += page.get('content_length', 0)
        elif status == 'failed':
            stats['failed_pages'] += 1
        elif status == 'processing':
            stats['processing_pages'] += 1
        else:
            stats['pending_pages'] += 1
        
        # 计算平均处理时间
        processing_time = page.get('processing_time')
        if processing_time:
            total_processing_time += processing_time
            processing_time_count += 1
    
    # 计算平均处理时间
    if processing_time_count > 0:
        stats['average_processing_time'] = total_processing_time / processing_time_count
    
    # 计算成功率
    if stats['total_pages'] > 0:
        stats['success_rate'] = (stats['completed_pages'] / stats['total_pages']) * 100
    
    return stats

if __name__ == "__main__":
    import uvicorn
    import argparse

    # 解析命令行参数
    parser = argparse.ArgumentParser(description="Web OCR API Server")
    # 从环境变量读取默认端口，如果没有则使用 54188
    default_port = int(os.getenv("PORT", 54188))
    parser.add_argument("--port", type=int, default=default_port, help=f"Port to run the server on (default: {default_port})")
    parser.add_argument("--host", type=str, default="0.0.0.0", help="Host to bind the server to (default: 0.0.0.0)")
    args = parser.parse_args()

    # 自定义日志过滤器 - 过滤掉轮询请求的日志
    class PollingFilter(logging.Filter):
        def filter(self, record):
            # 过滤掉GET请求（轮询）的访问日志
            message = record.getMessage()
            # 只过滤200状态的GET请求，保留错误日志
            if '"GET' in message and '200' in message:
                return False
            return True

    # 配置uvicorn日志 - 确保所有日志输出到控制台
    log_config = {
        "version": 1,
        "disable_existing_loggers": False,  # 不禁用现有的loggers
        "formatters": {
            "default": {
                "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            },
            "access": {
                "format": '%(asctime)s - %(levelname)s - %(message)s',
            },
        },
        "filters": {
            "polling_filter": {
                "()": PollingFilter,
            }
        },
        "handlers": {
            "default": {
                "formatter": "default",
                "class": "logging.StreamHandler",
                "stream": "ext://sys.stdout",
            },
            "access": {
                "formatter": "access",
                "class": "logging.StreamHandler",
                "stream": "ext://sys.stdout",
                "filters": ["polling_filter"],  # 为access日志添加过滤器
            },
        },
        "root": {
            "level": "INFO",  # 设置为INFO级别
            "handlers": ["default"],
        },
        "loggers": {
            "uvicorn": {
                "handlers": ["default"],
                "level": "INFO",
                "propagate": False
            },
            "uvicorn.error": {
                "handlers": ["default"],
                "level": "INFO",
                "propagate": False
            },
            "uvicorn.access": {
                "handlers": ["access"],
                "level": "INFO",
                "propagate": False
            },
            "core.page_processor": {"level": "INFO"},
            "core.task_manager": {"level": "INFO"},
            "models.database": {"level": "INFO"},
        },
    }

    # 显示启动信息
    try:
        print("\n" + "=" * 70)
        print(f"启动 Web OCR API 服务器")
        print("=" * 70)
        print(f"访问地址: http://{args.host}:{args.port}")
        if PASSWORD:
            print(f"受密码保护的路径: http://{args.host}:{args.port}/{PASSWORD}")
        else:
            print(f"主页: http://{args.host}:{args.port}/")
        print(f"API文档: http://{args.host}:{args.port}/docs")
        print("=" * 70 + "\n")
    except UnicodeEncodeError:
        # Windows控制台编码问题，使用ASCII字符
        print("\n" + "=" * 70)
        print(f"Starting Web OCR API Server")
        print("=" * 70)
        print(f"URL: http://{args.host}:{args.port}")
        if PASSWORD:
            print(f"Protected path: http://{args.host}:{args.port}/{PASSWORD}")
        else:
            print(f"Home: http://{args.host}:{args.port}/")
        print(f"API Docs: http://{args.host}:{args.port}/docs")
        print("=" * 70 + "\n")

    uvicorn.run(
        app,
        host=args.host,
        port=args.port,
        log_config=log_config  # 使用自定义日志配置
    )