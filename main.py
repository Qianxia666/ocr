from dotenv import load_dotenv
from fastapi.middleware.cors import CORSMiddleware
import fitz  # PyMuPDF
import logging
from io import BytesIO
from PIL import Image
import aiohttp
import asyncio
from fastapi import FastAPI, Request, UploadFile, Query, HTTPException, Response
from fastapi.responses import JSONResponse, HTMLResponse, StreamingResponse, RedirectResponse
from starlette.exceptions import HTTPException as StarletteHTTPException
from typing import Optional, Dict, Any
from contextlib import asynccontextmanager
import os
import json
from datetime import datetime
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from core.runtime_settings import (
    RuntimeConfig,
    ApiRuntimeConfig,
    TurnstileRuntimeConfig,
    _mask_secret,
    _parse_bool,
)
from core.image_utils import build_image_data_url
from core.routes.system_routes import register_system_routes
from core.routes.ui_websocket_routes import register_ui_websocket_routes
from core.routes.task_basic_routes import register_task_basic_routes
from core.routes.task_advanced_routes import register_task_advanced_routes

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
API_KEY = os.getenv("OPENAI_API_KEY", "")
MODEL = os.getenv("MODEL", "gpt-4o")
PASSWORD = os.getenv("PASSWORD", "")
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

# 图片压缩配置
IMAGE_COMPRESSION_RATIO = float(os.getenv("IMAGE_COMPRESSION_RATIO", 0.2))  # 图片目标体积比例(0.1-1.0)，默认0.2即目标为原文件20%

# 运行时配置管理器
runtime_config = RuntimeConfig()

api_runtime_config = ApiRuntimeConfig(API_BASE_URL, API_KEY, MODEL)
turnstile_config = TurnstileRuntimeConfig(
    TURNSTILE_SITE_KEY,
    TURNSTILE_SECRET_KEY,
    os.getenv("TURNSTILE_ENABLED")
)


def get_current_api_settings() -> Dict[str, str]:
    """获取当前生效的API配置（包含敏感信息）"""
    return api_runtime_config.get_effective_config(include_sensitive=True)


def apply_api_runtime_config():
    """将最新的API运行时配置应用到任务管理器和页面处理器"""
    effective = get_current_api_settings()

    try:
        if task_manager:
            task_manager.api_config.update({
                'api_base_url': effective.get('api_base_url'),
                'api_key': effective.get('api_key'),
                'model': effective.get('model'),
            })
    except Exception as e:
        logger.warning(f"更新任务管理器API配置失败: {e}")

    try:
        from core.page_processor import page_processor
        if page_processor:
            page_processor.api_config.update({
                'api_base_url': effective.get('api_base_url'),
                'api_key': effective.get('api_key'),
                'model': effective.get('model'),
            })
    except Exception as e:
        logger.warning(f"更新页面处理器API配置失败: {e}")


async def apply_runtime_settings():
    """将运行时设置同步到当前内存对象。"""
    effective = runtime_config.get_all()

    try:
        if task_manager:
            if hasattr(task_manager, 'refresh_runtime_config'):
                await task_manager.refresh_runtime_config()
            else:
                task_manager.api_config.update({
                    'concurrency': effective.get('concurrency'),
                    'image_concurrency': effective.get('image_concurrency'),
                    'pdf_concurrency': effective.get('pdf_concurrency'),
                    'batch_size': effective.get('batch_size'),
                    'pdf_dpi': effective.get('pdf_dpi'),
                    'max_retries': effective.get('max_retries'),
                })
    except Exception as e:
        logger.warning(f"更新任务管理器运行时配置失败: {e}")

    try:
        from core.page_processor import page_processor
        if page_processor:
            page_processor.api_config.update({
                'concurrency': effective.get('concurrency'),
                'batch_size': effective.get('batch_size'),
                'pdf_dpi': effective.get('pdf_dpi'),
            })
    except Exception as e:
        logger.warning(f"更新页面处理器运行时配置失败: {e}")


async def get_turnstile_enabled_status() -> tuple[bool, str]:
    """获取 Turnstile 验证开关状态"""
    return turnstile_config.get_enabled_with_source()

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
    secret_key = turnstile_config.get_config('secret_key')
    if not secret_key:
        logger.warning("Turnstile Secret Key 未配置,无法验证")
        return False, "人机验证配置错误,请联系管理员"

    if not token:
        return False, "缺少验证令牌"

    url = "https://challenges.cloudflare.com/turnstile/v0/siteverify"
    data = {
        "secret": secret_key,
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

        effective_api_config = api_runtime_config.get_effective_config(include_sensitive=True)

        api_config = {
            'api_base_url': effective_api_config.get('api_base_url', API_BASE_URL),
            'api_key': effective_api_config.get('api_key', API_KEY),
            'model': effective_api_config.get('model', MODEL),
            'concurrency': runtime_config.concurrency,
            'image_concurrency': runtime_config.image_concurrency,
            'pdf_concurrency': runtime_config.pdf_concurrency,
            'batch_size': runtime_config.batch_size,
            'max_retries': runtime_config.max_retries,
            'retry_delay': RETRY_DELAY,
            'pdf_dpi': runtime_config.pdf_dpi,
            'api_timeout': runtime_config.api_timeout,
        }

        # 初始化页面处理器
        from core.page_processor import init_page_processor, is_page_processor_ready, shutdown_page_processor
        logger.info("开始初始化页面处理器")

        # 准备静态API配置(不会变化的配置)
        static_api_config = {
            'api_base_url': effective_api_config.get('api_base_url', API_BASE_URL),
            'api_key': effective_api_config.get('api_key', API_KEY),
            'model': effective_api_config.get('model', MODEL),
            'concurrency': runtime_config.concurrency,
            'batch_size': runtime_config.batch_size,
            'pdf_dpi': runtime_config.pdf_dpi,
            'retry_delay': RETRY_DELAY,
            'health_check_interval': 300,
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

    api_base_url = api_runtime_config.get_config('api_base_url') or API_BASE_URL
    api_key = api_runtime_config.get_config('api_key') or API_KEY
    api_model = api_runtime_config.get_config('model') or MODEL

    if not api_key:
        raise Exception('API密钥未配置')

    system_prompt = """
You are an expert OCR assistant. Your job is to extract all text from the provided image and convert it into a well-structured, easy-to-read Markdown document that mirrors the intended structure of the original. Follow these precise guidelines:

- Use Markdown headings, paragraphs, lists, and tables to match the document’s hierarchy and flow.  
- For tables, use standard Markdown table syntax and merge cells if needed. If a table has a title, include it as plain text above the table.  
- Render mathematical formulas with LaTeX syntax: use $...$ for inline and $$...$$ for display equations.  
- For images, use the syntax ![descriptive alt text](link) with a clear, descriptive alt text.  
- Remove unnecessary line breaks so that the text flows naturally without awkward breaks.
- Your final Markdown output must be direct text (do not wrap it in code blocks).

Ensure your output is clear, accurate, and faithfully reflects the original image’s content and structure.
    """
    for attempt in range(max_retries):
        try:
            async with semaphore:
                image_data_url = build_image_data_url(image_data)
                response = await session.post(
                    f"{api_base_url}/v1/chat/completions",
                    headers={"Authorization": f"Bearer {api_key}"},
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
                                        "text": "Analyze the image and return the recognized content directly. Do not wrap the result in JSON and do not add any extra explanations."
                                    },
                                    {
                                        "type": "image_url",
                                        "image_url": {
                                            "url": image_data_url
                                        }
                                    }
                                ]
                            }
                        ],
                        "stream": False,
                        "model": api_model,
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

def check_pdf_info(pdf_source: Any) -> tuple[int, float]:
    """
    Return basic PDF metadata.
    :param pdf_source: PDF bytes or a local file path
    :return: (page_count, file_size_mb)
    """
    pdf_document = None
    try:
        if isinstance(pdf_source, (str, os.PathLike)):
            pdf_path = os.fspath(pdf_source)
            pdf_document = fitz.open(pdf_path)
            file_size_mb = os.path.getsize(pdf_path) / (1024 * 1024)
        elif isinstance(pdf_source, (bytes, bytearray, memoryview)):
            pdf_bytes = bytes(pdf_source)
            pdf_document = fitz.open(stream=pdf_bytes, filetype="pdf")
            file_size_mb = len(pdf_bytes) / (1024 * 1024)
        else:
            raise TypeError("pdf_source must be bytes-like or a filesystem path")

        page_count = len(pdf_document)
        logger.info("pdf_info_ok pages=%s size_mb=%.2f", page_count, file_size_mb)
        return page_count, file_size_mb
    except Exception as exc:
        logger.exception("pdf_info_failed source_type=%s error=%s", type(pdf_source).__name__, exc)
        raise
    finally:
        if pdf_document:
            pdf_document.close()


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

                content = (result or "").strip()
                if content.startswith("```") and content.endswith("```"):
                    lines = content.splitlines()
                    if len(lines) >= 2:
                        content = "\n".join(lines[1:-1]).strip()
                    else:
                        content = content.strip("`").strip()

                if content:
                    logger.info(f"第{page_number} 页OCR处理成功")
                    return content

                raise Exception("OCR返回内容为空")

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

register_ui_websocket_routes(
    app=app,
    password=PASSWORD,
    templates=templates,
    favicon_url=FAVICON_URL,
    title=TITLE,
    back_url=BACK_URL,
    runtime_config=runtime_config,
    api_runtime_config=api_runtime_config,
    default_model=MODEL,
    get_auth_manager=lambda: auth_manager_instance,
    websocket_manager=websocket_manager,
    logger=logger,
)

register_task_basic_routes(
    app=app,
    password=PASSWORD,
    runtime_config=runtime_config,
    websocket_manager=websocket_manager,
    logger=logger,
    get_task_manager=lambda: task_manager,
    get_auth_manager=lambda: auth_manager_instance,
    get_client_ip=get_client_ip,
    check_pdf_info=check_pdf_info,
)

register_task_advanced_routes(
    app=app,
    password=PASSWORD,
    runtime_config=runtime_config,
    logger=logger,
    get_task_manager=lambda: task_manager,
    get_auth_manager=lambda: auth_manager_instance,
    get_client_ip=get_client_ip,
    check_pdf_info=check_pdf_info,
)

@app.get(f"/{PASSWORD}/api/broadcast/config" if PASSWORD else "/api/broadcast/config")
async def get_broadcast_config():
    """获取广播条配置 - 所有用户可访问"""
    try:
        from models.database import db_manager
        async with db_manager.get_connection() as db:
            cursor = await db.execute("""
                SELECT key, value FROM system_settings
                WHERE key IN ('broadcast_enabled', 'broadcast_message', 'broadcast_link', 'broadcast_type', 'broadcast_closable')
            """)
            rows = await cursor.fetchall()
            config = {row[0]: row[1] for row in rows}

        return JSONResponse({
            "status": "success",
            "enabled": config.get('broadcast_enabled') == 'true',
            "message": config.get('broadcast_message', ''),
            "link": config.get('broadcast_link', ''),
            "type": config.get('broadcast_type', 'info'),
            "closable": config.get('broadcast_closable') == 'true'
        })
    except Exception as e:
        logger.error(f"获取广播配置失败: {e}")
        return JSONResponse({"status": "error", "message": str(e)}, status_code=500)

@app.get(f"/{PASSWORD}/api/announcement/config" if PASSWORD else "/api/announcement/config")
async def get_announcement_config():
    """获取公告配置 - 所有用户可访问"""
    try:
        from models.database import db_manager
        async with db_manager.get_connection() as db:
            cursor = await db.execute("""
                SELECT key, value FROM system_settings
                WHERE key IN ('announcement_enabled', 'announcement_content',
                             'announcement_type', 'announcement_closable')
            """)
            rows = await cursor.fetchall()
            config = {row[0]: row[1] for row in rows}

        return JSONResponse({
            "status": "success",
            "enabled": config.get('announcement_enabled') == 'true',
            "content": config.get('announcement_content', ''),
            "type": config.get('announcement_type', 'info'),
            "closable": config.get('announcement_closable') == 'true'
        })
    except Exception as e:
        logger.error(f"获取公告配置失败: {e}")
        return JSONResponse({"status": "error", "message": str(e)}, status_code=500)

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

    turnstile_site_key = turnstile_config.get_config('site_key')

    return templates.TemplateResponse(request, "login.html", {
        "request": request,
        "favicon_url": FAVICON_URL,
        "title": TITLE,
        "registration_enabled": registration_enabled,
        "turnstile_site_key": turnstile_site_key,
        "turnstile_enabled": turnstile_enabled
    })

@app.get("/test-login", response_class=HTMLResponse)
async def test_login_page(request: Request):
    """测试登录页面"""
    return templates.TemplateResponse(request, "test_login.html", {
        "request": request
    })

@app.get("/register", response_class=HTMLResponse)
@app.get(f"/{PASSWORD}/register" if PASSWORD else "/register-disabled", response_class=HTMLResponse)
async def register_page(request: Request, token: str = ""):
    """注册页面"""
    # 获取 Turnstile 验证开关状态
    turnstile_enabled, _ = await get_turnstile_enabled_status()
    turnstile_site_key = turnstile_config.get_config('site_key')

    return templates.TemplateResponse(request, "register.html", {
        "request": request,
        "token": token,
        "favicon_url": FAVICON_URL,
        "title": TITLE,
        "turnstile_site_key": turnstile_site_key,
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
        effective_config = turnstile_config.get_effective_config(include_sensitive=False)
        config_sources = turnstile_config.get_sources()

        return JSONResponse({
            "status": "success",
            "enabled": enabled,
            "source": source,
            "env_forced": env_forced,
            "env_value": env_value if env_forced else None,
            "db_value": db_enabled,
            "site_key": effective_config.get('site_key', ''),
            "has_secret_key": turnstile_config.has_secret_key(),
            "config_sources": config_sources
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

        enabled_raw = settings_data.get('enabled', False)
        parsed_enabled = _parse_bool(enabled_raw)
        if parsed_enabled is None:
            raise HTTPException(status_code=400, detail="enabled 参数无效")
        enabled = parsed_enabled

        # 更新数据库配置（运行时立即生效）
        async with db_manager.get_connection() as db:
            await db.execute("""
                UPDATE system_settings
                SET value = ?, updated_at = ?
                WHERE key = 'turnstile_enabled'
            """, ('true' if enabled else 'false', datetime.now(timezone.utc).isoformat()))
            await db.commit()

        turnstile_config.update_runtime({'enabled': enabled})

        logger.info(f"管理员{admin['username']} 更新 Turnstile 验证: {enabled} (运行时生效)")

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


# ==================== API 配置管理API ====================

@app.get(f"/{PASSWORD}/api/admin/settings/api-config" if PASSWORD else "/api/admin/settings/api-config")
async def get_api_runtime_settings_api(request: Request):
    """获取 OCR API 运行时配置(仅管理员)"""
    try:
        await auth_manager_instance.require_admin(request)

        config = api_runtime_config.get_effective_config(include_sensitive=True)
        sources = api_runtime_config.get_sources()

        return JSONResponse({
            "status": "success",
            "data": {
                "api_base_url": config.get('api_base_url', ''),
                "api_key": config.get('api_key', ''),
                "model": config.get('model', '')
            },
            "sources": sources,
            "masked": {
                "api_key": _mask_secret(config.get('api_key', ''))
            }
        })
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"获取 API 配置失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post(f"/{PASSWORD}/api/admin/settings/api-config" if PASSWORD else "/api/admin/settings/api-config")
async def update_api_runtime_settings_api(request: Request, config_data: dict):
    """更新 OCR API 运行时配置(仅管理员)"""
    try:
        admin = await auth_manager_instance.require_admin(request)

        data = config_data or {}
        update_payload = {}

        for key in ('api_base_url', 'api_key', 'model'):
            if key in data and data[key] is not None:
                update_payload[key] = data[key]

        if data.get('clear_api_key'):
            update_payload['clear_api_key'] = True

        if not update_payload:
            raise HTTPException(status_code=400, detail="没有可更新的配置项")

        api_runtime_config.update_runtime(update_payload)
        apply_api_runtime_config()

        safe_payload = update_payload.copy()
        if 'api_key' in safe_payload:
            safe_payload['api_key'] = "***"
        logger.info(f"管理员{admin['username']} 更新 API 配置: {safe_payload}")

        config = api_runtime_config.get_effective_config(include_sensitive=True)

        return JSONResponse({
            "status": "success",
            "message": "API 配置已更新（仅本次运行有效，重启后恢复ENV默认值）",
            "data": {
                "api_base_url": config.get('api_base_url', ''),
                "api_key": config.get('api_key', ''),
                "model": config.get('model', '')
            },
            "sources": api_runtime_config.get_sources()
        })
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"更新 API 配置失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post(f"/{PASSWORD}/api/admin/settings/api-config/reset" if PASSWORD else "/api/admin/settings/api-config/reset")
async def reset_api_runtime_settings_api(request: Request):
    """重置 OCR API 运行时配置(仅管理员)"""
    try:
        admin = await auth_manager_instance.require_admin(request)

        api_runtime_config.reset_runtime()
        apply_api_runtime_config()

        config = api_runtime_config.get_effective_config(include_sensitive=True)

        logger.info(f"管理员{admin['username']} 重置 API 配置为 ENV 默认值")

        return JSONResponse({
            "status": "success",
            "message": "API 配置已重置为ENV默认值",
            "data": {
                "api_base_url": config.get('api_base_url', ''),
                "api_key": config.get('api_key', ''),
                "model": config.get('model', '')
            },
            "sources": api_runtime_config.get_sources()
        })
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"重置 API 配置失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ==================== Turnstile 运行时配置管理API ====================

@app.get(f"/{PASSWORD}/api/admin/settings/turnstile/config" if PASSWORD else "/api/admin/settings/turnstile/config")
async def get_turnstile_runtime_config_api(request: Request):
    """获取 Turnstile 运行时配置(仅管理员)"""
    try:
        await auth_manager_instance.require_admin(request)

        config = turnstile_config.get_effective_config(include_sensitive=True)
        sources = turnstile_config.get_sources()

        return JSONResponse({
            "status": "success",
            "data": {
                "enabled": bool(config.get('enabled', False)),
                "site_key": config.get('site_key', ''),
                "secret_key": config.get('secret_key', '')
            },
            "sources": sources,
            "masked": {
                "secret_key": _mask_secret(config.get('secret_key', ''))
            }
        })
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"获取 Turnstile 运行时配置失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post(f"/{PASSWORD}/api/admin/settings/turnstile/config" if PASSWORD else "/api/admin/settings/turnstile/config")
async def update_turnstile_runtime_config_api(request: Request, config_data: dict):
    """更新 Turnstile 运行时配置(仅管理员)"""
    try:
        admin = await auth_manager_instance.require_admin(request)

        data = config_data or {}
        update_payload = {}

        if 'enabled' in data:
            parsed = _parse_bool(data.get('enabled'))
            if parsed is None:
                raise HTTPException(status_code=400, detail="enabled 参数无效")
            update_payload['enabled'] = parsed

        if 'site_key' in data and data['site_key'] is not None:
            update_payload['site_key'] = str(data['site_key']).strip()

        if 'secret_key' in data and data['secret_key'] is not None:
            update_payload['secret_key'] = str(data['secret_key']).strip()

        if data.get('clear_secret_key'):
            update_payload['clear_secret_key'] = True

        if not update_payload:
            raise HTTPException(status_code=400, detail="没有可更新的配置项")

        turnstile_config.update_runtime(update_payload)

        safe_payload = update_payload.copy()
        if 'secret_key' in safe_payload:
            safe_payload['secret_key'] = "***"
        logger.info(f"管理员{admin['username']} 更新 Turnstile 运行时配置: {safe_payload}")

        config = turnstile_config.get_effective_config(include_sensitive=True)

        return JSONResponse({
            "status": "success",
            "message": "Turnstile 配置已更新（仅本次运行有效，重启后恢复ENV默认值）",
            "data": {
                "enabled": bool(config.get('enabled', False)),
                "site_key": config.get('site_key', ''),
                "secret_key": config.get('secret_key', '')
            },
            "sources": turnstile_config.get_sources()
        })
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"更新 Turnstile 运行时配置失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post(f"/{PASSWORD}/api/admin/settings/turnstile/config/reset" if PASSWORD else "/api/admin/settings/turnstile/config/reset")
async def reset_turnstile_runtime_config_api(request: Request):
    """重置 Turnstile 运行时配置(仅管理员)"""
    try:
        admin = await auth_manager_instance.require_admin(request)

        turnstile_config.reset_runtime()

        logger.info(f"管理员{admin['username']} 重置 Turnstile 运行时配置为 ENV 默认值")

        config = turnstile_config.get_effective_config(include_sensitive=True)

        return JSONResponse({
            "status": "success",
            "message": "Turnstile 配置已重置为ENV默认值",
            "data": {
                "enabled": bool(config.get('enabled', False)),
                "site_key": config.get('site_key', ''),
                "secret_key": config.get('secret_key', '')
            },
            "sources": turnstile_config.get_sources()
        })
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"重置 Turnstile 运行时配置失败: {e}")
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
        await apply_runtime_settings()

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

@app.post(f"/{PASSWORD}/api/admin/settings/broadcast" if PASSWORD else "/api/admin/settings/broadcast")
async def update_broadcast_settings(request: Request):
    """更新广播条配置 - 仅管理员"""
    try:
        admin = await auth_manager_instance.require_admin(request)
        data = await request.json()

        from models.database import db_manager
        now = datetime.now().isoformat()
        async with db_manager.get_connection() as db:
            # 更新所有广播相关配置 - 使用 INSERT OR REPLACE 确保key存在
            for key in ['broadcast_enabled', 'broadcast_message', 'broadcast_link', 'broadcast_type', 'broadcast_closable']:
                if key in data:
                    value = str(data[key]).lower() if isinstance(data[key], bool) else str(data[key])
                    await db.execute(
                        "INSERT OR REPLACE INTO system_settings (key, value, updated_at, description) VALUES (?, ?, ?, ?)",
                        (key, value, now, f'{key}配置')
                    )
            await db.commit()

        # 通过WebSocket推送更新通知 (添加安全检查)
        if websocket_manager:
            try:
                await websocket_manager.broadcast({
                    'type': 'broadcast_updated',
                    'message': '广播条配置已更新'
                })
            except Exception as ws_error:
                logger.warning(f"WebSocket广播失败(非致命): {ws_error}")
        else:
            logger.warning("WebSocket管理器未初始化,跳过广播")

        return JSONResponse({
            "status": "success",
            "message": "广播设置已保存并推送更新"
        })
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        logger.error(f"更新广播配置失败: {e}")
        logger.error(f"堆栈跟踪: {traceback.format_exc()}")
        logger.error(f"请求数据: {data if 'data' in locals() else 'N/A'}")
        return JSONResponse({"status": "error", "message": f"更新失败: {str(e)}"}, status_code=500)

@app.post(f"/{PASSWORD}/api/admin/settings/announcement" if PASSWORD else "/api/admin/settings/announcement")
async def update_announcement_settings(request: Request):
    """更新公告配置 - 仅管理员"""
    try:
        admin = await auth_manager_instance.require_admin(request)
        data = await request.json()

        from models.database import db_manager
        now = datetime.now().isoformat()
        async with db_manager.get_connection() as db:
            # 更新所有公告相关配置 - 使用 INSERT OR REPLACE 确保key存在
            for key in ['announcement_enabled', 'announcement_content',
                       'announcement_type', 'announcement_closable']:
                if key in data:
                    value = str(data[key]).lower() if isinstance(data[key], bool) else str(data[key])
                    await db.execute(
                        "INSERT OR REPLACE INTO system_settings (key, value, updated_at, description) VALUES (?, ?, ?, ?)",
                        (key, value, now, f'{key}配置')
                    )
            await db.commit()

        # 通过WebSocket推送更新通知 (添加安全检查)
        if websocket_manager:
            try:
                await websocket_manager.broadcast({
                    'type': 'announcement_updated',
                    'message': '公告配置已更新'
                })
            except Exception as ws_error:
                logger.warning(f"WebSocket广播失败(非致命): {ws_error}")
        else:
            logger.warning("WebSocket管理器未初始化,跳过广播")

        return JSONResponse({
            "status": "success",
            "message": "公告设置已保存并推送更新"
        })
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        logger.error(f"更新公告配置失败: {e}")
        logger.error(f"堆栈跟踪: {traceback.format_exc()}")
        logger.error(f"请求数据: {data if 'data' in locals() else 'N/A'}")
        return JSONResponse({"status": "error", "message": f"更新失败: {str(e)}"}, status_code=500)


# ==================== 用户协议管理API ====================

@app.get(f"/{PASSWORD}/api/admin/settings/user-agreement" if PASSWORD else "/api/admin/settings/user-agreement")
async def get_user_agreement_admin(request: Request):
    """获取当前用户协议 - 管理员接口"""
    try:
        await auth_manager_instance.require_admin(request)
        from models.database import user_agreement_model

        agreement = await user_agreement_model.get_active_agreement()
        if not agreement:
            return JSONResponse({
                "status": "success",
                "agreement": None,
                "message": "暂无用户协议"
            })

        return JSONResponse({
            "status": "success",
            "agreement": {
                "id": agreement['id'],
                "content": agreement['content'],
                "version": agreement['version'],
                "created_at": agreement['created_at'],
                "updated_at": agreement['updated_at']
            }
        })
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        logger.error(f"获取用户协议失败: {e}")
        logger.error(f"堆栈跟踪: {traceback.format_exc()}")
        return JSONResponse({"status": "error", "message": f"获取失败: {str(e)}"}, status_code=500)


@app.post(f"/{PASSWORD}/api/admin/settings/user-agreement" if PASSWORD else "/api/admin/settings/user-agreement")
async def update_user_agreement_admin(request: Request):
    """更新用户协议 - 管理员接口"""
    try:
        await auth_manager_instance.require_admin(request)
        data = await request.json()
        content = data.get('content', '').strip()

        if not content:
            raise HTTPException(status_code=400, detail="协议内容不能为空")

        from models.database import user_agreement_model
        success = await user_agreement_model.update_agreement(content)

        if success:
            return JSONResponse({
                "status": "success",
                "message": "用户协议已更新"
            })
        else:
            raise HTTPException(status_code=500, detail="更新用户协议失败")
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        logger.error(f"更新用户协议失败: {e}")
        logger.error(f"堆栈跟踪: {traceback.format_exc()}")
        logger.error(f"请求内容长度: {len(content) if 'content' in locals() else 'N/A'}")
        return JSONResponse({"status": "error", "message": f"更新失败: {str(e)}"}, status_code=500)


@app.get("/api/user-agreement/current")
async def get_current_user_agreement():
    """获取当前激活的用户协议 - 公开接口"""
    try:
        from models.database import user_agreement_model
        agreement = await user_agreement_model.get_active_agreement()

        if not agreement:
            return JSONResponse({
                "status": "success",
                "agreement": None,
                "message": "暂无用户协议"
            })

        return JSONResponse({
            "status": "success",
            "agreement": {
                "id": agreement['id'],
                "content": agreement['content'],
                "version": agreement['version']
            }
        })
    except Exception as e:
        logger.error(f"获取用户协议失败: {e}")
        return JSONResponse({"status": "error", "message": str(e)}, status_code=500)


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
        await auth_manager_instance.require_admin(request)

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
                    "admin": admin_users,
                },
                "pages": {
                    "total_granted": total_pages_granted,
                    "total_used": total_pages_used,
                },
                "codes": {
                    "total": total_codes,
                    "active": active_codes,
                },
            }
        })

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"获取统计信息失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# 注册系统监控路由
register_system_routes(
    app=app,
    password=PASSWORD,
    get_task_manager=lambda: task_manager,
    get_auth_manager=lambda: auth_manager_instance,
)

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
