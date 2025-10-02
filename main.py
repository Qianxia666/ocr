import base64
from dotenv import load_dotenv
from fastapi.middleware.cors import CORSMiddleware
import fitz  # PyMuPDF
import logging
from fastapi.responses import JSONResponse
from io import BytesIO
from PIL import Image
import aiohttp
import asyncio
from fastapi import FastAPI, Request, UploadFile, Query, HTTPException, WebSocket, WebSocketDisconnect
from typing import Optional
from contextlib import asynccontextmanager
import os
import json
from datetime import datetime
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, Response, JSONResponse

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

# 并发限制和重试机制
CONCURRENCY = int(os.getenv("CONCURRENCY", 5))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", 5))
RETRY_DELAY = 0.5  # 重试延迟时间（秒）

# PDF处理优化配置
PDF_DPI = int(os.getenv("PDF_DPI", 200))  # 降低DPI减少内存占用
BATCH_SIZE = int(os.getenv("BATCH_SIZE", 8))  # 批处理大小

# 文件大小限制配置
MAX_IMAGE_SIZE_MB = int(os.getenv("MAX_IMAGE_SIZE_MB", 50))  # 图片文件最大大小(MB)
MAX_PDF_SIZE_MB = int(os.getenv("MAX_PDF_SIZE_MB", 200))  # PDF文件最大大小(MB)
MAX_PDF_PAGES = int(os.getenv("MAX_PDF_PAGES", 500))  # PDF最大页数限制

# 定义生命周期管理器
@asynccontextmanager
async def lifespan(app: FastAPI):
    """应用生命周期管理"""
    # 启动时执行
    try:
        # 初始化数据库
        from models.database import init_database
        await init_database()
        logger.info("数据库初始化完成")

        # 初始化WebSocket管理器
        await init_websocket_manager()
        logger.info("WebSocket管理器初始化完成")

        # 准备API配置
        api_config = {
            'api_base_url': API_BASE_URL,
            'api_key': API_KEY,
            'model': MODEL,
            'concurrency': CONCURRENCY,
            'max_retries': MAX_RETRIES,
            'retry_delay': RETRY_DELAY,
            'pdf_dpi': PDF_DPI
        }

        # 初始化页面处理器
        from core.page_processor import init_page_processor, is_page_processor_ready, shutdown_page_processor
        logger.info("开始初始化页面处理器")

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

            # 进行初始化
            await init_page_processor(api_config)

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

                # 强制重新初始化任务管理器
                await init_task_manager(api_config, websocket_manager)

                # 直接从模块获取最新的任务管理器实例
                from core.task_manager import task_manager as global_task_manager_instance

                # 更新本地全局变量
                global task_manager
                task_manager = global_task_manager_instance

                # 验证任务管理器状态
                if task_manager and hasattr(task_manager, '_initialized') and task_manager._initialized:
                    logger.info("任务管理器初始化成功")
                    init_success = True

                    # 验证核心组件
                    if not (task_manager.worker_pool and task_manager.worker_pool._running):
                        logger.error("工作者池未正确启动")

                    if not task_manager.task_queue:
                        logger.error("任务队列未正确初始化")

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

        logger.info("应用启动完成")

    except Exception as e:
        logger.error(f"应用启动失败: {e}")
        raise

    yield  # 应用运行期间

    # 关闭时执行
    try:
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

# 挂载静态文件目录
app.mount("/static", StaticFiles(directory="static"), name="static")

# 环境变量配置
FAVICON_URL = os.getenv("FAVICON_URL", "/static/favicon.ico")
TITLE = os.getenv("TITLE", "呱呱的oai图转文")
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


async def process_image(session, image_data, semaphore, max_retries=MAX_RETRIES):
    """使用 OCR 识别图像并进行 Markdown 格式化"""
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

def pdf_to_images_generator(pdf_bytes: bytes, dpi: int = PDF_DPI):
    """
    使用生成器流式处理PDF转图片，减少内存占用
    :param pdf_bytes: PDF 文件的字节数据
    :param dpi: 图像分辨率 (默认200 DPI)
    :yield: (页码, 图片字节数据) 元组
    """
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
    
    for attempt in range(MAX_RETRIES):
        try:
            async with semaphore:
                logger.info(f"开始OCR处理第 {page_number} 页 (尝试 {attempt + 1}/{MAX_RETRIES})")
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
            logger.warning(f"第 {page_number} 页处理失败 (尝试 {attempt + 1}/{MAX_RETRIES}): {e}")
            if attempt < MAX_RETRIES - 1:
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
async def process_image_endpoint(file: UploadFile):
    """同步图片处理端点（保持兼容性）- 立即返回任务ID"""
    import tempfile

    if not task_manager:
        raise HTTPException(status_code=503, detail="任务管理器未初始化")

    if not file:
        raise HTTPException(status_code=400, detail="未收到文件")

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

        # 提交异步任务(传递临时文件路径而非数据)
        task_id = await task_manager.submit_image_task_from_file(
            file_path=tmp_file_path,
            file_name=file.filename or "image",
            metadata={
                "source": "sync_endpoint",
                "sync_mode": True,
                "file_size_mb": file_size_mb
            }
        )

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
async def process_pdf_endpoint(file: UploadFile):
    """同步PDF处理端点（保持兼容性）- 立即返回任务ID"""
    import tempfile

    if not task_manager:
        raise HTTPException(status_code=503, detail="任务管理器未初始化")

    if not file:
        raise HTTPException(status_code=400, detail="未收到文件")

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

        # 提交异步任务(传递临时文件路径而非数据)
        task_id = await task_manager.submit_pdf_task_from_file(
            file_path=tmp_file_path,
            file_name=file.filename or "document.pdf",
            metadata={
                "source": "sync_endpoint",
                "sync_mode": True,
                "file_size_mb": file_size_mb,
                "total_pages": page_count
            }
        )

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
async def root_page(request: Request):
    """根路径 - 如果设置了密码则显示密码页面，否则显示主页"""
    if PASSWORD:
        # 检查是否有有效的密码cookie或session
        # 显示密码输入页面
        return templates.TemplateResponse(
            "password.html",
            {
                "request": request,
                "favicon_url": FAVICON_URL,
                "title": TITLE
            }
        )
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
async def access_with_password(request: Request):
    """受密码保护的主页"""
    return templates.TemplateResponse(
        "web.html",
        {
            "request": request,
            "favicon_url": FAVICON_URL,
            "title": TITLE,
            "backurl": BACK_URL
        }
    )

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
            
        except Exception as e:
            logger.error(f"WebSocket连接异常 {actual_client_id}: {e}")
            
    except Exception as e:
        logger.error(f"WebSocket连接失败: {e}")
        
    finally:
        # 清理连接
        if 'actual_client_id' in locals():
            await websocket_manager.disconnect(actual_client_id)

# 任务管理API路由
@app.post(f"/{PASSWORD}/api/tasks/image" if PASSWORD else "/api/tasks/image")
async def submit_image_task_api(file: UploadFile):
    """提交图片OCR任务API"""
    if not task_manager:
        raise HTTPException(status_code=503, detail="任务管理器未初始化")
    
    if not file:
        raise HTTPException(status_code=400, detail="未收到文件")
    
    try:
        file_data = await file.read()
        if not file_data:
            raise HTTPException(status_code=400, detail="文件内容为空")
        
        task_id = await task_manager.submit_image_task(
            file_data=file_data,
            file_name=file.filename or "image",
            metadata={"source": "api"}
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
async def submit_pdf_task_api(file: UploadFile):
    """提交PDF OCR任务API"""
    if not task_manager:
        raise HTTPException(status_code=503, detail="任务管理器未初始化")
    
    if not file:
        raise HTTPException(status_code=400, detail="未收到文件")
    
    try:
        file_data = await file.read()
        if not file_data:
            raise HTTPException(status_code=400, detail="文件内容为空")
        
        task_id = await task_manager.submit_pdf_task(
            file_data=file_data,
            file_name=file.filename or "document.pdf",
            metadata={"source": "api"}
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
async def get_task_result_api(task_id: str):
    """获取任务结果API"""
    if not task_manager:
        logger.error(f"任务管理器未初始化，无法获取任务结果: {task_id}")
        raise HTTPException(status_code=503, detail="任务管理器未初始化")

    try:
        task_result = await task_manager.get_task_result(task_id)

        if not task_result:
            logger.warning(f"任务不存在: {task_id}")
            raise HTTPException(status_code=404, detail="任务不存在")

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
    status: Optional[str] = Query(None, description="任务状态筛选"),
    limit: int = Query(100, description="返回数量限制"),
    offset: int = Query(0, description="偏移量")
):
    """获取任务列表API"""
    # 从全局模块导入任务管理器，确保获取到最新的实例
    from core.task_manager import task_manager as global_task_manager
    
    if not global_task_manager:
        raise HTTPException(status_code=503, detail="任务管理器未初始化")
    
    try:
        # 使用任务管理器的get_tasks_list方法获取任务列表
        tasks = await global_task_manager.get_tasks_list(status, limit)
        
        return JSONResponse({
            "status": "success",
            "data": tasks
        })
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
# 新增：异步任务提交端点
@app.post(f"/{PASSWORD}/api/async/image" if PASSWORD else "/api/async/image")
async def submit_async_image_task(file: UploadFile):
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
            max_file_size = MAX_IMAGE_SIZE_MB * 1024 * 1024

            while chunk := await file.read(chunk_size):
                total_size += len(chunk)
                # 检查文件大小
                if total_size > max_file_size:
                    tmp_file.close()
                    os.unlink(tmp_file_path)
                    raise HTTPException(status_code=400, detail=f"文件过大，最大支持{MAX_IMAGE_SIZE_MB}MB")
                tmp_file.write(chunk)

        logger.info(f"图片文件已写入临时文件: {tmp_file_path}")

        # 获取文件大小
        file_size_mb = os.path.getsize(tmp_file_path) / (1024 * 1024)

        task_id = await task_manager.submit_image_task_from_file(
            file_path=tmp_file_path,
            file_name=file.filename or "image",
            metadata={
                "source": "async_api",
                "file_type": file.content_type,
                "original_filename": file.filename,
                "file_size_mb": file_size_mb
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
async def submit_async_pdf_task(file: UploadFile):
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
        max_file_size = MAX_PDF_SIZE_MB * 1024 * 1024
        if len(file_data) > max_file_size:
            raise HTTPException(status_code=400, detail=f"文件过大，最大支持{MAX_PDF_SIZE_MB}MB")
        
        # 获取PDF文件信息
        try:
            page_count, file_size_mb = check_pdf_info(file_data)
            logger.info(f"PDF文件信息: {page_count} 页, {file_size_mb:.2f} MB")
        except Exception as e:
            logger.error(f"PDF文件信息获取失败: {e}")
            raise HTTPException(status_code=400, detail="PDF文件格式错误或损坏")
        
        # 页数限制检查（使用环境变量配置）
        if page_count > MAX_PDF_PAGES:
            raise HTTPException(status_code=400, detail=f"PDF页数过多，最大支持{MAX_PDF_PAGES}页")
        
        task_id = await task_manager.submit_pdf_task(
            file_data=file_data,
            file_name=file.filename or "document.pdf",
            metadata={
                "source": "async_api",
                "file_type": file.content_type,
                "original_filename": file.filename,
                "file_size_mb": file_size_mb
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
async def submit_batch_tasks(files: list[UploadFile]):
    """提交批量异步任务"""
    if not task_manager:
        raise HTTPException(status_code=503, detail="任务管理器未初始化")
    
    if not files or len(files) == 0:
        raise HTTPException(status_code=400, detail="未收到文件")
    
    if len(files) > 20:  # 限制批量上传数量
        raise HTTPException(status_code=400, detail="批量上传最多支持20个文件")
    
    try:
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
                        max_file_size = MAX_IMAGE_SIZE_MB * 1024 * 1024

                        while chunk := await file.read(chunk_size):
                            total_size += len(chunk)
                            if total_size > max_file_size:
                                tmp_file.close()
                                os.unlink(tmp_file_path)
                                failed_files.append({"filename": file.filename, "error": f"图片文件过大(最大{MAX_IMAGE_SIZE_MB}MB)"})
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
                                "file_size_mb": file_size_mb
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
                        max_file_size = MAX_PDF_SIZE_MB * 1024 * 1024

                        while chunk := await file.read(chunk_size):
                            total_size += len(chunk)
                            if total_size > max_file_size:
                                tmp_file.close()
                                os.unlink(tmp_file_path)
                                failed_files.append({"filename": file.filename, "error": f"PDF文件过大(最大{MAX_PDF_SIZE_MB}MB)"})
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

                            if page_count > MAX_PDF_PAGES:
                                os.unlink(tmp_file_path)
                                failed_files.append({"filename": file.filename, "error": f"PDF页数过多(最大{MAX_PDF_PAGES}页)"})
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
                                        "total_pages": page_count
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