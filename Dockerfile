FROM python:3.9-slim-buster

WORKDIR /app

# 复制依赖文件
COPY requirements.txt .

# 安装依赖
RUN pip install --no-cache-dir -r requirements.txt

# 复制应用代码
COPY . .

# 创建数据目录和临时文件目录
RUN mkdir -p /app/data /app/tmp && \
    chmod 777 /app/tmp

# 环境变量配置
ENV PYTHONUNBUFFERED=1 \
    TMPDIR=/app/tmp \
    TEMP=/app/tmp \
    TMP=/app/tmp

# 数据卷配置（用于持久化数据库）
VOLUME ["/app/data"]

# 暴露端口（默认54188，可通过环境变量覆盖）
EXPOSE 54188

# 健康检查（使用专用端点，无需密码）
HEALTHCHECK --interval=30s --timeout=10s --start-period=40s --retries=3 \
    CMD python -c "import urllib.request, os; port = os.getenv('PORT', '54188'); urllib.request.urlopen(f'http://localhost:{port}/healthz').read()" || exit 1

# 启动命令（支持环境变量配置端口）
CMD ["sh", "-c", "uvicorn main:app --host 0.0.0.0 --port ${PORT:-54188}"]
