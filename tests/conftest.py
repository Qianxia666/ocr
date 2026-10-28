import os
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# 在导入任何项目模块之前隔离数据库，避免测试写入仓库内的 data/tasks.db
os.environ.setdefault(
    "DATABASE_PATH",
    os.path.join(tempfile.mkdtemp(prefix="ocr-tests-"), "tasks.db"),
)
os.environ.setdefault("PASSWORD", "")
os.environ.setdefault("OPENAI_API_KEY", "")
