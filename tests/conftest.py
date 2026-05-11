"""pytest 自动把项目根加到 sys.path，让 `from llm...` `from core...` 这种导入能用。"""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
