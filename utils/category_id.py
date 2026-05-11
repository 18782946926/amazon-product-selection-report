"""从 BSR 文件名中抽取 category_id（仅用于缓存 key 和报告标题，不参与任何业务决策）。"""
from __future__ import annotations

import re
from pathlib import Path


def slugify(text: str) -> str:
    text = text.lower().strip()
    text = re.sub(r"[^\w一-龥]+", "_", text)
    text = re.sub(r"_+", "_", text).strip("_")
    return text or "unknown"


def extract_from_bsr_filename(bsr_path: str | Path) -> str:
    """从形如 'BSR(Job-Site-Lighting(Current))-100-US-20260409.xlsx' 抽出 'job_site_lighting'。

    规则：
    - 提取第一个圆括号内的内容
    - 去掉 (Current) 等子括号
    - slugify
    - 失败时返回 'unknown_<filename_slug>'
    """
    name = Path(bsr_path).stem
    m = re.search(r"BSR\(([^()]+(?:\([^)]*\))?)\)", name)
    if m:
        raw = m.group(1)
        raw = re.sub(r"\([^)]*\)", "", raw).strip()
        return slugify(raw)
    return f"unknown_{slugify(name)[:32]}"
