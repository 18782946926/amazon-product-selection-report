"""ComplianceAnalyzer：品类 → 美国市场必备认证清单 + 典型退货率。

替代 app.py:3811-3834 硬编码的 UL8750/UL1598/UL60950（LED 专属）+ "工作灯退货率 5-8%"。
"""
from __future__ import annotations

import hashlib
import logging

import pandas as pd

from llm.analyzers.base import BaseAnalyzer
from llm.analyzers.bsr_analyzer import resolve_col
from llm.prompts import compliance as comp_prompt
from llm.schemas import CompliancePack

log = logging.getLogger(__name__)


class ComplianceAnalyzer(BaseAnalyzer[CompliancePack]):
    name = "Compliance"

    def _cache_key(self, input_data: dict) -> str:
        category = input_data.get("category_name", "unknown")
        titles = input_data.get("titles", [])
        h = hashlib.sha256()
        h.update(category.encode())
        h.update(str(titles[:20]).encode())
        return f"compliance_{h.hexdigest()[:16]}"

    def _call_llm(self, input_data: dict) -> CompliancePack:
        category = input_data.get("category_name", "未知品类")
        titles = input_data.get("titles", [])
        if not titles:
            return CompliancePack(is_fallback=True)
        messages = comp_prompt.build_messages(category, titles)
        return self.client.chat_json(
            messages=messages,
            schema=CompliancePack,
            tier="fast",
            cache_key=self._cache_key(input_data),
            temperature=0.1,
            max_tokens=3000,
            timeout=120,
        )

    def _fallback(self, input_data: dict) -> CompliancePack:
        """LLM 不可用时返回空 Pack（不再回落到 LED 专属的 UL8750 等认证）。
        Sheet 9 风险清单见到空 Pack 会跳过 LLM-driven 的认证行，保留 app.py 里品类无关的
        通用风险项（包装/发货/电池运输等）。
        """
        return CompliancePack(is_fallback=True)
