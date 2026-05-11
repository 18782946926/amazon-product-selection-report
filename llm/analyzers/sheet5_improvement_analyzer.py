"""Sheet5ImprovementAnalyzer：voc.pain_clusters 前 4 条 → 4 条独占 token 预算的改进计划。

设计目的：把 sheet5_improvement_plan 从 Synthesizer 巨型 schema 里拆出来独立 LLM 调用，
避免「LLM 写到第 3 条 plan 时接近 8K token 上限，第 4 条被 client 截断丢弃」。
失败时返回空 list，由 packs_runtime.sheet5_improvement_plan() 自动回落到 Synthesizer 输出。
"""
from __future__ import annotations

import hashlib
import json
import logging

from llm.analyzers.base import BaseAnalyzer
from llm.prompts import sheet5_improvement as s5i_prompt
from llm.schemas import Sheet5ImprovementPlanList

log = logging.getLogger(__name__)


class Sheet5ImprovementAnalyzer(BaseAnalyzer[Sheet5ImprovementPlanList]):
    name = "Sheet5Improvement"

    def _cache_key(self, input_data: dict) -> str:
        category = input_data.get("category_hint", "unknown")
        pain_clusters = input_data.get("pain_clusters") or []
        pain_names = [str(p.get("name", "")) for p in pain_clusters[:4]]
        h = hashlib.sha256()
        h.update(category.encode())
        h.update(json.dumps(pain_names, ensure_ascii=False).encode())
        return f"sheet5_improvement_{h.hexdigest()[:16]}"

    def _call_llm(self, input_data: dict) -> Sheet5ImprovementPlanList:
        category = input_data.get("category_hint", "未知品类")
        pain_clusters = input_data.get("pain_clusters") or []
        if not pain_clusters:
            return Sheet5ImprovementPlanList(is_fallback=True)
        messages = s5i_prompt.build_messages(
            category_hint=category,
            pain_clusters=pain_clusters,
            stats=input_data.get("stats"),
        )
        return self.client.chat_json(
            messages=messages,
            schema=Sheet5ImprovementPlanList,
            tier="fast",
            cache_key=self._cache_key(input_data),
            temperature=0.2,
            max_tokens=6000,  # 独占预算，4 条 plan 绰绰有余
            timeout=180,
        )

    def _fallback(self, input_data: dict) -> Sheet5ImprovementPlanList:
        """LLM 不可用时返回空 plans。
        packs_runtime.sheet5_improvement_plan() 会自动回落到 Synthesizer.sheet5_improvement_plan。
        """
        return Sheet5ImprovementPlanList(is_fallback=True)
