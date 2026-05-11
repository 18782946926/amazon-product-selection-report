"""LifecycleAnalyzer：5 维生命周期评分 + 品类元数据 → 业务洞察 4 段文案。

替代 app.py classify_market_pattern() 写死的硬编码模板，让文案带本品类的具体品牌/价格段/关键词。
LLM 不可用或异常时由调用方走规则模板兜底（不走本 analyzer 的 _fallback）。
"""
from __future__ import annotations

import hashlib
import json
import logging

from llm.analyzers.base import BaseAnalyzer
from llm.prompts import lifecycle as lc_prompt
from llm.schemas import LifecycleInsight

log = logging.getLogger(__name__)


class LifecycleAnalyzer(BaseAnalyzer[LifecycleInsight]):
    name = "Lifecycle"

    def _cache_key(self, input_data: dict) -> str:
        # 缓存 key 含品类名 + 5 维评分签名 + 决策表 stage + TOP 品牌（任一变化即缓存失效）
        category = input_data.get("category_name", "unknown")
        score_detail = input_data.get("score_detail") or {}
        score_sig = {k: v.get("score") for k, v in score_detail.items()}
        rule_stage = input_data.get("rule_stage", "")
        top_brands = input_data.get("top_brands") or []
        h = hashlib.sha256()
        h.update(category.encode())
        h.update(json.dumps(score_sig, sort_keys=True).encode())
        h.update(rule_stage.encode())
        h.update(str(top_brands[:8]).encode())
        return f"lifecycle_{h.hexdigest()[:16]}"

    def _call_llm(self, input_data: dict) -> LifecycleInsight:
        score_detail = input_data.get("score_detail") or {}
        if not score_detail:
            return LifecycleInsight(is_fallback=True)
        messages = lc_prompt.build_messages(
            category_name=input_data.get("category_name", "未知品类"),
            score_detail=score_detail,
            rule_stage=input_data.get("rule_stage", ""),
            rule_stage_reasoning=input_data.get("rule_stage_reasoning", ""),
            top_brands=input_data.get("top_brands"),
            top_keywords=input_data.get("top_keywords"),
            price_distribution=input_data.get("price_distribution"),
            segments=input_data.get("segments"),
            china_pct=input_data.get("china_pct"),
            new_contribution=input_data.get("new_contribution"),
            eff_ratio=input_data.get("eff_ratio"),
        )
        return self.client.chat_json(
            messages=messages,
            schema=LifecycleInsight,
            tier="fast",
            cache_key=self._cache_key(input_data),
            temperature=0.3,
            max_tokens=2500,
            timeout=120,
        )

    def _fallback(self, input_data: dict) -> LifecycleInsight:
        """LLM 不可用时返回空 Pack（is_fallback=True）。
        app.py classify_market_pattern() 见到 None / is_fallback 会自动走规则模板兜底。
        """
        return LifecycleInsight(is_fallback=True)
