from __future__ import annotations

import hashlib
import logging
from typing import Any

from llm.analyzers.base import BaseAnalyzer
from llm.prompts import synthesis as synth_prompt
from llm.schemas import (
    EntryRecommendation,
    Sheet10DimensionReason,
    Sheet10Verdict,
    Sheet6PriorityItem,
    StrategySynthesis,
)
from llm.validators import filter_synthesis

log = logging.getLogger(__name__)

EIGHT_DIMS = ["市场体量", "需求趋势", "竞争难度", "利润率",
              "供应链", "推广压力", "风险可控性", "差异化机会"]


class Synthesizer(BaseAnalyzer[StrategySynthesis]):
    name = "Synthesizer"

    def _cache_key(self, input_data: dict) -> str:
        h = hashlib.sha256()
        # 品类名加入 cache key，避免 LED 和 Battery 在 BSR/VOC Pack 内容哈希相同时误命中
        category_hint = input_data.get("display_name") or input_data.get("category_id") or ""
        h.update(str(category_hint).encode())
        for k in ("market", "voc", "traffic", "trend"):
            pack = input_data.get(k)
            if pack is not None:
                h.update(pack.model_dump_json().encode())
        return f"synthesis_{h.hexdigest()[:16]}"

    def _call_llm(self, input_data: dict) -> StrategySynthesis:
        market = input_data["market"].model_dump(mode="json") if input_data.get("market") else {}
        voc = input_data["voc"].model_dump(mode="json") if input_data.get("voc") else {}
        traffic = input_data["traffic"].model_dump(mode="json") if input_data.get("traffic") else {}
        trend = input_data["trend"].model_dump(mode="json") if input_data.get("trend") else {}
        stats = input_data.get("stats", {})
        category_hint = (
            input_data.get("display_name")
            or input_data.get("category_id")
            or "未知品类"
        )

        cache_key = self._cache_key(input_data)
        messages = synth_prompt.build_messages(market, voc, traffic, trend, stats, category_hint=category_hint)
        raw = self.client.chat_json(
            messages=messages,
            schema=StrategySynthesis,
            tier="fast",
            cache_key=cache_key,
            temperature=0.2,
            max_tokens=10000,
            timeout=180,
        )
        # post-check：每条结论的 supporting_fields 必须能解析、文案必须原样引用被解析的值；
        # 未兑现的 upgrade/differentiation/dim_reason 被直接丢弃，含禁用词的文案被清空。
        packs_dict = {
            "market": market, "voc": voc, "traffic": traffic, "trend": trend, "stats": stats,
        }
        try:
            filtered = filter_synthesis(raw, packs_dict)
            filtered_obj = StrategySynthesis.model_validate(filtered)
            # 把 filter 后的干净结果回写缓存，避免下次缓存命中时再带回老的"自然流量 100%/广告占比 0%"
            # 等历史虚构（chat_json 默认缓存的是 LLM raw，filter 一旦异常就会泄漏）。
            if cache_key and getattr(self.client, "cache", None):
                try:
                    self.client.cache.set(cache_key, filtered_obj.model_dump(mode="json"))
                except Exception as ce:
                    log.warning("[Synthesizer] 回写缓存失败：%s", ce)
            return filtered_obj
        except Exception as e:
            # 异常显形：旧代码用 log.warning，但 Flask 默认 root logger 不打 WARNING 到 stdout，
            # 导致 schema-path 泄露这种"filter 应该拦但放过去了"的诊断信息丢失。
            # 这里直接 traceback.print_exc 到 stderr，无视 logging 配置必现身。
            import sys
            import traceback as _tb
            print(f"[Synthesizer] filter_synthesis 失败，使用原始输出：{e}", file=sys.stderr, flush=True)
            _tb.print_exc(file=sys.stderr)
            log.exception("[Synthesizer] filter_synthesis 失败，使用原始输出")
            return raw

    def _fallback(self, input_data: dict) -> StrategySynthesis:
        """LLM 不可用时返回空 Synthesis（is_fallback=True）。
        - entry_recommendation 仅填 recommended_segment（从 market 取一个），reasoning 留空
        - sheet6_priority_matrix 按 BSR 排序给 P1-P4（structural），action_plan 留空
        - sheet10_final_verdict 的 dimension_reasons 留空 —— Sheet 10 前 4 维由 Python 直接落盘，
          后 4 维在 LLM 不可用时直接跳过（由渲染层保护），不再写"LLM 不可用，无叙述"
        """
        market = input_data.get("market")
        recommended = ""
        if market and market.product_segments:
            recommended = market.product_segments[0].name

        priority_matrix = []
        if market and market.product_segments:
            for i, seg in enumerate(market.product_segments[:6]):
                pri = ["P1", "P2", "P3", "P4"][min(i, 3)]
                priority_matrix.append(Sheet6PriorityItem(
                    segment=seg.name, priority=pri,
                    action_plan="",
                    improvements=[],
                ))

        return StrategySynthesis(
            entry_recommendation=EntryRecommendation(
                recommended_segment=recommended,
                reasoning="",
                supporting_fields=[],
            ),
            sheet6_priority_matrix=priority_matrix,
            sheet10_final_verdict=Sheet10Verdict(headline="", dimension_reasons=[]),
            is_fallback=True,
        )
