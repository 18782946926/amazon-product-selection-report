"""TaxonomyAggregator：基于视觉描述聚类成 4-8 个 segments。

替代 BSR LLM 单纯凭"标题/品牌/价格"切 taxonomy 的旧路径——视觉 LLM 已经基于
图+标题+卖点产出每个 ASIN 的精准 product_type_free 描述，本 analyzer 只在这些
精准描述上做聚类，避免标题营销词污染 segment 边界。

输入：list[dict] of {asin, product_type_free, material_label} —— 来自 packs.market.visual_labels
输出：MarketInsightPack（仅 product_segments 字段有意义）
"""
from __future__ import annotations

import hashlib
import json
import logging

from llm.analyzers.base import BaseAnalyzer
from llm.prompts import taxonomy as tx_prompt
from llm.schemas import MarketInsightPack

log = logging.getLogger(__name__)


class TaxonomyAggregator(BaseAnalyzer[MarketInsightPack]):
    name = "Taxonomy"

    def _cache_key(self, input_data: dict) -> str:
        # cache key = prompt 版本 + 视觉描述列表（按 asin 排序保稳）
        h = hashlib.sha256()
        h.update(tx_prompt._PROMPT_VERSION.encode())
        descs = input_data.get("visual_descriptions") or []
        sorted_descs = sorted(
            [
                {
                    "a": d.get("asin", ""),
                    "p": d.get("product_type_free", "") or "",
                    "m": d.get("material_label", "") or "",
                    "f": d.get("form_label", "") or "",
                }
                for d in descs
            ],
            key=lambda d: d["a"],
        )
        h.update(json.dumps(sorted_descs, sort_keys=True, ensure_ascii=False).encode())
        return f"taxonomy_{h.hexdigest()[:16]}"

    def _call_llm(self, input_data: dict) -> MarketInsightPack:
        descs = input_data.get("visual_descriptions") or []
        if len(descs) < 4:
            log.warning("[Taxonomy] 视觉描述数量不足 (%d < 4)，跳过聚合", len(descs))
            return MarketInsightPack(is_fallback=True)

        valid = [d for d in descs if d.get("product_type_free")]
        if len(valid) < 4:
            log.warning("[Taxonomy] 有效视觉描述不足 (%d < 4)，跳过聚合", len(valid))
            return MarketInsightPack(is_fallback=True)

        # 给每条 valid 加 1-based idx，让 LLM 用整数序号输出 members（紧凑、避免长 JSON 漂移）
        for i, d in enumerate(valid, start=1):
            d["idx"] = i

        messages = tx_prompt.build_messages(
            visual_descriptions=valid,
            category_hint=input_data.get("category_hint", ""),
        )
        result = self.client.chat_json(
            messages=messages,
            schema=MarketInsightPack,
            tier="fast",
            cache_key=self._cache_key(input_data),
            temperature=0.0,
            max_tokens=10000,
            timeout=240,
        )

        # v7-merged：把 LLM 输出的 members（idx list）反查成 member_asins（asin string list）
        try:
            if result and result.product_segments:
                idx_to_asin = {d["idx"]: d["asin"] for d in valid if d.get("asin")}
                total_assigned: set[str] = set()
                duplicate_asins: set[str] = set()
                for seg in result.product_segments:
                    member_idxs = list(getattr(seg, "members", None) or [])
                    seg_asins: list[str] = []
                    for i in member_idxs:
                        try:
                            asin = idx_to_asin.get(int(i))
                        except (TypeError, ValueError):
                            asin = None
                        if asin:
                            if asin in total_assigned:
                                duplicate_asins.add(asin)
                            else:
                                total_assigned.add(asin)
                                seg_asins.append(asin)
                    seg.member_asins = seg_asins

                # 全覆盖校验：缺失的 ASIN 落到第一个 segment（容错；下游 _resolve_product_type 仍可能 reroute）
                all_asins = {d["asin"] for d in valid if d.get("asin")}
                missing = all_asins - total_assigned
                log.info(
                    "[Taxonomy] v7-merged 切出 %d 个桶 + 直接分配 ASIN：覆盖 %d / %d，缺失 %d，重复 %d",
                    len(result.product_segments), len(total_assigned), len(all_asins),
                    len(missing), len(duplicate_asins),
                )
                if missing:
                    log.warning("[Taxonomy] 缺失 ASIN（LLM 漏装，下游会落兜底）: %s", sorted(missing)[:10])
                if duplicate_asins:
                    log.warning("[Taxonomy] 重复 ASIN（LLM 重复装，已去重保留首次出现）: %s",
                                sorted(duplicate_asins)[:10])

                for seg in result.product_segments:
                    kws = (seg.representative_keywords or [])[:8]
                    log.info(
                        "[Taxonomy]   桶: %r (%d 个 ASIN) | material=%r | form=%r | keywords=%s",
                        seg.name, len(seg.member_asins or []),
                        seg.material_attribute or "-",
                        getattr(seg, "form_attribute", "") or "-", kws,
                    )
        except Exception as e:
            log.warning("[Taxonomy] members → member_asins 转换异常（不阻断）: %s", e)
        return result

    def _fallback(self, input_data: dict) -> MarketInsightPack:
        """LLM 不可用：返回 is_fallback=True，调用方应保留 BSR Analyzer 原 segments 作兜底。"""
        return MarketInsightPack(is_fallback=True)
