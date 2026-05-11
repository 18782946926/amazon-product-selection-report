from __future__ import annotations

import hashlib
import logging
import os
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

import pandas as pd

from llm.analyzers.base import BaseAnalyzer
from llm.exceptions import LLMSchemaError, LLMUnavailable
from llm.prompts import reviews as reviews_prompt
from llm.schemas import VOCPack
from utils.review_batcher import batch_reviews, select_for_analysis

log = logging.getLogger(__name__)


# 只保留用于 fallback 兜底识别退货/保修的"流程型"关键词。
# 原先的 GENERIC_PAIN_KEYWORDS / GENERIC_PRAISE_KEYWORDS 已删除——
# 那是 LED 工作灯留下的通用词典，跨品类会输出"质量问题/好用"这类无意义聚类，
# 与全品类选品报告的"严格基于源数据、不输出废话"原则冲突。
AFTER_SALE_KEYWORDS = ["refund", "return", "warranty", "customer service", "no response"]


class ReviewsAnalyzer(BaseAnalyzer[VOCPack]):
    name = "Reviews"

    def _cache_key(self, input_data: dict) -> str:
        reviews = input_data["reviews"]
        # 品类指纹必须进缓存键：同一份 reviews.xlsx 在不同品类（LED vs Battery）下必须独立缓存，
        # 否则跨品类会复用上次 merged VOC（出现过"亮度不足"串入 Battery Chargers 的事故）
        cat = str(input_data.get("category_hint") or "").strip()
        h = hashlib.sha256()
        h.update(cat.encode("utf-8"))
        for r in reviews:
            h.update(str(r.get("asin", "")).encode())
            h.update(str(r.get("date", ""))[:10].encode())
        return f"voc_{h.hexdigest()[:16]}"

    def _normalize_reviews(self, reviews: list[dict]) -> list[dict]:
        out = []
        for r in reviews:
            out.append({
                "asin": str(r.get("asin", "") or r.get("ASIN", ""))[:20],
                "rating": r.get("rating", r.get("Rating", 0)),
                "title": str(r.get("title", "") or r.get("Title", ""))[:200],
                "content": str(r.get("content", "") or r.get("Content", ""))[:600],
                "date": str(r.get("date", "") or r.get("Date", ""))[:10],
            })
        return out

    def _call_llm(self, input_data: dict) -> VOCPack:
        normalized = self._normalize_reviews(input_data["reviews"])
        sampled = select_for_analysis(normalized)
        if not sampled:
            return VOCPack(is_fallback=True)

        batches = batch_reviews(sampled)
        from llm.schemas import PainCluster as _PC
        log.info("[Reviews] PainCluster 当前字段=%s", list(_PC.model_fields.keys()))
        log.info("[Reviews] 共 %d 条评论，采样 %d 条，分 %d 批", len(normalized), len(sampled), len(batches))

        def _run_batch(i: int, batch: list[dict]) -> dict | None:
            stats = {
                "total": len(batch),
                "neg": sum(1 for r in batch if _rating_lt(r, 3)),
                "pos": sum(1 for r in batch if _rating_gt(r, 3)),
                "asin_count": len({r["asin"] for r in batch if r["asin"]}),
            }
            messages = reviews_prompt.build_messages(batch, stats)
            cache_key = f"voc_batch_{self._cache_key(input_data)}_{i}"
            try:
                pack = self.client.chat_json(
                    messages=messages,
                    schema=VOCPack,
                    tier="fast",
                    cache_key=cache_key,
                    temperature=0.2,
                    max_tokens=12000,
                    timeout=180,
                )
                log.info("[Reviews] 批次 %d/%d 完成", i + 1, len(batches))
                return pack.model_dump(mode="json")
            except (LLMUnavailable, LLMSchemaError) as e:
                log.warning("[Reviews] 批次 %d 失败: %s（继续后续批次）", i, e)
                return None

        max_workers = min(len(batches), int(os.getenv("VOC_MAX_WORKERS", "12")))
        sub_packs: list[dict] = []
        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            futures = [ex.submit(_run_batch, i, b) for i, b in enumerate(batches)]
            for fut in as_completed(futures):
                result = fut.result()
                if result is not None:
                    sub_packs.append(result)

        if not sub_packs:
            raise LLMUnavailable("所有批次均失败")
        if len(sub_packs) == 1:
            return VOCPack.model_validate({k: v for k, v in sub_packs[0].items() if k != "reviewed"})

        category_hint = str(input_data.get("category_hint") or "").strip()
        merge_msgs = reviews_prompt.build_merge_messages(sub_packs, category_hint)
        merge_key = f"voc_merge_{self._cache_key(input_data)}"
        return self.client.chat_json(
            messages=merge_msgs,
            schema=VOCPack,
            tier="fast",
            cache_key=merge_key,
            temperature=0.1,
            max_tokens=12000,
            timeout=180,
        )

    def _fallback(self, input_data: dict) -> VOCPack:
        """LLM 不可用时返回空 Pack。
        由 Python 词典硬聚类出来的"质量问题/好用"是跨品类无意义的 LED 遗毒——
        下游渲染层看到 is_fallback=True 会跳过相关段落。
        """
        return VOCPack(is_fallback=True)


def _rating_lt(r: dict, n: int) -> bool:
    try:
        return float(r.get("rating", 0)) < n
    except (TypeError, ValueError):
        return False


def _rating_gt(r: dict, n: int) -> bool:
    try:
        return float(r.get("rating", 0)) > n
    except (TypeError, ValueError):
        return False
