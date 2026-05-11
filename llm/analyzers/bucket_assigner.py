"""BucketAssigner：把 N 个 ASIN 通过 LLM 语义判定分到聚合 LLM 已切好的 N 个桶里。

为什么需要这一步：
- 聚合 LLM (TaxonomyAggregator) 切桶质量好，但不分配 ASIN（避免长 JSON 漂移）
- 代码 keyword 子串匹配在 CJK 文本上不够鲁棒（被 tokenizer 局限 / 缺乏语义理解）
- 这一层用 LLM 的语义理解填补：100 个视觉描述 + 桶定义 → 100 个映射
- 输出紧凑（每行 5-10 字），不触发长 JSON 漂移问题
"""
from __future__ import annotations

import hashlib
import json
import logging

from pydantic import BaseModel, Field

from llm.analyzers.base import BaseAnalyzer
from llm.prompts import bucket_assigner as ba_prompt

log = logging.getLogger(__name__)


class _Assignment(BaseModel):
    item_idx: int = Field(..., description="产品序号（从 1 开始）")
    bucket_idx: int = Field(..., description="桶序号（0 表示「其他」，1+ 表示具体桶）")


class BucketAssignmentResult(BaseModel):
    assignments: list[_Assignment] = Field(default_factory=list)
    is_fallback: bool = False


class BucketAssigner(BaseAnalyzer[BucketAssignmentResult]):
    name = "BucketAssigner"

    def _cache_key(self, input_data: dict) -> str:
        # cache key = prompt 版本 + 桶定义 hash + 视觉描述 hash
        # 同桶定义 + 同描述 → 同 cache key → 同分配（跨次稳定）
        h = hashlib.sha256()
        h.update(ba_prompt._PROMPT_VERSION.encode())
        # 桶定义按 name 排序后序列化（dict 不能直接 sorted，必须显式 key）
        bucket_sig = sorted(
            (
                {
                    "n": b.get("name", ""),
                    "k": list(sorted(b.get("keywords", []) or [])),
                    "m": b.get("material", "") or "",
                    "f": b.get("form", "") or "",
                }
                for b in input_data.get("bucket_defs", [])
            ),
            key=lambda d: d["n"],
        )
        h.update(json.dumps(bucket_sig, sort_keys=True, ensure_ascii=False).encode())
        # 视觉描述按 asin 排序后序列化
        items_sig = sorted(
            [
                (v.get("asin", ""), v.get("product_type_free", "") or "",
                 v.get("material", "") or "", v.get("form", "") or "")
                for v in input_data.get("visual_items", [])
            ]
        )
        h.update(json.dumps(items_sig, sort_keys=True, ensure_ascii=False).encode())
        return f"bucket_assignment_{h.hexdigest()[:16]}"

    def _call_llm(self, input_data: dict) -> BucketAssignmentResult:
        bucket_defs = input_data.get("bucket_defs") or []
        visual_items = input_data.get("visual_items") or []
        if len(bucket_defs) < 2 or len(visual_items) < 2:
            log.warning("[BucketAssigner] 输入太少（桶 %d, 产品 %d）→ 跳过",
                        len(bucket_defs), len(visual_items))
            return BucketAssignmentResult(is_fallback=True)

        messages = ba_prompt.build_messages(bucket_defs, visual_items)
        result = self.client.chat_json(
            messages=messages,
            schema=BucketAssignmentResult,
            tier="fast",
            cache_key=self._cache_key(input_data),
            temperature=0.0,
            max_tokens=8000,
            timeout=180,
        )
        try:
            if result and result.assignments:
                # 统计：多少 → 桶 0（其他）vs 真实桶
                n_other = sum(1 for a in result.assignments if a.bucket_idx == 0)
                n_assigned = len(result.assignments) - n_other
                log.info("[BucketAssigner] LLM 分配完成：%d 归桶 + %d 归其他（共 %d 个产品）",
                         n_assigned, n_other, len(result.assignments))
        except Exception:
            pass
        return result

    def _fallback(self, input_data: dict) -> BucketAssignmentResult:
        return BucketAssignmentResult(is_fallback=True)
