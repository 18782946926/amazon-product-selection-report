"""LabelMerger：轻量 LLM 把语义重叠的 form_label 合并。

设计意图：视觉 LLM per-ASIN 输出 form_label 时是孤岛判断（不知道其他 ASIN 用了什么标签），
导致同义标签（如「手持式」「便携式」）被随机分散。LabelMerger 在 Groupby 之前把所有
form_label 去重后整体交给 LLM 一次，让它从全局视角给出合并 mapping。

输入只 ~10 个标签字符串、输出只一个小 mapping JSON——比 100-ASIN 全局聚类轻量，漂移率低。
"""
from __future__ import annotations

import hashlib
import json
import logging

from llm.analyzers.base import BaseAnalyzer
from llm.prompts import label_merge as lm_prompt
from llm.schemas import LabelMergeResult

log = logging.getLogger(__name__)


class LabelMerger(BaseAnalyzer[LabelMergeResult]):
    name = "LabelMerger"

    def _cache_key(self, input_data: dict) -> str:
        labels = sorted(input_data.get("labels") or [])
        h = hashlib.sha256()
        h.update(lm_prompt._PROMPT_VERSION.encode())
        h.update(json.dumps(labels, ensure_ascii=False).encode())
        return f"label_merge_{h.hexdigest()[:16]}"

    def _call_llm(self, input_data: dict) -> LabelMergeResult:
        labels = input_data.get("labels") or []
        if len(labels) < 2:
            return LabelMergeResult(merge_mapping={}, is_fallback=False)

        messages = lm_prompt.build_messages(labels=list(labels))
        result = self.client.chat_json(
            messages=messages,
            schema=LabelMergeResult,
            tier="fast",
            cache_key=self._cache_key(input_data),
            temperature=0.0,
            max_tokens=500,
            timeout=60,
        )

        # 校验 mapping：from/to 都必须在输入 labels 内（避免 LLM 写出输入里没有的标签）
        if result and result.merge_mapping:
            valid = {
                k: v
                for k, v in result.merge_mapping.items()
                if k in labels and v in labels and k != v
            }
            dropped = set(result.merge_mapping) - set(valid)
            if dropped:
                log.warning("[LabelMerger] 丢弃无效 mapping 项（标签不在输入或自映射）: %s", dropped)
            result.merge_mapping = valid
            log.info("[LabelMerger] 输入 %d 个标签，合并 %d 对", len(labels), len(valid))
        return result

    def _fallback(self, input_data: dict) -> LabelMergeResult:
        return LabelMergeResult(merge_mapping={}, is_fallback=True)
