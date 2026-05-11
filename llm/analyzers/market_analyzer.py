from __future__ import annotations

import hashlib
import logging
from typing import Any

import pandas as pd

from llm.analyzers.base import BaseAnalyzer
from llm.prompts import market as market_prompt
from llm.schemas import (
    DemandDirection,
    LifecycleStage,
    Seasonality,
    TrendInsightPack,
)

log = logging.getLogger(__name__)


class MarketAnalyzer(BaseAnalyzer[TrendInsightPack]):
    name = "Market"

    def _cache_key(self, input_data: dict) -> str:
        market_data = input_data["market_data"]
        h = hashlib.sha256()
        h.update(str(sorted(market_data.keys())).encode())
        for k, df in market_data.items():
            if isinstance(df, pd.DataFrame) and not df.empty:
                h.update(str(df.head(5).to_dict()).encode())
        return f"trend_{h.hexdigest()[:16]}"

    def _summarize(self, market_data: dict) -> dict:
        """把 12 个 sheet 压缩为关键字段送 LLM。
        数据量扩容：每 sheet 从 head(8) → head(30)，列保留全部（原来 columns[:15] 会截断）。
        全品类报告需要看到完整的月度趋势 + 多列维度（如分产品型号/价格带的分月销量）才能
        判断周期性。扩容后 trend/seasonality 分析不再被"只看前 8 行"拉低到"未知"。
        """
        summary = {}
        for sheet_name, df in market_data.items():
            if not isinstance(df, pd.DataFrame) or df.empty:
                continue
            summary[sheet_name] = {
                "columns": list(df.columns),
                "rows": len(df),
                "head": df.head(30).to_dict(orient="records"),
            }
        return summary

    def _call_llm(self, input_data: dict) -> TrendInsightPack:
        summary = self._summarize(input_data["market_data"])
        if not summary:
            return TrendInsightPack(is_fallback=True)
        cache_key = self._cache_key(input_data)
        messages = market_prompt.build_messages(summary)
        return self.client.chat_json(
            messages=messages,
            schema=TrendInsightPack,
            tier="fast",
            cache_key=cache_key,
            temperature=0.2,
            max_tokens=6000,
            timeout=180,
        )

    def _fallback(self, input_data: dict) -> TrendInsightPack:
        """LLM 不可用时返回空 Pack，字段 schema 默认值（stage="未知"）仅作结构占位，
        渲染层见到 is_fallback=True 会跳过 Sheet 9 趋势段。"""
        return TrendInsightPack(is_fallback=True)
