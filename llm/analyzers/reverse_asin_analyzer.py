from __future__ import annotations

import hashlib
import logging

import pandas as pd

from llm.analyzers.base import BaseAnalyzer
from llm.prompts import reverse_asin as ra_prompt
from llm.schemas import (
    NicheGap,
    SearchIntentCluster,
    TrafficInsightPack,
)

log = logging.getLogger(__name__)


class ReverseAsinAnalyzer(BaseAnalyzer[TrafficInsightPack]):
    name = "ReverseASIN"

    def _cache_key(self, input_data: dict) -> str:
        df: pd.DataFrame = input_data["df"]
        h = hashlib.sha256()
        for col in ("Keyword", "Search Volume"):
            if col in df.columns:
                h.update(str(df[col].head(50).tolist()).encode())
        return f"traffic_{h.hexdigest()[:16]}"

    def _prepare_rows(self, df: pd.DataFrame, top_n: int = 150) -> tuple[list[dict], dict]:
        if df.empty:
            return [], {}

        col_map = {c.lower().strip(): c for c in df.columns}

        def col(*names: str) -> str | None:
            for n in names:
                if n.lower() in col_map:
                    return col_map[n.lower()]
            return None

        kw_col = col("keyword", "search term")
        vol_col = col("m. searches", "search volume", "monthly searches", "estimated weekly impressions")
        rank_col = col("organic rank", "rank")
        # 真实卖家精灵列（经 normalize_keyword_columns 映射过）：
        #   点击/转化总占比 = 前 3 ASIN 点击/转化占比之和（头部垄断信号）
        #   广告竞品数 / PPC 竞价 / SPR / 商品数 = 推广压力复合指标组件
        click_share_col = col("click share")
        conv_share_col = col("conversion share")
        # 卖家精灵 ReverseASIN 实际列名是 "Sponsored ASINs"，旧代码硬编码 "ads competitor count" 找不到
        ads_comp_col = col("ads competitor count", "sponsored asins", "sponsored competitor")
        ppc_bid_col = col("ppc bid", "suggested bid")
        spr_col = col("spr")
        products_col = col("products", "title density")

        if not kw_col:
            return [], {}

        sub = df.copy()
        if vol_col:
            sub = sub.sort_values(vol_col, ascending=False)
        sub = sub.head(top_n)

        rows = []
        for _, r in sub.iterrows():
            row = {"keyword": str(r[kw_col])[:80]}
            if vol_col:
                row["volume"] = _to_int(r.get(vol_col))
            if rank_col:
                row["rank"] = _to_int(r.get(rank_col))
            if click_share_col:
                row["click_share_pct"] = _to_pct(r.get(click_share_col))
            if conv_share_col:
                row["conversion_share_pct"] = _to_pct(r.get(conv_share_col))
            if ads_comp_col:
                row["ads_competitor_count"] = _to_int(r.get(ads_comp_col))
            if ppc_bid_col:
                row["ppc_bid"] = _to_float(r.get(ppc_bid_col))
            if spr_col:
                row["spr"] = _to_int(r.get(spr_col))
            if products_col:
                row["products"] = _to_int(r.get(products_col))
            rows.append(row)

        total_vol = sum(r.get("volume", 0) for r in rows)

        # 推广压力复合指标（取前 20 核心词的均值/中位数）
        top20 = rows[:20]
        def _avg(key: str) -> float:
            vals = [r.get(key) for r in top20 if r.get(key)]
            vals = [v for v in vals if v]
            return round(sum(vals) / len(vals), 2) if vals else 0.0

        def _median(key: str) -> float:
            vals = sorted(r.get(key, 0) for r in top20 if r.get(key))
            if not vals:
                return 0.0
            n = len(vals)
            return round((vals[n // 2] if n % 2 else (vals[n // 2 - 1] + vals[n // 2]) / 2), 2)

        return rows, {
            "total_kw": len(rows),
            "total_volume": total_vol,
            "promo_ppc_bid_avg": _avg("ppc_bid"),
            "promo_ads_competitor_avg": _avg("ads_competitor_count"),
            "promo_click_share_avg_top": _avg("click_share_pct"),
            "promo_conversion_share_avg_top": _avg("conversion_share_pct"),
            "promo_spr_median": _median("spr"),
            "promo_products_median": _median("products"),
        }

    def _call_llm(self, input_data: dict) -> TrafficInsightPack:
        df = input_data["df"]
        rows, stats = self._prepare_rows(df)
        if not rows:
            return TrafficInsightPack(is_fallback=True)
        cache_key = self._cache_key(input_data)
        messages = ra_prompt.build_messages(rows, stats)
        return self.client.chat_json(
            messages=messages,
            schema=TrafficInsightPack,
            tier="fast",
            cache_key=cache_key,
            temperature=0.2,
            max_tokens=6000,
        )

    def _fallback(self, input_data: dict) -> TrafficInsightPack:
        """LLM 不可用时返回空 TrafficInsightPack。
        推广压力数据已经通过 stats.promo_*（packs_runtime._build_stats_for_synthesis）落地，
        这里不再构造任何派生字段——避免 LLM 不可用时仍生成"广告主导/自然流量为主"这类虚构叙述。
        """
        return TrafficInsightPack(is_fallback=True)


def _to_int(v) -> int:
    try:
        return int(float(str(v).replace(",", "")))
    except (TypeError, ValueError):
        return 0


def _to_pct(v) -> float:
    try:
        s = str(v).replace("%", "").replace(",", "")
        f = float(s)
        return f * 100 if 0 <= f <= 1 else f
    except (TypeError, ValueError):
        return 0.0


def _to_float(v) -> float:
    """容错浮点转换：剥除 $ 和千分位逗号；区间字符串（如 '$0.50-$1.20'）取中点。"""
    if v is None:
        return 0.0
    s = str(v).replace("$", "").replace(",", "").strip()
    if not s or s.lower() == "nan":
        return 0.0
    if "-" in s and not s.startswith("-"):
        parts = [p for p in s.split("-") if p.strip()]
        try:
            nums = [float(p.strip()) for p in parts]
            if nums:
                return sum(nums) / len(nums)
        except ValueError:
            pass
    try:
        return float(s)
    except ValueError:
        return 0.0
