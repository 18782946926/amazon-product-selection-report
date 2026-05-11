"""ReverseASIN Analyzer 的 prompt：关键词反查表 → 流量结构洞察。"""
from __future__ import annotations

import json

from llm.prompts.common import wrap_system_prompt

_RULES = """你是亚马逊关键词流量分析师。你将拿到头部竞品的关键词反查数据，请输出**严格符合 JSON schema** 的流量洞察。

核心原则：
1. search_intent_clusters 把关键词按搜索意图聚类（功能向/场景向/品牌向/价格向/属性向/其他）。每簇 competition_note 必须含具体词数或总搜索量。
2. niche_gaps：搜索量可观但头部 ASIN 自然排名 <50 的词。why_gap_exists 必须包含具体搜索量或排名数字。
3. traffic_strategy_summary：必须同时包含 (a) 至少一个补充统计里的 promo_* 数字（PPC/广告竞品数/点击占比/转化占比/SPR）(b) 至少一个具体关键词。无具体数据支撑时返回空字符串。严禁虚构"广告占比 X%""自然流量占比 X%""无广告竞争"这类概念——卖家精灵 ReverseASIN 源数据里根本没有这两个单列字段，所有广告/自然流量分配类结论均属编造，一律返回空串。
"""

SYSTEM_PROMPT = wrap_system_prompt(_RULES)

USER_TEMPLATE = """以下是 ReverseASIN 数据（JSON 数组）：

```json
{kw_json}
```

补充统计（唯一合法的推广压力数字来源）：
- 总关键词数：{total_kw}
- 总搜索量：{total_volume}
- Top20 平均 PPC 竞价：${promo_ppc_bid_avg}
- Top20 平均广告竞品数：{promo_ads_competitor_avg}
- Top20 前 3 ASIN 点击总占比均值：{promo_click_share_avg_top}%
- Top20 前 3 ASIN 转化总占比均值：{promo_conversion_share_avg_top}%
- Top20 SPR 中位数：{promo_spr_median}

请输出 JSON：

{{
  "search_intent_clusters": [
    {{"intent_type": "功能向|场景向|品牌向|价格向|属性向", "keywords": ["..."], "total_volume": 12345, "competition_note": "..."}}
  ],
  "niche_gaps": [
    {{"keyword": "...", "volume": 2000, "why_gap_exists": "..."}}
  ],
  "traffic_strategy_summary": "..."
}}

只输出 JSON，无 markdown 围栏。"""


def build_messages(keyword_rows: list[dict], stats: dict) -> list[dict]:
    kw_json = json.dumps(keyword_rows, ensure_ascii=False, indent=1)
    user = USER_TEMPLATE.format(
        kw_json=kw_json,
        total_kw=stats.get("total_kw", len(keyword_rows)),
        total_volume=stats.get("total_volume", 0),
        promo_ppc_bid_avg=stats.get("promo_ppc_bid_avg", 0),
        promo_ads_competitor_avg=stats.get("promo_ads_competitor_avg", 0),
        promo_click_share_avg_top=stats.get("promo_click_share_avg_top", 0),
        promo_conversion_share_avg_top=stats.get("promo_conversion_share_avg_top", 0),
        promo_spr_median=stats.get("promo_spr_median", 0),
    )
    return [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": user},
    ]
