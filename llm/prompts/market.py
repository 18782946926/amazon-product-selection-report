"""Market Analyzer 的 prompt：US-Market 文件 → 趋势/生命周期/季节性洞察。"""
from __future__ import annotations

import json

from llm.prompts.common import wrap_system_prompt

_RULES = """你是亚马逊品类趋势分析师。你将拿到一份 US-Market 品类数据（含销售趋势、搜索趋势、需求周期、新品上架量），请输出**严格符合 JSON schema** 的趋势洞察。

核心原则：
1. lifecycle_stage 判断（导入期/成长期/成熟期/衰退期）：
   - 销量持续上升 + 新品涌入 = 成长期
   - 销量平稳 + 新品涌入放缓 = 成熟期
   - 销量下滑 = 衰退期
   - evidence 字段必须包含具体月份或 YoY 百分数（如 "2025-11 销量较 2024-11 +24%"），否则 stage="未知"
2. seasonality 必须基于实际月度数据观察，振幅 amplitude_pct = (peak - trough) / mean × 100
   - 仅在有 ≥12 个月数据且振幅 >20% 时才能写 "显著"
3. demand_direction.recent_yoy_change 必须含具体百分数，否则留空字符串
4. risk_signals：仅列举有数据可引的异常信号（如 "search volume -18% 但 sales +7%"），无则空数组
5. category_summary：必须含**至少一个具体数字**（月销量/YoY/某月份）。没有数据支撑时返回空字符串（""），绝对不要写"本品类稳定增长"这类空话
"""

SYSTEM_PROMPT = wrap_system_prompt(_RULES)

USER_TEMPLATE = """以下是 Market 数据（多 sheet 关键字段已抽取为 JSON）：

```json
{market_json}
```

请输出 JSON：

{{
  "lifecycle_stage": {{"stage": "成长期|成熟期|衰退期|导入期", "confidence": "高|中|低", "evidence": "..."}},
  "seasonality": {{"pattern": "显著季节性|弱季节性|无季节性", "peak_months": ["11月", "12月"], "trough_months": ["6月"], "amplitude_pct": 35.0}},
  "demand_direction": {{"direction": "上升|稳定|下滑|震荡", "inflection_points": ["..."], "recent_yoy_change": "..."}},
  "risk_signals": [
    {{"signal": "...", "severity": "高|中|低", "evidence": "..."}}
  ],
  "category_summary": "..."
}}

只输出 JSON，无 markdown 围栏。"""


def build_messages(market_data: dict) -> list[dict]:
    market_json = json.dumps(market_data, ensure_ascii=False, indent=1, default=str)
    user = USER_TEMPLATE.format(market_json=market_json)
    return [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": user},
    ]
