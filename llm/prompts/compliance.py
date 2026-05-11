"""Compliance Analyzer 的 prompt：品类 → 美国市场必备认证 + 退货率估算。

用途：替代 app.py 里硬编码的 UL8750/UL1598（LED 专属）认证清单和 "工作灯退货率 5-8%"。
LLM 按品类动态给出：该品类常见认证、典型退货率范围、TOP 退货原因。
"""
from __future__ import annotations

import json

from llm.prompts.common import wrap_system_prompt

_RULES = """你是亚马逊跨品类合规顾问。你将拿到一个品类的中文名 + 若干头部竞品标题，请输出该品类在美国市场的合规要求。

核心原则：
1. required_certifications：仅列出**明确适用于该品类**的美国认证/合规项。示例：
   - 含电池/AC 适配器 → UL 60950 或 UL 62368
   - LED 灯具 → UL 8750 + FCC Part 15
   - 儿童/3 岁以下用品 → CPSIA + ASTM F963
   - 食品接触 → FDA 21 CFR
   - 含 Wi-Fi/蓝牙 → FCC Part 15 subpart C + SDOC
   不明确是否适用则不列
2. mandatory=true 仅当缺失会直接被亚马逊下架或海关扣留；mandatory=false 表示"大型分销商或 B2B 客户会要求但不是强制"
3. typical_return_rate_pct：基于亚马逊常见品类退货率知识给一个合理区间中点（如 5%-8% 填 6.5）。无法估算时填 0
4. top_return_reasons：基于品类特性给 3-5 条常见退货原因，要**品类特异**（如充气机"压力实测不达标"、洗车枪"接口不兼容"），不要写"质量问题"这类通用词
5. 禁止编造不存在的认证编号（如 "UL 99999"）；拿不准的认证直接不列
"""

SYSTEM_PROMPT = wrap_system_prompt(_RULES)


USER_TEMPLATE = """品类中文名：{category_name}

竞品标题摘要（关键词参考，用于判断产品形态）：
{titles}

请输出 JSON：

{{
  "required_certifications": [
    {{"name": "UL 8750", "mandatory": true, "applies_to": "LED 灯具", "risk_if_missing": "亚马逊下架"}}
  ],
  "typical_return_rate_pct": 6.5,
  "top_return_reasons": ["电池续航不达标", "开关松动易断", "配件缺失"]
}}

只输出 JSON，无 markdown 围栏。"""


def build_messages(category_name: str, titles_sample: list[str]) -> list[dict]:
    titles_txt = "\n".join(f"- {t}" for t in titles_sample[:30])
    user = USER_TEMPLATE.format(category_name=category_name or "未知品类", titles=titles_txt)
    return [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": user},
    ]
