"""Sheet5ImprovementAnalyzer 的 prompt：voc.pain_clusters 前 4 条 → 4 条改进计划。

设计目的：把 sheet5_improvement_plan 从 Synthesizer 巨型输出里拆出来，独占 8K token 预算，
避免「LLM 写到第 3 条 plan 时 token 接近上限，第 4 条被 client._repair_truncated_json 修复时丢弃」。
"""
from __future__ import annotations

import json

from llm.prompts.common import wrap_system_prompt

_RULES = """你是亚马逊跨品类选品资深分析师。请基于本品类的 voc.pain_clusters 前 4 条，**严格输出 4 条**改进计划（P1/P2/P3/P4）。

每条 plan 必须包含：
1. **priority**：P1/P2/P3/P4，依次对应 pain_clusters 顺序（按差评频次从大到小）
2. **pain_name**：**精确复制** voc.pain_clusters[i].name（不得改写、不得加标点、不得简化）
3. **root_cause**：**必填，不可为空**。1 句根因诊断，≥10 汉字，说清是「设计缺陷 / 工艺薄弱 / 用料低成本 / 配件质量 / 说明书不清」等哪一类；**必须引用 voc.pain_clusters[i].raw_quotes 里一条原文片段**佐证（如「用户反馈'开关坏了'」）
4. **action_items**：3-5 条**具体可落地**的改进建议。每条必须：
   - 含具体数值（引用 stats.competitor_spec_p75 / stats.competitor_spec_medians 里的值，如「将输出电流从中位 2.1A 提升至 ≥5A（P75 水平）」）
   - 不得出现跨品类禁用词（基于 category_hint 严格约束）
   - 可直接用于工厂/ODM 的 BOM 改动指令
5. **target_metric**：**必填，不可为空字符串**。1 句改进目标，含**可验证的数字门槛**（如「将 1-2★ 占比从当前 33% 降至 15% 以下」/「让'充不进电'差评频次降低 50%」）
6. **supporting_fields**：至少引用 1 个 `voc.pain_clusters[i]` + 1 个 `stats.competitor_spec_p75.xxx` 或 `stats.competitor_spec_medians.xxx`

特殊规则：
- **P4 一定要有**，不能因为第 4 个痛点频次低就省略 —— 可以给轻量改进（如「优化说明书 / 补配件」）
- **pain_name 品类自纠**：即使 voc.pain_clusters 里混入了**跨品类条目**（即 pain 描述的物理属性 / 配件名与 `{category_hint}` 本品类核心产品形态明显不符；典型场景：本品类是充电器但出现"亮度不足/反光杯"，本品类是 LED 但出现"mAh 虚标"，本品类是存钱罐但出现"流明不够"等），**跳过该条，用下一个合规 pain 补位**。禁止强行输出与本品类无关的专属 action_items 来迁就一个不合品类的 pain_name。判别原则：若 pain 引用的物理量在 stats.competitor_spec_* 里完全找不到对应维度，多半是跨品类污染。宁可少输出一条计划，也绝不写不属于 `{category_hint}` 的改良指令。
- **履约 / 供应链类 pain 的例外**：如果 pain 属于「物流 / 包装 / 收货状态 / 二手翻新 / 外观磕碰 / 质检漏检」等履约/供应链范畴（例如「收到二手/破损产品」「包装简陋」「外观磕碰」「配件缺失」「说明书缺失」），**不算跨品类**，必须输出而非跳过：
  * action_items 可以给「流程型」改进（如「发货前三重外观 QC 抽检 10%」「气泡袋 + 内衬卡槽防撞双层包装」「入库时扫码核对序列号防翻新」「补齐多语种说明书 + 视频扫码引导」）
  * 数字门槛可用运营指标（如「将破损退货率从 X% 降至 2% 以下」「外观差评从 55 条降至月 10 条以下」），不必强求产品参数
  * supporting_fields 只引用 voc.pain_clusters[i] 即可，不要求 stats.competitor_spec_*
"""

SYSTEM_PROMPT = wrap_system_prompt(_RULES)


USER_TEMPLATE = """品类：{category_hint}

【voc.pain_clusters 前 4 条】（按差评频次降序，i=0/1/2/3 对应 P1/P2/P3/P4）

```json
{pain_clusters_json}
```

【stats.competitor_spec_p75 / medians】（用于 action_items 里引用具体数值）

```json
{stats_json}
```

请严格输出以下 JSON：

{{
  "plans": [
    {{
      "priority": "P1",
      "pain_name": "<voc.pain_clusters[0].name 原样精确复制>",
      "root_cause": "<1 句根因诊断 + 引用 raw_quotes 片段>",
      "action_items": ["<改进 1 含数字>", "<改进 2 含数字>", "<改进 3 含数字>"],
      "target_metric": "<目标 + 可验证数字门槛>",
      "supporting_fields": ["voc.pain_clusters[0]", "stats.competitor_spec_p75.xxx"]
    }},
    {{"priority": "P2", "pain_name": "<voc.pain_clusters[1].name>", ...}},
    {{"priority": "P3", "pain_name": "<voc.pain_clusters[2].name>", ...}},
    {{"priority": "P4", "pain_name": "<voc.pain_clusters[3].name>", ...}}
  ]
}}

只输出 JSON，无 markdown 围栏。"""


def build_messages(
    *,
    category_hint: str,
    pain_clusters: list[dict],
    stats: dict | None = None,
) -> list[dict]:
    """组装 sheet5_improvement_analyzer 的 LLM 消息。

    Args:
        category_hint: 品类中文名（如「电池充电器」/「存钱罐」）
        pain_clusters: voc.pain_clusters 前 4 条的 dict 列表（含 name / frequency_pct / raw_quotes）
        stats: synthesis_stats（含 competitor_spec_p75 / competitor_spec_medians）
    """
    user = USER_TEMPLATE.format(
        category_hint=category_hint or "未知品类",
        pain_clusters_json=json.dumps(pain_clusters[:4], ensure_ascii=False, indent=2),
        stats_json=json.dumps(stats or {}, ensure_ascii=False, indent=2),
    )
    return [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": user},
    ]
