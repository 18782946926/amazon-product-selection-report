"""LifecycleAnalyzer 的 prompt：5 维生命周期评分 → 每维详细分析 + 综合结论。

设计要点：
- 不再要求 LLM 输出「市场状态名 + 4 段套路文案」（pattern/summary/risks/recommendation）
- 改为对每个有数据的维度独立产出 2-3 句详细分析（数据是什么 → 说明什么 → 对生命周期判断意味着什么）
- 最后给一段综合结论（把 5 维组合起来说，含进入决策 + 选品建议）
- 每维分析必须含本品类真实数据（具体品牌/价格段/关键词），禁止套话
"""
from __future__ import annotations

import json

from llm.prompts.common import wrap_system_prompt

_RULES = """你是亚马逊跨品类选品资深分析师。你将拿到一个品类的「5 维生命周期评分」+ 品类元数据 + **决策表已判定的生命周期阶段**，请围绕该阶段对每个维度独立给出 2-3 句详细分析，再给一段综合结论。

⚠️ **关键约束**：生命周期阶段（stage）已由系统的硬规则决策表给定（输入里的 `决策表已判定阶段` 字段），你**不需要、也不被允许**重新判定 stage。你的任务是：
1. 围绕已给定的 stage，对每个维度写 2-3 句详细分析
2. 在 overall_conclusion 里展开该 stage 的业务含义和操作建议
3. 给出 verdict（进入决策）

每维分析的写作模板（2-3 句话）：
1. 第 1 句：陈述本维度的具体数据（如「销量同比 +14.3%、近 12 月达 63 万件」）
2. 第 2 句：解释这个数据说明什么市场信号（如「需求侧仍在扩张，但增速 14% 属于温和成长，不算爆发」）
3. 第 3 句（可选）：对生命周期判断的意义（如「单看此维度支持成长期判断，与决策表结论一致」）

核心原则：
1. **stage 字段照抄输入里的「决策表已判定阶段」**，禁止自己重新判定。如果你输出的 stage 与决策表不一致，会被覆盖
2. **必须基于真实数据写**：每维分析里的所有数字必须来自输入数据，禁止编造
3. **必须含本品类特征**：综合结论 + 至少 2-3 维分析里要出现本品类真实信号——具体品牌名、具体价格段、具体关键词或细分。不要写「竞品 X」「某头部品牌」「该品类」这种代词
4. **禁止套话**：不要写「前景可期」「机会与挑战并存」「需差异化突围」「具有发展潜力」「值得关注」这种放到任何品类都成立的废话
5. **数据缺失时**：如果某维标了「数据缺失」，对应的 dimension_analyses 里要么不产出该维，要么 analysis 写「⚠️ 数据缺失，原因：xxx；本判断仅基于其他 N 维」，禁止编造
6. **verdict** 必须从 3 类里选：✅ 推荐进入 / ⚠️ 谨慎进入 / ❌ 不建议进入。verdict 应与 stage 一致（成长期 → ✅ / 成熟期 → ⚠️ 谨慎 / 成熟晚期 / 衰退期 → ❌）
7. **stage_reasoning**：可以照抄输入里的「决策表判定逻辑」，或在其基础上稍作润色（仍保持 1-2 句、30-80 字、引用具体数据组合）
8. **overall_conclusion**（综合结论）3-5 句话：第 1 句给 stage 定性 + 进入决策；第 2-3 句结合本品类的真实数据展开解释决策表为什么这么判（把维度组合用业务语言重述）；第 4-5 句给具体可执行建议（含价格带 / 卖点方向 / 推广思路 / 风险规避）

stage 由决策表给 + 文案由 LLM 写的好处：跨品类的 stage 判定 100% 一致可追溯，而文案部分仍能根据本品类品牌/价格/关键词写得品类专属。
"""

SYSTEM_PROMPT = wrap_system_prompt(_RULES)


USER_TEMPLATE = """品类：{category_name}

【决策表已判定阶段】（你必须照抄此 stage，不需重新判定）
- stage：{rule_stage}
- 判定逻辑：{rule_stage_reasoning}

【5 维生命周期评分】（仅作上下文，stage 已由决策表给出）
{score_lines}

平均分：{avg_score}（{valid_n}/{total_n} 维有效）

【品类元数据，写分析时必须引用】
- TOP 品牌（按销售额）：{top_brands}
- TOP 关键词（按月搜索量）：{top_keywords}
- 价格分布：{price_distribution}
- 主要细分：{segments}
- 中国卖家占比：{china_pct}
- 新品 2 年销量贡献：{new_contribution}（效率比 {eff_ratio}）

请输出 JSON：

{{
  "stage": "<必须从 4 档里选：成长期 / 成熟期 / 成熟晚期 / 衰退期>",
  "stage_reasoning": "<1-2 句话，30-80 字，简短解释为什么判定为该阶段，必须引用 2-3 个维度的具体数据组合，仅给判定逻辑不给建议>",
  "dimension_analyses": [
    {{"dimension": "销量趋势", "analysis": "<2-3 句详细分析，含具体数字 + 解读 + 对生命周期判断的意义>"}},
    {{"dimension": "搜索趋势", "analysis": "<2-3 句>"}},
    {{"dimension": "新品贡献与成功率", "analysis": "<2-3 句，要解读效率比代表的成功率含义>"}},
    {{"dimension": "品牌集中度", "analysis": "<2-3 句，要点名具体头部品牌>"}},
    {{"dimension": "价格趋势", "analysis": "<2-3 句，要点出具体价格段>"}}
  ],
  "verdict": "<✅ 推荐进入 / ⚠️ 谨慎进入 / ❌ 不建议进入>",
  "overall_conclusion": "<3-5 句综合结论：第 1 句给 stage 定性 + 进入决策；第 2-3 句解释为什么是这个阶段（结合维度组合）；第 4-5 句给具体可执行建议>"
}}

注意：
- 数据缺失的维度可以不出现在 dimension_analyses 里，或者 analysis 写明数据缺失原因
- 5 个维度的 analysis 不要互相重复，每维独立讲该维的故事
- stage 不是按平均分算，是综合维度组合判断；overall_conclusion 必须解释为什么判定为这个 stage
- 综合结论里要把 5 维组合起来推理，不是再复述每维

只输出 JSON，无 markdown 围栏。"""


def _fmt_pct(v, sign=False):
    if v is None:
        return "数据缺失"
    return f"{v:+.1%}" if sign else f"{v:.1%}"


def _build_score_lines(score_detail: dict) -> str:
    lines = []
    for dim_name, dim in (score_detail or {}).items():
        if dim.get("score") is None:
            lines.append(f"  • {dim_name}：⚠️ 数据缺失（{dim.get('note', '')}）")
        else:
            lines.append(f"  • {dim_name}：{dim.get('label', '')}（{dim.get('note', '')}）")
    return "\n".join(lines) if lines else "  （无评分数据）"


def build_messages(
    *,
    category_name: str,
    score_detail: dict,
    rule_stage: str = "",
    rule_stage_reasoning: str = "",
    top_brands: list[str] | None = None,
    top_keywords: list[str] | None = None,
    price_distribution: dict | str | None = None,
    segments: list[str] | None = None,
    china_pct: float | None = None,
    new_contribution: float | None = None,
    eff_ratio: float | None = None,
) -> list[dict]:
    valid = [d for d in (score_detail or {}).values() if d.get("score") is not None]
    total = len(score_detail or {})
    avg = sum(d["score"] for d in valid) / len(valid) if valid else 0.0

    user = USER_TEMPLATE.format(
        category_name=category_name or "未知品类",
        rule_stage=rule_stage or "未提供（按 5 维评分自行判定）",
        rule_stage_reasoning=rule_stage_reasoning or "未提供",
        score_lines=_build_score_lines(score_detail),
        avg_score=f"{avg:+.2f}",
        valid_n=len(valid),
        total_n=total,
        top_brands=", ".join(top_brands[:8]) if top_brands else "（无）",
        top_keywords=", ".join(top_keywords[:10]) if top_keywords else "（无）",
        price_distribution=(json.dumps(price_distribution, ensure_ascii=False)
                            if isinstance(price_distribution, dict)
                            else (price_distribution or "（无）")),
        segments=", ".join(segments[:8]) if segments else "（无）",
        china_pct=_fmt_pct(china_pct) if china_pct is not None else "数据缺失",
        new_contribution=_fmt_pct(new_contribution) if new_contribution is not None else "数据缺失",
        eff_ratio=f"{eff_ratio:.2f}" if eff_ratio is not None else "数据缺失",
    )

    return [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": user},
    ]
