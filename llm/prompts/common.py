"""所有 Analyzer prompt 共享的"禁用词 + 必引数据"核心约束。

设计意图：LLM 最容易在"自由叙述"字段（summary / description / reading / playbook）里
塞"前景可期/建议深入分析/潜力可观"这类跨品类空话。这些废话既不能被用户追溯，
也不能反映本次上传数据的真实情况，是选品报告的主要污染源。

本模块提供一段可直接拼进 system prompt 的硬约束，四个 Analyzer 共用。
配合 llm/validators.py 的 post-check 做双保险（prompt 约束 + 产出校验）。
"""

EVIDENCE_REQUIREMENT = """

【硬约束：文案质量】

一、禁用词（以下词汇一旦出现在任何文案字段里，视为废话，整条结论作废）：
- "前景可期" / "前景广阔" / "市场空间广阔" / "潜力可观" / "潜力巨大"
- "建议深入分析" / "值得重点关注" / "有较大机会" / "不容忽视"
- "总体来看" / "综合来看" / "从整体上" / "整体而言"
- "具有一定优势" / "具备较好基础" / "呈现良好态势"

二、自由叙述字段的硬规则（summary / description / reading / playbook / justification 等）：
1. **必须包含至少一个可追溯的具体证据**：
   - 具体数字（如 "$34"、"月销 12,388 件"、"CR5=42%"、"4200mAh"）
   - 具体 ASIN（"B0C..."）
   - 具体品牌名（"DeWalt"）
   - 具体关键词（"rechargeable work light"）
   - 评论原文片段（用引号包裹：「evidence quoted here」）
2. **没有具体证据时，必须返回空字符串 ""**，绝对不允许用"某某情况良好/存在机会"这类无证据叙述凑字数
3. 一句话里只讲一件事，不要堆砌"xx 且 yy 同时 zz"

三、基于**本次输入数据**生成，不得引入 LED 工作灯或任何品类的先验知识（除非输入数据本身就是该品类）
"""


def wrap_system_prompt(role_and_rules: str) -> str:
    """给每个 Analyzer 的 system prompt 末尾追加统一的硬约束。"""
    return role_and_rules.rstrip() + "\n" + EVIDENCE_REQUIREMENT
