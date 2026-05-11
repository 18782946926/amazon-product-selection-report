"""Reviews Analyzer 的 prompt：批量评论原文 → 痛点/卖点聚类。"""
from __future__ import annotations

import json

from llm.prompts.common import wrap_system_prompt

_RULES = """你是亚马逊用户评论分析师。你将拿到一批商品评论原文（多 ASIN 混合），请输出**严格符合 JSON schema** 的 Voice of Customer 洞察。

核心原则：
1. 不要预设品类。根据评论内容动态聚类痛点和卖点，类别数量 4-10 个
2. 每个 cluster 的 raw_quotes 必须**直接引用评论原文片段**，每簇**最多 2 条**，每条**不超过 100 字符**，不得改写或编造
2a. **raw_quotes 内禁止出现未转义的英文双引号**：若原文含 `"` 或弯引号，请统一替换为单引号 `'`（避免 JSON 嵌套转义错误导致解析失败）
3. severity 判断：抱怨退货/质量崩坏 = 高，使用不便/说明不清 = 中，小瑕疵 = 低
4. frequency_pct 是该类痛点在所有差评（1-2 星）中的占比估算，必须基于实际匹配评论数/总差评数
5. emerging_complaints 仅当存在"近期评论才反复出现、旧评论没有"的痛点时才填，否则空数组
6. unmet_needs 是用户明确表达"希望能 X 但市面没有"的诉求，evidence_quotes 必填原文
7. deal_breakers 限 3-5 条最致命的退货/换货原因，quote 必填原文
8. description 字段需要**具体归纳本簇评论的共性**，不要写"质量问题""使用体验"这类跨品类空话——
   写 "多条评论反映使用 X 次后 Y 坏掉" 这类可追溯的归纳。无法归纳时留空字符串
9. **keywords 字段**：从该 cluster 的 raw_quotes 中提炼 **5-8 个真实出现**的代表性词组（中英混合均可，每个长度 ≥ 2 字符），用于后续按 ASIN 评论文本做正则匹配。
   - **禁止编造未在评论中出现的词**
   - 优先选高频、有区分度的词组（如 "battery dies"、"flimsy plastic"、"漏液"、"投币卡顿"）
   - 避免无区分度的通用词（如 "bad"、"good"、"the"、"product"）
   - 若评论是英文，关键词可用英文小写；若是中文，用中文原词
"""

SYSTEM_PROMPT = wrap_system_prompt(_RULES)

USER_TEMPLATE = """以下是评论数据（JSON 数组，含 asin/rating/title/content/date）：

```json
{reviews_json}
```

样本概况：
- 总评论数：{total_count}
- 1-2 星差评数：{neg_count}
- 4-5 星好评数：{pos_count}
- 涉及 ASIN 数：{asin_count}

请输出 JSON：

{{
  "pain_clusters": [
    {{"name": "<中文痛点名>", "description": "<描述>", "raw_quotes": ["原文1", "原文2", ...], "affected_asins": ["B0..."], "severity": "高|中|低", "frequency_pct": 25.0, "keywords": ["<词组1>", "<词组2>", "..."]}},
    ...4-10 个（每个 cluster 必须含 5-8 个 keywords，从 raw_quotes 中真实提炼）
  ],
  "praise_clusters": [
    {{"name": "<好评卖点>", "description": "...", "raw_quotes": ["q1", "q2"], "affected_asins": ["..."], "frequency_pct": 20.0}},
    ...3-5 个（根据好评内容聚类；若 4-5★ 评论数 >= 20 条必须输出 >= 3 条；frequency_pct 为该卖点在所有好评中的占比估算；raw_quotes 最多 2 条，每条 ≤80 字符）
  ],
  "emerging_complaints": [
    {{"complaint": "...", "first_seen_recent": true, "evidence_quotes": ["..."]}}
  ],
  "unmet_needs": [
    {{"need": "...", "evidence_quotes": ["..."]}}
  ],
  "deal_breakers": [
    {{"reason": "...", "quote": "...", "return_related": true}}
  ],
  "quality_signal_by_asin": [
    {{"asin": "B0...", "positive_pct": 70.0, "negative_pct": 30.0, "top_pain": "...", "top_praise": "..."}}
  ]
}}

只输出 JSON，无 markdown 围栏。raw_quotes 必须是原文片段，禁止编造。"""


def build_messages(reviews: list[dict], stats: dict) -> list[dict]:
    reviews_json = json.dumps(reviews, ensure_ascii=False, indent=1)
    user = USER_TEMPLATE.format(
        reviews_json=reviews_json,
        total_count=stats.get("total", len(reviews)),
        neg_count=stats.get("neg", 0),
        pos_count=stats.get("pos", 0),
        asin_count=stats.get("asin_count", 0),
    )
    return [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": user},
    ]


_MERGE_RULES = """你是评论聚类合并师。本次合并的品类 = `{category_hint}`。你拿到多个分批分析的 VOC Pack，请合并为一个 Pack：
1. 同名/同义的 pain_clusters / praise_clusters 合并，raw_quotes 去重保留 5 条以内
2. frequency_pct 用加权平均
2a. **keywords 字段（仅 pain_clusters）**：合并同名 cluster 时取并集去重，最终保留 5-8 个最具代表性的关键词；保持原词形态，禁止改写或新增评论中未出现的词
3. 输出格式与单批 VOC Pack 一致
4. 合并后 description 字段保留具有最具体数字/证据的那一条，删除"质量问题""使用体验"这类空话
5. **品类守门**：如果某条 pain/praise cluster 的 name 语义明显**不属于**本品类 `{category_hint}`，**直接丢弃**，不要升格为全局 pain。判别原则：
   - 看 pain/praise 描述的**核心物理属性 / 配件名 / 工艺词**——是否可能出现在 `{category_hint}` 本品类的真实产品上？若引用的物理量在本品类典型规格里完全找不到对应维度，多半是跨品类 ASIN 污染（典型场景：评论批次里混入了非本品类的合体产品 / 配件类商品 / 同卖家其他品类商品的评论）
   - **多品类对比示例**（仅展示判别 pattern，不是穷举规则）：本品类是充电器但出现"亮度不足/反光杯"→丢弃；本品类是 LED 但出现"mAh 虚标"→丢弃；本品类是充气机但出现"电池容量虚标"→丢弃；本品类是存钱罐但出现"流明不够"→丢弃。其余品类按同样逻辑判别。
   - 例外：履约/包装/物流/外观 QC 类（"包装简陋/外观磕碰/配件缺失/说明书不清/收到二手翻新"）属于跨品类共性问题，**不算跨品类污染**，必须保留。
   少数批次因 ASIN 跨类（如 USB 充电 + LED 合体产品）产生的跨品类聚类，**合并时必须剔除**，不要因为它在多批次出现就升格。
6. 宁可 pain_clusters 少几条，也不要输出一条与本品类不符的 pain；合并后 pain_clusters 不得少于 3 条，praise_clusters 不得少于 3 条（praise 地板线仅在输入批次累计至少有 1 条 praise 时生效；输入全无好评时可为 0）。数量不足时由 LLM 从剩余批次中归并出更细的同品类 cluster 补位
7. 品类守门规则对 pain 和 praise 同等严格：不属于本品类的一律丢弃；但**禁止**因"品类守门"把合格的本品类 praise 全部清空——如果疑似合格的 praise 都被你误判跨品类丢弃，应回查是否过度严格，至少保留 3 条与本品类 `{category_hint}` 相关的 praise
"""


def _build_merge_system(category_hint: str) -> str:
    return wrap_system_prompt(_MERGE_RULES.replace("{category_hint}", category_hint or "未知品类"))


def build_merge_messages(packs: list[dict], category_hint: str = "未知品类") -> list[dict]:
    user = "请合并以下 {} 批 VOC Pack（本次品类 = `{}`）：\n```json\n{}\n```\n只输出合并后的 JSON。".format(
        len(packs), category_hint or "未知品类", json.dumps(packs, ensure_ascii=False, indent=1)
    )
    return [
        {"role": "system", "content": _build_merge_system(category_hint)},
        {"role": "user", "content": user},
    ]
