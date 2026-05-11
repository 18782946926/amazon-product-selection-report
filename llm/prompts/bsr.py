"""BSR Analyzer 的 prompt：把 BSR TOP100 行数据交给 LLM 做市场结构分析。"""
from __future__ import annotations

import json

from llm.prompts.common import wrap_system_prompt

# Prompt 版本号：写入 cache_key，prompt 升级会自动失效旧缓存
# v3-2026-04-30：Phase 3 抽象化（删 LED 反例、改"如LED那套"为通用规则、扩展 category_display_name 多品类示例）
# v4-2026-04-30-material-decoupled：segment 名字与材质解耦，材质改填新字段 material_attribute（视觉 material_label 是 ground truth，
#   名字嵌材质会与视觉打架——参见行 17/23 案例：木质 segment 装着塑料品、亚克力 segment 装着金属品）
# v5-2026-04-30-material-strict：把"segment 名字不嵌材质"提升为顶层硬约束规则 11，补三种规避模式反例
#   （前缀/中段/括号）跨多个品类示例 + 新增输出前材质洁净度自检规则 14；
#   解决 v4 下 LLM 仍偶发把材质塞进 segment.name 的各种形式（括号附注、中段嵌入、前缀嵌入），
#   规则与示例完全品类无关，对 LED/充电器/工具/玩具等全品类同样生效
# v6-2026-04-30-coverage-strict：新增规则 15 强制 member_asins 全量覆盖自检——
#   解决 v5 下 LLM 偶尔只装 62/100 个 ASIN，剩 38 个走 classify_with_packs 第 5 步降级
#   兜底归到"member_asins 最多 segment（电子ATM式存钱罐）"导致塑料款被错分的问题
# v7-2026-04-30-segment-purity：尝试加 rule 15 严格兜底名 + rule 16 ≤35% 禁混类，但 LLM
#   反应过度 —— 74% 全丢进「其他/通用款」，segment 切分失效。已回退到 v6 状态。
# v7-2026-05-06-theme-material-decouple：与上面的旧 v7 是**完全不同的改动方向**（不是同一次尝试，命名巧合）。
#   背景：存钱罐场景（Kids' Money Banks）跑出现"木质字母/动物造型存钱罐"违规 segment —— 把"木质"
#   材质和"动物造型"主题绑死，导致塑胶动物造型款（如 B0C651CM46 恐龙塑胶 piggy bank）无家可归，
#   走 classify_with_packs step 5 兜底归到最大 segment「电子ATM式存钱罐」。
#   v6 → v7 改动：
#     (a) 规则 10 维度清单加第 6 个维度"造型 / 题材主题" + 显式声明"主题维度与材质维度正交"
#     (b) 规则 11 禁词范围从"材质词"扩到"任何外形属性词（材质 / 工艺 / 颜色 / 尺寸）"
#         + 新增"维度纠缠型"违规模式（单 segment.name 混入两个独立维度必须拆）
#     (c) 新增规则 16 主题 / 材质交叉自检（输出前最后一步），含工业品类跳过本维度的适配规则
# v8-2026-05-06-allow-material-in-name：Sheet 3 N 列已改用 visual_product_type（视觉 LLM 自由描述
#   每 ASIN），与 BSR segment.name 解耦。所以 segment.name 不再需要"洁净"——下游 Sheet 4/5/6/10
#   反而需要 segment 名带材质（如「木质字母/主题存钱罐」），让业务人员一眼看出细分。
#   v7 → v8 改动：
#     (a) 删除规则 11「禁外形属性词入名」
#     (b) 删除规则 14「材质洁净度自检」
#     (c) 规则 10 移除「主题与材质维度正交」硬约束段落（与"允许材质入名"冲突）
#     (d) 同步删除 bsr_analyzer.py:_scrub_material_from_segment_names 函数
_PROMPT_VERSION = "v8-2026-05-06-allow-material-in-name"

_RULES = """你是亚马逊选品资深分析师。你将拿到一份 BSR TOP100 数据（任意品类），请输出**严格符合 JSON schema** 的市场洞察。

核心原则：
1. 不要预设品类。根据标题特征动态识别细分（如：手持式/挂壁式/便携式/工业级/户外用），细分数量 4-8 个
2. **member_asins 必须全量覆盖**：输入数据里**每一个** ASIN 都必须归属到某一个 segment，所有 segments 的 member_asins 加起来 = 输入 ASIN 总数（无重复、无遗漏）。不要只列"代表性的 3-5 个 ASIN"，必须把每条 ASIN 都明确归类。归类时优先看标题里**该细分独有的强特征词**（材质 / 功能 / 形态 / 场景 / 授权等强语义 token），不要被本品类通用名词（产品类目名、"new"/"large"/"mini" 等装饰词）干扰
3. **representative_keywords 必须是该细分独有或强特征**的词，不要放本品类通用名词（即所有细分都会出现在标题里的产品类目名 / 材料通称 / 装饰词）。每个 segment 至少要有 2 个 token 是其他 segment 的 representative_keywords 列表里都不出现的
4. 价格带划分根据本数据的实际价格分布动态决定，不要套用任何固定区间（不同品类的合理价格分档完全不同，必须基于本次输入数据的 P25/中位/P75/P90 自行划档）
5. 中国卖家判断标准：seller_country 含 "CN"/"China" 或卖家名为汉语拼音
6. 新品窗口：上架日期在近 12 个月内且 BSR 排名 ≤50 视为"活下来的新品"
7. brand_concentration_reading：必须包含具体 CR5 百分数或 TOP3 品牌名占比（格式示意："CR5=X%，<品牌1> + <品牌2> + <品牌3> 合计占 TOP100 Y%"——品牌名按本次输入数据真实品牌填，不要照抄示例品牌）；若无法算出具体数则返回空字符串
8. category_display_name 给一个简洁的中文品类名（按本次输入数据动态推断；不同品类示例："LED工作灯" / "便携充气机" / "压力洗车枪" / "儿童存钱罐" / "蓝牙音箱" 等），用于报告标题
9. playbook / description 字段要么包含具体数字/ASIN/品牌，要么返回空字符串，禁止写"竞争激烈但有机会"这类空话
10. **维度分化自检**（适用于所有品类）：在输出 segments 前，扫描数据自检以下"通用维度"是否存在明显分化：
    - 目标人群（成人 vs 儿童 / 男 vs 女 / 专业 vs 入门 / 商用 vs 家用 / 工业 vs 居家）
    - 使用场景（户外 vs 室内 / 移动 vs 固定 / 单次 vs 长期）
    - 材质工艺（如 金属 / 塑料 / 木质 / 陶瓷 / 玻璃 / 不锈钢 等；具体术语贴合本品类）
    - 品牌授权（IP / 联名 / 知名动漫游戏体育卡通形象等 —— 强调商标 / 联名权）
    - 形态结构（手持 / 挂壁 / 桌面 / 便携 / 嵌入）
    - **造型 / 题材主题**（按"产品外形是否以某具体形象 / 角色 / 物件为主体"切分。**具体取值由你按当次数据识别** —— 抽象类型示例：基础几何形 / 拟物造型 / 拟人造型 / 角色形象 / 艺术风格。不同品类下识别出的取值完全不同：玩具品类多见动物 / 卡通主题，工业品类多见纯功能造型，家居装饰多见艺术 / 复古风。**与"品牌授权"维度的区别**：造型主题不要求授权 —— 凡是产品外形以某形象 / 物件为主体都算（无论是否有 IP 授权）。**工业品类如本维度所有标题都属"纯功能造型"一档，跳过即可，不要硬切**）
    任一维度上每一侧 ≥3 条标题时，**必须**切出独立 segment，禁止把弱势子类塞进主流 segment。判别原则：若两侧的目标人群 / 场景 / 材质 / 授权 / 造型主题属性显著不同，应视为不同细分。

    **segment 命名规则**：让差异维度的差异在名字里直接可读（例如形态维度命名为"挂壁式XX"vs"桌面式XX"，材质维度命名为"木质XX"vs"塑料XX"）。segment.name 可以包含材质 / 工艺 / 颜色等外形属性词，让业务人员一眼看出该细分的核心特征。同时把材质信息也填到独立字段 `material_attribute`（用于下游视觉一致性校验）。
11. **representative_keywords 独有度自检**：输出前对照检查，每个 segment 的 representative_keywords 至少要有 2 个 token 是该 segment 独有（即该 token 在其他任何 segment 的 representative_keywords 列表里都不出现）。若做不到，请重新调整切分粒度
12. **覆盖度自检**：输出前对每条输入 ASIN 的标题，至少有 1 个该 ASIN 所属 segment 的 representative_keywords token 出现在标题里。若覆盖率 <80%，请扩充 representative_keywords 或重切 segment
13. **【硬约束】member_asins 全覆盖自检（输出前最关键一步）**：
    - 输出前数一遍所有 segments 的 member_asins 合并去重总数
    - 该总数**必须严格等于**输入 ASIN 总数 {n}（无重复、无遗漏）
    - 若数量不等（不论是漏装、重复装、误删），**必须**新建一个兜底 segment（如「其他/通用款」，material_attribute=""，representative_keywords=[]，description="未明确归类的 ASIN，含基础款 / 低特征产品"）承载剩余 ASIN
    - **绝不允许**让任何 ASIN 不在任何 segment 的 member_asins 里——否则下游分类逻辑会误兜底到"member_asins 最多的 segment"，造成基础款被错分到强功能 segment 的灾难性结果（已观察到普通塑料存钱罐被错分到电子ATM式存钱罐的案例）
    - 数量自检失败时宁可多输出一个「其他/通用款」segment，也绝不能漏装
"""

SYSTEM_PROMPT = wrap_system_prompt(_RULES)

USER_TEMPLATE = """以下是 BSR TOP{n} 的产品数据（JSON 数组）：

```json
{bsr_json}
```

补充统计：
- 价格分布：{price_distribution}
- 中国卖家占比：{china_pct:.1%}
- TOP10 品牌：{top_brands}

请输出 JSON，严格符合以下结构（字段不可省）：

{{
  "category_display_name": "<中文品类名>",
  "product_segments": [
    {{"name": "<细分1，可包含材质/形态/主题等核心特征词，让业务一眼看出该细分定位>", "description": "<特征>", "member_asins": ["B0...", ...每条输入 ASIN 必须归属到某个 segment], "representative_keywords": ["该细分独有的特征词，不放通用词"], "material_attribute": "<若按材质维度切分则填该材质（如 塑料/金属/木质/亚克力 等，与下游 vision material_label 用同一组术语）；其他维度切分留空字符串>"}},
    ...4-8 个
  ],
  "positioning_tiers": [
    {{"tier": "低端|中端|高端", "price_range_desc": "...", "representative_brands": ["..."], "playbook": "..."}}
  ],
  "price_ladder": [
    {{"band": "<$25", "description": "...", "competition_intensity": "高", "profit_room": "低", "representative_asins": ["..."]}},
    ...按本数据价格分布动态
  ],
  "china_seller_patterns": {{
    "concentrated_segments": ["..."],
    "concentrated_bands": ["..."],
    "typical_strategies": "..."
  }},
  "new_entry_windows": [
    {{"segment": "...", "evidence_asins": ["..."], "success_rate_note": "..."}}
  ],
  "brand_concentration_reading": "<一句话>"
}}

只输出 JSON，无任何额外解释或 markdown 围栏。"""


def build_messages(bsr_rows: list[dict], price_distribution: dict, china_pct: float, top_brands: list[str]) -> list[dict]:
    bsr_json = json.dumps(bsr_rows, ensure_ascii=False, indent=2)
    user = USER_TEMPLATE.format(
        n=len(bsr_rows),
        bsr_json=bsr_json,
        price_distribution=json.dumps(price_distribution, ensure_ascii=False),
        china_pct=china_pct,
        top_brands=", ".join(top_brands[:10]),
    )
    return [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": user},
    ]
