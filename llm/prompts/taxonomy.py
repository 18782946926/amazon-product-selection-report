"""TaxonomyAggregator prompt v7-merged：单次 LLM 完成"切桶 + 分配 ASIN"。

合并自 v4-decoupled (只切桶) + BucketAssigner (单独分配)。
设计要点：
- 视觉描述本身已是最准的输入信号（per-ASIN 由视觉 LLM 看图+标题+卖点产出），无需中间层加工
- LLM 用语义理解一次性产出 4-8 个桶 + 每个桶的成员产品序号
- 用 idx (1-based 整数) 替代 ASIN 字符串作为 members 输出，控制输出长度避免长 JSON 漂移
- prompt 强调"每序号必须且只能进一桶"+ 切完自检 + 禁止杂物桶
"""
from __future__ import annotations

_PROMPT_VERSION = "tx-v7-merged-2026-05-08"


def build_messages(visual_descriptions: list[dict], category_hint: str = "") -> list[dict]:
    """构建聚合 LLM 的 messages（v7-merged：切桶 + 分配 ASIN 一次完成）。

    visual_descriptions: 每条形如
        {'idx': int, 'asin': str, 'product_type_free': str, 'material_label': str, 'form_label': str}
    category_hint: 品类提示（仅作辅助，不强制）
    """
    n = len(visual_descriptions)
    rows_text = "\n".join(
        f"{d.get('idx', i+1)}. {d.get('product_type_free', '') or '(空描述)'}"
        f"（材质: {d.get('material_label', '') or '?'}, 形态: {d.get('form_label', '') or '?'}）"
        for i, d in enumerate(visual_descriptions)
    )

    text = (
        f"你是亚马逊产品分类师。下面是 {n} 个产品的「视觉描述」——每条由视觉 LLM 已经基于"
        f"图+标题+卖点提取（不是凭标题猜的，是看图认的，准确度高）。\n"
        f"品类范围：{category_hint or '未指定（你自己从描述里识别）'}\n"
        "本任务跨品类通用——产品可能是任何类型（电子、家居、玩具、工具、配件、户外、服装、美妆等），"
        "请按下方通用规则聚合，不要预设品类、不要套用任何固定品类的命名模板。\n\n"
        "你的任务（单次完成）：\n"
        "1) 把这 {n} 个产品聚合成 **4-8 个**互不重叠的细分 segments\n"
        "2) 给每个 segment 起名 + 写描述 + 列 keywords + 标主导材质/形态\n"
        "3) 直接列出每个 segment 的成员（用产品序号）\n\n"
        "字段要求：\n\n"
        "**name**（8-15 个汉字）\n"
        "  反映该桶产品的核心特征组合。通用结构：[材质或核心规格] + [形态/功能特征] + [基础品类名]。\n"
        "  名字应来自当前数据观察到的特征，不要套用其他品类的固定模板词。\n\n"
        "**description**（1-2 句）\n"
        "  说明判别特征：什么材质 + 什么形态 + 主要功能 + 与相邻桶的区别。\n\n"
        "**representative_keywords**（8-15 个，多粒度）\n"
        "  - 必须给 2-4 字精简词（如「猪形」「ATM」「字母」「立方体」「磁吸」），保证变体描述也能命中\n"
        "  - 可选给 4-6 字组合短语（如「电子 ATM」「字母造型」），提供更精准的命中\n"
        "  - 高判别力——能区分本桶 vs 其他桶；通用词（产品/商品/用品）不要单独作 keyword\n"
        "  - 来自实际数据特征词，不要凭空想\n\n"
        "**material_attribute**（主导材质）\n"
        "  优先从常见枚举里选：塑料/陶瓷/亚克力/金属/木质/玻璃/布艺/纸质/橡胶/硅胶/皮革；\n"
        "  特殊品类可填具体材质名（2-6 字，如「碳纤维」「EVA 泡沫」）；混合无主导留空。\n\n"
        "**form_attribute**（主导形态/形式）\n"
        "  通用例（跨品类参考）：'ATM式' / '猪形' / '立方体' / '字母' / '圆筒' / '磁吸式' / '手持式' /\n"
        "  '挂壁式' / '挑战盒' / '礼盒' / '球形' / '多格' / '夹式'。\n"
        "  2-6 字精简形态词，**不要写品类基础名**（如「存钱罐」「灯」「适配器」）；\n"
        "  不以形态切分则留空。\n\n"
        f"**members**（产品序号列表，从 1 到 {n} 之间的整数）\n"
        "  列出归属本 segment 的所有产品序号。**用整数序号，不要写 ASIN 字符串**。\n\n"
        "**严格约束（违反会被视为不合格输出）**：\n\n"
        f"1) **互斥 + 全覆盖**：所有 segment 的 members 加起来去重后必须等于 {{1, 2, 3, ..., {n}}} "
        f"全集（{n} 个序号一个不漏、一个不重）。\n\n"
        "2) **禁止杂物桶**：不要造「其他 / 通用款 / 杂物桶」。如果某产品实在难归类，"
        "归到与它最相近的 segment（哪怕该 segment 名字略宽泛）。\n\n"
        "3) **语义匹配自检**：切完后逐个检查每个 segment 的 members——\n"
        "   每个产品的视觉描述（材质 + 形态 + 自由描述）都应跟该 segment 的 name/keywords 语义吻合。\n"
        "   不吻合的产品要重新分桶。\n\n"
        "4) **互斥边界**：相邻 segments 之间的 representative_keywords 不要有 ≥3 个重叠（否则就是切重了，要合并）。\n\n"
        f"产品视觉描述列表（共 {n} 条）：\n{rows_text}\n\n"
        "只输出 JSON 对象：\n"
        "{\n"
        '  "product_segments": [\n'
        '    {\n'
        '      "name": "...",\n'
        '      "description": "...",\n'
        '      "representative_keywords": ["...", "..."],\n'
        '      "material_attribute": "...",\n'
        '      "form_attribute": "...",\n'
        '      "members": [1, 5, 12, 23]\n'
        "    },\n"
        "    ...\n"
        "  ]\n"
        "}"
    )
    return [{"role": "user", "content": text}]
