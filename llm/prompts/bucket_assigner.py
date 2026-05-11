"""BucketAssigner prompt：把 N 个 ASIN 的视觉描述映射到聚合 LLM 已切好的 4-8 个桶里。

设计要点：
- 输入：桶定义（name + description + keywords + material + form）+ ASIN 视觉描述
- 输出：每个 ASIN 归哪个桶（按 ASIN 索引输出，紧凑结构避免长 JSON 漂移）
- LLM 用语义理解做匹配，比代码 keyword 子串匹配更鲁棒

跨品类通用——产品可能是任何类型，prompt 不预设品类。
"""
from __future__ import annotations

_PROMPT_VERSION = "ba-v1-2026-05-08"


def build_messages(bucket_defs: list[dict], visual_items: list[dict]) -> list[dict]:
    """构建 BucketAssigner 的 messages。

    bucket_defs: [{'idx': int, 'name': str, 'description': str, 'keywords': list, 'material': str, 'form': str}]
    visual_items: [{'idx': int, 'asin': str, 'product_type_free': str, 'material': str, 'form': str}]
    """
    n = len(visual_items)
    m = len(bucket_defs)

    bucket_text = "\n".join(
        f"桶{b['idx']}：{b['name']}\n"
        f"  描述：{b.get('description', '') or '(无)'}\n"
        f"  关键词：{', '.join(b.get('keywords', []) or [])[:200]}\n"
        f"  材质：{b.get('material', '') or '(无)'}\n"
        f"  形态：{b.get('form', '') or '(无)'}"
        for b in bucket_defs
    )

    items_text = "\n".join(
        f"{v['idx']}. {v.get('product_type_free', '') or '(空)'}"
        f"（材质: {v.get('material', '') or '?'}，形态: {v.get('form', '') or '?'}）"
        for v in visual_items
    )

    text = (
        f"你是亚马逊产品分桶员。下面有 {m} 个细分桶（已由聚合 LLM 切好），"
        f"和 {n} 个产品的视觉描述。请把每个产品归到最合适的一个桶。\n\n"
        "本任务跨品类通用——产品可能是任何类型（电子、家居、玩具、工具、配件、户外、服装、美妆等），"
        "请仅按下方桶定义和产品描述判别，不要预设品类。\n\n"
        "判桶原则（按优先级）：\n"
        "1) **材质相等**：产品材质等于桶的材质 → 强信号（同材质优先）\n"
        "2) **形态相等或相关**：产品形态与桶形态语义相同/相关（如「猪形」属于「动物形」）→ 强信号\n"
        "3) **关键词包含**：产品视觉描述里出现桶的关键词 → 中等信号\n"
        "4) **整体语义贴合**：综合产品描述与桶名/桶描述的语义相似度判断 → 兜底\n\n"
        "**重要约束**：\n"
        "- 每个产品只能归一个桶\n"
        "- 必须从给定的桶里选；如果产品明显不属于任何桶（描述与所有桶语义都对不上）→ 输出 0 表示「其他」\n"
        "- 不要硬塞——「其他」是合法选择，让杂项产品落到「其他」比错塞到某桶更负责\n"
        "- 同样的产品同样的桶定义应得出同样答案（确定性）\n\n"
        f"=== {m} 个桶定义 ===\n{bucket_text}\n\n"
        f"=== {n} 个产品视觉描述 ===\n{items_text}\n\n"
        "输出格式（紧凑、按产品序号顺序）：\n"
        "对每个产品输出一行 `<产品序号>:<桶序号>`（桶序号 0 表示「其他」），共 N 行。\n"
        "示例：\n"
        "1:3\n"
        "2:7\n"
        "3:0\n"
        "...\n\n"
        "**只输出 JSON**，结构如下：\n"
        "{\n"
        '  "assignments": [\n'
        '    {"item_idx": 1, "bucket_idx": 3},\n'
        '    {"item_idx": 2, "bucket_idx": 7},\n'
        '    {"item_idx": 3, "bucket_idx": 0},\n'
        "    ...\n"
        "  ]\n"
        "}\n"
        f"必须包含全部 {n} 条记录。"
    )
    return [{"role": "user", "content": text}]
