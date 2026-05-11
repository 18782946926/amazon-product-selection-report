"""评论分批工具：按字符预算把评论列表切成多批。

简化的 token 估算：1 token ≈ 3.5 字符（中英混合保守估计）。
"""
from __future__ import annotations

from typing import Iterable

DEFAULT_TOKEN_BUDGET = 20000  # 单批 LLM 输入 token 预算（不含 prompt）—— DeepSeek 上下文 64K，留足余量
CHARS_PER_TOKEN = 3.5


def chars_budget(token_budget: int = DEFAULT_TOKEN_BUDGET) -> int:
    return int(token_budget * CHARS_PER_TOKEN)


def batch_reviews(reviews: list[dict], *, token_budget: int = DEFAULT_TOKEN_BUDGET,
                  max_per_batch: int = 300) -> list[list[dict]]:
    """按字符数预算切批。每条 review 估算字符 = title + content 长度。"""
    budget = chars_budget(token_budget)
    batches: list[list[dict]] = []
    cur: list[dict] = []
    cur_chars = 0
    for r in reviews:
        size = len(str(r.get("title", ""))) + len(str(r.get("content", ""))) + 50
        if cur and (cur_chars + size > budget or len(cur) >= max_per_batch):
            batches.append(cur)
            cur, cur_chars = [], 0
        cur.append(r)
        cur_chars += size
    if cur:
        batches.append(cur)
    return batches


def select_for_analysis(reviews: list[dict], *, neg_full: bool = True,
                        pos_sample_pct: float = 0.7) -> list[dict]:
    """从所有评论中筛选送 LLM 的样本：1-2 星全量，4-5 星按 70% 采样。
    采样率从原先 30% 提到 70%：praise_clusters 的 frequency_pct 必须基于足够样本才可信；
    原 30% 意味着 LLM 只看到 30% 的正面证据，导致卖点聚类偏向稀有特性。"""
    neg = [r for r in reviews if _rating_value(r) <= 2]
    pos = [r for r in reviews if _rating_value(r) >= 4]
    mid = [r for r in reviews if _rating_value(r) == 3]

    selected: list[dict] = []
    if neg_full:
        selected.extend(neg)
    if pos and pos_sample_pct > 0:
        step = max(1, int(1 / pos_sample_pct))
        selected.extend(pos[::step])
    selected.extend(mid[:30])
    return selected


def _rating_value(r: dict) -> float:
    raw = r.get("rating", 0)
    try:
        return float(raw)
    except (TypeError, ValueError):
        return 0.0
