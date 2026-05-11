"""BSRAnalyzer._validate_segments 单测。验证 3 类质量警告：
(a) 独有 token 不足、(b) 标题覆盖率不足、(c) 漏切维度候选词。
全部用抽象 fixture，不绑业务领域。"""
from __future__ import annotations

import logging
import pandas as pd
import pytest

from llm.analyzers.bsr_analyzer import BSRAnalyzer
from llm.schemas import MarketInsightPack, ProductSegment


def _make_analyzer():
    """构一个能调 _validate_segments 的 BSRAnalyzer，不需要真 client（_validate_segments 不调 LLM）。"""
    return object.__new__(BSRAnalyzer)


def test_unique_token_insufficient_warns(caplog):
    # 两个 segment 全部 token 重叠 → 独有 token 都是 0
    pack = MarketInsightPack(product_segments=[
        ProductSegment(name="segA", representative_keywords=["common", "shared"]),
        ProductSegment(name="segB", representative_keywords=["common", "shared"]),
    ])
    df = pd.DataFrame({"asin": ["B1", "B2"], "title": ["common shared widget", "shared common gadget"]})
    a = _make_analyzer()
    with caplog.at_level(logging.WARNING):
        a._validate_segments(pack, df)
    msgs = [r.getMessage() for r in caplog.records]
    assert any("独有 token 不足" in m for m in msgs)


def test_low_coverage_warns(caplog):
    # 关键词太狭窄，多数标题命不中 → coverage <80%
    pack = MarketInsightPack(product_segments=[
        ProductSegment(name="segA", representative_keywords=["zzzqqq", "uniqueA"]),
        ProductSegment(name="segB", representative_keywords=["yyywww", "uniqueB"]),
    ])
    df = pd.DataFrame({
        "asin": [f"B{i}" for i in range(10)],
        "title": ["plain widget"] * 10,
    })
    a = _make_analyzer()
    with caplog.at_level(logging.WARNING):
        a._validate_segments(pack, df)
    msgs = [r.getMessage() for r in caplog.records]
    assert any("标题覆盖率" in m for m in msgs)


def test_missing_dimension_candidates_warns(caplog):
    # 标题里有高频词 "premium"，但没在任何 segment keyword 里 → 应被列为候选
    pack = MarketInsightPack(product_segments=[
        ProductSegment(name="segA", representative_keywords=["alpha", "uniqueA"]),
        ProductSegment(name="segB", representative_keywords=["beta", "uniqueB"]),
    ])
    df = pd.DataFrame({
        "asin": [f"B{i}" for i in range(5)],
        "title": [
            "alpha premium widget", "alpha premium gadget", "alpha premium device",
            "beta premium thing", "beta premium item",
        ],
    })
    a = _make_analyzer()
    with caplog.at_level(logging.WARNING):
        a._validate_segments(pack, df)
    msgs = [r.getMessage() for r in caplog.records]
    assert any("漏切维度" in m and "premium" in m for m in msgs)


def test_clean_taxonomy_no_warn(caplog):
    # 完美分类法：每个 segment 独有 token 充足、覆盖率 100%、无漏切
    pack = MarketInsightPack(product_segments=[
        ProductSegment(name="segA", representative_keywords=["alpha", "uniqueA", "fooA", "barA"]),
        ProductSegment(name="segB", representative_keywords=["beta", "uniqueB", "fooB", "barB"]),
    ])
    df = pd.DataFrame({
        "asin": ["B1", "B2", "B3", "B4"],
        "title": ["alpha widget", "uniqueA gadget", "beta widget", "uniqueB gadget"],
    })
    a = _make_analyzer()
    with caplog.at_level(logging.WARNING):
        a._validate_segments(pack, df)
    msgs = [r.getMessage() for r in caplog.records if "[BSR taxonomy]" in r.getMessage()]
    assert msgs == []
