"""classify_with_packs 单测。
全部用品类无关的抽象 fixture（segA/segB/segC + 通用词 + 各自独有词），不绑死任何业务领域。"""
from __future__ import annotations

import pytest

from core.packs_runtime import (
    analyze_with_vision,
    classify_with_packs,
    classify_with_vision,
    Packs,
    MIN_OVERLAP_SCORE,
)
from llm.schemas import MarketInsightPack, ProductSegment


def _default(_title: str) -> str:
    return "未分类"


def _make_packs(segments: list[ProductSegment]) -> Packs:
    pack = MarketInsightPack(product_segments=segments, is_fallback=False)
    return Packs(market=pack)


@pytest.fixture
def three_seg_packs() -> Packs:
    """3 个抽象 segment：
    - segA: 独有词 alpha/uniqueA
    - segB: 独有词 beta/uniqueB
    - segC: 独有词 gamma/uniqueC
    所有 segment 共用 "common" 这个通用词。"""
    return _make_packs([
        ProductSegment(name="segA", representative_keywords=["alpha", "uniqueA", "common"], member_asins=["B001", "B002"]),
        ProductSegment(name="segB", representative_keywords=["beta", "uniqueB", "common"], member_asins=["B003"]),
        ProductSegment(name="segC", representative_keywords=["gamma", "uniqueC", "common"], member_asins=["B004"]),
    ])


def test_step1_asin_match_wins(three_seg_packs):
    # ASIN B001 在 segA 的 member_asins → 直接命中 segA，不论标题
    assert classify_with_packs("nothing matches here common", three_seg_packs, _default, asin="B001") == "segA"


def test_step2_unique_token_match(three_seg_packs):
    # 标题含 "alpha"（仅 segA 独有）→ 直接归 segA
    assert classify_with_packs("Some product with alpha feature", three_seg_packs, _default, asin="B999") == "segA"


def test_step4_weighted_overlap_above_threshold(three_seg_packs):
    # 标题含 "uniqueB" 独有词（权重 1.0），刚好达 MIN_OVERLAP_SCORE → 归 segB
    assert MIN_OVERLAP_SCORE == 1.0
    assert classify_with_packs("widget with uniqueB", three_seg_packs, _default, asin="B999") == "segB"


def test_step4_below_threshold_falls_through(three_seg_packs):
    # 标题只擦"common"通用词（共有词权重 0.2，低于阈值 1.0），且 biggest=segA 也没独有词被命中 →
    # 由于"common"出现在所有 segment 的 keywords 里，biggest segment 与标题 token 的交集非空 → 返回最大段名
    # 这种情况下 step 5 仍会贴最大段（segA）。本断言验证"低于阈值不会乱归 segB/segC"，最大兜底是允许的
    result = classify_with_packs("widget common common", three_seg_packs, _default, asin="B999")
    assert result == "segA"  # biggest（member_asins 最多）


def test_step5_unrelated_returns_unclassified(three_seg_packs):
    # 标题里没有任何 segment 关键词 → biggest 与标题 token 不相交 → 走 default → "未分类"
    assert classify_with_packs("totally unrelated zzzzz", three_seg_packs, _default, asin="B999") == "未分类"


def test_no_packs_falls_back(three_seg_packs):
    # packs=None → 直接走 default
    assert classify_with_packs("alpha beta gamma", None, _default, asin="B001") == "未分类"


def test_empty_segments_falls_back():
    # segments=[] → 走 default
    empty = _make_packs([])
    assert classify_with_packs("alpha", empty, _default, asin="B001") == "未分类"


# ---------- classify_with_vision ----------

class _MockVisionClient:
    """模拟 LLMClient：可控返回值 / 可控 supports_vision。"""
    def __init__(self, supports=True, ret=None):
        self._supports = supports
        self._ret = ret
        self.calls = 0

    def supports_vision(self) -> bool:
        return self._supports

    def chat_multimodal_json(self, **kwargs):
        self.calls += 1
        return self._ret


def test_vision_unsupported_returns_none(three_seg_packs):
    client = _MockVisionClient(supports=False)
    assert classify_with_vision("title", "https://x/img.jpg", "B999", three_seg_packs, client) is None
    assert client.calls == 0


def test_vision_no_image_url(three_seg_packs):
    client = _MockVisionClient(supports=True, ret=None)
    assert classify_with_vision("title", None, "B999", three_seg_packs, client) is None
    assert client.calls == 0


def test_vision_invalid_url_skipped(three_seg_packs):
    client = _MockVisionClient(supports=True, ret=None)
    assert classify_with_vision("title", "ftp://x/img.jpg", "B999", three_seg_packs, client) is None
    assert client.calls == 0


def test_vision_unknown_returns_none(three_seg_packs):
    from llm.schemas import VisionClassifyResult
    client = _MockVisionClient(supports=True, ret=VisionClassifyResult(segment_name="unknown"))
    assert classify_with_vision("t", "https://x/img.jpg", "B1", three_seg_packs, client) is None
    assert client.calls == 1


def test_vision_invalid_segment_returns_none(three_seg_packs):
    from llm.schemas import VisionClassifyResult
    client = _MockVisionClient(supports=True, ret=VisionClassifyResult(segment_name="segZ"))
    assert classify_with_vision("t", "https://x/img.jpg", "B1", three_seg_packs, client) is None


def test_vision_valid_match_returns_segment(three_seg_packs):
    from llm.schemas import VisionClassifyResult
    client = _MockVisionClient(supports=True, ret=VisionClassifyResult(segment_name="segB"))
    assert classify_with_vision("t", "https://x/img.jpg", "B1", three_seg_packs, client) == "segB"


def test_analyze_with_vision_preserves_visual_labels(three_seg_packs):
    from llm.schemas import VisionClassifyResult
    client = _MockVisionClient(
        supports=True,
        ret=VisionClassifyResult(
            segment_name="unknown",
            material_label="metal",
        ),
    )
    result = analyze_with_vision("t", "https://x/img.jpg", "B1", three_seg_packs, client)
    assert result is not None
    assert result.segment_name == "unknown"
    assert result.material_label == "metal"
