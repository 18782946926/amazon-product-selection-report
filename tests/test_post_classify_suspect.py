"""BSRAnalyzer._post_classify suspect 模式单测：
(a) suspect 模式正确识别"标题 token 与当前所归 segment 的 keywords token 集不相交"的 ASIN
(b) vision 改写时正确从原 segment 移除并加到新 segment

全部用品类无关的抽象数据（segA/segB/segC + uniqueA/uniqueB 等抽象 token）。
"""
from __future__ import annotations

import pandas as pd
import pytest

from llm.analyzers.bsr_analyzer import BSRAnalyzer
from llm.schemas import MarketInsightPack, ProductSegment


class _MockVisionProvider:
    """模拟支持视觉的 provider。mapping: {keyword_in_user_text: segment_name}。"""
    name = "mock_vision"

    def __init__(self, mapping=None):
        self._mapping = mapping or {}

    def supports_vision(self) -> bool:
        return True

    def chat_multimodal(self, messages, **kwargs):
        text_parts = []
        for m in messages:
            content = m.get("content", "")
            if isinstance(content, list):
                for part in content:
                    if isinstance(part, dict) and part.get("type") == "text":
                        text_parts.append(part.get("text", ""))
        full_text = " ".join(text_parts)
        for key, seg_name in self._mapping.items():
            if key in full_text:
                return f'{{"segment_name": "{seg_name}", "confidence": "high", "reason": "mock"}}'
        return '{"segment_name": "unknown", "confidence": "low", "reason": "mock"}'


def test_suspect_mode_identifies_token_mismatch(monkeypatch):
    """ASIN B_SWAP 标题里全是 segB 的 token（uniqueB / featB），
    却被 LLM 错归到 segA（keywords 是 uniqueA / featA）→ suspect 应识别为可疑并触发 vision，改写到 segB。"""
    monkeypatch.setenv("VISION_AUDIT_MODE", "suspect")

    pack = MarketInsightPack(product_segments=[
        ProductSegment(name="segA",
                       representative_keywords=["uniqueA", "featA"],
                       member_asins=["B_SWAP"]),         # ← 错归到这里
        ProductSegment(name="segB",
                       representative_keywords=["uniqueB", "featB"],
                       member_asins=["B_OK"]),
    ], is_fallback=False)

    df = pd.DataFrame({
        "asin": ["B_SWAP", "B_OK"],
        "title": [
            "Sample item with uniqueB and featB tokens only",   # ← 标题全是 segB token
            "Sample item with uniqueB content here",
        ],
        "Main Image": [
            "https://example.com/a.jpg",
            "https://example.com/b.jpg",
        ],
    })

    mock_vision = _MockVisionProvider({"B_SWAP": "segB"})

    import config.llm_config as cfg
    monkeypatch.setattr(cfg, "make_vision_provider", lambda: mock_vision)

    a = object.__new__(BSRAnalyzer)
    a.client = None
    a._post_classify(pack, df, {"category_id": "test"})

    seg_a = next(s for s in pack.product_segments if s.name == "segA")
    seg_b = next(s for s in pack.product_segments if s.name == "segB")
    assert "B_SWAP" not in seg_a.member_asins, "应从原 segment 移除"
    assert "B_SWAP" in seg_b.member_asins, "应加入新 segment"


def test_low_conf_mode_skips_misclassified(monkeypatch):
    """low_conf 模式只 audit 未分类的 ASIN，不会救 LLM 错归的（即使标题 vs keywords 不一致）。"""
    monkeypatch.setenv("VISION_AUDIT_MODE", "low_conf")

    pack = MarketInsightPack(product_segments=[
        ProductSegment(name="segA",
                       representative_keywords=["uniqueA", "featA"],
                       member_asins=["B_SWAP"]),
        ProductSegment(name="segB",
                       representative_keywords=["uniqueB", "featB"],
                       member_asins=["B_OK"]),
    ], is_fallback=False)

    df = pd.DataFrame({
        "asin": ["B_SWAP", "B_OK"],
        "title": [
            "Sample item with uniqueB and featB tokens only",
            "Sample item with uniqueB content here",
        ],
        "Main Image": ["https://x/a.jpg", "https://x/b.jpg"],
    })

    mock_vision = _MockVisionProvider({"B_SWAP": "segB"})

    import config.llm_config as cfg
    monkeypatch.setattr(cfg, "make_vision_provider", lambda: mock_vision)

    a = object.__new__(BSRAnalyzer)
    a.client = None
    a._post_classify(pack, df, {"category_id": "test"})

    seg_a = next(s for s in pack.product_segments if s.name == "segA")
    assert "B_SWAP" in seg_a.member_asins, "low_conf 模式不应改写"


def test_vision_no_change_keeps_original(monkeypatch):
    """vision 返回的 segment 与当前一致 → 不做任何改动。"""
    monkeypatch.setenv("VISION_AUDIT_MODE", "all")

    pack = MarketInsightPack(product_segments=[
        ProductSegment(name="segA", representative_keywords=["keyA"], member_asins=["B1"]),
        ProductSegment(name="segB", representative_keywords=["keyB"], member_asins=["B2"]),
    ], is_fallback=False)

    df = pd.DataFrame({
        "asin": ["B1"],
        "title": ["zzz"],
        "Main Image": ["https://x/img.jpg"],
    })

    mock_vision = _MockVisionProvider({"B1": "segA"})  # vision 也说 B1 → segA，与当前一致

    import config.llm_config as cfg
    monkeypatch.setattr(cfg, "make_vision_provider", lambda: mock_vision)

    a = object.__new__(BSRAnalyzer)
    a.client = None
    a._post_classify(pack, df, {"category_id": "test"})

    seg_a = next(s for s in pack.product_segments if s.name == "segA")
    seg_b = next(s for s in pack.product_segments if s.name == "segB")
    assert "B1" in seg_a.member_asins
    assert "B1" not in seg_b.member_asins


def test_vision_unknown_segment_still_records_labels(monkeypatch):
    """segment unknown 时不改写产品类型，但保留材质/形态视觉标签。"""
    monkeypatch.setenv("VISION_AUDIT_MODE", "all")

    pack = MarketInsightPack(product_segments=[
        ProductSegment(name="segA", representative_keywords=["keyA"], member_asins=["B1"]),
    ], is_fallback=False)

    df = pd.DataFrame({
        "asin": ["B1"],
        "title": ["sample"],
        "Main Image": ["https://x/img.jpg"],
    })

    class _LabelOnlyVisionProvider(_MockVisionProvider):
        def chat_multimodal(self, messages, **kwargs):
            return '{"segment_name": "unknown", "material_label": "metal"}'

    import config.llm_config as cfg
    monkeypatch.setattr(cfg, "make_vision_provider", lambda: _LabelOnlyVisionProvider())

    a = object.__new__(BSRAnalyzer)
    a.client = None
    a._post_classify(pack, df, {"category_id": "test"})

    seg_a = next(s for s in pack.product_segments if s.name == "segA")
    assert "B1" in seg_a.member_asins
    assert len(pack.visual_labels) == 1
    assert pack.visual_labels[0].asin == "B1"
    assert pack.visual_labels[0].material_label == "metal"
