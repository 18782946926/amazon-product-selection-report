"""SpecAnalyzer：从 BSR TOP 竞品标题/bullets 识别品类关键规格维度 + 正则模式。

替代 app.py:476-614 两套 LED 专属 extract_specs：
- LLM 按本次品类动态输出 spec_dimensions（含 Python 可用的正则模式）
- Python 用这些 pattern 抓取每个竞品的具体值
- Synthesizer 的 upgrade_directions 引用 stats.competitor_spec_p75 给具体目标值
"""
from __future__ import annotations

import hashlib
import logging

import pandas as pd

from llm.analyzers.base import BaseAnalyzer
from llm.analyzers.bsr_analyzer import resolve_col
from llm.prompts import spec as spec_prompt
from llm.schemas import SpecInsightPack

log = logging.getLogger(__name__)


class SpecAnalyzer(BaseAnalyzer[SpecInsightPack]):
    name = "Spec"

    def _cache_key(self, input_data: dict) -> str:
        df: pd.DataFrame = input_data["df"]
        category_id = input_data.get("category_id", "unknown")
        h = hashlib.sha256()
        h.update(spec_prompt._PROMPT_VERSION.encode())
        h.update(category_id.encode())
        title_col = resolve_col(df, "title")
        if title_col:
            h.update(str(df[title_col].fillna("").head(30).tolist()).encode())
        return f"spec_{category_id}_{h.hexdigest()[:12]}"

    def _prepare_competitors(self, df: pd.DataFrame, top_n: int = 20) -> list[dict]:
        """从 BSR 取 TOP 竞品的标题（如果有 bullets 字段也带上）。"""
        title_col = resolve_col(df, "title")
        asin_col = resolve_col(df, "asin")
        if not title_col or not asin_col:
            return []

        bullet_cols = [c for c in df.columns if "bullet" in c.lower() or "feature" in c.lower()
                       or "description" in c.lower()]

        rows = []
        for _, r in df.head(top_n).iterrows():
            row = {
                "asin": str(r.get(asin_col, ""))[:20],
                "title": str(r.get(title_col, ""))[:300],
            }
            if bullet_cols:
                bullets = " | ".join(str(r[c])[:200] for c in bullet_cols if pd.notna(r[c]))
                if bullets.strip():
                    row["bullets"] = bullets[:800]
            rows.append(row)
        return rows

    def _call_llm(self, input_data: dict) -> SpecInsightPack:
        df = input_data["df"]
        competitors = self._prepare_competitors(df)
        if not competitors:
            return SpecInsightPack(is_fallback=True)
        category_hint = (
            input_data.get("display_name")
            or input_data.get("category_id")
            or "未知品类"
        )
        messages = spec_prompt.build_messages(competitors, category_hint=category_hint)
        result = self.client.chat_json(
            messages=messages,
            schema=SpecInsightPack,
            tier="fast",
            cache_key=self._cache_key(input_data),
            temperature=0.1,
            max_tokens=6000,
            timeout=180,
        )

        # Post-filter：货币单位维度一律剔除（跨品类硬约束）
        # 任何品类的物理规格 unit 都不是货币（PSI/mAh/IP/inch/dB/lm/V/A 等）；
        # unit 为 $/USD/￥/CNY/RMB 的"维度"必然是营销话术金额（价格/挑战目标/折扣等），
        # prompt v3 已禁止但 LLM 偶尔不听——代码层兜底确保剔除。
        _CURRENCY_UNITS = {'$', 'USD', '￥', 'CNY', 'RMB', '$USD', 'US$'}
        if result and result.spec_dimensions:
            kept_dims = []
            dropped_names = []
            for d in result.spec_dimensions:
                unit_norm = str(getattr(d, 'unit', '') or '').strip().upper()
                if unit_norm in _CURRENCY_UNITS:
                    dropped_names.append(getattr(d, 'name', '?'))
                    continue
                kept_dims.append(d)
            if dropped_names:
                log.info("[Spec] post-filter 剔除货币单位维度: %s", dropped_names)
            result.spec_dimensions = kept_dims

            # 同步从 representative_specs_by_asin 里清掉这些维度的字段
            if dropped_names and result.representative_specs_by_asin:
                drop_set = set(dropped_names)
                for sample in result.representative_specs_by_asin:
                    if sample.specs:
                        sample.specs = {k: v for k, v in sample.specs.items() if k not in drop_set}

        return result

    def _fallback(self, input_data: dict) -> SpecInsightPack:
        """LLM 不可用时返回空 Pack：
        不再回落到 LED 专属的 mAh/IP/COB 抓取（那是 v2.0 LED 单类目遗留），
        空 Pack 会让 Sheet 4 的竞品规格列自动跳过。
        """
        return SpecInsightPack(is_fallback=True)


def extract_specs_by_dimensions(title: str,
                                 bullets: str,
                                 dimensions: list,
                                 max_patterns_per_dim: int = 4) -> dict[str, str]:
    """给定一个 ASIN 的标题 + bullets，按 SpecInsightPack.spec_dimensions 提供的正则抓取具体值。

    Args:
        title: 标题
        bullets: Bullet points 拼接文本（可空）
        dimensions: [SpecDimension...] 或 [{"name": ..., "extract_patterns": [...]}...]
        max_patterns_per_dim: 单维度最多试多少正则

    Returns:
        {维度名: 抓到的具体值（数字会带单位）}；没抓到的维度不在返回 dict 里
    """
    import re as _re

    text = f"{title or ''} {bullets or ''}".strip()
    if not text or not dimensions:
        return {}
    text_lower = text.lower()

    out: dict[str, str] = {}
    for dim in dimensions:
        # 支持 dict 或 pydantic model
        if hasattr(dim, "name"):
            name = dim.name
            unit = dim.unit or ""
            patterns = dim.extract_patterns or []
        else:
            name = dim.get("name")
            unit = dim.get("unit", "")
            patterns = dim.get("extract_patterns", [])

        if not name or not patterns:
            continue

        for pat in patterns[:max_patterns_per_dim]:
            try:
                m = _re.search(pat, text_lower, flags=_re.IGNORECASE)
            except _re.error as e:
                log.debug("[Spec] 正则错误 dim=%s pattern=%s err=%s", name, pat, e)
                continue
            if m:
                # 有捕获组就取第 1 个；否则取整个匹配
                # group(1) 可能是 None（可选捕获组未参与匹配，如 `(\d+)?w` 匹配 "w"），降级到 group(0)
                val = m.group(1) if m.groups() else m.group(0)
                val = (val if val is not None else m.group(0) or '').strip()
                if not val:
                    continue
                # 全 0 数字视为无效抓取（如 "0"、"0000"、"0.00"）——pattern 可能命中 bullet 里的
                # 占位文案 "$0000"、"$0.00 shipping" 等，业务上无意义。跳过当前 pattern 试下一个。
                val_digits = _re.sub(r'[^\d]', '', val)
                if val_digits and all(c == '0' for c in val_digits):
                    continue
                out[name] = f"{val}{unit}" if unit and val.replace(".", "").isdigit() else val
                break
    return out
