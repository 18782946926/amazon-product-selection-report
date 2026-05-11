"""统一的 Packs 准备入口与 6 处硬编码替换的辅助函数。

设计意图：
- 让 app.py 改动最小化：只在主流程入口调一次 prepare_packs()，并在 6 处硬编码点
  调用本模块的 helper 选择"用 Pack 字段"或"用原硬编码"
- Packs 由 5 个 analyzer 并发生成；任一失败走该 analyzer 的 fallback，不影响其他 Pack
- 每个 helper 都接受 default 参数（原硬编码），LLM 不可用时透明回退
"""
from __future__ import annotations

import logging
import os
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pandas as pd

from llm.analyzers import (
    BSRAnalyzer,
    ComplianceAnalyzer,
    MarketAnalyzer,
    ReverseAsinAnalyzer,
    ReviewsAnalyzer,
    SpecAnalyzer,
    Synthesizer,
)
from llm.analyzers.bsr_analyzer import (
    resolve_col, normalize_bsr_columns,
    normalize_review_columns, normalize_keyword_columns,
    normalize_market_sheet_name,
)
from llm.analyzers.spec_analyzer import extract_specs_by_dimensions
from llm.client import LLMClient
from llm.exceptions import LLMUnavailable
from llm.schemas import (
    CompliancePack,
    MarketInsightPack,
    SpecInsightPack,
    StrategySynthesis,
    TrafficInsightPack,
    TrendInsightPack,
    VOCPack,
)
from utils.category_id import extract_from_bsr_filename

log = logging.getLogger(__name__)

# 分类器置信度门槛（品类无关）：classify_with_packs step 4 加权打分低于此值时，
# 视为信号不足、退到 step 5 兜底；step 5 也不可信时返回 default_classify_fn（"未分类"），
# 留给 BSRAnalyzer._post_classify 的视觉二审兜底
MIN_OVERLAP_SCORE = 1.0


@dataclass
class Packs:
    market: MarketInsightPack | None = None
    voc: VOCPack | None = None
    traffic: TrafficInsightPack | None = None
    trend: TrendInsightPack | None = None
    spec: SpecInsightPack | None = None
    compliance: CompliancePack | None = None
    synthesis: StrategySynthesis | None = None
    category_id: str = "unknown"
    display_name: str = ""
    synthesis_stats: dict = field(default_factory=dict)
    sheet5_plans: list[dict] = field(default_factory=list)  # 独立 Sheet5ImprovementAnalyzer 的输出（避免 Synthesizer 截断）

    def is_market_real(self) -> bool:
        return self.market is not None and not self.market.is_fallback

    def is_voc_real(self) -> bool:
        return self.voc is not None and not self.voc.is_fallback

    def is_traffic_real(self) -> bool:
        return self.traffic is not None and not self.traffic.is_fallback

    def is_trend_real(self) -> bool:
        return self.trend is not None and not self.trend.is_fallback

    def is_spec_real(self) -> bool:
        return self.spec is not None and not self.spec.is_fallback

    def is_compliance_real(self) -> bool:
        return self.compliance is not None and not self.compliance.is_fallback

    def is_synthesis_real(self) -> bool:
        return self.synthesis is not None and not self.synthesis.is_fallback


def _try_make_client(api_key: str | None = None) -> LLMClient | None:
    """初始化 LLM client。api_key 显式传入时覆盖环境变量（key pool 多窗口并发场景用）。"""
    try:
        from llm import make_client
        return make_client(api_key=api_key)
    except (LLMUnavailable, Exception) as e:
        log.warning("LLM client 初始化失败，所有 analyzer 走降级: %s", e)
        return None


def _load_reviews(review_paths: list[str]) -> list[dict]:
    out = []
    for f in review_paths:
        try:
            df = pd.read_excel(f, sheet_name=0)
        except Exception as e:
            log.warning("读取评论失败 %s: %s", f, e)
            continue
        # 评论文件可能是中文列名（标题/内容/星级/评论时间），统一规范化为英文
        df = normalize_review_columns(df)
        asin = os.path.basename(f).split("-")[0]
        for _, r in df.iterrows():
            d = r.to_dict()
            d.setdefault("asin", asin)
            out.append(d)
    return out


def _load_market(market_path: str | None) -> dict:
    """读 Market 文件。Sheet 名如果是中文（"市场分析概况"等），在返回 dict 里同时用英文 key
    （"Market Analysis" 等），这样 MarketAnalyzer 的 _summarize 和 app.py 的渲染层都能按
    英文名取到，兼容中英两种卖家精灵导出版本。"""
    if not market_path or not os.path.exists(market_path):
        return {}
    try:
        from llm.analyzers.bsr_analyzer import normalize_market_columns
        xl = pd.ExcelFile(market_path)
        out = {}
        for s in xl.sheet_names:
            df = pd.read_excel(market_path, sheet_name=s)
            df = normalize_market_columns(df)
            en_name = normalize_market_sheet_name(s)
            # 始终按英文 key 存（中文文件）或保留原英文（英文文件）
            out[en_name] = df
        return out
    except Exception as e:
        log.warning("读取 Market 文件失败 %s: %s", market_path, e)
        return {}


def _build_stats_for_synthesis(market: MarketInsightPack | None,
                                voc: VOCPack | None,
                                spec: SpecInsightPack | None,
                                compliance: CompliancePack | None,
                                bsr_df: pd.DataFrame | None,
                                keyword_df: pd.DataFrame | None = None,
                                market_data: dict | None = None) -> dict:
    """组装供 Synthesizer 用的"代码已算好的数值"。
    LLM 只造句，所有数字必须来自这里或 Pack 自身字段。

    扩展字段：
    - competitor_spec_medians / competitor_spec_p75：用 SpecPack 的维度正则回抓 BSR TOP20 ASIN
      的具体规格值，求中位数 / P75，供 Synthesizer 写 upgrade_directions 的 target_spec
    - cr5_pct / brand_count / china_seller_pct / new_product_pct：结构指标
    - required_certs / typical_return_rate：从 CompliancePack 透传（LLM 可在文案里引用）
    """
    stats: dict[str, Any] = {}
    if bsr_df is not None and not bsr_df.empty:
        price_col = resolve_col(bsr_df, "price")
        if price_col:
            prices = pd.to_numeric(bsr_df[price_col], errors="coerce").dropna()
            if len(prices) > 0:
                stats["price_median"] = float(prices.median())
                stats["price_p75"] = float(prices.quantile(0.75))
                stats["price_min"] = float(prices.min())
                stats["price_max"] = float(prices.max())

        sales_col = resolve_col(bsr_df, "monthly_sales")
        if sales_col:
            sales = pd.to_numeric(bsr_df[sales_col], errors="coerce").dropna()
            if len(sales) > 0:
                stats["sales_total"] = int(sales.sum())
                stats["sales_median"] = float(sales.median())

        # 品牌集中度
        brand_col = resolve_col(bsr_df, "brand")
        if brand_col:
            brand_counts = bsr_df[brand_col].fillna("").astype(str).value_counts()
            brand_counts = brand_counts[brand_counts.index != ""]
            stats["brand_count"] = int(len(brand_counts))

            cr5_pct_v = None
            if isinstance(market_data, dict):
                bd = (market_data.get("brand_concentration")
                      or market_data.get("Brand Concentration"))
                if bd is not None and "Sales Proportion" in getattr(bd, "columns", []):
                    try:
                        _sp = pd.to_numeric(bd["Sales Proportion"], errors="coerce").dropna()
                        if len(_sp) >= 5:
                            cr5_pct_v = round(float(_sp.head(5).sum()) * 100, 1)
                    except Exception:
                        pass
            if cr5_pct_v is None and len(brand_counts) >= 5:
                cr5_pct_v = round(float(brand_counts.head(5).sum() / brand_counts.sum()) * 100, 1)
            if cr5_pct_v is not None:
                stats["cr5_pct"] = cr5_pct_v

        # 中国卖家占比（BuyBox 维度）—— 全报告唯一口径
        # 判定逻辑：BuyBox Location ∈ {"CN", "CN(HK)"}（卖家精灵 BSR 的固定取值之一）
        # 为兼容老数据（部分字段可能是 "China" 字样），保留 contains 兜底
        sc_col = resolve_col(bsr_df, "seller_country")
        if sc_col:
            sc_raw = bsr_df[sc_col].fillna("").astype(str).str.strip()
            sc_up = sc_raw.str.upper()
            cn_mask = sc_up.isin(["CN", "CN(HK)"]) | sc_up.str.contains("CHINA")
            cn_pct = float(cn_mask.mean())
            stats["china_seller_pct"] = round(cn_pct * 100, 1)
            stats["cn_buybox_pct"] = round(cn_pct * 100, 1)  # 显示层统一字段名
            # 中国卖家月收入占比
            rev_col = resolve_col(bsr_df, "revenue")
            if rev_col is not None and rev_col in bsr_df.columns:
                try:
                    rev_series = pd.to_numeric(bsr_df[rev_col], errors="coerce").fillna(0.0)
                    total_rev = float(rev_series.sum())
                    if total_rev > 0:
                        stats["cn_buybox_rev_pct"] = round(
                            float(rev_series[cn_mask].sum()) / total_rev * 100, 1
                        )
                except Exception:
                    pass

        # 新品占比
        age_col = "Available days" if "Available days" in bsr_df.columns else None
        if age_col:
            ages = pd.to_numeric(bsr_df[age_col], errors="coerce").dropna()
            if len(ages) > 0:
                stats["new_product_pct"] = round(float((ages < 365).mean()) * 100, 1)

        # 价格带分档（与 app.py Sheet 1/4 的 PRICE_BANDS 一致）：供 LLM price_band_insights 按档叙述
        if price_col:
            _bins = [0, 15, 25, 35, 50, 70, 100, 99999]
            _labels = ['<$15', '$15-25', '$25-35', '$35-50', '$50-70', '$70-100', '>$100']
            prices_all = pd.to_numeric(bsr_df[price_col], errors="coerce").dropna()
            sales_col2 = resolve_col(bsr_df, "monthly_sales")
            _band_list = []
            for _i, _lab in enumerate(_labels):
                _lo, _hi = _bins[_i], _bins[_i + 1]
                _mask = (prices_all >= _lo) & (prices_all < _hi)
                _cnt = int(_mask.sum())
                if _cnt == 0:
                    continue
                _entry = {"band": _lab, "sku_count": _cnt}
                if sales_col2:
                    _s = pd.to_numeric(bsr_df[sales_col2], errors="coerce").fillna(0)
                    _entry["avg_monthly_sales"] = int(_s[_mask.reindex(_s.index, fill_value=False)].mean()) \
                        if _mask.any() else 0
                _band_list.append(_entry)
            if _band_list:
                stats["price_band_analysis"] = _band_list

        # 竞品规格 P75 / 中位数：用 SpecPack 维度回抓 BSR TOP20
        if spec and spec.spec_dimensions and not spec.is_fallback:
            title_col = resolve_col(bsr_df, "title")
            if title_col:
                bullet_cols = [c for c in bsr_df.columns if "bullet" in c.lower()
                               or "feature" in c.lower() or "description" in c.lower()]
                extracted: dict[str, list[str]] = {d.name: [] for d in spec.spec_dimensions}
                for _, row in bsr_df.head(20).iterrows():
                    title = str(row.get(title_col, "") or "")
                    bullets = " ".join(str(row[c]) for c in bullet_cols if pd.notna(row.get(c)))
                    specs = extract_specs_by_dimensions(title, bullets, list(spec.spec_dimensions))
                    for dim_name, val in specs.items():
                        if dim_name in extracted:
                            extracted[dim_name].append(val)
                medians: dict[str, Any] = {}
                p75s: dict[str, Any] = {}
                for dim_name, vals in extracted.items():
                    nums = _parse_numeric_values(vals)
                    if len(nums) >= 3:
                        s = pd.Series(nums)
                        medians[dim_name] = float(s.median())
                        p75s[dim_name] = float(s.quantile(0.75))
                if medians:
                    stats["competitor_spec_medians"] = medians
                    stats["competitor_spec_p75"] = p75s

    if voc and voc.pain_clusters:
        stats["top_pain_names"] = [c.name for c in voc.pain_clusters[:3]]
        stats["top_pain_freqs"] = [round(c.frequency_pct, 1) for c in voc.pain_clusters[:3]]

    if compliance and not compliance.is_fallback:
        if compliance.required_certifications:
            stats["required_certs"] = [c.name for c in compliance.required_certifications
                                       if c.mandatory][:5]
        if compliance.typical_return_rate_pct > 0:
            stats["typical_return_rate_pct"] = compliance.typical_return_rate_pct

    # ========= 推广压力复合指标 =========
    # 单独的"广告占比"列在卖家精灵 ReverseASIN 导出里并不存在（原代码找 sponsored share / ads share
    # 永远失败 → ads_heavy_count=0 → LLM 生成"广告占比 0%、广告高占比词 0"假结论）。
    # 改由多个真实字段综合反映：
    # - promo_ppc_bid_avg / promo_ads_competitor_avg：Top20 核心词的烧钱强度与对手密度
    # - promo_click_share_avg_top / promo_conversion_share_avg_top：头部前 3 ASIN 的垄断程度
    # - promo_spr_median：新品 8 天上首页所需销量门槛中位数
    # - bsr_sp_ads_pct：BSR TOP100 中开 SP 广告的 ASIN 占比（反向验证）
    if keyword_df is not None and not keyword_df.empty:
        def _col_ci(df: pd.DataFrame, *names: str) -> str | None:
            lmap = {c.lower().strip(): c for c in df.columns}
            for n in names:
                if n.lower() in lmap:
                    return lmap[n.lower()]
            return None

        def _to_float_cell(v) -> float:
            if v is None or pd.isna(v):
                return 0.0
            s = str(v).replace("$", "").replace(",", "").replace("%", "").strip()
            if not s or s.lower() == "nan":
                return 0.0
            if "-" in s and not s.startswith("-"):
                parts = [p.strip() for p in s.split("-") if p.strip()]
                try:
                    nums = [float(p) for p in parts]
                    return sum(nums) / len(nums) if nums else 0.0
                except ValueError:
                    return 0.0
            try:
                return float(s)
            except ValueError:
                return 0.0

        def _norm_pct(v: float) -> float:
            return v * 100 if 0 <= v <= 1 else v

        vol_col = _col_ci(keyword_df, "m. searches", "search volume", "monthly searches")
        top_kw = keyword_df.copy()
        if vol_col:
            top_kw[vol_col] = pd.to_numeric(top_kw[vol_col], errors="coerce").fillna(0)
            top_kw = top_kw.sort_values(vol_col, ascending=False).head(20)
        else:
            top_kw = top_kw.head(20)

        ppc_col = _col_ci(top_kw, "ppc bid", "suggested bid")
        ads_comp_col = _col_ci(top_kw, "ads competitor count")
        click_col = _col_ci(top_kw, "click share")
        conv_col = _col_ci(top_kw, "conversion share")
        spr_col = _col_ci(top_kw, "spr")
        products_col = _col_ci(top_kw, "products")

        if ppc_col:
            vals = [_to_float_cell(v) for v in top_kw[ppc_col]]
            vals = [v for v in vals if v > 0]
            if vals:
                stats["promo_ppc_bid_avg"] = round(sum(vals) / len(vals), 2)
        if ads_comp_col:
            vals = [_to_float_cell(v) for v in top_kw[ads_comp_col]]
            vals = [v for v in vals if v > 0]
            if vals:
                stats["promo_ads_competitor_avg"] = round(sum(vals) / len(vals), 0)
        if click_col:
            vals = [_norm_pct(_to_float_cell(v)) for v in top_kw[click_col]]
            vals = [v for v in vals if v > 0]
            if vals:
                stats["promo_click_share_avg_top"] = round(sum(vals) / len(vals), 1)
        if conv_col:
            vals = [_norm_pct(_to_float_cell(v)) for v in top_kw[conv_col]]
            vals = [v for v in vals if v > 0]
            if vals:
                stats["promo_conversion_share_avg_top"] = round(sum(vals) / len(vals), 1)
        if spr_col:
            vals = sorted([_to_float_cell(v) for v in top_kw[spr_col] if _to_float_cell(v) > 0])
            if vals:
                n = len(vals)
                stats["promo_spr_median"] = round(vals[n // 2] if n % 2 else (vals[n // 2 - 1] + vals[n // 2]) / 2, 0)
        if products_col:
            vals = sorted([_to_float_cell(v) for v in top_kw[products_col] if _to_float_cell(v) > 0])
            if vals:
                n = len(vals)
                stats["promo_products_median"] = round(vals[n // 2] if n % 2 else (vals[n // 2 - 1] + vals[n // 2]) / 2, 0)

    # BSR TOP100 开 SP 广告比例（反向验证：越高说明推广内卷越严重）
    if bsr_df is not None and not bsr_df.empty:
        sp_ads_col = None
        for c in bsr_df.columns:
            if c.lower().strip() in ("sp ads", "sp广告", "sp_ads"):
                sp_ads_col = c
                break
        if sp_ads_col:
            raw = bsr_df[sp_ads_col].astype(str).str.strip().str.lower()
            truthy = raw.isin(["yes", "y", "true", "1", "是", "✓", "有"]) | raw.str.contains(r"\d", regex=True, na=False)
            pct = float(truthy.mean()) * 100
            if 0 < pct <= 100:
                stats["bsr_sp_ads_pct"] = round(pct, 1)

    return stats


# 跨品类 pain 语义禁用词：按"目标品类特征" → "禁用的其他品类术语"映射
# 判定方式：display_name 小写后若不包含任何 "allow 特征词" 才启用对应禁用表
_CROSS_CAT_PAIN_BANS: list[tuple[tuple[str, ...], tuple[str, ...]]] = [
    # (本品类允许的特征词, 若不在本品类则禁用这些 pain name 关键词)
    (("led", "light", "flashlight", "lantern", "headlamp", "lamp", "灯", "手电", "工作灯"),
     ("亮度", "流明", "lumen", "反光杯", "光束", "光通量", "光效", "色温", "聚光")),
    (("inflator", "pump", "tire", "充气", "胎压", "打气"),
     ("PSI", "气管", "泵体")),
    (("battery", "charger", "powerbank", "power station", "电池", "充电器", "充电宝", "储能"),
     ("mAh", "电池容量",)),
]


def _filter_cross_category_voc_pains(voc_pack, display_name: str) -> None:
    """就地过滤 voc_pack.pain_clusters：移除明显不属于本品类的聚类（如 Battery Chargers 里的"亮度不足"）。

    - merge LLM prompt 已有品类守门（reviews.py _MERGE_RULES 规则 5），这是兜底：
      如果 LLM 漏了（少数批次跨类 ASIN 合成全局 pain）Python 再拦一次
    - 不做 praise_clusters 过滤（praise 一般是笼统夸奖，跨品类风险低）
    """
    if voc_pack is None or not getattr(voc_pack, "pain_clusters", None):
        return
    name_lower = str(display_name or "").lower()
    banned_keywords: list[str] = []
    for allow_tokens, ban_tokens in _CROSS_CAT_PAIN_BANS:
        # 本品类名不含任何 allow token → 启用对应 ban 列表
        if not any(tok.lower() in name_lower for tok in allow_tokens):
            banned_keywords.extend(ban_tokens)

    if not banned_keywords:
        return

    kept = []
    dropped = []
    for c in voc_pack.pain_clusters:
        pname = str(getattr(c, "name", "") or "")
        pname_lower = pname.lower()
        if any(bw.lower() in pname_lower for bw in banned_keywords):
            dropped.append(pname)
            continue
        kept.append(c)

    if dropped:
        log.info("[VOC 品类守门] display_name=%r 剔除跨品类 pain_clusters: %s",
                 display_name, dropped)
        voc_pack.pain_clusters = kept


def _parse_numeric_values(raw: list[str]) -> list[float]:
    """从 "150PSI" / "4200mAh" / "12V" 这类带单位的字符串里抓数字。"""
    import re as _re
    out: list[float] = []
    for s in raw:
        if not s:
            continue
        m = _re.search(r"-?\d+(?:\.\d+)?", str(s))
        if m:
            try:
                out.append(float(m.group()))
            except ValueError:
                pass
    return out


def groupby_visual_labels(visual_labels: list) -> list:
    """按 form_label 单键 groupby，得到 4-8 个粗粒度形态大类。

    输入：list[ProductVisualLabel]——视觉 LLM per-ASIN 输出的结构化标签
    输出：list[ProductSegment]——按 SKU 数降序排列

    设计要点：
    - 仅按 form_label 聚类，**material 不参与分类**（不切「塑料猪形」「陶瓷猪形」两桶，统一为「猪形款」）
    - vision prompt v6+ 已引导 form_label 输出粗粒度类（动物/IP → 卡通造型；礼盒变体 → 礼盒式），
      所以单键 form_label 自然落到 4-8 个大类
    - 不依赖任何 LLM 调用，纯代码 groupby → 跨次完全确定
    - form_label 为空（视觉拒答）→ 落"其他"桶
    - SKU < 3 的小桶合并到"其他"
    - material_attribute 仍保留为该桶主导材质（信息字段，用于下游 Sheet 4 等），但不进桶名
    - keywords 从该组所有 product_type_free 文本抽 token 频次 Top 10
    """
    from collections import defaultdict, Counter
    from llm.schemas import ProductSegment

    _TOK = re.compile(r"[一-鿿]+|[a-z0-9]+")

    groups: dict[str, list] = defaultdict(list)
    for label in (visual_labels or []):
        asin = str(getattr(label, "asin", "") or "").strip()
        if not asin:
            continue
        form = str(getattr(label, "form_label", "") or "").strip()
        key = form if form else "__OTHER__"
        groups[key].append(label)

    other_labels: list = list(groups.pop("__OTHER__", []))
    valid_groups = []
    for form, labels in groups.items():
        if len(labels) < 3:
            other_labels.extend(labels)
        else:
            valid_groups.append((form, labels))

    valid_groups.sort(key=lambda kv: -len(kv[1]))

    segments: list = []
    for form, labels in valid_groups:
        token_counter: Counter = Counter()
        for lbl in labels:
            ptf = str(getattr(lbl, "product_type_free", "") or "").lower()
            for tok in _TOK.findall(ptf):
                if len(tok) >= 2:
                    token_counter[tok] += 1
        kws = [tok for tok, _ in token_counter.most_common(10)]

        # 主导材质（多数派）作信息字段保留——不进桶名
        material_counter: Counter = Counter()
        for lbl in labels:
            mat = str(getattr(lbl, "material_label", "") or "").strip()
            if mat:
                material_counter[mat] += 1
        dominant_material = material_counter.most_common(1)[0][0] if material_counter else ""

        segments.append(ProductSegment(
            name=f"{form}款",
            description=f"{form} 形态（共 {len(labels)} 个 SKU"
                        + (f"，主导材质 {dominant_material}）" if dominant_material else "）"),
            representative_keywords=kws,
            material_attribute=dominant_material,
            form_attribute=form,
            member_asins=[str(getattr(lbl, "asin", "") or "").strip() for lbl in labels],
        ))

    if other_labels:
        token_counter = Counter()
        for lbl in other_labels:
            ptf = str(getattr(lbl, "product_type_free", "") or "").lower()
            for tok in _TOK.findall(ptf):
                if len(tok) >= 2:
                    token_counter[tok] += 1
        kws = [tok for tok, _ in token_counter.most_common(10)]
        segments.append(ProductSegment(
            name="其他",
            description=f"视觉信号不足或独立小品类（共 {len(other_labels)} 个 SKU）",
            representative_keywords=kws,
            material_attribute="",
            form_attribute="",
            member_asins=[str(getattr(lbl, "asin", "") or "").strip() for lbl in other_labels],
        ))

    return segments


def prepare_packs(
    *,
    bsr_path: str,
    review_paths: list[str] | None = None,
    keyword_path: str | None = None,
    market_path: str | None = None,
    bsr_df: pd.DataFrame | None = None,
    api_key: str | None = None,
) -> Packs:
    """并发跑 4 个 analyzer + Synthesizer，返回 Packs。

    任一 analyzer 失败走自身降级，不影响其他 Pack；client 初始化失败则全部走降级。
    api_key 显式传入时，本次 prepare_packs 所有 LLM 调用都用该 key（key pool 多窗口并发用）。
    """
    review_paths = review_paths or []
    category_id = extract_from_bsr_filename(bsr_path)
    client = _try_make_client(api_key=api_key)

    if bsr_df is None:
        bsr_df = pd.read_excel(bsr_path, sheet_name="US")
    # 中英列名规范化（对已是英文的文件幂等，Battery Chargers 这类中文列名版本会被映射）
    bsr_df = normalize_bsr_columns(bsr_df)

    bsr_input = {"df": bsr_df, "category_id": category_id}
    # category_hint 用于 VOC cache 键隔离 + merge prompt 品类守门
    # 此刻 BSR/Market 尚未跑完，display_name 未知，先用 category_id 占位（Battery_Chargers → "Battery Chargers"）
    _voc_cat_hint = (category_id or "").replace("_", " ").strip() or "未知品类"
    reviews_input = {"reviews": _load_reviews(review_paths), "category_hint": _voc_cat_hint}

    keyword_df = pd.DataFrame()
    if keyword_path and os.path.exists(keyword_path):
        try:
            xl = pd.ExcelFile(keyword_path)
            # 优先取 US- 开头的 sheet（ReverseASIN / ExpandKeywords 都用这种命名）
            target_sheet = None
            for s in xl.sheet_names:
                if s.startswith('US-') or 'US-' in s:
                    target_sheet = s
                    break
            if target_sheet is None:
                target_sheet = xl.sheet_names[0]
            keyword_df = pd.read_excel(keyword_path, sheet_name=target_sheet)
            # 关键词文件中文列名也统一映射（关键词→Keyword，月搜索量→M. Searches 等）
            keyword_df = normalize_keyword_columns(keyword_df)
        except Exception as e:
            log.warning("读取关键词文件失败: %s", e)
    traffic_input = {"df": keyword_df}

    market_data = _load_market(market_path)
    trend_input = {"market_data": market_data}

    # 首批 4 个 analyzer 并发（Market/VOC/Traffic/Trend + Spec）
    # 注意：Compliance 依赖 category_name（由 BSR 返回），所以 Compliance 放到第二阶段
    # display_name 先用 category_id 占位（Market/BSR 还没跑完），Synthesizer 再用真实 display_name；
    # SpecAnalyzer 与 BSRAnalyzer 并行，所以取 category_id 作为提示，已足以避免 LED 串品类
    spec_input = {"df": bsr_df, "category_id": category_id, "display_name": category_id.replace('_', ' ')}
    analyzers = {
        "market": (BSRAnalyzer(client), bsr_input),
        "voc": (ReviewsAnalyzer(client), reviews_input),
        "traffic": (ReverseAsinAnalyzer(client), traffic_input),
        "trend": (MarketAnalyzer(client), trend_input),
        "spec": (SpecAnalyzer(client), spec_input),
    }

    results: dict[str, Any] = {}
    with ThreadPoolExecutor(max_workers=5) as ex:
        futures = {ex.submit(a.run, inp): name for name, (a, inp) in analyzers.items()}
        for fut in as_completed(futures):
            name = futures[fut]
            try:
                results[name] = fut.result()
            except Exception as e:
                log.error("[%s] 异常: %s", name, e, exc_info=True)
                results[name] = None

    market_pack: MarketInsightPack | None = results.get("market")
    voc_pack: VOCPack | None = results.get("voc")
    traffic_pack: TrafficInsightPack | None = results.get("traffic")
    trend_pack: TrendInsightPack | None = results.get("trend")
    spec_pack: SpecInsightPack | None = results.get("spec")

    display_name = ""
    if market_pack and market_pack.category_display_name:
        display_name = market_pack.category_display_name
    if not display_name:
        display_name = category_id.replace("_", " ").title()

    # 跨品类 pain_clusters 硬守门：即使 merge LLM 漏了品类守门，Python 再过滤一次
    # 触发场景：少数批次 ASIN 跨类（USB 充电 + LED 合体产品）使 merge 误把"亮度不足"升格为全局 pain
    _filter_cross_category_voc_pains(voc_pack, display_name)

    # 第一阶段半-a：轻量 LLM 标签合并（解决「手持式 vs 便携式」等语义重叠）
    # 视觉 LLM per-ASIN 输出 form_label 时是孤岛判断，同义标签会被随机分散。
    # 这里把所有 form_label 去重后整体交给 LabelMerger，让它从全局视角给合并 mapping。
    # 输入 ~10 个字符串、输出小 mapping JSON——比 100-ASIN 全局聚类轻量得多。
    if market_pack and market_pack.visual_labels and not market_pack.is_fallback and client is not None:
        try:
            from llm.analyzers.label_merger import LabelMerger
            unique_labels = sorted({
                str(getattr(lbl, "form_label", "") or "").strip()
                for lbl in market_pack.visual_labels
                if str(getattr(lbl, "form_label", "") or "").strip()
            })
            if len(unique_labels) >= 2:
                merge_pack = LabelMerger(client).run({"labels": unique_labels})
                if not merge_pack.is_fallback and merge_pack.merge_mapping:
                    log.info("[LabelMerger] 合并 %d 对标签：%s",
                             len(merge_pack.merge_mapping), merge_pack.merge_mapping)
                    for label in market_pack.visual_labels:
                        fl = str(getattr(label, "form_label", "") or "").strip()
                        if fl in merge_pack.merge_mapping:
                            label.form_label = merge_pack.merge_mapping[fl]
                else:
                    log.info("[LabelMerger] 无需合并（mapping 空 or fallback）")
        except Exception as e:
            log.warning("[LabelMerger] 异常，跳过合并: %s", e)

    # 第一阶段半-b：用 form_label 单键 Groupby 做确定性聚类（替代 LLM Aggregator）
    # 视觉 LLM 已 per-ASIN 输出结构化的 form_label（经 LabelMerger 合并后跨次稳定）。
    # 这里纯代码 groupby，不依赖 LLM 抓阄——同输入必产同 segments。
    # 失败兜底：保留 BSR Analyzer 原 segments
    if market_pack and market_pack.visual_labels and not market_pack.is_fallback:
        try:
            new_segments = groupby_visual_labels(market_pack.visual_labels)
            if new_segments and len(new_segments) >= 2:
                log.info("[Taxonomy] Groupby 切出 %d 个桶（确定性，基于 form_label 单键，材质不参与分类），覆盖 BSR 原 %d 个 segments",
                         len(new_segments), len(market_pack.product_segments or []))
                for seg in new_segments:
                    log.info("[Taxonomy]   桶: %r (%d ASIN) | material=%r | form=%r | keywords=%s",
                             seg.name, len(seg.member_asins or []),
                             seg.material_attribute or "-",
                             getattr(seg, "form_attribute", "") or "-",
                             (seg.representative_keywords or [])[:8])
                market_pack.product_segments = new_segments
            else:
                log.warning("[Taxonomy] Groupby 切桶不足 2 个，保留 BSR 原 segments 兜底")
        except Exception as e:
            log.warning("[Taxonomy] Groupby 异常，保留 BSR 原 segments 兜底: %s", e)

    # 注：BucketAssigner / TaxonomyAggregator LLM 已下线
    # llm/analyzers/bucket_assigner.py 与 llm/analyzers/taxonomy_aggregator.py 保留作备份

    # 第二阶段：Compliance + Synthesizer 并发
    # Compliance 只读 display_name + 竞品标题，不依赖 Synthesizer
    # Synthesizer 读 market/voc/traffic/trend/spec + stats，stats 里的 compliance 字段（required_certs /
    # typical_return_rate_pct）是可选增强——并发下 Synthesizer 跑时 compliance 可能还没返回，那两个 stats
    # 字段会缺席，但 Synthesizer 主要推理不受影响。Sheet 9 的 Compliance 渲染仍使用完整的 compliance_pack。
    titles_sample: list[str] = []
    title_col = resolve_col(bsr_df, "title") if not bsr_df.empty else None
    if title_col:
        titles_sample = [str(t) for t in bsr_df[title_col].head(30).tolist() if pd.notna(t)]

    # Synthesizer 的 stats 不含 compliance 字段（并发时 Compliance 可能还没跑完）
    stats = _build_stats_for_synthesis(market_pack, voc_pack, spec_pack, None, bsr_df, keyword_df, market_data=market_data)
    _synth_display_name = getattr(market_pack, 'category_display_name', '') or category_id

    def _run_compliance():
        try:
            return ComplianceAnalyzer(client).run({
                "category_name": display_name, "titles": titles_sample,
            })
        except Exception as e:
            log.error("[compliance] 异常: %s", e, exc_info=True)
            return None

    def _run_synth():
        return Synthesizer(client).run({
            "market": market_pack, "voc": voc_pack,
            "traffic": traffic_pack, "trend": trend_pack,
            "stats": stats,
            "display_name": _synth_display_name,
            "category_id": category_id,
        })

    compliance_pack: CompliancePack | None = None
    synth = None
    with ThreadPoolExecutor(max_workers=2) as ex2:
        fut_comp = ex2.submit(_run_compliance)
        fut_synth = ex2.submit(_run_synth)
        for fut in as_completed([fut_comp, fut_synth]):
            try:
                res = fut.result()
            except Exception as e:
                log.error("[stage2] 任务异常: %s", e, exc_info=True)
                res = None
            if fut is fut_comp:
                compliance_pack = res
            else:
                synth = res

    # Sheet5ImprovementAnalyzer 不再在此阶段调用——它需要的"首推子品类过滤后的 top4 pain"
    # 在 prepare_packs 时还算不出（需要 type_agg + 综合评分，那段逻辑在 app.py 渲染层）。
    # 改成由 Sheet5 渲染层在 _pain_list 算好后再调，让 LLM 输入的 pain_clusters
    # 与渲染层会用的 pain 列表完全对齐，避免"全局 top4 ≠ 子品类 top4"导致的 N/4 缺失。
    # sheet5_plans 在这里始终留空，渲染层调用成功后由 sheet5_improvement_plan() helper
    # 走 Synthesizer 兜底（详见 packs_runtime.py:sheet5_improvement_plan）。
    sheet5_plans: list[dict] = []

    packs = Packs(
        market=market_pack, voc=voc_pack, traffic=traffic_pack,
        trend=trend_pack, spec=spec_pack, compliance=compliance_pack,
        synthesis=synth,
        category_id=category_id, display_name=display_name,
        synthesis_stats=stats,
        sheet5_plans=sheet5_plans,
    )
    # 把本次请求的 api_key 私下挂到 packs，让 generate_report 阶段再调 LLM 时复用同一 key
    # （Sheet5 改进分析、lifecycle 分析等都会读 packs._api_key）
    packs._api_key = api_key  # type: ignore[attr-defined]
    return packs


# =====================================================================
# 6 处硬编码替换的 helper 函数
# 调用方式：每个 helper 接受 packs 和 default（原硬编码值），优先用 packs，否则回退
# =====================================================================

def classify_with_packs(title: str, packs: Packs | None, default_classify_fn, asin: str = "") -> str:
    """替代原 classify(title)：
    1) ASIN 命中 segment.member_asins → 该 segment
    2) 标题包含某 segment 独有的"强特征词"→ 该 segment
       （独有 = 该词只出现在一个 segment 的 representative_keywords 里）
    3) 标题完整包含某 segment 的 representative_keywords 任一短语 → 该 segment
    4) 标题与 segment.representative_keywords 有 token 重叠，按"独有词权重 1.0、共有词权重 0.2"加权打分取最高
    5) 最终仍无匹配时，回退到 packs.market.product_segments 里 ASIN 数量最多的 segment
    6) 若 packs 不可用，退化到 default_classify_fn。

    第 2/4 步的"独有词"机制是为了解决"通用词污染"：
    任何品类下，多个细分的标题里都会出现品类通用名词（如品类名本身、"new"/"large"/"mini"
    等装饰词），仅靠这些词无法区分细分。但每个细分通常各有强特征 token（材质 / 功能 / 形态
    / 授权关键字），这些 token 仅出现在该细分的 representative_keywords 里——命中独有词
    时应直接判给该 segment，避免通用词分摊导致归类漂移。
    """
    if packs is None or not packs.is_market_real() or not packs.market.product_segments:
        return default_classify_fn(title)

    segments = packs.market.product_segments
    asin_str = str(asin or "").strip()
    title_lower = str(title or "").lower()

    # 1) ASIN 精确命中
    if asin_str:
        for seg in segments:
            if asin_str in (seg.member_asins or []):
                return seg.name

    # 预计算各 segment 的 token 集合 + 全局词频（用于识别"独有词" vs "共有词"）
    import re as _re
    _TOKEN_RE = _re.compile(r"[a-z0-9一-鿿]+")
    seg_tokens_map: dict[str, set[str]] = {}
    token_seg_count: dict[str, int] = {}
    for seg in segments:
        toks: set[str] = set()
        for kw in (seg.representative_keywords or []):
            toks.update(_TOKEN_RE.findall(str(kw).lower()))
        seg_tokens_map[seg.name] = toks
        for t in toks:
            token_seg_count[t] = token_seg_count.get(t, 0) + 1

    title_tokens = set(_TOKEN_RE.findall(title_lower))

    # 2) 独有强特征词命中：标题含某 segment 独有的关键词 token（仅在一个 segment 出现）→ 直接归该 segment
    for seg in segments:
        unique_tokens = {t for t in seg_tokens_map[seg.name] if token_seg_count.get(t, 0) == 1}
        if unique_tokens & title_tokens:
            return seg.name

    # 3) 关键词完整包含（短语整体出现在标题）
    for seg in segments:
        kws = seg.representative_keywords or []
        if any(kw and kw.lower() in title_lower for kw in kws):
            return seg.name

    # 4) 加权 token 重叠：独有词权重 1.0、共有词权重 0.2，取分数最高的 segment
    #    加置信度门槛 MIN_OVERLAP_SCORE：低于则视信号不足、让位 step 5 兜底
    best_seg = None
    best_score = 0.0
    for seg in segments:
        score = 0.0
        for t in seg_tokens_map[seg.name] & title_tokens:
            score += 1.0 if token_seg_count.get(t, 0) == 1 else 0.2
        if score > best_score:
            best_score = score
            best_seg = seg
    if best_seg is not None and best_score >= MIN_OVERLAP_SCORE:
        return best_seg.name

    # 5) 最终兜底：取 member_asins 最多的 segment；但若该 segment 的 keywords
    #    与标题 token 集**完全不相交**，说明硬贴会错配，返回 "未分类" 让视觉二审兜底
    biggest = max(segments, key=lambda s: len(s.member_asins or []))
    if biggest and biggest.name:
        biggest_toks = seg_tokens_map.get(biggest.name, set())
        if not (biggest_toks & title_tokens):
            return default_classify_fn(title)
        return biggest.name

    return default_classify_fn(title)


def analyze_with_vision(
    title: str,
    image_url: str | None,
    asin: str,
    packs: "Packs | None",
    vision_client: "LLMClient | None",
    *,
    selling_points: str = "",
) -> "VisionClassifyResult | None":
    """对单个 ASIN 用图像 + 标题 + 产品卖点 + 已知 segments 让多模态 LLM 复判并提取视觉标签。

    selling_points: 卖家精灵导出里的产品卖点列文本（截断至 ~400 字），与图+标题一起做三方信号交叉判定，
                    主要用于解决"图本身判材质模糊"的边缘 case（如电子玩具外壳、亮面塑料 vs 金属）。
    返回 VisionClassifyResult 或 None（vision 不可用 / 图无效 / LLM 拒答）。
    所有判断品类无关——仅用 LLM 给出的 segment 名 + 描述 + 关键词作为候选集。
    """
    if not vision_client or not vision_client.supports_vision():
        return None
    if packs is None or not packs.is_market_real() or not packs.market.product_segments:
        return None
    if not image_url or not isinstance(image_url, str) or not image_url.lower().startswith(("http://", "https://")):
        return None

    segments = packs.market.product_segments

    import hashlib
    # prompt 版本：每当 prompt 文本（材质枚举 / 归一化规则等）发生变化，bump 此版本号让旧缓存失效
    _VISION_PROMPT_VERSION = "v8-form-priority-2026-05-08"
    h = hashlib.sha256()
    h.update(_VISION_PROMPT_VERSION.encode())
    h.update(str(image_url).encode())
    h.update(str(title or "").encode())
    h.update(str(selling_points or "").encode())
    # cache key 含 prompt 版本 + 图 + 标题 + 产品卖点；不含 segments 签名：
    # - prompt 改了（如材质枚举调整）→ 版本号变 → cache 失效重抓
    # - 卖点变了（用户上传新数据）→ cache 失效重抓，对的
    # - segments 变了（BSR LLM 重切 taxonomy）→ cache 仍命中，下游 _resolve_product_type 会做本地兜底匹配
    cache_key = f"vision_classify_{h.hexdigest()[:16]}"

    candidate_lines = []
    for s in segments:
        kws = ", ".join((s.representative_keywords or [])[:6]) or "—"
        desc = (s.description or "").strip() or "—"
        candidate_lines.append(f"- {s.name}：{desc}；关键词：{kws}")
    candidates_text = "\n".join(candidate_lines)

    selling_points_block = ""
    if selling_points and selling_points.strip():
        selling_points_block = f"产品卖点（来自卖家页面，可与图+标题交叉验证；不要复读，作为判材质/类型的辅助证据）：\n{selling_points.strip()}\n\n"

    text_part = (
        "你是亚马逊产品分类员。基于产品图片、标题、产品卖点（若有），综合判定，完成三件事。\n"
        "本任务跨品类通用——产品可能是任何类型（电子、家居、玩具、工具、配件等），"
        "请按下方通用规则判别，不要预设品类。\n\n"
        "1) 把产品归到下列候选 segment 中最贴切的一项；如果明显不符或无法判断，segment_name 返回 \"unknown\"，不要硬选。\n"
        "2.5) 提取形态标签 form_label：2-6 字**粗粒度**形态/形状/形式描述（与具体品类无关）。\n"
        "   **关键原则——粗粒度归一**：把多种相近变体归到统一抽象类，不要写过细的具体造型。\n"
        "   通用归一规则（跨品类）：\n"
        "   - 动物 / 虚构角色 / 人物 / 影视 IP 造型 → 统一归到「卡通造型」\n"
        "     （如独角兽、熊猫、恐龙、猫、狗、蜘蛛侠、米奇、皮卡丘、机器人 → 都写「卡通造型」）\n"
        "   - 各类礼盒变体 → 统一归到「礼盒式」\n"
        "     （如爆炸礼盒、毕业礼盒、折叠礼盒、惊喜盒、花束盒 → 都写「礼盒式」）\n"
        "   - 各类几何体造型 → 用通用几何名（立方体 / 圆筒 / 球形 / 多边形）\n"
        "   - 各类挂件 / 壁挂 / 吸附变体 → 「挂壁式」「磁吸式」\n"
        "   - 各类手持 / 便携变体 → 「手持式」「便携式」\n"
        "   - **跨品类兜底**：上述举例不是穷举。遇到工具/电子/家居/服装等其他品类时，\n"
        "     按相同**多变体归一**原则识别本品类的粗粒度形态类——\n"
        "     如多种插头变体 → 「插头式」，多种支架变体 → 「支架式」，\n"
        "     多种喷枪/喷头变体 → 「喷枪式」，多种照明造型变体按用途粗类（探照式/工作灯式/装饰式）。\n"
        "     宁可用稍宽的粗类，不要切碎成具体造型词。\n"
        "   - **多形态优先级（跨品类）**：当产品同时具备多种形态特征时（如手持+磁吸两用工作灯、\n"
        "     便携+夹式风扇、可手提+壁挂式音箱），**装配/连接特征**优先于**动作/姿态特征**。\n"
        "     即：磁吸式 / 夹式 / 挂钩式 / 支架式 / 三脚架式 / 壁挂式 / 吸盘式 / 卡扣式\n"
        "     **优先于** 手持式 / 便携式 / 桌面式。\n"
        "     理由：装配特征二值清晰（要么有磁吸底座要么没有）、跨品牌差异显著、分桶更有业务价值；\n"
        "     动作特征主观（多数便携工具都能「手持」），用作 form_label 会让同类产品被随机分散。\n"
        "   通用例（跨品类）：'ATM 式' / '卡通造型' / '礼盒式' / '立方体' / '字母' / '圆筒' / '磁吸式' /\n"
        "                  '手持式' / '挂壁式' / '便携式' / '挑战盒' / '球形' / '多格' / '夹式'。\n"
        "   写法要求：宁可粒度粗一档，不要写过细具体名（不要写「独角兽形」「蜘蛛侠形」「爆炸礼盒」），\n"
        "   不要写品类基础名（如「存钱罐」「灯」）。形态不明显或多种形态混合时返回空。\n"
        "2) 提取材质标签 material_label。\n"
        "   **优先**从下列常见标准枚举里选一个（保证下游分桶聚合）：\n"
        "   塑料 / 陶瓷 / 亚克力 / 金属 / 木质 / 玻璃 / 布艺 / 纸质 / 橡胶 / 硅胶 / 皮革\n"
        "   归一化规则（重要，避免下游分散桶）：\n"
        "   - PVC、ABS、聚乙烯、聚丙烯、聚碳酸酯（PC）→ 归到「塑料」\n"
        "   - 亚克力（含 PMMA、有机玻璃 / Acrylic / Plexiglass）→ 「亚克力」（独立类别，不要归塑料、不要归玻璃）\n"
        "   - 纸板、cardboard、paperboard、卡纸 → 归到「纸质」\n"
        "   - 不锈钢、铸铁、铝合金、锡、铜 → 归到「金属」\n"
        "   - 实木、竹、密度板、刨花板 → 归到「木质」\n"
        "   - 透明材质区分要点（与具体品类无关）：亚克力质轻、不易碎、有韧性、表面平滑可塑形；"
        "玻璃质重、易碎、有反光纹理、棱边锋利。\n"
        "     图看不清就看标题/卖点：含 acrylic / 亚克力 / shatterproof / unbreakable plastic / 不破裂 → 亚克力；\n"
        "     含 glass / 玻璃 / tempered → 玻璃。\n"
        "   - 复合材质：以视觉上占主导面积或卖点首要描述的材质为准；多种材质并列且无主导时返回空。\n"
        "   **若产品材质明显不属于上述常见枚举**（例如某些品类的特殊复合材料、电子元件主导、"
        "非标涂层等），可写具体材质名（2-6 字简明，如「碳纤维」「EVA 泡沫」「合成纤维」"
        "「石材」「混凝土」），但**优先使用上面枚举**——只在硬塞到枚举会失真时才自由发挥。\n"
        "   都没法判断（图看不清 + 文本无明示）→ 返回空字符串，宁缺毋滥。\n"
        "   材质是独立于主分类的维度，不要把材质塞进 segment_name。\n"
        "3) 自由描述本产品的具体类型 product_type_free（**不受候选 segment 列表限制**），8-20 个汉字，"
        "体现该产品的核心特征组合。\n"
        "   通用结构（按需组合，**不限品类**）：[材质] + [关键形态/规格] + [核心功能/主题] + [目标人群或场景]\n"
        "   写法要求：根据「图+标题+卖点」实际看到什么就写什么，不要套用任何固定品类的模板词。\n"
        "   该字段用于报告里精确展示每个产品的类型，不必硬塞 segment 名字。\n\n"
        f"候选 segment 列表：\n{candidates_text}\n\n"
        f"产品 ASIN：{asin}\n产品标题：{title or ''}\n\n"
        f"{selling_points_block}"
        "只输出 JSON：{\"segment_name\": \"<候选名 或 unknown>\", \"form_label\": \"<2-6 字**粗粒度**形态：动物/IP/人物 → 卡通造型；各种礼盒 → 礼盒式；ATM式/立方体/磁吸式/手持式 等；无法判断则空>\", \"material_label\": \"<优先用常见枚举（塑料/陶瓷/亚克力/金属/木质/玻璃/布艺/纸质/橡胶/硅胶/皮革），特殊品类可填具体材质名如「碳纤维」「EVA 泡沫」等，无法判断则空>\", \"product_type_free\": \"<8-20 字自由描述>\"}"
    )
    messages = [{
        "role": "user",
        "content": [
            {"type": "text", "text": text_part},
            {"type": "image_url", "image_url": {"url": image_url}},
        ],
    }]

    from llm.schemas import VisionClassifyResult  # 局部导入避免循环
    result = vision_client.chat_multimodal_json(
        messages=messages,
        schema=VisionClassifyResult,
        cache_key=cache_key,
        temperature=0.0,
        max_tokens=512,
        timeout=60.0,
    )
    if result is None:
        return None
    return result


def classify_with_vision(
    title: str,
    image_url: str | None,
    asin: str,
    packs: "Packs | None",
    vision_client: "LLMClient | None",
) -> str | None:
    """向后兼容包装：只返回合法 segment.name 或 None。"""
    result = analyze_with_vision(title, image_url, asin, packs, vision_client)
    if result is None:
        return None
    name = (result.segment_name or "").strip()
    if not name or name.lower() == "unknown":
        return None
    valid_names = {s.name for s in (packs.market.product_segments if packs and packs.market else [])}
    if name not in valid_names:
        return None
    return name


def classify_by_asin(asin: str, packs: Packs | None, default_value: str) -> str:
    """通过 ASIN 直接查 packs.market.product_segments[*].member_asins，找不到返回 default_value。"""
    if packs is None or not packs.is_market_real():
        return default_value
    for seg in packs.market.product_segments:
        if asin in (seg.member_asins or []):
            return seg.name
    return default_value


def neg_keywords_dict(packs: Packs | None, default: dict[str, list[str]] | None = None) -> dict[str, list[str]]:
    """把 voc.pain_clusters 转换为 {category: keywords} 形式（用于 app.py 差评计数硬编码兼容）。
    LLM 不可用时返回**空 dict**（不再回落到 LED 专属词典），上游统计段应检测空 dict 跳过。
    """
    if packs is None or not packs.is_voc_real():
        return {}
    result: dict[str, list[str]] = {}
    for cluster in packs.voc.pain_clusters:
        words: list[str] = []
        for q in (cluster.raw_quotes or [])[:5]:
            words.extend(w.lower() for w in str(q).split() if len(w) > 3)
        if words:
            result[cluster.name] = list(set(words))[:15]
        else:
            result[cluster.name] = [cluster.name.lower()]
    return result


def pos_keywords_dict(packs: Packs | None, default: dict[str, list[str]] | None = None) -> dict[str, list[str]]:
    """把 voc.praise_clusters 转换为 {category: keywords} 形式。
    LLM 不可用时返回空 dict（不再回落到 LED 通用词典）。
    """
    if packs is None or not packs.is_voc_real():
        return {}
    result: dict[str, list[str]] = {}
    for cluster in packs.voc.praise_clusters:
        words: list[str] = []
        for q in (cluster.raw_quotes or [])[:5]:
            words.extend(w.lower() for w in str(q).split() if len(w) > 3)
        if words:
            result[cluster.name] = list(set(words))[:15]
        else:
            result[cluster.name] = [cluster.name.lower()]
    return result


def price_band_descriptions(packs: Packs | None, default: dict[str, str]) -> dict[str, str]:
    if packs is None or not packs.is_market_real():
        return default
    out = {}
    for entry in packs.market.price_ladder:
        out[entry.band] = entry.description or ""
    return out or default


def display_name_for_title(packs: Packs | None, default: str = "选品") -> str:
    if packs is None:
        return default
    return packs.display_name or default


PYTHON_OWNED_DIMS = {"市场体量", "需求趋势", "竞争难度", "利润率"}
LLM_OWNED_DIMS = {"供应链", "推广压力", "风险可控性", "差异化机会"}


# 渲染层最后一道防线：把 LLM 偶尔泄露的 schema 字段路径（如 voc.pain_clusters[0].name、
# stats.gross_margin_median）从 narrative 文本里清掉。即使 prompt 收紧 + validator
# 拦截都漏过去，渲染前最后一遍 scrub 保证业务人员看到的 xlsx 100% 干净。
# 全品类通用：只匹 ASCII 顶级 Pack 名 + 路径分隔符，零品类专用词；用 (?<![A-Za-z0-9_]) 边界
# 而不是 \b（Python 把中文当 \w，"结合stats.xxx" 用 \b 抓不到）。
_SCHEMA_PATH_SCRUB_RE = re.compile(
    r"(?<![A-Za-z0-9_])(?:stats|voc|market|traffic|trend|synthesis|spec|compliance|packs?)"
    r"(?:[._][A-Za-z0-9_]+|\[\d+\])+",
    re.IGNORECASE,
)
# 检测"括号内只剩 schema 路径"的模式——把这种括号整个删掉，避免留下"...（）..."空括号
# 中文括号 （...） + 英文括号 (...)，括号内只能包含被 _SCHEMA_PATH_SCRUB_RE 匹中的内容
# （允许括号内有少量空格 + 英文等号 + 数字 + 单位字符等"赋值表达式"）
_SCHEMA_PATH_PAREN_RE = re.compile(
    r"[（(]\s*(?:[A-Za-z_][A-Za-z0-9_]*[._\[\]\d]*"
    r"(?:\s*=\s*[^（()）]*)?[、,，\s]*)+\s*[)）]",
)


def _scrub_schema_paths(text: str) -> str:
    """从中文叙述里清除 LLM 泄露的代码字段路径（如 stats.xxx, voc.pain_clusters[0].name）。

    步骤：
    1. 先把"括号内只剩 schema 路径或 xx=数值 形式的赋值"整体（含括号）删掉
    2. 再把残留的孤立 schema 路径 token 替换为空
    3. 把"清理后变得空洞的括号"——空括号 + 只剩连接词（结合/含/如/即/见/据/参考/引用/基于）
       的括号——也整段去掉
    4. 归一化连续空白和孤立标点（避免出现"。  ，"、"，，"等异常）

    输入一个字符串，返回清洗后的字符串。空字符串/None 直接返回原值。
    """
    if not text or not isinstance(text, str):
        return text or ""
    # 第 1 步：清掉"括号内只剩 schema 路径"的整段括号
    cleaned = _SCHEMA_PATH_PAREN_RE.sub("", text)
    # 第 2 步：清掉残留的裸路径
    cleaned = _SCHEMA_PATH_SCRUB_RE.sub("", cleaned)
    # 第 3 步：清掉"空括号"和"只剩连接词的括号"（路径被掏空后留下的悬空壳）
    # 空括号：（） / ()
    cleaned = re.sub(r"[（(]\s*[)）]", "", cleaned)
    # 只剩连接词的括号：（结合 推断）/（含 ）/（基于 ）等
    cleaned = re.sub(
        r"[（(]\s*(?:结合|含|如|即|见|据|参考|引用|基于|来自|对应|对照)?\s*"
        r"(?:结合|含|如|即|见|据|参考|引用|基于|来自|对应|对照|推断|分析|计算|参见)*\s*[)）]",
        "",
        cleaned,
    )
    # 第 4 步：归一化空白与孤立标点
    cleaned = re.sub(r"\s+", " ", cleaned)
    # 修掉连续标点（如"，，"/"。。"等）
    cleaned = re.sub(r"([，。、；])\s*[，。、；]+", r"\1", cleaned)
    # 去掉首尾的孤立标点
    cleaned = re.sub(r"^[、，。；\s]+", "", cleaned)
    cleaned = re.sub(r"[、，；\s]+$", "", cleaned)
    return cleaned.strip()


def sheet10_dimension_reasons(packs: Packs | None, default_rows: list[tuple]) -> list[tuple]:
    """混合模式八维理由。数字由 Python（default_rows）提供，叙述由 LLM 补充。
    - 前 4 维（市场体量/需求趋势/竞争难度/利润率）：以 Python 数字事实为主；
      如 LLM 的 `recommendation_reasons` 有对应 narrative，拼在事实后增加判断/对比叙述
    - 后 4 维（供应链/推广压力/风险可控性/差异化机会）：优先用 LLM `sheet10_final_verdict.dimension_reasons`；
      退化时用 `recommendation_reasons`；再退化用 Python 兜底句

    default_rows 允许是 8 维全量（推荐）；若 default_rows 缺少某个维度，该维度直接跳过不渲染。
    """
    default_map = {d: r for (d, r) in default_rows}

    # LLM recommendation_reasons：八维都可能有 narrative（新字段，Python 事实 + LLM 叙述混拼）
    rec_narratives: dict[str, str] = {}
    # LLM sheet10_final_verdict.dimension_reasons：只有后 4 维有（老字段，可直接替换整句）
    verdict_reasons: dict[str, str] = {}
    if packs is not None and packs.is_synthesis_real():
        for rr in getattr(packs.synthesis, 'recommendation_reasons', []) or []:
            if (rr.narrative or "").strip():
                rec_narratives[rr.dimension] = rr.narrative.strip()
        for dr in packs.synthesis.sheet10_final_verdict.dimension_reasons or []:
            if dr.dimension in LLM_OWNED_DIMS and (dr.reason_with_evidence or "").strip():
                verdict_reasons[dr.dimension] = dr.reason_with_evidence.strip()

    out: list[tuple] = []

    _PCT_RE = re.compile(r'\d+(?:\.\d+)?\s*%')

    # 前 4 维：Python 事实 + （可选）LLM 叙述拼接
    for d in ("市场体量", "需求趋势", "竞争难度", "利润率"):
        fact = default_map.get(d, "").strip()
        narr = rec_narratives.get(d, "").strip()
        if narr and _PCT_RE.search(narr):
            log.warning("[sheet10] 维度 %s 的 LLM narrative 含百分号被剔除：%s", d, narr[:80])
            narr = ""
        # 渲染前最后一道防线：清掉 LLM 残留的 schema 字段路径
        narr = _scrub_schema_paths(narr) if narr else ""
        if fact and narr:
            sep = "" if fact.endswith(("。", ".", "!", "？", "?")) else "。"
            out.append((d, f"{fact}{sep}{narr}"))
        elif fact:
            out.append((d, fact))
        elif narr:
            out.append((d, narr))

    # 后 4 维：verdict_reasons > recommendation_reasons > Python 兜底
    for d in ("供应链", "推广压力", "风险可控性", "差异化机会"):
        text = verdict_reasons.get(d) or rec_narratives.get(d) or default_map.get(d, "")
        text = text.strip()
        # 同样在渲染前 scrub（verdict_reasons / rec_narratives 都可能含泄露）
        text = _scrub_schema_paths(text) if text else ""
        if text:
            out.append((d, text))

    return out


def sheet10_headline(packs: Packs | None, default: str) -> str:
    if packs is None or not packs.is_synthesis_real():
        return default
    return packs.synthesis.sheet10_final_verdict.headline or default


def sheet6_priority_matrix(packs: Packs | None) -> list[dict]:
    """返回 [{segment, priority, action_plan, improvements}]。
    LLM 不可用时直接返回空列表 —— 渲染层应跳过整段，不再渲染占位文案。
    action_plan 为空字符串的条目也会被过滤掉（空则跳过）。

    返回前对 action_plan / improvements 走一次 _scrub_schema_paths，把 LLM 偶尔泄露的
    `voc.xxx` / `stats.xxx` / `market.xxx` 字段路径清理掉——保证下游所有读取点（Sheet 5
    「七、卖点对比矩阵」/ Sheet 6「产品上新方向」path A / Sheet 10「五、综合升级建议」）
    都拿到干净文本，无需各自 scrub。"""
    if packs is None or not packs.is_synthesis_real():
        return []
    out = []
    for item in packs.synthesis.sheet6_priority_matrix:
        if not (item.action_plan or "").strip() and not (item.improvements or []):
            continue
        # 过滤兜底 segment：「其他/通用款」是 _ensure_full_coverage 内部桶，业务展示无意义
        if (item.segment or "").strip() == "其他/通用款":
            continue
        out.append({
            "segment": item.segment,
            "priority": item.priority,
            "action_plan": _scrub_schema_paths(item.action_plan or ""),
            "improvements": [_scrub_schema_paths(s) for s in (item.improvements or [])],
        })
    return out


def spec_dimensions(packs: Packs | None) -> list[dict]:
    """返回品类识别出的关键规格维度（供 app.py 的竞品规格列动态渲染）。
    LLM 不可用时返回空列表 —— 渲染层应回退到只显示基础字段（标题/价格/评分）。
    每项：{"name": ..., "unit": ..., "extract_patterns": [...], "importance": "核心|辅助"}
    """
    if packs is None or not packs.is_spec_real():
        return []
    return [
        {
            "name": d.name,
            "unit": d.unit,
            "extract_patterns": d.extract_patterns,
            "importance": d.importance,
        }
        for d in packs.spec.spec_dimensions
    ]


def extract_specs_for_title(title: str, bullets: str, packs: Packs | None) -> dict[str, str]:
    """对单个 ASIN 按 SpecPack 的维度抓取具体规格值（如 {"最大压力": "150PSI"}）。
    LLM 不可用时返回空 dict。
    """
    if packs is None or not packs.is_spec_real() or not packs.spec.spec_dimensions:
        return {}
    return extract_specs_by_dimensions(title, bullets, list(packs.spec.spec_dimensions))


def compliance_certs(packs: Packs | None) -> list[dict]:
    """返回 CompliancePack 的认证列表。每项：
    {"name", "mandatory", "applies_to", "risk_if_missing"}
    LLM 不可用时返回空列表。
    """
    if packs is None or not packs.is_compliance_real():
        return []
    return [
        {
            "name": c.name,
            "mandatory": c.mandatory,
            "applies_to": c.applies_to,
            "risk_if_missing": c.risk_if_missing,
        }
        for c in packs.compliance.required_certifications
    ]


def compliance_return_info(packs: Packs | None) -> tuple[float, list[str]]:
    """返回 (typical_return_rate_pct, top_return_reasons)。
    LLM 不可用时返回 (0.0, [])。
    """
    if packs is None or not packs.is_compliance_real():
        return 0.0, []
    return packs.compliance.typical_return_rate_pct, list(packs.compliance.top_return_reasons)


def upgrade_directions(packs: Packs | None) -> list[dict]:
    """返回 [{dimension, target_spec, justification, supporting_fields}]。"""
    if packs is None or not packs.is_synthesis_real():
        return []
    return [
        {
            "dimension": d.dimension,
            "target_spec": d.target_spec,
            "justification": d.justification,
            "supporting_fields": d.supporting_fields,
        }
        for d in packs.synthesis.upgrade_directions
    ]


def pricing_segment_narratives(packs: Packs | None) -> dict[str, str]:
    """Sheet 4 推荐入场价表：每个细分对应的 LLM 定性叙述。
    返回 {segment_name: narrative}。LLM 不可用或该细分未覆盖时返回空 dict / 缺 key。"""
    if packs is None or not packs.is_synthesis_real():
        return {}
    out: dict[str, str] = {}
    for psi in getattr(packs.synthesis, 'pricing_segment_insights', []) or []:
        if psi.segment and (psi.narrative or '').strip():
            out[psi.segment] = psi.narrative.strip()
    return out


def price_band_narratives(packs: Packs | None) -> dict[str, str]:
    """Sheet 4 各价格带入场建议：每档价格带对应的 LLM 定性叙述。
    返回 {band_label: narrative}，band_label 如 '$15-25'。"""
    if packs is None or not packs.is_synthesis_real():
        return {}
    out: dict[str, str] = {}
    for pbi in getattr(packs.synthesis, 'price_band_insights', []) or []:
        if pbi.band and (pbi.narrative or '').strip():
            out[pbi.band.strip()] = pbi.narrative.strip()
    return out


def sheet5_improvement_plan(packs: Packs | None) -> list[dict]:
    """Sheet 5「六、核心改进方向总结」的 LLM 改进计划。
    返回 [{priority, pain_name, root_cause, action_items: list[str], target_metric, supporting_fields}]。

    优先级：
    1. packs.sheet5_plans（独立 Sheet5ImprovementAnalyzer 输出，独占 8K token，几乎不会被截断）
    2. packs.synthesis.sheet5_improvement_plan（Synthesizer 巨型输出里的字段，可能因 token 截断丢失最后几条）
    3. 都没有则返回空列表，渲染层显示「本条 LLM 综合未覆盖」提示

    返回前对 root_cause / action_items / target_metric 走一次 _scrub_schema_paths，
    防止 LLM 偶尔在改进计划里泄露 schema 字段路径（已观察到 voc.pain_clusters[0].name
    被写进 justification 的情况）。
    """
    if packs is None:
        return []

    def _scrub_plan(plan: dict) -> dict:
        """对单条 plan 内的所有叙述字段过 schema-path scrub。"""
        return {
            **plan,
            "root_cause": _scrub_schema_paths(plan.get("root_cause", "")),
            "action_items": [_scrub_schema_paths(a) for a in (plan.get("action_items") or [])],
            "target_metric": _scrub_schema_paths(plan.get("target_metric", "")),
        }

    # 优先用独立 analyzer 的输出
    if packs.sheet5_plans:
        return [_scrub_plan(p) for p in packs.sheet5_plans]
    # 兜底：用 Synthesizer 的输出（兼容旧路径）
    if not packs.is_synthesis_real():
        return []
    out = []
    for sip in getattr(packs.synthesis, 'sheet5_improvement_plan', []) or []:
        out.append(_scrub_plan({
            "priority": sip.priority,
            "pain_name": sip.pain_name,
            "root_cause": sip.root_cause,
            "action_items": list(sip.action_items),
            "target_metric": sip.target_metric,
            "supporting_fields": list(sip.supporting_fields),
        }))
    return out


def sheet1_sales_tier_narratives(packs: Packs | None) -> dict[str, str]:
    """Sheet 1「五、销量与价格关系分析」3 档（top25/middle/bottom25）的 LLM 结论。
    返回 {tier: narrative}，LLM 不可用或某档缺失时对应 key 不存在，渲染层应有硬编码兜底。"""
    if packs is None or not packs.is_synthesis_real():
        return {}
    out: dict[str, str] = {}
    for item in getattr(packs.synthesis, 'sheet1_sales_tier_narratives', []) or []:
        tier = (item.tier or "").strip().lower()
        narr = (item.narrative or "").strip()
        if tier in ("top25", "middle", "bottom25") and narr:
            out[tier] = narr
    return out


def sheet2_risk_bullets(packs: Packs | None) -> list[str]:
    """Sheet 2「五、竞争进入建议」风险提示的 LLM bullet 列表。
    返回 list[str]，LLM 不可用或空时返回 []，渲染层应有硬编码兜底。"""
    if packs is None or not packs.is_synthesis_real():
        return []
    return [s.strip() for s in getattr(packs.synthesis, 'sheet2_risk_bullets', []) or [] if (s or "").strip()]


def sheet1_market_conclusions(packs: Packs | None) -> dict[str, str]:
    """Sheet 1「四、市场分析结论」3 行（scale/structure/concentration）的 LLM 定性判断。
    返回 {key: narrative}，LLM 不可用或某 key 缺失时对应 key 不存在，渲染层走 Python 阈值兜底。"""
    if packs is None or not packs.is_synthesis_real():
        return {}
    out: dict[str, str] = {}
    for item in getattr(packs.synthesis, 'sheet1_market_conclusions', []) or []:
        k = (item.key or "").strip().lower()
        narr = (item.narrative or "").strip()
        if k in ("scale", "structure", "concentration") and narr:
            out[k] = narr
    return out


def sheet2_brand_strategies(packs: Packs | None) -> dict[str, tuple[str, str]]:
    """Sheet 2「六、TOP品牌竞争策略分析」3 档（head/mid/entry）的 LLM 客户群体 + 策略建议。
    返回 {tier: (customer_segment, strategy)}，LLM 不可用或某档缺失时对应 key 不存在。"""
    if packs is None or not packs.is_synthesis_real():
        return {}
    out: dict[str, tuple[str, str]] = {}
    for bs in getattr(packs.synthesis, 'sheet2_brand_strategies', []) or []:
        tier = (bs.tier or "").strip().lower()
        cs = (bs.customer_segment or "").strip()
        strat = (bs.strategy or "").strip()
        if tier in ("head", "mid", "entry") and (cs or strat):
            out[tier] = (cs, strat)
    return out
