"""5 个 Insight Pack 的 pydantic schema 定义。

设计原则：
- 字段尽量给默认值，缺失字段不阻断校验（LLM 偶尔遗漏不至于全军覆没）
- 数值类字段全部由 Python 代码算好传给 LLM 或写报告，**LLM 不负责造数**
- 文案类字段（reasoning/description/summary）由 LLM 生成
- supporting_fields 强制声明引用，从机制上防幻觉
"""
from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field


# ============ 冒烟用 ============

class SmokeResponse(BaseModel):
    ok: bool = Field(..., description="是否成功")
    echo: str = Field(..., description="回显的内容")


# ============ 视觉分类结果（图像二审） ============

class VisionClassifyResult(BaseModel):
    """多模态 LLM 对单个产品（图+标题）的结构化复判。
    segment_name 为 "unknown" 表示模型也判不出主分类，但材质/形态标签仍可保留。"""
    segment_name: str = Field(..., description="选中的 segment 名（必须是候选列表里之一），或 'unknown'")
    material_label: str = Field("", description="材质标签，如 塑料/陶瓷/亚克力/金属/木质；无法判断则空")
    form_label: str = Field(
        "",
        description="形态标签：2-6 字精简形态/形状/形式描述（与具体品类无关，纯描述视觉形态），"
                    "示例：'ATM式' / '猪形' / '立方体' / '字母' / '圆筒' / '磁吸式' / '手持式' / '挂壁式' / '挑战盒' 等。"
                    "用于下游分桶时与 segment.form_attribute 做强匹配。无法判断则空。",
    )
    product_type_free: str = Field(
        "",
        description="不受候选 segment 列表限制的本产品具体类型自由描述（8-20 个汉字），"
                    "体现该产品的核心特征组合（材质 + 形态 + 主题/功能 + 目标人群）。"
                    "用于 Sheet 3 N 列的精准产品类型展示，不影响 segment 聚合分析。"
                    "无法判断则返回空字符串。",
    )


class ProductVisualLabel(BaseModel):
    asin: str = Field(..., description="ASIN")
    material_label: str = Field("", description="材质标签")
    form_label: str = Field("", description="形态标签：2-6 字精简形态描述（与具体品类无关）")
    product_type_free: str = Field(
        "",
        description="视觉 LLM 自由描述的产品类型（不受 BSR segment 列表限制），"
                    "用于 Sheet 3 N 列展示。空字符串表示视觉未给，下游 fallback 到 BSR segment.name。",
    )


# ============ Pack 1: Market Insight ============

class ProductSegment(BaseModel):
    name: str = Field(..., description="产品细分名称（中文）")
    description: str = Field("", description="该细分的特征描述")
    member_asins: list[str] = Field(default_factory=list, description="归属此细分的 ASIN 列表")
    representative_keywords: list[str] = Field(default_factory=list, description="该细分的产品标题关键词")
    material_attribute: str = Field(
        "",
        description=(
            "若该 segment 由材质维度定义，填该材质（用与 vision material_label 一致的术语，"
            "例如 塑料/金属/木质/亚克力 等）；若不是材质维度切分则留空。"
            "**重要：本字段是材质信息的唯一承载位置——name 字段中绝对不能出现任何材质词，"
            "不论以前缀、中段、括号、引号、连字符、附注（如『XX材质』/『XX款』）任何形式，"
            "都必须把材质从 name 移除并改填到这里**"
        ),
    )
    form_attribute: str = Field(
        "",
        description=(
            "若该 segment 由形态维度定义（与具体品类无关的形状/形式描述），填 2-6 字的形态词；"
            "如 'ATM式' / '猪形' / '立方体' / '字母' / '圆筒' / '磁吸式' / '手持式' / '挂壁式' 等。"
            "若该 segment 不以形态切分则留空。下游分桶时与 vision form_label 做强匹配。"
        ),
    )
    members: list[int] = Field(
        default_factory=list,
        description=(
            "归属此 segment 的产品序号列表（1-based，对应 LLM 输入的产品序号）。"
            "TaxonomyAggregator v7-merged 用此字段回传 LLM 的语义判桶结果，"
            "下游代码会把 idx 反查成 ASIN 写入 member_asins。"
        ),
    )


class PositioningTier(BaseModel):
    tier: str = Field(..., description="定位档，如 低端/中端/高端/入门/专业")
    price_range_desc: str = Field("", description="价格范围描述")
    representative_brands: list[str] = Field(default_factory=list)
    playbook: str = Field("", description="该档的常见打法/卖点")


class PriceBandEntry(BaseModel):
    band: str = Field(..., description="价格带（如 <$15 / $15-25 / $25-35 / ...）")
    description: str = Field("", description="该价格带的市场角色描述")
    competition_intensity: str = Field("中", description="竞争强度描述，如 低/中/高/极高/中高")
    profit_room: str = Field("中", description="利润空间描述，如 低/中/高/中高/中低")
    representative_asins: list[str] = Field(default_factory=list)


class ChinaSellerPatterns(BaseModel):
    concentrated_segments: list[str] = Field(default_factory=list, description="中国卖家集中的细分")
    concentrated_bands: list[str] = Field(default_factory=list, description="中国卖家集中的价格带")
    typical_strategies: str = Field("", description="中国卖家的典型打法")


class NewEntryWindow(BaseModel):
    segment: str = Field(..., description="新品有机会的细分")
    evidence_asins: list[str] = Field(default_factory=list, description="近期上架仍存活的代表 ASIN")
    success_rate_note: str = Field("", description="存活率叙述性观察")


class MarketInsightPack(BaseModel):
    product_segments: list[ProductSegment] = Field(default_factory=list)
    visual_labels: list[ProductVisualLabel] = Field(default_factory=list)
    positioning_tiers: list[PositioningTier] = Field(default_factory=list)
    price_ladder: list[PriceBandEntry] = Field(default_factory=list)
    china_seller_patterns: ChinaSellerPatterns = Field(default_factory=ChinaSellerPatterns)
    new_entry_windows: list[NewEntryWindow] = Field(default_factory=list)
    brand_concentration_reading: str = Field("", description="品牌集中度的叙述性解读")
    category_display_name: str = Field("", description="品类的中文展示名（用于报告标题）")
    is_fallback: bool = Field(False, description="是否为降级产出")


# ============ Pack 2: Voice of Customer ============

class PainCluster(BaseModel):
    name: str = Field(..., description="痛点类别名（中文）")
    description: str = Field("", description="痛点描述")
    raw_quotes: list[str] = Field(default_factory=list, description="评论原文引用，用于追溯")
    affected_asins: list[str] = Field(default_factory=list)
    severity: str = Field("中", description="严重程度，如 高/中/低")
    frequency_pct: float = Field(0.0, ge=0, le=100, description="该类痛点在差评中的占比 0-100")
    keywords: list[str] = Field(default_factory=list, description="该痛点的5-8个代表关键词，用于按ASIN评论文本真实匹配")


class PraiseCluster(BaseModel):
    name: str = Field(..., description="好评卖点名（中文）")
    description: str = Field("")
    raw_quotes: list[str] = Field(default_factory=list)
    affected_asins: list[str] = Field(default_factory=list)


class EmergingComplaint(BaseModel):
    complaint: str = Field(..., description="新出现的痛点描述")
    first_seen_recent: bool = Field(True, description="是否仅在近期评论中出现")
    evidence_quotes: list[str] = Field(default_factory=list)


class UnmetNeed(BaseModel):
    need: str = Field(..., description="用户提到但市场未满足的需求")
    evidence_quotes: list[str] = Field(default_factory=list)


class DealBreaker(BaseModel):
    reason: str = Field(..., description="导致退货/换货的核心原因")
    quote: str = Field("", description="代表性评论原文")
    return_related: bool = Field(False)


class AsinQualitySignal(BaseModel):
    model_config = {"coerce_numbers_to_str": True}
    asin: str
    positive_pct: float = Field(0.0, ge=0, le=100)
    negative_pct: float = Field(0.0, ge=0, le=100)
    top_pain: str | None = Field("")
    top_praise: str | None = Field("")


class VOCPack(BaseModel):
    pain_clusters: list[PainCluster] = Field(default_factory=list)
    praise_clusters: list[PraiseCluster] = Field(default_factory=list)
    emerging_complaints: list[EmergingComplaint] = Field(default_factory=list)
    unmet_needs: list[UnmetNeed] = Field(default_factory=list)
    deal_breakers: list[DealBreaker] = Field(default_factory=list)
    quality_signal_by_asin: list[AsinQualitySignal] = Field(default_factory=list)
    is_fallback: bool = Field(False)


# ============ Pack 3: Traffic Insight ============

class SearchIntentCluster(BaseModel):
    intent_type: str = Field("其他", description="搜索意图类型，如 功能向/场景向/品牌向/价格向/属性向/其他")
    keywords: list[str] = Field(default_factory=list)
    total_volume: int = Field(0, ge=0, description="该聚类的总搜索量（由代码统计后填入）")
    competition_note: str = Field("", description="竞争激烈度叙述")


class NicheGap(BaseModel):
    keyword: str
    volume: int = Field(0, ge=0)
    why_gap_exists: str = Field("")


class TrafficInsightPack(BaseModel):
    # 卖家精灵 ReverseASIN 源数据里没有「广告占比 / 自然流量占比」这两个单列字段，
    # 所以不再定义 ads_heavy_keywords / organic_opportunity_keywords 两个列表
    # （历史版本保留过这两个字段，会被 LLM 在缺少数据时编造"80% 广告位"等假数字）。
    # 推广压力维度改由 stats.promo_*（PPC/广告竞品数/点击占比/转化占比/SPR）承载。
    search_intent_clusters: list[SearchIntentCluster] = Field(default_factory=list)
    niche_gaps: list[NicheGap] = Field(default_factory=list)
    traffic_strategy_summary: str = Field("")
    is_fallback: bool = Field(False)


# ============ Pack 4: Trend Insight ============

class LifecycleStage(BaseModel):
    stage: str = Field("未知", description="生命周期阶段，如 导入期/成长期/成熟期/衰退期/未知")
    confidence: str = Field("中", description="置信度，如 高/中/低")
    evidence: str = Field("")


class Seasonality(BaseModel):
    pattern: str = Field("未知", description="季节性模式，如 显著季节性/弱季节性/无季节性/未知")
    peak_months: list[str] = Field(default_factory=list)
    trough_months: list[str] = Field(default_factory=list)
    amplitude_pct: float = Field(0.0, description="峰谷振幅占均值百分比")


class DemandDirection(BaseModel):
    direction: str = Field("未知", description="需求方向，如 上升/稳定/下滑/震荡/未知")
    inflection_points: list[str] = Field(default_factory=list)
    recent_yoy_change: str = Field("", description="近期同比变化叙述")


class TrendRiskSignal(BaseModel):
    signal: str
    severity: str = Field("中", description="严重程度，如 高/中/低")
    evidence: str = Field("")


class TrendInsightPack(BaseModel):
    lifecycle_stage: LifecycleStage = Field(default_factory=LifecycleStage)
    seasonality: Seasonality = Field(default_factory=Seasonality)
    demand_direction: DemandDirection = Field(default_factory=DemandDirection)
    risk_signals: list[TrendRiskSignal] = Field(default_factory=list)
    category_summary: str = Field("")
    is_fallback: bool = Field(False)


# ============ Pack 5: Spec Insight（规格维度识别，全品类通用） ============

class SpecDimension(BaseModel):
    name: str = Field(..., description="规格维度中文名，如 '最大压力' / '电池容量'")
    unit: str = Field("", description="单位，如 'PSI' / 'mAh' / 'GPM'")
    extract_patterns: list[str] = Field(
        default_factory=list,
        description="用于从标题/bullets 抓取具体值的正则，如 '(\\d+)\\s*psi'",
    )
    importance: str = Field("辅助", description="重要性：核心/辅助")
    # Python 侧在全量 BSR 上抓完后回填的实际命中数（LLM 填 0 即可）
    match_count: int = Field(0, description="该维度在 BSR 全量 ASIN 中的命中数，Python 回填用于过滤低信号维度")
    sample_values: list[str] = Field(default_factory=list, description="Python 回填：命中的数值字符串样本，用于算中位/P75")


class AsinSpecSample(BaseModel):
    model_config = {"coerce_numbers_to_str": True}
    asin: str
    specs: dict[str, str] = Field(default_factory=dict, description="{维度名: 该 ASIN 的具体值}")


class SpecInsightPack(BaseModel):
    spec_dimensions: list[SpecDimension] = Field(default_factory=list)
    representative_specs_by_asin: list[AsinSpecSample] = Field(default_factory=list)
    is_fallback: bool = Field(False)


# ============ Pack 6: Compliance（品类合规清单，全品类通用） ============

class RequiredCertification(BaseModel):
    name: str = Field(..., description="认证名，如 'UL 8750' / 'FCC' / 'CPSIA'")
    mandatory: bool = Field(False, description="是否为必须认证（缺失会被下架）")
    applies_to: str = Field("", description="适用类型，如 'LED 灯具' / '含蓝牙电子产品'")
    risk_if_missing: str = Field("", description="缺失后果，如 '亚马逊下架' / '海关扣留'")


class CompliancePack(BaseModel):
    required_certifications: list[RequiredCertification] = Field(default_factory=list)
    typical_return_rate_pct: float = Field(0.0, ge=0, le=100, description="该品类历史退货率（估算）")
    top_return_reasons: list[str] = Field(default_factory=list)
    is_fallback: bool = Field(False)


# ============ Pack 7: Lifecycle Insight（生命周期 5 维评分 → 每维详细分析，全品类通用） ============

class DimensionAnalysis(BaseModel):
    """生命周期某一维度的 LLM 详细分析（2-3 句）。"""
    dimension: str = Field("", description="维度名（销量趋势 / 搜索趋势 / 新品贡献与成功率 / 品牌集中度 / 价格趋势）")
    analysis: str = Field("", description="2-3 句详细分析：数据是什么 → 说明什么 → 对生命周期判断意味着什么")


class LifecycleInsight(BaseModel):
    """Sheet 8 第四段「生命周期判断」的 LLM 综合判定 + 每维详细分析 + 综合结论。

    要求 LLM 对每个有数据的维度独立产出 2-3 句分析，引用本品类真实数据（品牌/价格/关键词）。
    并综合 5 维组合给出生命周期阶段判定（不是简单按 avg 算）。
    """
    stage: str = Field(
        "",
        description="生命周期阶段（必须从 4 档里选：成长期 / 成熟期 / 成熟晚期 / 衰退期）。"
                    "禁止简单按 avg 算，要综合 5 维组合判断"
    )
    stage_reasoning: str = Field(
        "",
        description="一句话（1-2 句，30-80 字）简短解释为什么判定为该阶段，"
                    "必须引用 2-3 个维度的具体数据组合（如「销量 +14% + 搜索 +18% 但效率比 0.84 < 1，"
                    "属于成长后期，归成熟期」）。仅给判定逻辑，不给建议"
    )
    dimension_analyses: list[DimensionAnalysis] = Field(
        default_factory=list,
        description="每维一段详细分析；缺失数据的维度可不产出或注明"
    )
    verdict: str = Field("", description="进入决策（✅ 推荐进入 / ⚠️ 谨慎进入 / ❌ 不建议进入）")
    overall_conclusion: str = Field(
        "",
        description="综合结论 + 选品建议（一段 3-5 句，把 5 维数据组合在一起说，"
                    "解释为什么判定为该阶段，给出可执行建议）"
    )
    is_fallback: bool = Field(False)


# ============ Strategy Synthesis ============

class SupportingClaim(BaseModel):
    """带数据引用的结论。supporting_fields 引用 Pack 字段路径（如 'voc.pain_clusters[0]'）。"""
    claim: str = Field(..., description="结论文本")
    supporting_fields: list[str] = Field(default_factory=list, description="引用的 Pack 字段路径")


class EntryRecommendation(BaseModel):
    recommended_segment: str = Field("", description="推荐入场的细分名")
    reasoning: str = Field("", description="推荐理由")
    supporting_fields: list[str] = Field(default_factory=list)


class UpgradeDirection(BaseModel):
    # LLM 偶尔把 target_spec 输出成 numeric（如 7.975 / 1400.0）而非 string，
    # 触发 pydantic v2 严格类型校验失败 → 整个 Synthesizer 走降级、Sheet 4/5/10 大量内容缺失。
    # 这里允许数字自动转字符串，让 LLM 数字输出也能通过校验。
    model_config = {"coerce_numbers_to_str": True}

    dimension: str = Field(..., description="升级维度（如 电池容量 / 防水等级 / 噪音控制）")
    target_spec: str = Field("", description="目标规格（数值由代码算好传入，LLM 只造句）")
    justification: str = Field("", description="升级依据")
    supporting_fields: list[str] = Field(default_factory=list)


class DifferentiationAngle(BaseModel):
    angle: str = Field(..., description="差异化角度")
    rationale: str = Field("")
    supporting_fields: list[str] = Field(default_factory=list)


class Sheet6PriorityItem(BaseModel):
    segment: str
    priority: str = Field("P3", description="优先级，如 P1/P2/P3/P4")
    action_plan: str = Field("")
    improvements: list[str] = Field(default_factory=list)


class Sheet10DimensionReason(BaseModel):
    dimension: str
    reason_with_evidence: str = Field("")


class Sheet10Verdict(BaseModel):
    headline: str = Field("", description="综合结论一句话标题")
    dimension_reasons: list[Sheet10DimensionReason] = Field(default_factory=list)


class RecommendationReason(BaseModel):
    """Sheet 10「一、推荐理由总结」八维各一条的 LLM 叙述。
    Python 负责拼数字事实，LLM 只负责写 1-2 句"对本品类的判断/对比"叙述。"""
    dimension: str = Field(..., description="八维之一：市场体量/需求趋势/竞争难度/利润率/供应链/推广压力/风险可控性/差异化机会")
    narrative: str = Field("", description="1-2 句叙述，含本品类特征判断 + 对比 + 前因后果")
    supporting_fields: list[str] = Field(default_factory=list)


class PricingSegmentInsight(BaseModel):
    """Sheet 4 第一个 section「推荐入场价」每个细分的 LLM 叙述。
    Python 负责 P25/中位/P75/SKU 数/单品收益/毛利这些数字，LLM 补一句该细分特征。"""
    segment: str = Field(..., description="产品细分名（必须匹配 market.product_segments 里的一条）")
    narrative: str = Field("", description="1 句话说清该细分的定位/竞争/中国卖家适配性/改良空间等")
    supporting_fields: list[str] = Field(default_factory=list)


class PriceBandInsight(BaseModel):
    """Sheet 4 「各价格带竞争强度分析」每档的 LLM 叙述。"""
    band: str = Field(..., description="价格区间（如 $15-25）")
    narrative: str = Field("", description="1 句话说清该价格带的竞品画像、利润空间、新品入场可行性")
    supporting_fields: list[str] = Field(default_factory=list)


class Sheet5ImprovementPlan(BaseModel):
    """Sheet 5「六、核心改进方向总结」每个痛点对应的 LLM 改进计划。
    Python 从 pain_clusters 前 4 条按差评条数排 P1-P4；LLM 提供 root_cause 诊断 + 3-5 条可落地建议 + 目标指标。
    """
    priority: str = Field(..., description="P1/P2/P3/P4")
    pain_name: str = Field(..., description="必须匹配 voc.pain_clusters[*].name 中的一条")
    root_cause: str = Field("", description="1 句话根因诊断（设计/工艺/用料/配件等）")
    action_items: list[str] = Field(default_factory=list,
        description="3-5 条可落地的改进建议（含具体规格数值，如 将充电电流提升至 5A 以上，禁止跨品类词）")
    target_metric: str = Field("", description="改进目标指标（如 将 1-2 星占比从 33% 降至 15% 以下）")
    supporting_fields: list[str] = Field(default_factory=list)


class Sheet5ImprovementPlanList(BaseModel):
    """Sheet5ImprovementAnalyzer 的独立 LLM 调用输出 wrapper。

    把 sheet5_improvement_plan 从 Synthesizer 巨型输出里拆出来独占 8K token 预算，
    避免「LLM 写到第 3 条 plan 时 token 接近 8K 上限，第 4 条被截断丢弃」。
    """
    plans: list[Sheet5ImprovementPlan] = Field(default_factory=list,
        description="P1-P4 共 4 条改进计划")
    is_fallback: bool = Field(False)


class Sheet1MarketConclusion(BaseModel):
    """Sheet 1「四、市场分析结论」3 行的一句定性判断。"""
    key: str = Field(..., description="结论键：scale（市场规模）/ structure（价格结构）/ concentration（集中度），仅这三个字面值")
    narrative: str = Field("", description="1 句话定性判断，≤ 40 字，必须贴合本品类数据")


class SalesTierNarrative(BaseModel):
    """Sheet 1「五、销量与价格关系分析」每档销量分层的一句结论。"""
    tier: str = Field(..., description="销量分层：top25 / middle / bottom25（仅这三个字面值）")
    narrative: str = Field("", description="1 句话结论，基于该档均价/评分/SKU 数给出品类特性判断")


class BrandTierStrategy(BaseModel):
    """Sheet 2「六、TOP品牌竞争策略分析」每档品牌的客户群体 + 竞争策略建议。"""
    tier: str = Field(..., description="品牌层级：head / mid / entry（仅这三个字面值）")
    customer_segment: str = Field("", description="客户群体画像（如 '品牌忠诚用户' / '性价比敏感用户'）")
    strategy: str = Field("", description="1 句话竞争策略建议，贴合本品类特征")


class StrategySynthesis(BaseModel):
    entry_recommendation: EntryRecommendation = Field(default_factory=EntryRecommendation)
    upgrade_directions: list[UpgradeDirection] = Field(default_factory=list)
    differentiation_angles: list[DifferentiationAngle] = Field(default_factory=list)
    sheet6_priority_matrix: list[Sheet6PriorityItem] = Field(default_factory=list)
    sheet10_final_verdict: Sheet10Verdict = Field(default_factory=Sheet10Verdict)
    recommendation_reasons: list[RecommendationReason] = Field(default_factory=list,
        description="Sheet 10 八维推荐理由的 LLM 叙述（Python 的数字事实会在渲染层拼接到 narrative 前）")
    pricing_segment_insights: list[PricingSegmentInsight] = Field(default_factory=list,
        description="Sheet 4「推荐入场价」表每个细分的一句叙述")
    price_band_insights: list[PriceBandInsight] = Field(default_factory=list,
        description="Sheet 4「各价格带竞争强度分析」每档的一句叙述")
    sheet5_improvement_plan: list[Sheet5ImprovementPlan] = Field(default_factory=list,
        description="Sheet 5「六、核心改进方向总结」4 条改进计划（按痛点频次 P1-P4）")
    sheet1_sales_tier_narratives: list[SalesTierNarrative] = Field(default_factory=list,
        description="Sheet 1「五、销量与价格关系分析」3 档（top25/middle/bottom25）的一句结论")
    sheet2_risk_bullets: list[str] = Field(default_factory=list,
        description="Sheet 2「五、竞争进入建议」风险提示 3-5 条短 bullet，贴合本品类实际风险")
    sheet2_brand_strategies: list[BrandTierStrategy] = Field(default_factory=list,
        description="Sheet 2「六、TOP品牌竞争策略分析」3 档品牌（head/mid/entry）的客户群体 + 策略建议")
    sheet1_market_conclusions: list[Sheet1MarketConclusion] = Field(default_factory=list,
        description="Sheet 1「四、市场分析结论」3 行定性判断（scale/structure/concentration）")
    is_fallback: bool = Field(False)


# ============ LabelMerger（轻量 form_label 语义合并） ============

class LabelMergeResult(BaseModel):
    """轻量 LLM 标签合并 analyzer 输出。

    merge_mapping: {from_label: to_label}——把 from 替换为 to。
    不需合并的标签不入 mapping（保持原样）。
    """
    merge_mapping: dict[str, str] = Field(default_factory=dict,
        description="语义重叠标签的合并 mapping。key 是要被替换的标签，value 是合并后的目标标签。")
    is_fallback: bool = Field(False)
