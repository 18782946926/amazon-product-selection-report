from __future__ import annotations

import hashlib
import logging
import threading
from typing import Any

import pandas as pd

# 视觉 LLM 信号量按 api_key 隔离：每个 key 独立 8 路上限，保护单 key 不被打爆。
# 不同 key 的请求各自有 8 路（key pool 多窗口并发时各跑各的，互不干扰）；
# 同 key 的多请求共享 8 路（仍优于完全串行）。
_VISION_LLM_SEMS: dict[str, threading.Semaphore] = {}
_VISION_LLM_SEMS_LOCK = threading.Lock()


def _vision_sem_for(api_key: str | None) -> threading.Semaphore:
    """取 api_key 对应的视觉 LLM 信号量；空 key（未配置 pool / 走环境变量）共享同一只 semaphore。"""
    k = api_key or ""
    with _VISION_LLM_SEMS_LOCK:
        sem = _VISION_LLM_SEMS.get(k)
        if sem is None:
            sem = threading.Semaphore(8)
            _VISION_LLM_SEMS[k] = sem
        return sem

from llm.analyzers.base import BaseAnalyzer
from llm.cache import LLMCache
from llm.prompts import bsr as bsr_prompt
from llm.schemas import (
    ChinaSellerPatterns,
    MarketInsightPack,
    NewEntryWindow,
    PositioningTier,
    PriceBandEntry,
    ProductSegment,
)

log = logging.getLogger(__name__)


COL_ALIASES = {
    "asin": ["asin"],
    "title": ["product title", "title", "商品标题", "标题"],
    "brand": ["brand", "品牌"],
    "price": ["price", "price($)", "price(usd)", "price (usd)", "价格", "价格($)"],
    "rating": ["rating", "stars", "评分", "评级"],
    "reviews_count": ["ratings", "reviews", "review count", "ratings count", "评论数", "评分数"],
    "bsr": ["bsr", "category bsr", "sub-category bsr", "大类bsr", "小类bsr"],
    "seller_country": ["seller country", "buybox location", "buybox seller location", "seller location", "卖家所属地"],
    "monthly_sales": ["monthly sales", "monthly revenue($)", "月销量"],
    "date_available": ["first available date", "date available", "上架日期", "上架时间"],
    "tags": ["tags", "amazon's choice", "best seller", "category", "类目路径"],
}


def resolve_col(df: pd.DataFrame, key: str) -> str | None:
    """根据 COL_ALIASES 在 DataFrame 列名中找匹配（不区分大小写）。"""
    aliases = COL_ALIASES.get(key, [key])
    lower_map = {c.lower().strip(): c for c in df.columns}
    for a in aliases:
        if a.lower() in lower_map:
            return lower_map[a.lower()]
    return None


# 卖家精灵 BSR 导出有两种列名版本：英文和中文。app.py / 计算统计的 Python 代码用硬编码的英文列名
# （'Product Title'/'Brand'/'Rating'/'Monthly Sales' 等），中文列名版会 KeyError。
# 此函数在 BSR DataFrame 读入后立即执行，把中文列名映射到英文标准名（已有英文列的保持不变）。
CN_EN_BSR_COLUMNS = {
    '商品标题': 'Product Title',
    '标题': 'Product Title',
    '品牌': 'Brand',
    '价格($)': 'Price($)',
    '价格': 'Price($)',
    'Prime价格($)': 'Prime Price($)',
    '月销量': 'Monthly Sales',
    '月销售额($)': 'Monthly Revenue($)',
    '评分': 'Rating',
    '评级': 'Rating',
    '评分数': 'Ratings',
    '上架天数': 'Available days',
    '上架时间': 'Date First Available',
    '毛利率': 'Gross Margin',
    '配送方式': 'Fulfillment',
    '卖家所属地': 'BuyBox Location',
    '产品卖点': 'Bullet Points',
    '产品卖点(翻译)': 'Bullet Points (CN)',
    '类目路径': 'Category',
    '大类BSR': 'Category BSR',
    '小类BSR': 'Sub-category BSR',
    '变体数': 'Variations',
    '父ASIN': 'Parent ASIN',
    '卖家数': 'Sellers',
    'Buybox卖家': 'BuyBox Seller',
    '商品重量': 'Weight',
    '商品重量（单位换算）': 'Weight (Converted)',
    '商品尺寸': 'Dimensions',
    '商品尺寸（单位换算）': 'Dimensions (Converted)',
    '包装重量': 'Package Weight',
    '包装尺寸': 'Package Dimensions',
    '买家运费($)': 'Shipping($)',
    'FBA($)': 'FBA($)',
    'Coupon': 'Coupon',
    'Q&A数': 'Q&A Count',
    '月新增\n评分数': 'New Ratings/Month',
    '留评率': 'Review Rate',
    'LQS': 'LQS',
    'A+页面': 'A+',
    'SP广告': 'SP Ads',
    '品牌故事': 'Brand Story',
    '品牌广告': 'Brand Ads',
    '视频介绍': 'Has Video',
    '7天促销': 'Promo 7D',
    'AC关键词': 'AC Keyword',
    'CPF绿标': 'CPF',
    'CPF绿标信息': 'CPF Info',
    "Amazon's Choice": "Amazon's Choice",
    'Best Seller标识': 'Best Seller',
    'New Release标识': 'New Release',
    '子体销量': 'Variation Sales',
    '子体销售额($)': 'Variation Revenue($)',
    '月销量增长率': 'Monthly Sales Growth',
    '大类BSR增长数': 'BSR Growth',
    '大类BSR增长率': 'BSR Growth Rate',
    '品牌链接': 'Brand URL',
    '商品详情页链接': 'Product URL',
    '商品主图': 'Main Image',
    '详细参数': 'Product Details',
    '卖家信息': 'Seller Info',
    '卖家首页': 'Seller URL',
    'SKU': 'SKU',
    '图片': 'Image',
    '#': '#',
    'ASIN': 'ASIN',
    '大类目': 'Main Category',
    '小类目': 'Sub Category',
    '包装尺寸分段': 'Package Size Tier',
    '包装重量（单位换算）': 'Package Weight (Converted)',
    '包装尺寸（单位换算）': 'Package Dimensions (Converted)',
}


def _safe_rename_no_collision(df: pd.DataFrame, mapping: dict) -> pd.DataFrame:
    """把 {old_col: new_col} 应用到 df。保证：
    - 多个 old_col 映射到同一个 new_col 时只保留第一个（其余保留原名），避免出现重名列
      导致 df['col'] 返回 DataFrame 而非 Series
    - 映射目标与 df 已有其他列冲突时也跳过
    - 幂等"""
    if df is None or df.empty or not mapping:
        return df
    existing = set(df.columns)
    applied = {}
    taken_targets = set()  # 已被分配的目标英文名
    for col in df.columns:
        target = mapping.get(col)
        if not target or target == col:
            continue
        # 目标已在 df 里（有个"真的英文列"）→ 跳过，原中文列保持不变
        if target in existing and target != col:
            continue
        # 多个中文列指向同一个目标 → 只让第一个拿到
        if target in taken_targets:
            continue
        applied[col] = target
        taken_targets.add(target)
    if applied:
        df = df.rename(columns=applied)
    return df


def normalize_bsr_columns(df: pd.DataFrame) -> pd.DataFrame:
    """把卖家精灵 BSR 中文列名映射为英文标准名。幂等、不产生重名列。"""
    return _safe_rename_no_collision(df, CN_EN_BSR_COLUMNS)


# ========= Reviews 文件中英列名映射 =========
CN_EN_REVIEW_COLUMNS = {
    'ASIN': 'ASIN',
    '标题': 'Title',
    '标题(翻译)': 'Title (CN)',
    '内容': 'Content',
    '内容(翻译)': 'Content (CN)',
    'VP评论': 'Verified Purchase',
    'Vine Voice评论': 'Vine Voice',
    '型号': 'Model',
    '星级': 'Rating',
    '评分': 'Rating',
    '赞同数': 'Helpful',
    '图片数量': 'Image Count',
    '图片地址': 'Image URLs',
    '是否有视频': 'Has Video',
    '视频地址': 'Video URL',
    '评论链接': 'Reviews URL',
    '评论人': 'Author',
    '头像地址': 'Avatar URL',
    '所属国家': 'Nation',
    '评论人主页': 'Author Homepage',
    '红人计划链接': 'Influencer URL',
    '评论时间': 'Date',
}


def normalize_review_columns(df: pd.DataFrame) -> pd.DataFrame:
    """评论文件：标题→Title、内容→Content、星级→Rating、评论时间→Date。幂等、不产生重名列。"""
    return _safe_rename_no_collision(df, CN_EN_REVIEW_COLUMNS)


# ========= Keyword 文件中英列名映射（ReverseASIN / ExpandKeywords） =========
CN_EN_KEYWORD_COLUMNS = {
    # Unique Words sheet
    '词语': 'Phrase',
    '出现频次': 'Frequency',
    '频次': 'Frequency',
    '百分比': 'Percentage',
    # ReverseASIN/ExpandKeywords 主表
    '关键词': 'Keyword',
    '关键词翻译': 'Keyword (CN)',
    'AC推荐词': 'AC Keyword',
    '流量占比': 'Traffic Share',
    '流量词类型': 'Traffic Type',
    '预估周曝光量': 'Estimated Weekly Impressions',
    '相关产品': 'Related Products',
    '相关ASIN': 'Related ASINs',
    'ABA周排名': 'ABA Rank/W',
    '月搜索量': 'M. Searches',
    '月购买量': 'Units Sold',
    '购买率': 'Purchase Rate',
    '展示量': 'Impressions',
    '点击量': 'Clicks',
    'SPR': 'SPR',
    '标题密度': 'Title Density',
    '商品数': 'Products',
    '需供比': 'Demand Supply Ratio',
    '广告竞品数': 'Ads Competitor Count',
    '点击总占比': 'Click Share',
    '点击份额': 'Click Share',
    '转化份额': 'Conversion Share',
    '转化总占比': 'Conversion Share',
    '转化占比': 'Conversion Share',
    '平均报价': 'PPC Bid',
    '建议报价': 'Suggested Bid',
    # 新卖家精灵 ExpandKeywords 导出的真实中文列名
    'PPC竞价': 'PPC Bid',
    '建议竞价范围': 'Suggested Bid',
    # 简写兼容
    '竞价': 'PPC Bid',
    '建议竞价': 'Suggested Bid',
    '自然排名': 'Organic Rank',
    '广告排名': 'SP Rank',
}


def normalize_keyword_columns(df: pd.DataFrame) -> pd.DataFrame:
    """关键词文件：关键词→Keyword、月搜索量→M. Searches、购买率→Purchase Rate 等。幂等、不产生重名列。"""
    return _safe_rename_no_collision(df, CN_EN_KEYWORD_COLUMNS)


# ========= Market 文件 sheet 名中英映射 =========
CN_EN_MARKET_SHEETS = {
    '市场分析概况': 'Market Analysis',
    '行业需求及趋势': 'Industry Demand and Trends',
    '行业销售趋势': 'Industry Sell Trends',
    '商品集中度': 'Listing Concentration',
    '品牌集中度': 'Brand Concentration',
    '卖家集中度': 'Seller Concentration',
    '卖家类型分布': 'Fulfillment',
    'A+视频分布': 'A+ Content and Video',
    '卖家所属地分布': 'Origin of Seller',
    '上架时间分布': 'Publication Time',
    '上架趋势分布': 'Publication Time Trends',
    '评分数分布': 'Ratings',
    '评分值分布': 'Rating',
    '价格分布': 'Price',
}


def normalize_market_sheet_name(sheet_name: str) -> str:
    """把 Market 文件的中文 sheet 名映射为英文标准名。"""
    return CN_EN_MARKET_SHEETS.get(sheet_name, sheet_name)


# ========= Market 文件列名中英映射 =========
# 仅映射下游代码硬编码读取的列名（app.py:2136/4325/4337 读 'Sales Proportion'；
# infer_lifecycle_stage 读 'Launch Years'）。
# 其他 sheet 走 iloc 位置索引（sell_trends / fulfillment / aplus_video 等），
# 不需要 rename。故意不做全量映射避免 sheet 间语义冲突
# （例如"产品数量"在 A+ 视频 sheet 是 "Number of Product"，在其他是 "Products"）。
CN_EN_MARKET_COLUMNS = {
    '月销量占比': 'Sales Proportion',
    '月销售额占比': 'Revenue Proportion',
    '销量占比': 'Sales Proportion',
    '销售额占比': 'Revenue Proportion',
    '上架年份': 'Launch Years',
    '发布年份': 'Launch Years',
}


def normalize_market_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Market 文件：月销量占比→Sales Proportion、上架年份→Launch Years 等。
    仅覆盖下游硬编码的字段，其他列保持原名。幂等、不产生重名列。"""
    if df is None or df.empty:
        return df
    return _safe_rename_no_collision(df, CN_EN_MARKET_COLUMNS)


class BSRAnalyzer(BaseAnalyzer[MarketInsightPack]):
    name = "BSR"

    def _cache_key(self, input_data: dict) -> str:
        df: pd.DataFrame = input_data["df"]
        category_id = input_data.get("category_id", "unknown")
        h = hashlib.sha256()
        h.update(category_id.encode())
        # prompt 版本进 cache_key：prompt 升级自动失效旧缓存
        h.update(bsr_prompt._PROMPT_VERSION.encode())
        for key in ("asin", "title", "brand", "price"):
            col = resolve_col(df, key)
            if col:
                h.update(str(df[col].fillna("").tolist()).encode())
        return f"bsr_{category_id}_{h.hexdigest()[:12]}"

    def _prepare_rows(self, df: pd.DataFrame, top_n: int = 100) -> tuple[list[dict], dict]:
        rows = []
        sub = df.head(top_n)
        export_keys = ["asin", "title", "brand", "price", "rating", "reviews_count",
                       "bsr", "seller_country", "monthly_sales", "date_available", "tags"]
        for _, r in sub.iterrows():
            row = {}
            for k in export_keys:
                col = resolve_col(df, k)
                if col is None:
                    continue
                v = r[col]
                if pd.isna(v):
                    v = None
                elif hasattr(v, "isoformat"):
                    v = v.isoformat()[:10]
                row[k] = v
            rows.append(row)

        price_col = resolve_col(df, "price")
        prices = pd.to_numeric(df[price_col], errors="coerce").dropna() if price_col else pd.Series([], dtype=float)
        price_dist = {}
        if len(prices) > 0:
            price_dist = {
                "min": float(prices.min()),
                "median": float(prices.median()),
                "p75": float(prices.quantile(0.75)),
                "max": float(prices.max()),
            }

        china_pct = 0.0
        sc_col = resolve_col(df, "seller_country")
        if sc_col:
            sc = df[sc_col].fillna("").astype(str).str.upper()
            china_pct = float(((sc.str.contains("CN")) | (sc.str.contains("CHINA"))).mean())

        top_brands = []
        brand_col = resolve_col(df, "brand")
        if brand_col:
            top_brands = df[brand_col].fillna("").astype(str).value_counts().head(10).index.tolist()
            top_brands = [b for b in top_brands if b]

        return rows, {"price_distribution": price_dist, "china_pct": china_pct, "top_brands": top_brands}

    def _call_llm(self, input_data: dict) -> MarketInsightPack:
        df = input_data["df"]
        rows, stats = self._prepare_rows(df)
        cache_key = self._cache_key(input_data)
        messages = bsr_prompt.build_messages(
            bsr_rows=rows,
            price_distribution=stats["price_distribution"],
            china_pct=stats["china_pct"],
            top_brands=stats["top_brands"],
        )
        pack = self.client.chat_json(
            messages=messages,
            schema=MarketInsightPack,
            tier="fast",
            cache_key=cache_key,
            temperature=0.0,
            max_tokens=8000,
            timeout=180,
        )
        # 输出异常检测：BSR LLM 偶发产出"垃圾大桶"（如 1 个 segment 装 705 成员 + keywords=[]），
        # 现有 _ensure_full_coverage / _post_classify / _rescue_misplaced_asins 都救不了这种极端 case。
        # 这里检测 + 清理异常 segment，让漏出来的 ASIN 走「其他/通用款」+ Layer 3 救援。
        # 若清理后 segment 数 ≤ 1，重调 LLM 一次（带修正提示）。
        try:
            anomaly_cleaned = self._detect_and_clean_bsr_anomaly(pack, df)
            if anomaly_cleaned and pack.product_segments and len(pack.product_segments) <= 1:
                log.warning("[BSR sanity] 清理后 segment 数 ≤ 1，重调 LLM 一次（带修正提示）")
                retry_messages = list(messages) + [{
                    "role": "user",
                    "content": "上次输出 segment 数过少 / 含 keywords=[] 的垃圾大桶 / 同一 ASIN 重复装多次，已被自动清理。请按 prompt 规则重新切分 4-8 个 segment，每个 segment 必须有 ≥2 个独有 representative_keywords，member_asins 总和（去重后）= 输入 ASIN 总数，不要重复装 ASIN。",
                }]
                # cache_key 加上 retry 后缀避免覆盖原 cache（但下次同输入会复用 retry 结果）
                pack = self.client.chat_json(
                    messages=retry_messages,
                    schema=MarketInsightPack,
                    tier="fast",
                    cache_key=cache_key + "_retry1",
                    temperature=0.0,
                    max_tokens=8000,
                    timeout=180,
                )
                # 重试后再清一次（防止重试还是吐出垃圾桶）
                self._detect_and_clean_bsr_anomaly(pack, df)
        except Exception as e:
            log.warning("[BSR sanity] 异常检测/清理失败（不阻断流水线）: %s", e)
        # taxonomy 质量自检（仅 log，不调二次 LLM、不阻断）
        try:
            self._validate_segments(pack, df)
        except Exception as e:
            log.warning("[BSR taxonomy] 自检异常（不阻断流水线）: %s", e)
        # 兜底补全：LLM 偶尔漏装 ASIN（即使 prompt rule 15 强调过），自动建「其他/通用款」segment 容纳漏网，
        # 避免 classify_with_packs step 5 "兜底归到最大 segment" 把基础款错分到强功能 segment
        try:
            self._ensure_full_coverage(pack, df)
        except Exception as e:
            log.warning("[BSR coverage] 兜底补全异常（不阻断流水线）: %s", e)
        # 视觉二审：对低置信度 ASIN 用图+标题让多模态 LLM 复判，命中后写回 member_asins
        try:
            self._post_classify(pack, df, input_data)
        except Exception as e:
            log.warning("[BSR vision] 视觉二审异常（不阻断流水线）: %s", e)
        return pack

    def _detect_and_clean_bsr_anomaly(self, pack: "MarketInsightPack", df: pd.DataFrame) -> bool:
        """检测 BSR LLM 输出严重异常并清理。返回 True 表示触发了清理。

        异常信号（任一命中即认定该 segment 异常 → 清理）：
        - representative_keywords 为空 且 member_asins > 10（无关键词的大桶 = 垃圾混合桶）
        - member_asins 数量 > 输入 ASIN 总数 × 1.5（重复装信号，例：100 输入 → 一个 segment 装 705）

        注：之前加过"members > 输入×50%"判定，但充电器这类品类天然有"主流款占大头"
        （如多电压兼容款占 92%），会触发误清理把真实大类全删。删掉了——重复装 bug 由 150%
        判定捕获，垃圾桶由 keywords=[] 判定捕获，50% 是冗余的。

        清理策略：
        - 异常 segment 整段删除（释放它的 ASIN）
        - 漏出的 ASIN 由后续 _ensure_full_coverage 自动扫进「其他/通用款」桶
        - Layer 3 _rescue_misplaced_asins_by_token_overlap 把孤儿桶里 token 重叠 ≥2 的 ASIN
          救援到合理 segment

        全品类通用：阈值都是纯算法常量，不依赖任何品类专用词。
        """
        asin_col = resolve_col(df, "asin")
        if not asin_col:
            return False
        all_asins = {str(a).strip() for a in df[asin_col].dropna().astype(str)}
        all_asins = {a for a in all_asins if a and a.lower() != "nan"}
        input_total = len(all_asins)
        if input_total == 0 or not pack.product_segments:
            return False

        threshold_150pct = int(input_total * 1.5)
        anomaly_segments: list[str] = []

        for seg in list(pack.product_segments or []):
            members = seg.member_asins or []
            kw = seg.representative_keywords or []
            members_count = len(members)
            reasons: list[str] = []
            if not kw and members_count > 10:
                reasons.append(f"keywords=[] 且 members={members_count}>10（垃圾混合桶）")
            if members_count > threshold_150pct:
                reasons.append(f"members={members_count} > 输入×150%={threshold_150pct}（重复装）")
            if reasons:
                anomaly_segments.append(seg.name)
                log.warning(
                    "[BSR sanity] segment %r 异常 → 清理：%s",
                    seg.name, "；".join(reasons),
                )

        if not anomaly_segments:
            return False

        # 清理：删除异常 segments，让 _ensure_full_coverage 把漏的 ASIN 扫进「其他/通用款」
        pack.product_segments = [
            s for s in pack.product_segments if s.name not in anomaly_segments
        ]
        log.warning(
            "[BSR sanity] 共清理 %d 个异常 segment：%s；剩余 %d 个 segment",
            len(anomaly_segments), anomaly_segments, len(pack.product_segments),
        )
        return True

    def _ensure_full_coverage(self, pack: "MarketInsightPack", df: pd.DataFrame) -> None:
        """检查 BSR LLM 是否把所有输入 ASIN 都装进了 member_asins。
        若有漏装，自动新建一个'其他/通用款'兜底 segment 承载漏网 ASIN。

        全品类通用：兜底 segment 名是占位字符串、material_attribute 为空、
        无 representative_keywords，不针对任何品类。下游 classify_with_packs 走
        step 1 ASIN 精确命中就能找到正确归属，不会再走 step 5 错乱兜底。
        """
        asin_col = resolve_col(df, "asin")
        if not asin_col:
            return
        all_asins = {str(a).strip() for a in df[asin_col].dropna().astype(str)}
        all_asins = {a for a in all_asins if a and a.lower() != "nan"}
        if not all_asins:
            return

        covered: set[str] = set()
        for seg in pack.product_segments or []:
            covered.update(str(a).strip() for a in (seg.member_asins or []))

        missing = sorted(all_asins - covered)
        if not missing:
            return

        log.warning(
            "[BSR coverage] LLM 漏装 %d/%d ASIN，自动补到「其他/通用款」segment: %s%s",
            len(missing), len(all_asins),
            missing[:5],
            f"（共 {len(missing)} 个，省略后续）" if len(missing) > 5 else "",
        )
        pack.product_segments = list(pack.product_segments or []) + [
            ProductSegment(
                name="其他/通用款",
                description="LLM 未明确归类的 ASIN（系统自动兜底，含基础款 / 低特征产品）",
                member_asins=missing,
                representative_keywords=[],
                material_attribute="",
            )
        ]

    def _validate_segments(self, pack: MarketInsightPack, df: pd.DataFrame) -> None:
        """对 LLM 输出的 product_segments 做品类无关的质量校验，仅记日志，不修改 pack。
        三类检查全部基于"输入数据 vs LLM 输出"的算法对比，不含任何品类词表。"""
        import re
        import collections
        segments = pack.product_segments or []
        if not segments:
            return
        TOKEN_RE = re.compile(r"[a-z0-9一-鿿]+")

        # (a) representative_keywords 独有度：每个 segment 至少 2 个独有 token
        seg_tokens: dict[str, set[str]] = {s.name: set() for s in segments}
        for s in segments:
            for kw in (s.representative_keywords or []):
                seg_tokens[s.name].update(TOKEN_RE.findall(str(kw).lower()))
        token_seg_count: collections.Counter[str] = collections.Counter()
        for toks in seg_tokens.values():
            for t in toks:
                token_seg_count[t] += 1
        for s in segments:
            unique = [t for t in seg_tokens[s.name] if token_seg_count[t] == 1]
            if len(unique) < 2:
                log.warning(
                    "[BSR taxonomy] segment %r 独有 token 不足（仅 %d 个：%s），可能与其他 segment 共用通用词",
                    s.name, len(unique), unique,
                )

        # (b) 标题覆盖度：每条标题至少应被 1 个 segment 的 keyword 命中
        title_col = resolve_col(df, "title")
        if title_col:
            titles = df[title_col].fillna("").astype(str).str.lower().tolist()
            all_kw_lower = []
            for s in segments:
                all_kw_lower.extend(str(k).lower() for k in (s.representative_keywords or []) if k)
            if titles and all_kw_lower:
                hit = sum(1 for t in titles if any(k in t for k in all_kw_lower))
                coverage = hit / len(titles)
                if coverage < 0.8:
                    log.warning(
                        "[BSR taxonomy] 标题覆盖率仅 %.1f%%（%d/%d），representative_keywords 可能太通用或太少",
                        coverage * 100, hit, len(titles),
                    )

        # (c) 漏切维度候选词：高频出现在标题、但不在任何 segment keyword 里的 token
        if title_col:
            titles = df[title_col].fillna("").astype(str).str.lower().tolist()
            title_toks: collections.Counter[str] = collections.Counter()
            for t in titles:
                title_toks.update(TOKEN_RE.findall(t))
            all_seg_toks: set[str] = set().union(*seg_tokens.values()) if seg_tokens else set()
            STOP = {
                "the", "a", "an", "of", "for", "with", "and", "to", "in", "on", "by",
                "is", "or", "as", "at", "be", "are", "from", "this", "that", "it",
                "amazon", "new",
            }
            candidates = [
                (tok, n) for tok, n in title_toks.items()
                if n >= 3 and tok not in all_seg_toks and tok not in STOP
                and len(tok) >= 3 and not tok.isdigit()
            ]
            candidates.sort(key=lambda x: -x[1])
            if candidates[:5]:
                log.warning(
                    "[BSR taxonomy] 高频但未被任何 segment 关键词覆盖的 token（可能漏切维度）：%s",
                    candidates[:10],
                )

    def _post_classify(self, pack: MarketInsightPack, df: pd.DataFrame, input_data: dict) -> None:
        """视觉二审：根据 VISION_AUDIT_MODE 选择 audit 范围，对低置信 / 可疑 / 全部 ASIN
        调 analyze_with_vision（图+标题→segment + 材质/形态标签），命中后写回 / 改写 member_asins。
        视觉不可用 / 没图 / LLM 拒答 时静默退化，不阻断。

        VISION_AUDIT_MODE：
          - low_conf：仅 audit step 5 兜底返回"未分类"的 ASIN（最省，~3-5 次/100 行）
          - suspect：除 low_conf 外，再 audit "标题 token 与当前所归 segment 的
            representative_keywords token 集**完全不相交**" 的 ASIN（即 LLM 给的归类无任何
            文本支持，可疑度高）。典型 10-25 次/100 行
          - all（默认）：100 行全过 vision，把 LLM 的 member_asins 当 hint，并提取材质/形态标签
        """
        import os as _os
        import re as _re

        if not pack.product_segments:
            return
        from core.packs_runtime import classify_with_packs, analyze_with_vision, Packs
        from llm.schemas import ProductVisualLabel

        packs = Packs(market=pack)
        packs.market.is_fallback = False

        title_col = resolve_col(df, "title")
        asin_col = resolve_col(df, "asin")
        if not title_col or not asin_col:
            return

        # 找图片列：按优先级匹配 + 校验列里至少有 1 条 http(s) URL。
        # 卖家精灵 BSR 导出里的 'Image' 列是缩略图字段（多为 NaN），不是 URL；
        # 按 df.columns 顺序查找会先匹到它而错过真正的 'Main Image'，导致 vision 全部空转。
        IMAGE_COL_PRIORITY = ("main image", "image url", "商品主图", "主图", "image")
        image_col = None
        cols_lower = {str(c).lower(): c for c in df.columns}
        for name in IMAGE_COL_PRIORITY:
            if name in cols_lower:
                candidate = cols_lower[name]
                sample = df[candidate].dropna().astype(str).head(5).tolist()
                if any(s.lower().startswith(("http://", "https://")) for s in sample):
                    image_col = candidate
                    break
        if image_col is None:
            log.warning("[BSR vision] 未找到含 URL 的图片列，跳过视觉二审")
            return
        log.info("[BSR vision] 使用图片列：%r", image_col)

        # 找产品卖点列：传给视觉 LLM 做"图+标题+卖点"三方信号交叉判定，提高材质 / 类型识别准确率。
        # 卖家精灵中文导出常用「产品卖点」/「产品卖点(翻译)」；英文导出常用 selling points / bullet points。
        SELLING_POINTS_COL_PRIORITY = (
            "产品卖点", "产品卖点(翻译)", "selling points", "bullet points", "feature bullets", "about this item"
        )
        selling_points_col = None
        for name in SELLING_POINTS_COL_PRIORITY:
            if name in cols_lower:
                candidate = cols_lower[name]
                sample_non_empty = df[candidate].fillna("").astype(str).str.strip().ne("").sum()
                if sample_non_empty >= 1:
                    selling_points_col = candidate
                    break
        if selling_points_col:
            log.info("[BSR vision] 使用产品卖点列：%r（非空 %d 行）",
                     selling_points_col, df[selling_points_col].fillna("").astype(str).str.strip().ne("").sum())
        else:
            log.info("[BSR vision] 未找到产品卖点列，仅按 图+标题 判定")

        # 视觉 client：复用本次请求的 api_key（key pool 多窗口并发时每个请求一把 key）
        from config.llm_config import make_vision_provider
        from llm.client import LLMClient
        from llm.cache import LLMCache
        request_api_key: str | None = getattr(self.client.provider, "api_key", None) if self.client else None
        try:
            vision_provider = make_vision_provider(api_key=request_api_key)
        except Exception as e:
            log.warning("[BSR vision] 视觉 provider 不可用（%s），跳过", e)
            return
        if vision_provider is None:
            log.warning("[BSR vision] 未配置可用的视觉 provider，跳过")
            return
        vision_client = LLMClient(provider=vision_provider, cache=getattr(self.client, "cache", None) or LLMCache("llm_cache"))
        vision_sem = _vision_sem_for(request_api_key)

        mode = _os.getenv("VISION_AUDIT_MODE", "all").lower().strip()
        if mode not in ("low_conf", "suspect", "all"):
            log.warning("[BSR vision] 未知 VISION_AUDIT_MODE=%r，回退默认 all", mode)
            mode = "all"
        log.warning("[BSR vision] VISION_AUDIT_MODE=%s", mode)

        # 预计算每个 segment 的 keyword token 集合（用于 suspect 判别）
        TOKEN_RE = _re.compile(r"[a-z0-9一-鿿]+")
        seg_tokens_map: dict[str, set[str]] = {}
        for s in pack.product_segments:
            toks: set[str] = set()
            for kw in (s.representative_keywords or []):
                toks.update(TOKEN_RE.findall(str(kw).lower()))
            seg_tokens_map[s.name] = toks

        def _default_unclassified(_: str) -> str:
            return "未分类"

        # 收集要 audit 的 ASIN：(asin, title, image_url, current_label, selling_points)
        # current_label 在 vision 改写时用于"先从原 segment 删除"
        # selling_points 截断到 400 字以控制 prompt 长度
        audit_rows: list[tuple[str, str, str, str, str]] = []
        for _, r in df.iterrows():
            asin = str(r.get(asin_col, "")).strip()
            title = str(r.get(title_col, "")).strip()
            if not asin or not title:
                continue
            current = classify_with_packs(title, packs, _default_unclassified, asin=asin)
            need_audit = False
            if mode == "all":
                need_audit = True
            elif mode == "low_conf":
                need_audit = (current == "未分类")
            else:  # suspect
                if current == "未分类":
                    need_audit = True
                else:
                    title_tokens = set(TOKEN_RE.findall(title.lower()))
                    seg_toks = seg_tokens_map.get(current, set())
                    if not (title_tokens & seg_toks):
                        need_audit = True
            if need_audit:
                img = r.get(image_col, "")
                sp_raw = ""
                if selling_points_col:
                    sp_val = r.get(selling_points_col, "")
                    if sp_val and not (isinstance(sp_val, float) and str(sp_val) == "nan"):
                        sp_raw = str(sp_val).strip()[:400]
                audit_rows.append((asin, title, str(img) if img else "", current, sp_raw))

        if not audit_rows:
            log.warning("[BSR vision] 模式=%s，无 ASIN 需要 audit，跳过", mode)
            return

        log.warning("[BSR vision] 模式=%s，共 %d 个 ASIN 进入视觉二审", mode, len(audit_rows))

        # 视觉二审：并发调用 LLM，结果回主线程后串行写回（避免 member_asins 列表的竞争）
        # 并发数从环境变量读，默认 8（DashScope 配额足够；vision 调用 1-2 秒/次，8 并发约 4-8 QPS）
        from concurrent.futures import ThreadPoolExecutor, as_completed
        max_workers = int(_os.getenv("VISION_MAX_WORKERS", "8"))

        def _audit_one(asin: str, title: str, img_url: str, current: str, selling_points: str = ""):
            """单个 ASIN 的 vision 调用，纯只读，可安全并发。
            外层信号量按 api_key 隔离：不同 key 各 8 路，同 key 多请求共享 8 路。"""
            try:
                with vision_sem:
                    result = analyze_with_vision(
                        title, img_url or None, asin, packs, vision_client,
                        selling_points=selling_points,
                    )
                return (asin, title, current, result, None)
            except Exception as e:
                return (asin, title, current, None, e)

        results: list[tuple] = []
        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            futures = [ex.submit(_audit_one, *row) for row in audit_rows]
            for fut in as_completed(futures):
                try:
                    results.append(fut.result())
                except Exception as e:
                    log.warning("[BSR vision] worker 异常: %s", e)

        # 主线程串行写回（这里不会有竞争）
        seg_by_name = {s.name: s for s in pack.product_segments}
        hit_new = 0       # 原本未分类，现在归到了某个 segment
        hit_rewrite = 0   # 原本归错的，现在被改写到正确 segment
        label_hits = 0
        visual_labels: list[ProductVisualLabel] = []
        for asin, title, current, result, err in results:
            if err is not None:
                log.warning("[BSR vision] %s 视觉判别异常: %s", asin, err)
                continue
            if result is None:
                continue

            material = str(getattr(result, "material_label", "") or "").strip()
            form = str(getattr(result, "form_label", "") or "").strip()
            ptype_free = str(getattr(result, "product_type_free", "") or "").strip()
            if material or form or ptype_free:
                visual_labels.append(ProductVisualLabel(
                    asin=asin,
                    material_label=material,
                    form_label=form,
                    product_type_free=ptype_free,
                ))
                if material:
                    label_hits += 1

            chosen = str(getattr(result, "segment_name", "") or "").strip()
            if not chosen or chosen.lower() == "unknown" or chosen == current:
                continue
            seg = seg_by_name.get(chosen)
            if seg is None:
                continue
            # Sanity check：视觉返回的 segment_name 必须与产品标题**至少 2 个 token 重叠**
            # （chosen segment 的 representative_keywords ∩ 标题 tokens 数量 ≥ 2）。
            # 防止 LLM 把毫不沾边的产品硬选到强特征 segment。
            # 阈值 ≥ 2 而不是 ≥ 1：避免品类通用词（如存钱罐域的 "coin", "bank", "money", "kids"）
            # 单 token 误命中——已观察案例：独角兽塑料标题里的 "coin" 命中电子ATM keywords 里的
            # "Coin Recognition"，1 token 重叠刚好通过 ≥1 → 视觉硬选被错误接受。
            # BSR prompt 规则 12 要求每 segment 至少 2 个独有 token，正常匹配时 ≥2 容易达到。
            # 例外：chosen segment 的 representative_keywords 为空（如「其他/通用款」自身）→ 不做检查，放行
            chosen_kw_tokens = seg_tokens_map.get(chosen, set())
            title_tokens_for_chosen = set(TOKEN_RE.findall(title.lower()))
            if chosen_kw_tokens:
                overlap_count = len(chosen_kw_tokens & title_tokens_for_chosen)
                if overlap_count < 2:
                    log.warning(
                        "[BSR vision] %s 拒绝视觉选择 %s：标题与该 segment keywords 仅 %d token 重叠（< 2 阈值），保留原归属 %s",
                        asin, chosen, overlap_count, current,
                    )
                    continue
            # 把 ASIN 从所有"原 segment"里移除（防止多归属）
            for s in pack.product_segments:
                if s.name != chosen and asin in (s.member_asins or []):
                    s.member_asins = [a for a in s.member_asins if a != asin]
            # 加入新 segment（如果还没在里面）
            if asin not in (seg.member_asins or []):
                seg.member_asins = list(seg.member_asins or []) + [asin]
            if current == "未分类":
                hit_new += 1
                log.warning("[BSR vision] %s 未分类 → %s", asin, chosen)
            else:
                hit_rewrite += 1
                log.warning("[BSR vision] %s 改写 %s → %s", asin, current, chosen)
        if visual_labels:
            existing = {v.asin: v for v in (pack.visual_labels or [])}
            for label in visual_labels:
                existing[label.asin] = label
            pack.visual_labels = list(existing.values())

        # ===== 层 2：材质冲突调和 =====
        # 视觉 material_label（O 列）是 ground truth；当 ASIN 当前所属 segment 的 material_attribute
        # 与视觉判定的材质不一致时，把 ASIN 移到 material_attribute 匹配（或为空）的候选 segment 上。
        # 永远是单向修正——以视觉为准，绝不反向用 segment.material_attribute 覆盖视觉结果。
        # 全程零硬编码材质词表：依赖 LLM 自报的 material_attribute 字段 + 字符串相等比较。
        material_reroute_count = 0
        if pack.visual_labels:
            asin_to_title: dict[str, str] = {}
            for _, r in df.iterrows():
                a = str(r.get(asin_col, "")).strip()
                t = str(r.get(title_col, "")).strip()
                if a and t:
                    asin_to_title[a] = t

            for label in pack.visual_labels:
                vmat = str(getattr(label, "material_label", "") or "").strip()
                if not vmat:
                    continue
                asin = str(getattr(label, "asin", "") or "").strip()
                if not asin:
                    continue
                current_seg = None
                for s in pack.product_segments:
                    if asin in (s.member_asins or []):
                        current_seg = s
                        break
                if current_seg is None:
                    continue
                cur_mat = str(getattr(current_seg, "material_attribute", "") or "").strip()
                if not cur_mat or cur_mat == vmat:
                    continue
                title = asin_to_title.get(asin, "")
                title_tokens = set(TOKEN_RE.findall(title.lower()))
                best_seg = None
                best_score = -1
                for s in pack.product_segments:
                    if s is current_seg:
                        continue
                    s_mat = str(getattr(s, "material_attribute", "") or "").strip()
                    s_name = s.name or ""
                    # 候选条件三选一：
                    # 1) material_attribute 完全匹配视觉材质（最强信号，权重 +2000）
                    # 2) material_attribute 为空（中性 segment，可接受任何材质，权重 +500）
                    # 3) segment.name 里出现视觉材质子串（如「透明亚克力/塑料基础款」name 含「塑料」），
                    #    用于解决 LLM 给混合材质 segment 只填了一个 material_attribute 导致另一种材质
                    #    产品无法 reroute 进来的问题（权重 +500，与 ma="" 相当）
                    name_contains_vmat = bool(vmat and vmat in s_name)
                    if s_mat not in ("", vmat) and not name_contains_vmat:
                        continue
                    seg_toks = seg_tokens_map.get(s.name, set())
                    overlap = len(title_tokens & seg_toks)
                    if overlap == 0:
                        continue
                    if s_mat == vmat:
                        match_bonus = 2000
                    elif s_mat == "":
                        match_bonus = 500
                    else:
                        match_bonus = 500  # name 含 vmat 子串
                    score = overlap + match_bonus
                    if score > best_score:
                        best_score = score
                        best_seg = s
                if best_seg is None:
                    continue
                current_seg.member_asins = [a for a in (current_seg.member_asins or []) if a != asin]
                if asin not in (best_seg.member_asins or []):
                    best_seg.member_asins = list(best_seg.member_asins or []) + [asin]
                material_reroute_count += 1
                log.warning(
                    "[BSR vision] %s 材质冲突调和：%s(material_attribute=%s) → %s(material_attribute=%s)，视觉=%s",
                    asin, current_seg.name, cur_mat or "-", best_seg.name,
                    str(getattr(best_seg, "material_attribute", "") or "") or "-", vmat,
                )

        log.warning("[BSR vision] 命中：新归 %d 个，改写 %d 个，标签 %d 个，材质调和 %d 个，共审 %d 个（并发=%d）",
                    hit_new, hit_rewrite, label_hits, material_reroute_count, len(audit_rows), max_workers)

        # ===== Layer 3：错位救援 OR-match =====
        # 对所有 ASIN 跑 token overlap：若有备选 segment 重叠 ≥ 2 且严格 > 当前 segment 重叠，
        # reroute 过去。覆盖两种 case：
        #   a) 「其他/通用款」里的孤儿（current overlap = 0，empty keywords）
        #   b) 被错放到强特征 segment 的产品（如独角兽被错放到电子ATM，current overlap 弱、theme segment overlap 强）
        # 完全忽略 material 约束——material 由 O 列单独负责
        self._rescue_misplaced_asins_by_token_overlap(pack, df)

        # 注：v8 起 segment.name 允许带材质词（"木质字母/主题"等），不再做名字清洗。
        # Sheet 3 N 列改用 visual_product_type（视觉 LLM 自由描述每 ASIN）展示精确产品类型；
        # Sheet 4/5/6/10 继续用 segment.name 做聚合分析——带材质让业务一眼看出该细分核心特征。

    def _rescue_misplaced_asins_by_token_overlap(self, pack: "MarketInsightPack", df: pd.DataFrame) -> None:
        """对所有 ASIN 跑错位救援：若备选 segment 的 token 重叠**严格 > 当前 segment** 且**≥ 2**，reroute。

        覆盖两种 case：
        a) **孤儿**：在「其他/通用款」里（current overlap = 0，因 keywords 为空），
           被某个非"其他"segment 命中 ≥ 2 → reroute 过去
        b) **错位**：BSR LLM 把 ASIN 装错到强特征 segment（如塑料独角兽被装到「电子ATM」），
           当前 segment 的 keywords 与标题几乎不重叠（仅命中通用词如 "coin"），
           而另一个 segment（如「字母/主题」）keywords 与标题强重叠（unicorn + alphabet 命中 2）
           → reroute 到 best alt

        阈值 ≥ 2 + 严格 >current：
        - ≥ 2 防品类通用词单 token 误命中（如 "coin" 命中电子ATM 的 "Coin Recognition"）
        - 严格 > current 保证只在 best alt 真的更匹配时移动，避免 ties 反复迁移
        - BSR prompt 规则 12 要求每 segment ≥ 2 个独有 token，所以正常归属时 best alt 不会无故碾压

        完全忽略 material 约束——material 由 O 列单独负责。

        全品类通用：阈值是 pure 算法常量，词来源全是 LLM 自报的 representative_keywords + 标题。
        """
        # asin → title 映射（resolve_col 已在模块顶部导入）
        asin_col = resolve_col(df, "asin")
        title_col = resolve_col(df, "title")
        if not asin_col or not title_col:
            return
        asin_to_title: dict[str, str] = {}
        for _, r in df.iterrows():
            a = str(r.get(asin_col, "")).strip()
            t = str(r.get(title_col, "")).strip()
            if a and t:
                asin_to_title[a] = t

        # 预算每个 segment 的 keyword tokens
        import re as _re
        TOKEN_RE_LOC = _re.compile(r"[a-z0-9一-鿿]+")
        seg_kw_tokens: dict[str, set[str]] = {}
        for s in pack.product_segments or []:
            toks: set[str] = set()
            for kw in (s.representative_keywords or []):
                toks.update(TOKEN_RE_LOC.findall(str(kw).lower()))
            seg_kw_tokens[s.name] = toks

        # 建 ASIN → current_segment_name 映射（按 segment 顺序，第一个匹配为准——与 classify_with_packs 一致）
        asin_to_current_seg: dict[str, str] = {}
        for s in pack.product_segments or []:
            for a in (s.member_asins or []):
                if a not in asin_to_current_seg:
                    asin_to_current_seg[a] = s.name

        # 对每个 ASIN 找 best alternative
        rescued = 0
        for asin, current_name in list(asin_to_current_seg.items()):
            title = asin_to_title.get(asin, "")
            if not title:
                continue
            title_tokens = set(TOKEN_RE_LOC.findall(title.lower()))
            current_overlap = len(seg_kw_tokens.get(current_name, set()) & title_tokens)

            # 找 best alt segment（重叠最多的非 current）
            best_name = None
            best_overlap = 0
            for s_name, kw_toks in seg_kw_tokens.items():
                if s_name == current_name:
                    continue
                overlap = len(kw_toks & title_tokens)
                if overlap > best_overlap:
                    best_overlap = overlap
                    best_name = s_name

            # 触发条件：best alt overlap ≥ 2 且 严格 > current overlap
            if best_name is None or best_overlap < 2 or best_overlap <= current_overlap:
                continue

            # reroute：从所有原 segment 移除 asin，加入 best
            target_seg = next((s for s in pack.product_segments if s.name == best_name), None)
            if target_seg is None:
                continue
            for s in pack.product_segments or []:
                if s.name != best_name and asin in (s.member_asins or []):
                    s.member_asins = [a for a in s.member_asins if a != asin]
            if asin not in (target_seg.member_asins or []):
                target_seg.member_asins = list(target_seg.member_asins or []) + [asin]
            rescued += 1
            log.warning(
                "[BSR coverage] 错位救援：%s %s → %s（current重叠=%d，alt重叠=%d）",
                asin, current_name, best_name, current_overlap, best_overlap,
            )
        if rescued:
            log.warning("[BSR coverage] 共救援 %d 个错位 ASIN", rescued)

    def _fallback(self, input_data: dict) -> MarketInsightPack:
        df: pd.DataFrame = input_data["df"]
        category_id = input_data.get("category_id", "unknown")

        price_col = resolve_col(df, "price")
        prices = pd.to_numeric(df[price_col], errors="coerce").dropna() if price_col else pd.Series([], dtype=float)
        bands = []
        if len(prices) > 0:
            band_defs = [
                ("<$25", 0, 25), ("$25-50", 25, 50),
                ("$50-100", 50, 100), (">$100", 100, 1e9),
            ]
            for name, lo, hi in band_defs:
                in_band = prices[(prices >= lo) & (prices < hi)]
                if len(in_band) > 0:
                    bands.append(PriceBandEntry(
                        band=name,
                        description=f"占比 {len(in_band)/len(prices)*100:.1f}%",
                        competition_intensity="中",
                        profit_room="中",
                        representative_asins=[],
                    ))

        top_brands = []
        brand_col = resolve_col(df, "brand")
        if brand_col:
            top_brands = df[brand_col].fillna("").astype(str).value_counts().head(5).index.tolist()
            top_brands = [b for b in top_brands if b]

        china_pct = 0.0
        sc_col = resolve_col(df, "seller_country")
        if sc_col:
            sc = df[sc_col].fillna("").astype(str).str.upper()
            china_pct = float(((sc.str.contains("CN")) | (sc.str.contains("CHINA"))).mean())

        china_strategy = f"中国卖家占比 {china_pct:.1%}" if sc_col else ""
        brand_reading = f"TOP5 品牌：{', '.join(top_brands)}" if top_brands else ""
        return MarketInsightPack(
            category_display_name=category_id.replace("_", " ").title(),
            product_segments=[],
            positioning_tiers=[],
            price_ladder=bands,
            china_seller_patterns=ChinaSellerPatterns(typical_strategies=china_strategy),
            new_entry_windows=[],
            brand_concentration_reading=brand_reading,
            is_fallback=True,
        )
