"""ASIN 评论采集清单：基于 BSR + Market 数据，按规则筛 12-18 个高优先级 ASIN，
让运营按清单去卖家精灵定向下载评论，避免凭经验自由挑导致的样本片面。

不调 LLM、纯规则、秒级出清单。
"""
from __future__ import annotations

import logging
from typing import Optional

import pandas as pd

log = logging.getLogger(__name__)

OUT_COLUMNS = [
    '优先级', 'ASIN', '标题', '品牌', '月销量', '月收入($)', '评分', '评论数',
    'BuyBox国家', '在售天数', '入选标签', '推荐理由', '建议下载评论数',
    'Amazon评论页URL', '卖家精灵评论页URL',
]


def _norm_str(x) -> str:
    if x is None:
        return ''
    s = str(x).strip()
    return '' if s.lower() in ('nan', 'none') else s


def _resolve(df: pd.DataFrame, *names: str) -> Optional[str]:
    for n in names:
        if n in df.columns:
            return n
    return None


def _price_band(price: float) -> str:
    if price < 10: return '<$10'
    if price < 15: return '$10-15'
    if price < 25: return '$15-25'
    if price < 50: return '$25-50'
    if price < 100: return '$50-100'
    return '>$100'


def build_asin_collection_list(bsr_df: pd.DataFrame,
                                market_data: dict | None = None,
                                top_n: int = 15) -> pd.DataFrame:
    """按多标签规则筛选 ASIN 评论采集清单。

    返回列见 OUT_COLUMNS。BSR 必备列缺失则抛 ValueError。
    """
    if bsr_df is None or bsr_df.empty:
        raise ValueError('BSR 数据为空')

    asin_col = _resolve(bsr_df, 'ASIN', 'asin')
    if asin_col is None:
        raise ValueError('BSR 缺 ASIN 列')

    title_col = _resolve(bsr_df, 'Product Title', 'Title', '标题', 'title')
    brand_col = _resolve(bsr_df, 'Brand', '品牌')
    sales_col = _resolve(bsr_df, 'Monthly Sales', '月销量', 'monthly_sales')
    price_col = _resolve(bsr_df, 'Price($)', 'Price', '价格', 'price')
    rev_col = _resolve(bsr_df, 'Revenue', 'Monthly Revenue', '月销售额', '月销售额($)')
    rating_col = _resolve(bsr_df, 'Rating', '评分', '星级')
    ratings_col = _resolve(bsr_df, 'Ratings', '评论数', '评分数', 'review_count')
    buybox_col = _resolve(bsr_df, 'BuyBox Location', '配送地', 'buybox_location')
    days_col = _resolve(bsr_df, 'Available days', '在售天数', 'available_days')

    df = bsr_df.copy()
    df['_asin'] = df[asin_col].astype(str).str.strip()
    df = df[df['_asin'].str.match(r'^B[0-9A-Z]{9}$', na=False)].reset_index(drop=True)
    if df.empty:
        raise ValueError('BSR 无有效 ASIN（B0XXXXXXXX 格式）')

    df['_title'] = df[title_col].apply(_norm_str) if title_col else ''
    df['_brand'] = df[brand_col].apply(_norm_str) if brand_col else ''
    df['_sales'] = pd.to_numeric(df[sales_col], errors='coerce').fillna(0) if sales_col else 0
    df['_price'] = pd.to_numeric(df[price_col], errors='coerce').fillna(0) if price_col else 0
    if rev_col:
        df['_rev'] = pd.to_numeric(df[rev_col], errors='coerce').fillna(0)
    else:
        df['_rev'] = df['_sales'] * df['_price']
    df['_rating'] = pd.to_numeric(df[rating_col], errors='coerce').fillna(0) if rating_col else 0
    df['_ratings'] = pd.to_numeric(df[ratings_col], errors='coerce').fillna(0).astype(int) if ratings_col else 0
    df['_buybox'] = df[buybox_col].apply(_norm_str) if buybox_col else ''
    df['_days'] = pd.to_numeric(df[days_col], errors='coerce').fillna(0).astype(int) if days_col else 0

    sales_top30 = set(df.nlargest(30, '_sales')['_asin'])
    ratings_top10 = set(df.nlargest(10, '_ratings')['_asin'])

    df['_pb'] = df['_price'].apply(_price_band)
    pb_counts = df['_pb'].value_counts()
    main_bands = pb_counts.head(2).index.tolist() if len(pb_counts) > 0 else []

    cn_set = set(df[df['_buybox'].str.upper().isin(['CN', 'CN(HK)'])]['_asin']) & sales_top30

    scores: dict[str, dict] = {}

    def _add(asin: str, tag: str, score: int, reason: str) -> None:
        s = scores.setdefault(asin, {'tags': [], 'reasons': [], 'score': 0})
        if tag not in s['tags']:
            s['tags'].append(tag)
            s['reasons'].append(reason)
            s['score'] += score

    rev_sorted = df.sort_values('_rev', ascending=False)
    for _, r in rev_sorted.head(5).iterrows():
        _add(r['_asin'], '头部销量', 3, f'月收入 ${int(r["_rev"]):,} 进 Top5，主流销量代表')

    new_df = df[(df['_days'] > 0) & (df['_days'] < 365)].sort_values('_sales', ascending=False)
    new_added = 0
    for _, r in new_df.iterrows():
        if new_added >= 3:
            break
        if r['_asin'] in sales_top30:
            _add(r['_asin'], '新品代表', 2, f'上架 {int(r["_days"])} 天且月销 {int(r["_sales"]):,} 件，新品打法样本')
            new_added += 1

    bad_df = df[(df['_rating'] > 0) & (df['_rating'] < 4.0) & (df['_ratings'] >= 30)].sort_values('_ratings', ascending=False)
    for _, r in bad_df.head(3).iterrows():
        _add(r['_asin'], '差评异常', 2, f'评分 {r["_rating"]:.1f}★（{int(r["_ratings"])} 评），痛点挖掘高价值样本')

    rev_high = df[df['_asin'].isin(ratings_top10)].sort_values('_ratings', ascending=False)
    for _, r in rev_high.head(2).iterrows():
        _add(r['_asin'], '评论高活跃', 1, f'累计 {int(r["_ratings"])} 评论，VOC 样本厚')

    cn_df = df[df['_asin'].isin(cn_set)].sort_values('_rev', ascending=False)
    for _, r in cn_df.head(3).iterrows():
        _add(r['_asin'], '中国卖家代表', 2, f'中国 BuyBox + 月销 {int(r["_sales"]):,} 件，对标参考')

    band_added = 0
    for b in main_bands:
        sub = df[df['_pb'] == b].nlargest(2, '_rev')
        for _, r in sub.iterrows():
            if band_added >= 4:
                break
            _add(r['_asin'], f'{b} 价格带代表', 1, f'{b} 价格带头部 SKU，价格带画像样本')
            band_added += 1
        if band_added >= 4:
            break

    if not scores:
        raise ValueError('未筛选到任何 ASIN（数据可能过少）')

    rows = []
    for asin, s in scores.items():
        r = df[df['_asin'] == asin].iloc[0]
        tags = s['tags']
        if '差评异常' in tags:
            dl_hint = '全部 1-2★ + 最新 100 条'
        elif '头部销量' in tags or '评论高活跃' in tags:
            dl_hint = '最新 200 条'
        else:
            dl_hint = '全部'
        rows.append({
            '_score': s['score'],
            'ASIN': asin,
            '标题': r['_title'][:80],
            '品牌': r['_brand'],
            '月销量': int(r['_sales']),
            '月收入($)': int(r['_rev']),
            '评分': round(float(r['_rating']), 1) if r['_rating'] else '-',
            '评论数': int(r['_ratings']),
            'BuyBox国家': r['_buybox'] or '-',
            '在售天数': int(r['_days']) if r['_days'] else '-',
            '入选标签': ' + '.join(tags),
            '推荐理由': '；'.join(s['reasons']),
            '建议下载评论数': dl_hint,
            'Amazon评论页URL': f'https://www.amazon.com/product-reviews/{asin}/',
            '卖家精灵评论页URL': f'https://www.sellersprite.com/v2/reviews/asin?asin={asin}',
        })

    rows.sort(key=lambda x: -x['_score'])
    rows = rows[:top_n]
    for i, row in enumerate(rows, 1):
        row['优先级'] = i
        row.pop('_score')

    out = pd.DataFrame(rows)[OUT_COLUMNS]
    log.info('[ASINPlanner] 输出 %d 条采集清单', len(out))
    return out


SELECTION_RULES = [
    ('头部销量', 'Top 5 by 月收入', 5, '+3', '主流销量代表，覆盖最广用户群'),
    ('新品代表', 'Available days < 365 且月销进 Top30', 3, '+2', '看新品打法、定价、文案策略'),
    ('差评异常', 'Rating < 4.0 且评论数 ≥ 30', 3, '+2', '挖痛点，找改良/差异化方向'),
    ('评论高活跃', '评论数 Top 10', 2, '+1', '样本厚，VOC 聚类置信度高'),
    ('中国卖家代表', 'BuyBox ∈ CN/CN(HK) 且月销进 Top30', 3, '+2', '对标同类卖家打法'),
    ('价格带代表', '主力 2 个价格带各取头部 1-2 个', 4, '+1', '不同价格带画像样本'),
]
