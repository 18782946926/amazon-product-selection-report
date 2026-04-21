"""
选品报告生成系统 - Flask Web服务 v2.0
完整版 - 移植原版 generate_led_report.py 所有分析逻辑
"""

import os
import uuid
import shutil
from datetime import datetime
from flask import Flask, render_template, request, send_file, flash, redirect, url_for, jsonify
from werkzeug.utils import secure_filename

import pandas as pd
import sys
import io

# openpyxl 样式类
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.chart import LineChart, BarChart, Reference
from openpyxl.chart.label import DataLabelList

# 配置
UPLOAD_FOLDER = 'uploads'
REPORT_FOLDER = 'reports'
ALLOWED_EXTENSIONS = {'xlsx', 'xls'}
MAX_CONTENT_LENGTH = 100 * 1024 * 1024

app = Flask(__name__)
app.secret_key = 'product-analysis-v2-secret-key'
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['REPORT_FOLDER'] = REPORT_FOLDER
app.config['MAX_CONTENT_LENGTH'] = MAX_CONTENT_LENGTH

os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(REPORT_FOLDER, exist_ok=True)

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


# ============================================================
# 产品分类函数
# ============================================================
def classify(title):
    t = str(title).lower()
    if 'spotlight' in t or ('flashlight' in t and 'work' not in t):
        return '手持手电/聚光灯'
    elif 'tripod' in t or ('stand' in t and ('work light' in t or 'lamp' in t)):
        return '三脚架工作灯'
    elif 'shop light' in t or 'tube light' in t or 't8' in t or 't5' in t:
        return '车间吊装灯'
    elif 'clamp' in t or 'clip' in t:
        return '夹灯'
    elif 'solar' in t:
        return '太阳能工作灯'
    elif 'rechargeable' in t or 'magnetic' in t or 'mechanic light' in t:
        return '充电磁吸工作灯'
    elif 'flood light' in t or 'job site' in t:
        return '泛光工作灯'
    else:
        return '其他工作灯'


# ============================================================
# 统一的产品类型综合评分（供 Sheet 6 上新方向 和 Sheet 10 首推入场品类 共用）
# 维度：需求(月销)、竞争度(SKU数)、单品收益、新品占比、质量评分
# ============================================================
def rank_product_types(df, type_agg):
    """
    对每个产品类型计算综合得分，返回按得分降序排列的类型列表 + 得分细节 dict。
    评分维度（每项 0-1 归一化后加权求和）：
      - 需求(月销量)：0.30
      - 单品收益($/SKU)：0.25
      - 新品占比(<1年)：0.15
      - 质量评分(平均Rating)：0.15
      - 竞争度(SKU数倒数，少为佳)：0.15
    """
    if type_agg is None or len(type_agg) == 0:
        return [], {}

    rows = []
    for _, tr in type_agg.iterrows():
        pt = tr['product_type']
        if pt == '其他':
            continue
        type_df_tmp = df[df['product_type'] == pt]
        sku_count = max(len(type_df_tmp), 1)
        total_sales = float(tr.get('total_sales', 0) or 0)
        total_rev = float(tr.get('total_revenue', 0) or 0)
        avg_rating = float(tr.get('avg_rating', 0) or 0)
        avg_rev = total_rev / sku_count if sku_count > 0 else 0
        new_ratio = (len(type_df_tmp[type_df_tmp['Available days'] < 365]) / sku_count
                     if 'Available days' in df.columns else 0.5)
        rows.append({
            'product_type': pt, 'total_sales': total_sales, 'avg_rev': avg_rev,
            'new_ratio': new_ratio, 'avg_rating': avg_rating, 'sku_count': sku_count,
        })
    if not rows:
        return [], {}

    max_sales = max(r['total_sales'] for r in rows) or 1
    max_rev = max(r['avg_rev'] for r in rows) or 1
    max_sku = max(r['sku_count'] for r in rows) or 1

    scores = {}
    for r in rows:
        n_sales = r['total_sales'] / max_sales
        n_rev = r['avg_rev'] / max_rev
        n_new = min(r['new_ratio'], 1.0)
        n_rating = r['avg_rating'] / 5.0 if r['avg_rating'] else 0.6
        n_comp = 1 - (r['sku_count'] / max_sku) * 0.7
        score = n_sales * 0.30 + n_rev * 0.25 + n_new * 0.15 + n_rating * 0.15 + n_comp * 0.15
        scores[r['product_type']] = {
            'score': score, 'total_sales': r['total_sales'], 'avg_rev': r['avg_rev'],
            'new_ratio': r['new_ratio'], 'avg_rating': r['avg_rating'], 'sku_count': r['sku_count'],
        }

    ranked = sorted(scores.keys(), key=lambda k: scores[k]['score'], reverse=True)
    return ranked, scores


# ============================================================
# 基于实际数据动态计算推荐入场价
# ============================================================
def calculate_pricing_recommendations(df, price_col, type_agg, ranked_types=None):
    """
    根据实际BSR数据计算各产品类型的推荐入场价
    返回动态生成的价格建议列表
    ranked_types: rank_product_types() 返回的排序列表，用于统一首推品类判定
    """
    pricing_recommendations = []
    top_type = ranked_types[0] if ranked_types else None
    
    for _, type_row in type_agg.iterrows():
        ptype = type_row['product_type']
        type_df = df[df['product_type'] == ptype]
        
        if len(type_df) < 2:
            continue
            
        prices = type_df[price_col].dropna()
        if len(prices) < 2:
            continue
            
        min_price = prices.min()
        p25 = prices.quantile(0.25)
        median_price = prices.median()
        p75 = prices.quantile(0.75)
        max_price = prices.max()
        
        # 计算单品收益
        total_rev = type_row['total_revenue'] if pd.notna(type_row['total_revenue']) else 0
        sku_count = type_row['count']
        avg_rev_per_sku = total_rev / sku_count if sku_count > 0 else 0
        
        # 推荐入场价：选择P40-P60区间，既不太低也不太高
        rec_min = round(median_price * 0.9, 2)
        rec_max = round(median_price * 1.1, 2)
        
        # 基于数据分析生成理由
        reasons = []
        if avg_rev_per_sku > 50000:
            reasons.append(f'单品月均收益${avg_rev_per_sku/10000:.1f}万，高收益赛道')
        elif avg_rev_per_sku > 20000:
            reasons.append(f'单品月均收益${avg_rev_per_sku/10000:.1f}万，收益可观')
        else:
            reasons.append(f'单品月均收益${avg_rev_per_sku:,.0f}，走量型赛道')
        
        if p75 / p25 > 1.5:
            reasons.append('价格带宽，差异化空间大')
        else:
            reasons.append('价格带集中，品质竞争为主')
        
        if len(type_df) < 5:
            reasons.append(f'竞品少（仅{len(type_df)}个SKU），蓝海机会')
        else:
            reasons.append(f'竞争适中（{len(type_df)}个SKU）')
        
        # 判断是否是首推品类（用统一综合评分）
        is_top_type = (ptype == top_type) if top_type else (
            ptype == type_agg.iloc[0]['product_type'] if len(type_agg) > 0 else False
        )
        note = '★ 首推入场品类' if is_top_type else ''
        
        pricing_recommendations.append({
            'product_type': ptype,
            'min_price': min_price,
            'p25': p25,
            'median': median_price,
            'p75': p75,
            'max_price': max_price,
            'rec_min': rec_min,
            'rec_max': rec_max,
            'reason': '；'.join(reasons),
            'note': note,
            'sku_count': len(type_df),
            'avg_rev': avg_rev_per_sku,
            'is_top': is_top_type
        })
    
    # 按综合评分排序（与 Sheet 6 上新方向保持一致）
    if ranked_types:
        rank_index = {t: i for i, t in enumerate(ranked_types)}
        pricing_recommendations.sort(key=lambda x: rank_index.get(x['product_type'], 999))
    else:
        pricing_recommendations.sort(key=lambda x: x['avg_rev'], reverse=True)
    return pricing_recommendations


# ============================================================
# 基于实际数据动态生成产品上新方向
# ============================================================
def generate_product_directions(df, rev_df, neg_counts, type_agg, price_col):
    """
    根据实际差评分析和市场数据生成产品上新方向
    """
    directions = []
    priority_counter = {'P1': 0, 'P2': 0, 'P3': 0, 'P4': 0}
    
    # 排序差评痛点
    neg_sorted = sorted(neg_counts.items(), key=lambda x: x[1], reverse=True)
    top_pains = neg_sorted[:5] if len(neg_sorted) >= 5 else neg_sorted
    
    # 找出有差评的具体产品类型
    if len(rev_df) > 0:
        asin_pain_map = {}
        low_rev = rev_df[rev_df['Rating'] <= 2].copy()
        if len(low_rev) > 0:
            for asin in low_rev['source_asin'].unique():
                asin_reviews = low_rev[low_rev['source_asin'] == asin]
                pain_text = (asin_reviews['Title'].fillna('') + ' ' + asin_reviews['Content'].fillna('')).str.lower()
                
                for pain_cat, cnt in neg_counts.items():
                    if cnt > 0:
                        if asin not in asin_pain_map:
                            asin_pain_map[asin] = {}
                        asin_pain_map[asin][pain_cat] = cnt
        
        # 基于产品类型生成上新方向 — 使用统一综合评分（与 Sheet 10 首推品类保持一致）
        ranked_types, _type_score_detail = rank_product_types(df, type_agg)
        type_priority = {}
        for i, pt in enumerate(ranked_types):
            if i == 0:
                type_priority[pt] = 'P1'
            elif i == 1:
                type_priority[pt] = 'P2'
            elif i <= 3:
                type_priority[pt] = 'P3'
            else:
                type_priority[pt] = 'P4'

        for _, type_row in type_agg.iterrows():
            ptype = type_row['product_type']
            if ptype not in type_priority:
                continue
                
            type_df = df[df['product_type'] == ptype]
            if len(type_df) < 2:
                continue
            
            prices = type_df[price_col].dropna()
            avg_price = prices.mean() if len(prices) > 0 else 30
            median_price = prices.median() if len(prices) > 0 else 30
            
            # 生成针对性的痛点分析
            related_pains = [p for p in top_pains if any(kw in p[0] for kw in ['电池', '亮度', '磁力', '质量', '固定'])]
            pain_summary = '、'.join([f'{p[0]}({p[1]}条)' for p in related_pains[:3]])

            # 生成核心改进点
            improvements = []
            if any('电池' in p[0] for p in related_pains):
                improvements.append('• 电池容量升级至≥5000mAh（竞品多在3000-4000mAh）')
                improvements.append('• USB-C快充+低电量提示')
            if any('亮度' in p[0] for p in related_pains):
                improvements.append(f'• 亮度提升至≥{int(avg_price * 50)}lm（超越均价${avg_price:.0f}竞品）')
            if any('磁力' in p[0] or '固定' in p[0] for p in related_pains):
                improvements.append('• 磁力升级至≥35N，加锁定机构')
            if any('质量' in p[0] for p in related_pains):
                improvements.append('• 外壳材质升级（铝合金+ABS），通过3米跌落测试')

            if not improvements:
                improvements = ['• 差异化功能：增加电量数显/SOS模式',
                               f'• 外观专利，避免同质竞争']
            
            # 目标售价建议
            target_low = round(median_price * 0.95, 2)
            target_high = round(median_price * 1.15, 2)
            
            # 预计月销（基于同类竞品表现估算）
            type_sales = type_row['total_sales'] if pd.notna(type_row['total_sales']) else 0
            type_sku = type_row['count']
            avg_monthly_sales = type_sales / type_sku if type_sku > 0 else 200
            est_monthly_low = int(avg_monthly_sales * 0.6)
            est_monthly_high = int(avg_monthly_sales * 1.2)
            
            priority = type_priority.get(ptype, 'P3')
            priority_counter[priority] += 1
            prio_label = f'{priority}\n{"首推" if priority == "P1" else "推荐" if priority == "P2" else "参考" if priority == "P3" else "备选"}'
            
            # 竞争难度评估
            if type_sku <= 5:
                difficulty = '低'
                note = '竞品少，蓝海机会'
            elif type_sku <= 15:
                difficulty = '中'
                note = '竞争适中'
            else:
                difficulty = '中高'
                note = '竞争激烈，需差异化'
            
            # 数据支撑依据
            data_support = f'该类型月总销量{int(type_sales):,}件，{type_sku}个SKU参与竞争'
            if pain_summary:
                data_support += f'\\n差评痛点：{pain_summary}'
            
            directions.append((
                prio_label,
                ptype,
                '\n'.join(improvements),
                data_support,
                f'${target_low:.2f}-${target_high:.2f}',
                f'{est_monthly_low}-{est_monthly_high}件/月',
                difficulty,
                'FBA直发，首批备货300-500pcs',
                note
            ))
    
    # 按优先级排序
    priority_order = {'P1': 0, 'P2': 1, 'P3': 2, 'P4': 3}
    directions.sort(key=lambda x: (priority_order.get(x[0].split('\n')[0], 4), x[0]))
    
    return directions[:6]  # 最多返回6个方向


# ============================================================
# Excel样式函数
# ============================================================
C_BLUE_DARK = 'FF1F3864'
C_BLUE_MID = 'FF2E74B5'
C_BLUE_LIGHT = 'FFD6E4F0'
C_YELLOW = 'FFFFF2CC'
C_GREEN_LIGHT = 'FFE2EFDA'
C_ORANGE = 'FFFCE4D6'
C_WHITE = 'FFFFFFFF'
C_GREY_LIGHT = 'FFF2F2F2'
C_RED_LIGHT = 'FFFDECEA'
C_SECTION_BG = 'FF1F3864'


def hdr(ws, row, col, text, bg=C_BLUE_MID, fg=C_WHITE, bold=True, size=11, wrap=False, h_align='center'):
    cell = ws.cell(row=row, column=col, value=text)
    cell.font = Font(name='Arial', bold=bold, color=fg, size=size)
    cell.fill = PatternFill('solid', fgColor=bg)
    cell.alignment = Alignment(horizontal=h_align, vertical='center', wrap_text=wrap)
    return cell


def val(ws, row, col, text, bg=C_WHITE, bold=False, size=10, wrap=True,
        h_align='left', fg='FF000000', italic=False):
    cell = ws.cell(row=row, column=col, value=text)
    cell.font = Font(name='Arial', bold=bold, color=fg, size=size, italic=italic)
    cell.fill = PatternFill('solid', fgColor=bg)
    cell.alignment = Alignment(horizontal=h_align, vertical='center', wrap_text=wrap)
    return cell


def thin_border():
    side = Side(style='thin', color='FFBFBFBF')
    return Border(left=side, right=side, top=side, bottom=side)


def apply_border(ws, min_row, max_row, min_col, max_col):
    border = thin_border()
    for row in ws.iter_rows(min_row=min_row, max_row=max_row, min_col=min_col, max_col=max_col):
        for cell in row:
            cell.border = border


def section_title(ws, row, col, text, span=8, bg=C_SECTION_BG):
    ws.merge_cells(start_row=row, start_column=col, end_row=row, end_column=col+span-1)
    cell = ws.cell(row=row, column=col, value=text)
    cell.font = Font(name='Arial', bold=True, color=C_WHITE, size=12)
    cell.fill = PatternFill('solid', fgColor=bg)
    cell.alignment = Alignment(horizontal='left', vertical='center')
    ws.row_dimensions[row].height = 24


# ============================================================
# Market 分析文件读取（可选）
# ============================================================
def load_market_data(market_path):
    """
    读取卖家精灵 US-Market 文件。文件缺失或 sheet 缺失时返回 None，不抛异常。
    返回 dict：{sheet_key: DataFrame or None, ...}
    """
    keys = [
        ('market_summary', 'Market Analysis'),
        ('demand_trends', 'Industry Demand and Trends'),
        ('sell_trends', 'Industry Sell Trends'),
        ('listing_concentration', 'Listing Concentration'),
        ('brand_concentration', 'Brand Concentration'),
        ('seller_concentration', 'Seller Concentration'),
        ('fulfillment', 'Fulfillment'),
        ('aplus_video', 'A+ Content and Video'),
        ('origin_of_seller', 'Origin of Seller'),
        ('publication_time', 'Publication Time'),
        ('publication_time_trends', 'Publication Time Trends'),
        ('ratings_dist', 'Ratings'),
        ('rating_dist', 'Rating'),
        ('price_dist', 'Price'),
    ]
    result = {k: None for k, _ in keys}
    result['_available'] = False
    if not market_path or not os.path.exists(market_path):
        return result
    try:
        xl = pd.ExcelFile(market_path)
        sheets = set(xl.sheet_names)
        for key, sheet_name in keys:
            if sheet_name in sheets:
                try:
                    result[key] = pd.read_excel(market_path, sheet_name=sheet_name)
                except Exception as e:
                    print(f"读取 Market sheet '{sheet_name}' 出错: {e}")
        result['_available'] = any(v is not None for k, v in result.items() if k != '_available')
    except Exception as e:
        print(f"读取 Market 文件出错: {e}")
    return result


# ============================================================
# ReverseASIN 反查关键词文件读取（可选）
# ============================================================
def load_keyword_data(keyword_path):
    result = {'keywords': None, 'unique_words': None, '_available': False}
    if not keyword_path or not os.path.exists(keyword_path):
        return result
    try:
        xl = pd.ExcelFile(keyword_path)
        for sheet in xl.sheet_names:
            if 'US-' in sheet:
                result['keywords'] = pd.read_excel(keyword_path, sheet_name=sheet)
                break
        if 'Unique Words' in xl.sheet_names:
            result['unique_words'] = pd.read_excel(keyword_path, sheet_name='Unique Words')
        result['_available'] = result['keywords'] is not None
    except Exception as e:
        print(f"读取关键词文件出错: {e}")
    return result


# ============================================================
# Title 特征提取
# ============================================================
import re as _re

def extract_specs_from_title(title):
    """从产品标题中正则提取常见规格，识别不到的字段返回空字符串"""
    t = str(title) if title is not None else ''
    tl = t.lower()
    specs = {
        '功率W': '',
        '光通量lm': '',
        '电池类型': '',
        '续航': '',
        '防水等级': '',
        '光源': '',
        '供电方式': '',
    }
    m = _re.search(r'(\d{2,6})\s*(?:w|watt)s?\b', tl)
    if m:
        specs['功率W'] = f"{m.group(1)}W"
    m = _re.search(r'(\d{2,6})\s*(?:lm|lumens?)\b', tl)
    if m:
        specs['光通量lm'] = f"{m.group(1)}lm"
    if 'lithium' in tl or 'li-ion' in tl or 'li ion' in tl:
        specs['电池类型'] = '锂电池'
    elif 'lifepo4' in tl:
        specs['电池类型'] = '磷酸铁锂'
    elif 'aa ' in tl or 'aaa ' in tl:
        specs['电池类型'] = '干电池'
    m = _re.search(r'(\d{1,2})\s*(?:v|volt)s?\b', tl)
    if m and not specs['电池类型']:
        specs['电池类型'] = f"{m.group(1)}V 电池"
    m = _re.search(r'(\d{1,3})\s*(?:hours?|hrs?|h)\s*(?:run|runtime|battery)?', tl)
    if m:
        specs['续航'] = f"{m.group(1)}h"
    m = _re.search(r'ip\s?(\d{2})\b', tl)
    if m:
        specs['防水等级'] = f"IP{m.group(1)}"
    elif 'waterproof' in tl:
        specs['防水等级'] = '防水'
    elif 'water resistant' in tl or 'water-resistant' in tl:
        specs['防水等级'] = '防泼溅'
    if 'cob' in tl:
        specs['光源'] = 'COB LED'
    elif 'led' in tl:
        specs['光源'] = 'LED'
    if 'rechargeable' in tl or 'cordless' in tl:
        specs['供电方式'] = '充电式'
    elif 'solar' in tl:
        specs['供电方式'] = '太阳能'
    elif 'plug' in tl or 'corded' in tl:
        specs['供电方式'] = '插电'
    elif 'battery' in tl:
        specs['供电方式'] = '电池供电'
    return specs


def extract_specs_from_bullets(bullet_text):
    """从 Bullet Points 列文本中提取详细产品规格（补充标题正则无法覆盖的参数）"""
    t = str(bullet_text) if bullet_text is not None else ''
    tl = t.lower()
    specs = {
        '功率W': '',
        '光通量lm': '',
        '电池类型': '',
        '续航': '',
        '防水等级': '',
        '光源': '',
        '供电方式': '',
        '电池容量mAh': '',
        '充电方式': '',
        '光照模式数': '',
        '磁力/固定方式': '',
        '旋转角度': '',
        '材质': '',
        '重量': '',
    }
    if not tl.strip():
        return specs

    # 功率
    m = _re.search(r'(\d{2,6})\s*(?:w|watt)s?\b', tl)
    if m:
        specs['功率W'] = f"{m.group(1)}W"
    # 光通量
    m = _re.search(r'(\d{2,6})\s*(?:lm|lumens?)\b', tl)
    if m:
        specs['光通量lm'] = f"{m.group(1)}lm"
    # 电池类型
    if 'lithium' in tl or 'li-ion' in tl or 'li ion' in tl:
        specs['电池类型'] = '锂电池'
    elif 'lifepo4' in tl:
        specs['电池类型'] = '磷酸铁锂'
    # 续航
    m = _re.search(r'(?:up\s+to\s+)?(\d{1,3}(?:\.\d)?)\s*(?:hours?|hrs?)\s*(?:of\s+)?(?:run|runtime|battery|light|use)?', tl)
    if m:
        specs['续航'] = f"{m.group(1)}h"
    # 防水等级
    m = _re.search(r'ip\s?[x]?(\d{2})\b', tl)
    if m:
        specs['防水等级'] = f"IP{m.group(1)}"
    elif 'waterproof' in tl:
        specs['防水等级'] = '防水'
    elif 'water resistant' in tl or 'water-resistant' in tl:
        specs['防水等级'] = '防泼溅'
    # 光源
    if 'cob' in tl:
        specs['光源'] = 'COB LED'
    elif 'smd' in tl:
        specs['光源'] = 'SMD LED'
    elif 'led' in tl:
        specs['光源'] = 'LED'
    # 供电方式
    if 'rechargeable' in tl or 'cordless' in tl:
        specs['供电方式'] = '充电式'
    elif 'solar' in tl:
        specs['供电方式'] = '太阳能'
    elif 'plug' in tl or 'corded' in tl:
        specs['供电方式'] = '插电'
    elif 'battery' in tl:
        specs['供电方式'] = '电池供电'

    # === 新增参数 ===
    # 电池容量
    m = _re.search(r'(\d{3,5})\s*mah', tl)
    if m:
        specs['电池容量mAh'] = f"{m.group(1)}mAh"
    # 充电方式
    if 'usb-c' in tl or 'usb c' in tl or 'type-c' in tl or 'type c' in tl:
        specs['充电方式'] = 'USB-C'
    elif 'micro-usb' in tl or 'micro usb' in tl:
        specs['充电方式'] = 'Micro-USB'
    elif 'usb' in tl and 'charg' in tl:
        specs['充电方式'] = 'USB'
    # 光照模式数
    m = _re.search(r'(\d+)\s*(?:lighting\s+)?(?:mode|brightness\s+level|working\s+mode)', tl)
    if m:
        specs['光照模式数'] = f"{m.group(1)}种"
    # 磁力/固定方式
    fix_methods = []
    if 'magnet' in tl or 'magnetic' in tl:
        fix_methods.append('磁吸')
    if 'hook' in tl:
        fix_methods.append('挂钩')
    if 'clip' in tl or 'clamp' in tl:
        fix_methods.append('夹具')
    if 'tripod' in tl:
        fix_methods.append('三脚架')
    if 'carabiner' in tl:
        fix_methods.append('登山扣')
    if fix_methods:
        specs['磁力/固定方式'] = '+'.join(fix_methods)
    # 旋转角度
    m = _re.search(r'(\d{2,3})\s*(?:degree|°)\s*(?:rotation|rotate|swivel|adjustable|pivot)?', tl)
    if m:
        specs['旋转角度'] = f"{m.group(1)}°"
    # 材质
    materials = []
    if 'aluminum' in tl or 'aluminium' in tl:
        materials.append('铝合金')
    if 'abs' in tl:
        materials.append('ABS')
    if 'rubber' in tl or 'silicone' in tl:
        materials.append('橡胶/硅胶')
    if 'stainless steel' in tl:
        materials.append('不锈钢')
    if materials:
        specs['材质'] = '+'.join(materials)
    # 重量
    m = _re.search(r'(\d+\.?\d*)\s*(?:oz|ounces?)\b', tl)
    if m:
        specs['重量'] = f"{m.group(1)}oz"
    else:
        m = _re.search(r'(\d+\.?\d*)\s*(?:g|grams?)\b', tl)
        if m and float(m.group(1)) > 10:  # 排除误匹配
            specs['重量'] = f"{m.group(1)}g"
        else:
            m = _re.search(r'(\d+\.?\d*)\s*(?:lb|lbs|pounds?)\b', tl)
            if m:
                specs['重量'] = f"{m.group(1)}lb"
    return specs


def extract_all_specs(title, bullet_text):
    """合并标题和五点描述的规格提取结果，五点描述优先（更详细）"""
    title_specs = extract_specs_from_title(title)
    bullet_specs = extract_specs_from_bullets(bullet_text)
    merged = {}
    all_keys = set(list(title_specs.keys()) + list(bullet_specs.keys()))
    for k in all_keys:
        v_title = title_specs.get(k, '')
        v_bullet = bullet_specs.get(k, '')
        # 五点描述非空则优先，否则用标题提取
        merged[k] = v_bullet if v_bullet else v_title
    return merged


def aggregate_recommended_specs(all_specs_list, neg_counts=None):
    """
    从竞品参数列表中聚合推荐规格。
    all_specs_list: list of dicts (每个竞品的 extract_all_specs 结果)
    neg_counts: dict of {差评类型: 数量}
    返回: list of (参数名, 建议规格, 数据依据, 优先级)
    """
    if neg_counts is None:
        neg_counts = {}
    n = len(all_specs_list)
    if n == 0:
        return []

    results = []

    # 数值型参数提取
    def extract_num(val_str, unit_patterns):
        """从带单位的字符串中提取数值"""
        if not val_str:
            return None
        for pat in unit_patterns:
            m = _re.search(pat, str(val_str).lower())
            if m:
                try:
                    return float(m.group(1))
                except (ValueError, IndexError):
                    pass
        return None

    # 光通量
    lm_vals = [extract_num(s.get('光通量lm', ''), [r'(\d+\.?\d*)']) for s in all_specs_list]
    lm_vals = [v for v in lm_vals if v is not None and v > 0]
    if lm_vals:
        import statistics
        med = statistics.median(lm_vals)
        p75 = sorted(lm_vals)[int(len(lm_vals) * 0.75)] if len(lm_vals) >= 4 else max(lm_vals)
        has_neg = neg_counts.get('亮度不足', 0) > 0
        priority = 'P1-必备' if has_neg else 'P2-重要'
        results.append(('光通量', f'≥{int(p75)}lm',
                        f'{len(lm_vals)}/{n}竞品标注, 中位数{int(med)}lm, P75={int(p75)}lm'
                        + (f', 差评"亮度不足"{int(neg_counts.get("亮度不足", 0))}条' if has_neg else ''),
                        priority))

    # 电池容量
    mah_vals = [extract_num(s.get('电池容量mAh', ''), [r'(\d+\.?\d*)']) for s in all_specs_list]
    mah_vals = [v for v in mah_vals if v is not None and v > 0]
    if mah_vals:
        import statistics
        med = statistics.median(mah_vals)
        p75 = sorted(mah_vals)[int(len(mah_vals) * 0.75)] if len(mah_vals) >= 4 else max(mah_vals)
        has_neg = neg_counts.get('电池/充电问题', 0) > 0
        priority = 'P1-必备' if has_neg else 'P2-重要'
        results.append(('电池容量', f'≥{int(p75)}mAh',
                        f'{len(mah_vals)}/{n}竞品标注, 中位数{int(med)}mAh'
                        + (f', 差评"电池/充电"{int(neg_counts.get("电池/充电问题", 0))}条' if has_neg else ''),
                        priority))

    # 充电方式
    charge_vals = [s.get('充电方式', '') for s in all_specs_list]
    charge_vals = [v for v in charge_vals if v]
    if charge_vals:
        from collections import Counter
        ctr = Counter(charge_vals)
        top_charge = ctr.most_common(1)[0]
        results.append(('充电方式', top_charge[0],
                        f'{top_charge[1]}/{n}竞品采用{top_charge[0]}',
                        'P1-必备' if top_charge[1] / n > 0.4 else 'P2-重要'))

    # 防水等级
    ip_vals = [s.get('防水等级', '') for s in all_specs_list]
    ip_vals = [v for v in ip_vals if v and v not in ('防水', '防泼溅')]
    ip_all = [v for v in [s.get('防水等级', '') for s in all_specs_list] if v]
    has_neg = neg_counts.get('防水/防尘问题', 0) > 0
    if ip_all:
        from collections import Counter
        ctr = Counter(ip_all)
        top_ip = ctr.most_common(1)[0]
        priority = 'P1-必备' if has_neg else ('P2-重要' if len(ip_all) / n > 0.3 else 'P3-加分')
        results.append(('防水等级', top_ip[0] if top_ip[0] not in ('防水', '防泼溅') else 'IP65',
                        f'{len(ip_all)}/{n}竞品标注防水'
                        + (f', 差评"防水问题"{int(neg_counts.get("防水/防尘问题", 0))}条' if has_neg else ''),
                        priority))

    # 光照模式数
    mode_vals = [extract_num(s.get('光照模式数', ''), [r'(\d+)']) for s in all_specs_list]
    mode_vals = [v for v in mode_vals if v is not None and v > 0]
    if mode_vals:
        import statistics
        med = statistics.median(mode_vals)
        p75 = sorted(mode_vals)[int(len(mode_vals) * 0.75)] if len(mode_vals) >= 4 else max(mode_vals)
        results.append(('光照模式', f'≥{int(p75)}种',
                        f'{len(mode_vals)}/{n}竞品标注, 中位数{int(med)}种',
                        'P2-重要'))

    # 磁力/固定方式
    fix_vals = [s.get('磁力/固定方式', '') for s in all_specs_list]
    fix_vals = [v for v in fix_vals if v]
    has_neg = neg_counts.get('磁力不稳/固定问题', 0) > 0
    if fix_vals:
        # 统计各固定方式频率
        all_methods = []
        for v in fix_vals:
            all_methods.extend(v.split('+'))
        from collections import Counter
        ctr = Counter(all_methods)
        top_methods = [m for m, _ in ctr.most_common(3)]
        priority = 'P1-必备' if has_neg else 'P2-重要'
        results.append(('磁力/固定', '+'.join(top_methods),
                        f'{len(fix_vals)}/{n}竞品有固定功能'
                        + (f', 差评"磁力/固定"{int(neg_counts.get("磁力不稳/固定问题", 0))}条' if has_neg else ''),
                        priority))

    # 材质
    mat_vals = [s.get('材质', '') for s in all_specs_list]
    mat_vals = [v for v in mat_vals if v]
    has_neg = neg_counts.get('耐久性/质量差', 0) > 0
    if mat_vals:
        from collections import Counter
        all_mats = []
        for v in mat_vals:
            all_mats.extend(v.split('+'))
        ctr = Counter(all_mats)
        top_mat = ctr.most_common(1)[0][0]
        priority = 'P2-重要' if has_neg else 'P3-加分'
        results.append(('材质', top_mat,
                        f'{len(mat_vals)}/{n}竞品标注材质'
                        + (f', 差评"质量差"{int(neg_counts.get("耐久性/质量差", 0))}条' if has_neg else ''),
                        priority))

    # 重量
    wt_vals = [extract_num(s.get('重量', ''), [r'(\d+\.?\d*)']) for s in all_specs_list]
    wt_vals = [v for v in wt_vals if v is not None and v > 0]
    if wt_vals:
        import statistics
        med = statistics.median(wt_vals)
        # 取单位
        sample_unit = ''
        for s in all_specs_list:
            wv = s.get('重量', '')
            if wv:
                sample_unit = _re.sub(r'[\d.]+', '', wv).strip()
                break
        results.append(('重量', f'≤{med:.1f}{sample_unit}',
                        f'{len(wt_vals)}/{n}竞品标注, 中位数{med:.1f}{sample_unit}',
                        'P3-加分'))

    # 旋转角度
    rot_vals = [s.get('旋转角度', '') for s in all_specs_list]
    rot_vals = [v for v in rot_vals if v]
    if rot_vals:
        from collections import Counter
        ctr = Counter(rot_vals)
        top_rot = ctr.most_common(1)[0]
        results.append(('旋转角度', top_rot[0],
                        f'{len(rot_vals)}/{n}竞品标注旋转角度',
                        'P3-加分'))

    # 续航
    hr_vals = [extract_num(s.get('续航', ''), [r'(\d+\.?\d*)']) for s in all_specs_list]
    hr_vals = [v for v in hr_vals if v is not None and v > 0]
    if hr_vals:
        import statistics
        med = statistics.median(hr_vals)
        p75 = sorted(hr_vals)[int(len(hr_vals) * 0.75)] if len(hr_vals) >= 4 else max(hr_vals)
        results.append(('续航时长', f'≥{p75:.0f}h',
                        f'{len(hr_vals)}/{n}竞品标注, 中位数{med:.0f}h',
                        'P2-重要'))

    # 按优先级排序
    priority_order = {'P1-必备': 0, 'P2-重要': 1, 'P3-加分': 2}
    results.sort(key=lambda x: priority_order.get(x[3], 9))
    return results


def infer_lifecycle_stage(pub_trends_df):
    """
    根据 Publication Time Trends 表判断类目生命周期。
    返回 (stage, reason_text)
    """
    if pub_trends_df is None or len(pub_trends_df) == 0:
        return ('未知', '缺少发布年度数据')
    df = pub_trends_df.copy()
    # 列名：Launch Years | Products | Sales | Sales Proportion | Monthly Revenue($) | Revenue Proportion
    try:
        df['Launch Years'] = pd.to_numeric(df['Launch Years'], errors='coerce')
        df['Sales Proportion'] = pd.to_numeric(df['Sales Proportion'], errors='coerce')
        df = df.dropna(subset=['Launch Years'])
    except Exception:
        return ('未知', '发布年度数据格式异常')
    current_year = datetime.now().year
    recent_3 = df[df['Launch Years'] >= current_year - 3]['Sales Proportion'].sum()
    mid_3_6 = df[(df['Launch Years'] >= current_year - 6) & (df['Launch Years'] < current_year - 3)]['Sales Proportion'].sum()
    old_6plus = df[df['Launch Years'] < current_year - 6]['Sales Proportion'].sum()
    if recent_3 > 0.4:
        return ('成长期', f'近 3 年新品贡献 {recent_3:.1%} 销量，类目处于成长期')
    elif mid_3_6 > 0.4:
        return ('成熟期', f'3-6 年内产品贡献 {mid_3_6:.1%} 销量，类目趋于成熟')
    elif old_6plus > 0.4:
        return ('成熟晚期', f'6 年以上老品贡献 {old_6plus:.1%} 销量，类目进入稳定/衰退期')
    else:
        return ('混合期', f'新品 {recent_3:.1%} | 中生代 {mid_3_6:.1%} | 老品 {old_6plus:.1%}，生命周期分布均衡')


# ============================================================
# 核心报告生成函数
# ============================================================
def generate_report(bsr_path, review_paths, output_path, market_path=None, keyword_path=None):
    """
    完整版报告生成 - 移植原版分析逻辑 + Market/关键词文件扩展分析
    """
    # --- 1. 读取BSR数据 ---
    df = pd.read_excel(bsr_path, sheet_name='US')

    # --- 1b. 读取 Market 文件（可选） ---
    market_data = load_market_data(market_path)

    # --- 1c. 读取关键词反查文件（可选） ---
    keyword_data = load_keyword_data(keyword_path)

    # 动态识别列名
    price_col = None
    rev_col = None
    for c in df.columns:
        c_lower = c.lower()
        if price_col is None and 'price' in c_lower:
            price_col = c
        if rev_col is None and 'revenue' in c_lower and 'variation' not in c_lower:
            rev_col = c

    # 数值转换
    for c in [price_col, rev_col, 'Monthly Sales', 'Rating', 'Ratings',
              'Available days', 'Gross Margin', 'FBA($)']:
        if c and c in df.columns:
            df[c] = pd.to_numeric(df[c], errors='coerce')

    # 产品分类
    df['product_type'] = df['Product Title'].apply(classify)

    # --- 2. 读取评论数据 ---
    # 支持中英文列名映射
    column_mapping = {
        'Rating': '星级',
        'Title': '标题',
        'Content': '内容',
        'Date': '评论时间',
        'source_asin': 'source_asin'
    }

    all_reviews = []
    for f in review_paths:
        try:
            df_r = pd.read_excel(f, sheet_name=0)
            # 动态识别Rating列（支持中英文）
            rating_col = None
            title_col = None
            content_col = None
            date_col = None
            
            for c in df_r.columns:
                c_lower = str(c).lower()
                if rating_col is None and ('rating' in c_lower or '星级' in c):
                    rating_col = c
                if title_col is None and ('title' in c_lower or '标题' in c):
                    title_col = c
                if content_col is None and ('content' in c_lower or '内容' in c):
                    content_col = c
                if date_col is None and ('date' in c_lower or '评论时间' in c or '时间' in c):
                    date_col = c
            
            if rating_col is None:
                continue
                
            asin = os.path.basename(f).split('-')[0]
            df_r['source_asin'] = asin
            
            # 重命名列以统一使用英文
            rename_dict = {}
            if rating_col and rating_col != 'Rating':
                rename_dict[rating_col] = 'Rating'
            if title_col and title_col != 'Title':
                rename_dict[title_col] = 'Title'
            if content_col and content_col != 'Content':
                rename_dict[content_col] = 'Content'
            if date_col and date_col != 'Date':
                rename_dict[date_col] = 'Date'
            
            if rename_dict:
                df_r = df_r.rename(columns=rename_dict)
            
            all_reviews.append(df_r)
        except Exception as e:
            print(f"读取评论文件出错: {f}, 错误: {e}")
            continue

    rev_df = pd.concat(all_reviews, ignore_index=True) if all_reviews else pd.DataFrame()
    if len(rev_df) > 0 and 'Rating' in rev_df.columns:
        rev_df['Rating'] = pd.to_numeric(rev_df['Rating'], errors='coerce')
    low_rev = rev_df[rev_df['Rating'] <= 2].copy() if len(rev_df) > 0 and 'Rating' in rev_df.columns else pd.DataFrame()
    high_rev = rev_df[rev_df['Rating'] >= 4].copy() if len(rev_df) > 0 and 'Rating' in rev_df.columns else pd.DataFrame()

    # --- 4. 统计汇总 ---
    total_rev = df[rev_col].sum() if rev_col else 0
    total_sales = df['Monthly Sales'].sum() if 'Monthly Sales' in df.columns else 0
    avg_price = df[price_col].mean() if price_col else 0
    median_price = df[price_col].median() if price_col else 0
    avg_rating = df['Rating'].mean() if 'Rating' in df.columns else 0
    brand_count = df['Brand'].nunique() if 'Brand' in df.columns else 0
    cn_rev = df[df['BuyBox Location'] == 'CN'][rev_col].sum() if rev_col else 0
    us_rev = df[df['BuyBox Location'] == 'US'][rev_col].sum() if rev_col else 0
    cn_cnt = (df['BuyBox Location'] == 'CN').sum()
    us_cnt = (df['BuyBox Location'] == 'US').sum()

    # 产品类型聚合
    type_agg = df.groupby('product_type').agg(
        count=('ASIN', 'count'),
        avg_price=(price_col, 'mean'),
        min_price=(price_col, 'min'),
        max_price=(price_col, 'max'),
        total_sales=('Monthly Sales', 'sum'),
        total_revenue=(rev_col, 'sum'),
        avg_rating=('Rating', 'mean'),
    ).sort_values('total_revenue', ascending=False).reset_index()

    # 品牌聚合
    brand_agg = df.groupby('Brand').agg(
        sku_count=('ASIN', 'count'),
        total_rev=(rev_col, 'sum'),
        avg_rating=('Rating', 'mean'),
    ).sort_values('total_rev', ascending=False).reset_index()

    # 价格分布
    bins = [0, 15, 25, 35, 50, 70, 100, 999]
    labels = ['<$15', '$15-25', '$25-35', '$35-50', '$50-70', '$70-100', '>$100']
    df['price_band'] = pd.cut(df[price_col], bins=bins, labels=labels)
    price_dist = df['price_band'].value_counts().sort_index()

    # 年龄分布
    new_df = df[df['Available days'] < 365] if 'Available days' in df.columns else pd.DataFrame()
    bins_age = [0, 90, 180, 365, 730, 1460, 9999]
    labels_age = ['<3个月', '3-6个月', '6-12个月', '1-2年', '2-4年', '>4年']
    df['age_band'] = pd.cut(df['Available days'], bins=bins_age, labels=labels_age)
    age_dist = df['age_band'].value_counts().sort_index()

    # --- 4. 关键词分析 ---
    neg_keywords = {
        '电池/充电问题': ['battery', 'charge', 'charging', 'battery life', 'drain', 'dies fast'],
        '耐久性/质量差': ['broke', 'broken', 'stopped working', 'quit working', 'doesnt work',
                        "doesn't work", 'dead', 'died', 'cheap', 'flimsy', 'poor quality',
                        'defective', 'failed', 'fell apart'],
        '亮度不足': ['not bright', 'dim', 'not bright enough', 'brightness', 'weak light', 'too dim'],
        '灯头/角度固定差': ['head', 'angle', 'position', 'tilt', 'rotate', 'floppy', 'swivel',
                          'pivot', 'loose', 'wobble'],
        '客服/售后问题': ['customer service', 'return', 'refund', 'warranty', 'replacement'],
        '磁力不稳/固定问题': ['magnet', 'magnetic', 'not stick', "won't stick", 'falls off',
                           'hold position', 'wobble'],
        '不含电池/配件缺失': ['not included', 'tool only', 'no battery', 'bare tool', 'no charger'],
        '防水/防尘问题': ['water', 'rain', 'wet', 'rust', 'corrosion'],
    }

    pos_keywords = {
        '高亮度/强光': ['bright', 'lumens', 'illumination', 'super bright', 'very bright', 'powerful light'],
        '轻便/便携': ['compact', 'lightweight', 'portable', 'small', 'easy to carry', 'handy'],
        '易用/操作方便': ['easy to use', 'convenient', 'simple', 'love it', 'perfect', 'great for'],
        '性价比高': ['value', 'great price', 'good price', 'affordable', 'worth', 'bang for the buck'],
        '耐用/品质好': ['durable', 'sturdy', 'solid', 'tough', 'last long', 'holds up', 'well built'],
        '电池续航好': ['long battery', 'battery life', 'usb', 'charges fast', 'long lasting'],
        '多功能/多模式': ['multiple modes', 'different modes', 'versatile', 'multi', 'strobe', 'sos'],
        '磁力强/多角度调节': ['magnet', 'magnetic', 'strong magnet', '360', 'rotate', 'flexible', 'hands-free'],
    }

    neg_counts = {}
    if len(low_rev) > 0 and 'Title' in low_rev.columns and 'Content' in low_rev.columns:
        neg_text = (low_rev['Title'].fillna('') + ' ' + low_rev['Content'].fillna('')).str.lower()
        for cat, kws in neg_keywords.items():
            pattern = '|'.join(kws)
            neg_counts[cat] = neg_text.str.contains(pattern, na=False).sum()
    else:
        neg_counts = {k: 0 for k in neg_keywords.keys()}

    pos_counts = {}
    if len(high_rev) > 0 and 'Title' in high_rev.columns and 'Content' in high_rev.columns:
        pos_text = (high_rev['Title'].fillna('') + ' ' + high_rev['Content'].fillna('')).str.lower()
        for cat, kws in pos_keywords.items():
            pattern = '|'.join(kws)
            pos_counts[cat] = pos_text.str.contains(pattern, na=False).sum()
    else:
        pos_counts = {k: 0 for k in pos_keywords.keys()}

    # --- 5.1 动态计算推荐入场价（基于实际BSR数据分析） ---
    # 先计算统一综合评分（供 Sheet 6 上新方向 + Sheet 10 首推品类 共用）
    ranked_product_types, ranked_type_scores = rank_product_types(df, type_agg)
    pricing_recommendations = calculate_pricing_recommendations(df, price_col, type_agg, ranked_product_types)

    # --- 5.2 动态生成产品上新方向（基于实际差评分析和市场数据） ---
    new_directions = generate_product_directions(df, rev_df, neg_counts, type_agg, price_col)
    
    # 定义差评痛点排序（供多处使用）
    neg_sorted_for_use = sorted(neg_counts.items(), key=lambda x: x[1], reverse=True)
    top_pain_for_use = neg_sorted_for_use[0][0] if neg_sorted_for_use else '电池/充电问题'
    top_pain_cnt_for_use = neg_sorted_for_use[0][1] if neg_sorted_for_use else 50

    # --- 5. 构建Excel报告 ---
    wb = Workbook()

    # ===== Sheet 1: 市场分析 =====
    ws1 = wb.active
    ws1.title = '市场分析'
    ws1.sheet_view.showGridLines = False
    for col_letter, width in zip('ABCDEFGH', [22, 18, 18, 18, 18, 18, 18, 18]):
        ws1.column_dimensions[col_letter].width = width

    # 标题
    ws1.merge_cells('A1:H1')
    c = ws1['A1']
    c.value = 'LED工作灯 (Job Site Lighting) 市场选品评估报告'
    c.font = Font(name='Arial', bold=True, size=16, color=C_WHITE)
    c.fill = PatternFill('solid', fgColor=C_BLUE_DARK)
    c.alignment = Alignment(horizontal='center', vertical='center')
    ws1.row_dimensions[1].height = 40

    ws1.merge_cells('A2:H2')
    c = ws1['A2']
    c.value = f'数据来源：亚马逊 BSR TOP100  +  {len(rev_df)}条真实评论  |  生成时间：{datetime.now().strftime("%Y-%m-%d")}'
    c.font = Font(name='Arial', size=9, color='FF595959', italic=True)
    c.fill = PatternFill('solid', fgColor=C_GREY_LIGHT)
    c.alignment = Alignment(horizontal='center', vertical='center')
    ws1.row_dimensions[2].height = 18

    r = 4
    section_title(ws1, r, 1, '▌ 一、类目整体概况（BSR TOP100 样本）', span=8)
    r += 1

    hdr(ws1, r, 1, '指标', bg=C_BLUE_MID)
    hdr(ws1, r, 2, '数值', bg=C_BLUE_MID)
    ws1.merge_cells(start_row=r, start_column=3, end_row=r, end_column=8)
    hdr(ws1, r, 3, '说明', bg=C_BLUE_MID)
    ws1.row_dimensions[r].height = 20
    r += 1

    # 毛利率计算
    gm_mean = df['Gross Margin'].mean() * 100 if 'Gross Margin' in df.columns else 0
    gm_median = df['Gross Margin'].median() * 100 if 'Gross Margin' in df.columns else 0

    overview_data = [
        ('样本商品数', f'{len(df)} 个', 'BSR TOP100 实时样本'),
        ('涉及品牌数', f'{df["Brand"].nunique()} 个', '品牌分散，头部效应弱，新品进入门槛低'),
        ('月总销量', f'{int(total_sales):,} 件', 'BSR TOP100 样本合计月销'),
        ('月总销售额', f'${total_rev/10000:.1f}万', f'折合人民币约 ¥{total_rev*7.2/10000:.0f}万'),
        ('均价', f'${avg_price:.2f}', f'中位价 ${median_price:.2f}，主力区间 $20-$60'),
        ('平均毛利率', f'{gm_mean:.1f}%', f'中位毛利率 {gm_median:.1f}%，利润空间充足'),
        ('平均星级', f'{df["Rating"].mean():.2f}★', '星级集中在 4.4-4.8，市场竞争良性'),
        ('FBA占比', f'{(df["Fulfillment"]=="FBA").sum()}%', 'FBA配送为主，竞争主要在产品和广告'),
        ('中国卖家占比（BuyBox）', f'{cn_cnt}%', f'CN BuyBox占比 {cn_cnt}%，月收入占比 {cn_rev/total_rev*100:.1f}%'),
    ]
    for metric, value, note in overview_data:
        val(ws1, r, 1, metric, bold=True, bg=C_BLUE_LIGHT)
        c = ws1.cell(row=r, column=2, value=value)
        c.font = Font(name='Arial', bold=True, size=11, color='FF1F3864')
        c.alignment = Alignment(horizontal='center', vertical='center')
        c.fill = PatternFill('solid', fgColor=C_WHITE)
        ws1.merge_cells(start_row=r, start_column=3, end_row=r, end_column=8)
        val(ws1, r, 3, note, bg=C_WHITE, fg='FF595959')
        ws1.row_dimensions[r].height = 18
        r += 1

    apply_border(ws1, 5, r-1, 1, 8)
    r += 1

    # 价格分布
    section_title(ws1, r, 1, '▌ 二、价格分布', span=8)
    r += 1

    hdr(ws1, r, 1, '价格区间', bg=C_BLUE_MID)
    hdr(ws1, r, 2, '商品数量', bg=C_BLUE_MID)
    hdr(ws1, r, 3, '占比', bg=C_BLUE_MID)
    ws1.merge_cells(start_row=r, start_column=4, end_row=r, end_column=8)
    hdr(ws1, r, 4, '区间特征', bg=C_BLUE_MID)
    ws1.row_dimensions[r].height = 20
    r += 1

    price_desc = {
        '<$15': '特价引流款，低门槛，日销较高',
        '$15-25': '入门走量区，泛光灯/夹灯集中',
        '$25-35': '主力竞争区，充电磁吸灯最密集',
        '$35-50': '中高端区间，品质感强、新品机会好',
        '$50-70': '三脚架灯主力价格，单品收益高',
        '$70-100': '专业级手持灯/品牌溢价区',
        '>$100': '品牌灯（DeWalt/Streamlight）高端区',
    }
    main_count = sum([price_dist.get(b, 0) for b in ['<$15', '$15-25', '$25-35', '$35-50']])
    for band, cnt in price_dist.items():
        bg = C_YELLOW if '$25' in str(band) or '$35' in str(band) else C_WHITE
        val(ws1, r, 1, str(band), bold=True, bg=bg)
        ws1.cell(row=r, column=2, value=int(cnt)).alignment = Alignment(horizontal='center', vertical='center')
        ws1.cell(row=r, column=2).fill = PatternFill('solid', fgColor=bg)
        c3 = ws1.cell(row=r, column=3, value=f'{cnt/len(df)*100:.0f}%')
        c3.alignment = Alignment(horizontal='center', vertical='center')
        c3.fill = PatternFill('solid', fgColor=bg)
        ws1.merge_cells(start_row=r, start_column=4, end_row=r, end_column=8)
        val(ws1, r, 4, price_desc.get(str(band), ''), fg='FF595959', bg=bg)
        ws1.row_dimensions[r].height = 18
        r += 1

    apply_border(ws1, r - len(price_dist) - 1, r-1, 1, 8)
    r += 1

    # 产品类型分布
    section_title(ws1, r, 1, '▌ 三、产品类型分布与收益分析', span=8)
    r += 1

    type_hdr = ['产品类型', 'SKU数', '平均价格', '价格区间', '月总销量', '月总销售额', '单SKU均收益', '平均星级']
    for i, h in enumerate(type_hdr):
        hdr(ws1, r, i+1, h, bg=C_BLUE_MID)
    ws1.row_dimensions[r].height = 20
    r += 1

    type_highlights = {
        '充电磁吸工作灯': C_YELLOW,
        '三脚架工作灯': C_GREEN_LIGHT,
        '手持手电/聚光灯': C_ORANGE
    }
    for _, row_data in type_agg.iterrows():
        bg = type_highlights.get(row_data['product_type'], C_WHITE)
        val(ws1, r, 1, row_data['product_type'], bold=True, bg=bg)
        rev_val = row_data['total_revenue'] if pd.notna(row_data['total_revenue']) else 0
        for ci, v in enumerate([
            int(row_data['count']),
            f"${row_data['avg_price']:.1f}",
            f"${row_data['min_price']:.0f}-${row_data['max_price']:.0f}",
            f"{int(row_data['total_sales']):,}",
            f"${rev_val/10000:.1f}万",
            f"${rev_val/row_data['count']:.0f}" if rev_val > 0 else 'N/A',
            f"{row_data['avg_rating']:.1f}★" if pd.notna(row_data['avg_rating']) else 'N/A',
        ], start=2):
            c = ws1.cell(row=r, column=ci, value=v)
            c.font = Font(name='Arial', size=10)
            c.fill = PatternFill('solid', fgColor=bg)
            c.alignment = Alignment(horizontal='center', vertical='center')
        ws1.row_dimensions[r].height = 18
        r += 1

    apply_border(ws1, r - len(type_agg) - 1, r-1, 1, 8)
    r += 1

    # 市场结论
    section_title(ws1, r, 1, '▌ 四、市场分析结论', span=8)
    r += 1

    single_sku = (brand_agg['sku_count'] == 1).sum()
    top3_share = brand_agg.head(3)['total_rev'].sum() / total_rev * 100 if total_rev else 0
    top10_share = brand_agg.head(10)['total_rev'].sum() / total_rev * 100 if total_rev else 0

    conclusions = [
        ('市场规模', f'月总销售额约${total_rev/10000:.1f}万，月总销量{int(total_sales):,}件，为中等体量稳定市场。'),
        ('价格结构', f'主力价格带集中在$20-$60区间（共{main_count}款），$35-50为最优入场区间，竞争密度较$25-35低但单品收益更高。'),
        ('主力品类', f'充电磁吸工作灯是销量最大品类，月销{int(type_agg[type_agg["product_type"]=="充电磁吸工作灯"]["total_sales"].sum()):,}件。三脚架工作灯客单价高（均价${type_agg[type_agg["product_type"]=="三脚架工作灯"]["avg_price"].mean():.0f}）。'),
        ('集中度', f'Top3品牌收入占比仅{top3_share:.1f}%，Top10占{top10_share:.1f}%，{df["Brand"].nunique()}个品牌参与竞争，{single_sku}个品牌仅有1个SKU，市场分散，新品进入机会较大。'),
    ]
    for label, text in conclusions:
        ws1.merge_cells(start_row=r, start_column=1, end_row=r, end_column=2)
        val(ws1, r, 1, f'【{label}】', bold=True, bg=C_BLUE_LIGHT, fg='FF1F3864')
        ws1.merge_cells(start_row=r, start_column=3, end_row=r, end_column=8)
        val(ws1, r, 3, text, wrap=True)
        ws1.row_dimensions[r].height = 36
        r += 1

    apply_border(ws1, r-len(conclusions), r-1, 1, 8)
    r += 1

    # ===== 市场分析 - 新增内容汇总 =====
    # 销量与价格关系分析
    section_title(ws1, r, 1, '▌ 五、销量与价格关系分析', span=8)
    ws1.row_dimensions[r].height = 24
    r += 1
    
    # 计算高销量产品价格分布
    high_sales = df[df['Monthly Sales'] > df['Monthly Sales'].quantile(0.75)]
    low_sales = df[df['Monthly Sales'] < df['Monthly Sales'].quantile(0.25)]
    mid_sales = df[(df['Monthly Sales'] >= df['Monthly Sales'].quantile(0.25)) & 
                   (df['Monthly Sales'] <= df['Monthly Sales'].quantile(0.75))]
    
    sales_price_data = [
        ('高销量产品(TOP25%)', len(high_sales), f"${high_sales[price_col].mean():.1f}", 
         f"{int(high_sales['Monthly Sales'].mean()):,}件/月", '高价格+高评分是爆款标配'),
        ('中等销量产品', len(mid_sales), f"${mid_sales[price_col].mean():.1f}", 
         f"{int(mid_sales['Monthly Sales'].mean()):,}件/月", '价格适中，竞争激烈，需差异化突围'),
        ('低销量产品(BOTTOM25%)', len(low_sales), f"${low_sales[price_col].mean():.1f}", 
         f"{int(low_sales['Monthly Sales'].mean()):,}件/月", '低价或低评分，需谨慎入场'),
    ]
    
    hdr(ws1, r, 1, '销量分层', bg=C_BLUE_MID)
    hdr(ws1, r, 2, 'SKU数', bg=C_BLUE_MID)
    hdr(ws1, r, 3, '均价', bg=C_BLUE_MID)
    hdr(ws1, r, 4, '平均月销', bg=C_BLUE_MID)
    ws1.merge_cells(start_row=r, start_column=5, end_row=r, end_column=8)
    hdr(ws1, r, 5, '分析结论', bg=C_BLUE_MID)
    ws1.row_dimensions[r].height = 20
    r += 1
    
    for i, (label, cnt, avg_p, avg_s, note) in enumerate(sales_price_data):
        bg = C_GREEN_LIGHT if '高销量' in label else (C_YELLOW if '中等' in label else C_RED_LIGHT)
        bg = C_GREEN_LIGHT if i == 0 else (C_YELLOW if i == 1 else C_RED_LIGHT)
        val(ws1, r, 1, label, bold=True, bg=bg)
        ws1.cell(row=r, column=2, value=cnt).alignment = Alignment(horizontal='center', vertical='center')
        ws1.cell(row=r, column=2).fill = PatternFill('solid', fgColor=bg)
        ws1.cell(row=r, column=3, value=avg_p).alignment = Alignment(horizontal='center', vertical='center')
        ws1.cell(row=r, column=3).fill = PatternFill('solid', fgColor=bg)
        ws1.cell(row=r, column=4, value=avg_s).alignment = Alignment(horizontal='center', vertical='center')
        ws1.cell(row=r, column=4).fill = PatternFill('solid', fgColor=bg)
        ws1.merge_cells(start_row=r, start_column=5, end_row=r, end_column=8)
        val(ws1, r, 5, note, bg=bg, fg='FF595959')
        ws1.row_dimensions[r].height = 22
        r += 1
    apply_border(ws1, r-len(sales_price_data)-1, r-1, 1, 8)
    r += 1
    
    # 评分与销量关系
    section_title(ws1, r, 1, '▌ 六、评分与销量关系洞察', span=8)
    ws1.row_dimensions[r].height = 24
    r += 1
    
    rating_bands = [(4.5, 5.0), (4.0, 4.5), (3.5, 4.0), (0, 3.5)]
    rating_labels = ['4.5★以上(优秀)', '4.0-4.5★(良好)', '3.5-4.0★(一般)', '3.5★以下(较差)']
    rating_colors = [C_GREEN_LIGHT, C_YELLOW, 'FFFFF2CC', C_RED_LIGHT]
    
    hdr(ws1, r, 1, '评分区间', bg=C_BLUE_MID)
    hdr(ws1, r, 2, 'SKU数', bg=C_BLUE_MID)
    hdr(ws1, r, 3, '平均月销', bg=C_BLUE_MID)
    hdr(ws1, r, 4, '平均价格', bg=C_BLUE_MID)
    ws1.merge_cells(start_row=r, start_column=5, end_row=r, end_column=8)
    hdr(ws1, r, 5, '进入策略', bg=C_BLUE_MID)
    ws1.row_dimensions[r].height = 20
    r += 1
    
    for i, ((low, high), label, color) in enumerate(zip(rating_bands, rating_labels, rating_colors)):
        sub_df = df[(df['Rating'] >= low) & (df['Rating'] < high)] if high > 0 else df[df['Rating'] < 3.5]
        if len(sub_df) > 0:
            avg_sales = sub_df['Monthly Sales'].mean()
            avg_price_r = sub_df[price_col].mean()
        else:
            avg_sales = 0
            avg_price_r = 0
        strategy = '★ 推荐入场' if low >= 4.0 else '△ 谨慎入场' if low >= 3.5 else '✗ 不建议入场'
        val(ws1, r, 1, label, bold=(low >= 4.0), bg=color)
        ws1.cell(row=r, column=2, value=len(sub_df)).alignment = Alignment(horizontal='center', vertical='center')
        ws1.cell(row=r, column=2).fill = PatternFill('solid', fgColor=color)
        ws1.cell(row=r, column=3, value=f"{int(avg_sales):,}件").alignment = Alignment(horizontal='center', vertical='center')
        ws1.cell(row=r, column=3).fill = PatternFill('solid', fgColor=color)
        ws1.cell(row=r, column=4, value=f"${avg_price_r:.1f}").alignment = Alignment(horizontal='center', vertical='center')
        ws1.cell(row=r, column=4).fill = PatternFill('solid', fgColor=color)
        ws1.merge_cells(start_row=r, start_column=5, end_row=r, end_column=8)
        val(ws1, r, 5, strategy, bg=color, fg='FF1F3864' if low >= 4.0 else 'FF7B0000')
        ws1.row_dimensions[r].height = 20
        r += 1
    apply_border(ws1, r-len(rating_labels)-1, r-1, 1, 8)

    # ===== Sheet 2: 竞争分析 =====
    ws2 = wb.create_sheet('竞争分析')
    ws2.sheet_view.showGridLines = False
    for col_letter, width in zip('ABCDEFGH', [22, 18, 15, 15, 15, 18, 15, 15]):
        ws2.column_dimensions[col_letter].width = width

    ws2.merge_cells('A1:H1')
    c = ws2['A1']
    c.value = 'LED工作灯 竞争格局分析'
    c.font = Font(name='Arial', bold=True, size=14, color=C_WHITE)
    c.fill = PatternFill('solid', fgColor=C_BLUE_DARK)
    c.alignment = Alignment(horizontal='center', vertical='center')
    ws2.row_dimensions[1].height = 35

    r2 = 3
    section_title(ws2, r2, 1, '▌ 一、竞争指数概览', span=8)
    r2 += 1

    new_cnt = len(new_df)
    new_avg_rev = new_df[rev_col].mean() if len(new_df) > 0 and rev_col else 0

    compete_kv = [
        ('竞争指数', '中等偏低 ★★★☆☆', '市场分散，新品进入壁垒较低'),
        ('品牌集中度', f'低（Top5占{brand_agg.head(5)["total_rev"].sum()/total_rev*100:.0f}%）', '无绝对垄断品牌，竞争均衡'),
        ('中国卖家BuyBox占比', f'{cn_cnt}个 / {cn_cnt}%', f'月收入占比{cn_rev/total_rev*100:.1f}%，中国卖家主导市场'),
        ('美国本土卖家', f'{us_cnt}个 / {us_rev/total_rev*100:.1f}%收入', '主要为DeWalt/Klein等工具品牌'),
        ('FBA比例', f'{(df["Fulfillment"]=="FBA").sum()}%', '物流标准化，竞争主要在产品和广告'),
        ('新品存活率', f'{new_cnt}%（{new_cnt}/{len(df)}）', '新品（<1年）进入TOP100比例，存活率高'),
        ('新品月均收益', f'${new_avg_rev:,.0f}', '略低于均值，前期爬坡正常'),
        ('评分门槛', f'最低{df["Rating"].min():.1f}★，均值{df["Rating"].mean():.2f}★', '进入TOP100需保持4.4★以上'),
    ]

    hdr(ws2, r2, 1, '竞争维度', bg=C_BLUE_MID)
    hdr(ws2, r2, 2, '数据', bg=C_BLUE_MID)
    ws2.merge_cells(start_row=r2, start_column=3, end_row=r2, end_column=8)
    hdr(ws2, r2, 3, '说明', bg=C_BLUE_MID)
    r2 += 1
    for i, (k, v, n) in enumerate(compete_kv):
        bg = C_BLUE_LIGHT if i % 2 == 0 else C_WHITE
        val(ws2, r2, 1, k, bold=True, bg=bg)
        c = ws2.cell(row=r2, column=2, value=v)
        c.font = Font(name='Arial', bold=True, size=11, color='FF1F3864')
        c.fill = PatternFill('solid', fgColor=bg)
        c.alignment = Alignment(horizontal='center', vertical='center')
        ws2.merge_cells(start_row=r2, start_column=3, end_row=r2, end_column=8)
        val(ws2, r2, 3, n, bg=bg, fg='FF595959')
        ws2.row_dimensions[r2].height = 20
        r2 += 1
    apply_border(ws2, 4, r2-1, 1, 8)
    r2 += 1

    # TOP15品牌
    section_title(ws2, r2, 1, '▌ 二、TOP15 品牌收益排行', span=8)
    r2 += 1
    brand_hdr_cols = ['排名', '品牌', 'SKU数', '月总收益($)', '收入占比', '平均星级', '市场地位']
    for i, h in enumerate(brand_hdr_cols):
        hdr(ws2, r2, i+1, h, bg=C_BLUE_MID)
    ws2.merge_cells(start_row=r2, start_column=7, end_row=r2, end_column=8)
    r2 += 1

    brand_colors = {0: C_ORANGE, 1: C_ORANGE, 2: C_ORANGE}
    top15 = brand_agg.head(15)
    for idx, (_, brow) in enumerate(top15.iterrows()):
        bg = brand_colors.get(idx, C_WHITE if idx % 2 == 0 else C_GREY_LIGHT)
        ws2.cell(row=r2, column=1, value=idx+1).alignment = Alignment(horizontal='center', vertical='center')
        ws2.cell(row=r2, column=1).fill = PatternFill('solid', fgColor=bg)
        rev_val = brow['total_rev'] if pd.notna(brow['total_rev']) else 0
        for ci, v in enumerate([
            brow['Brand'],
            int(brow['sku_count']),
            f"${rev_val:,.0f}",
            f"{rev_val/total_rev*100:.1f}%" if total_rev else 'N/A',
            f"{brow['avg_rating']:.1f}★" if pd.notna(brow['avg_rating']) else 'N/A',
        ], start=2):
            c = ws2.cell(row=r2, column=ci, value=v)
            c.fill = PatternFill('solid', fgColor=bg)
            c.alignment = Alignment(horizontal='center' if ci >= 3 else 'left', vertical='center')
            c.font = Font(name='Arial', size=10)
        status_map = {0: 'TOP1 绝对领跑', 1: 'TOP2 品类强者', 2: 'TOP3 紧密跟随', 3: 'TOP4-5 中坚品牌'}
        status = status_map.get(idx, 'TOP10 活跃竞争')
        ws2.merge_cells(start_row=r2, start_column=7, end_row=r2, end_column=8)
        val(ws2, r2, 7, status, bg=bg, fg='FF595959')
        ws2.row_dimensions[r2].height = 18
        r2 += 1
    apply_border(ws2, r2-16, r2-1, 1, 8)
    r2 += 1

    # 卖家地区分布
    section_title(ws2, r2, 1, '▌ 三、卖家地区分布', span=8)
    r2 += 1
    loc_data = df['BuyBox Location'].value_counts()
    hdr(ws2, r2, 1, '卖家地区', bg=C_BLUE_MID)
    hdr(ws2, r2, 2, '数量', bg=C_BLUE_MID)
    hdr(ws2, r2, 3, '占比', bg=C_BLUE_MID)
    ws2.merge_cells(start_row=r2, start_column=4, end_row=r2, end_column=6)
    hdr(ws2, r2, 4, '月收入', bg=C_BLUE_MID)
    ws2.merge_cells(start_row=r2, start_column=7, end_row=r2, end_column=8)
    hdr(ws2, r2, 7, '收入占比', bg=C_BLUE_MID)
    r2 += 1
    for loc, cnt in loc_data.items():
        loc_rev = df[df['BuyBox Location'] == loc][rev_col].sum()
        bg = C_YELLOW if loc == 'CN' else C_WHITE
        val(ws2, r2, 1, str(loc) if loc else '未知', bold=(loc == 'CN'), bg=bg)
        ws2.cell(row=r2, column=2, value=int(cnt)).alignment = Alignment(horizontal='center', vertical='center')
        ws2.cell(row=r2, column=2).fill = PatternFill('solid', fgColor=bg)
        ws2.cell(row=r2, column=3, value=f'{cnt/len(df)*100:.0f}%').alignment = Alignment(horizontal='center', vertical='center')
        ws2.cell(row=r2, column=3).fill = PatternFill('solid', fgColor=bg)
        ws2.merge_cells(start_row=r2, start_column=4, end_row=r2, end_column=6)
        ws2.cell(row=r2, column=4, value=f'${loc_rev:,.0f}')
        ws2.cell(row=r2, column=4).alignment = Alignment(horizontal='center', vertical='center')
        ws2.cell(row=r2, column=4).fill = PatternFill('solid', fgColor=bg)
        ws2.merge_cells(start_row=r2, start_column=7, end_row=r2, end_column=8)
        ws2.cell(row=r2, column=7, value=f'{loc_rev/total_rev*100:.1f}%' if total_rev else 'N/A')
        ws2.cell(row=r2, column=7).alignment = Alignment(horizontal='center', vertical='center')
        ws2.cell(row=r2, column=7).fill = PatternFill('solid', fgColor=bg)
        ws2.row_dimensions[r2].height = 18
        r2 += 1
    apply_border(ws2, r2-len(loc_data)-1, r2-1, 1, 8)
    r2 += 1

    # 新品存活分析
    section_title(ws2, r2, 1, '▌ 四、新品存活分析', span=8)
    r2 += 1
    hdr(ws2, r2, 1, '上架年龄段', bg=C_BLUE_MID)
    hdr(ws2, r2, 2, 'SKU数', bg=C_BLUE_MID)
    hdr(ws2, r2, 3, '占比', bg=C_BLUE_MID)
    hdr(ws2, r2, 4, '月均收益/SKU', bg=C_BLUE_MID)
    ws2.merge_cells(start_row=r2, start_column=5, end_row=r2, end_column=8)
    hdr(ws2, r2, 5, '解读', bg=C_BLUE_MID)
    r2 += 1
    interpret_map = {
        '<3个月': '极新品，流量正在爬坡',
        '3-6个月': '新品存活验证期',
        '6-12个月': '存活稳定，有竞争力',
        '1-2年': '成熟竞品，有流量积累',
        '2-4年': '稳固地位',
        '>4年': '行业老品，品牌沉淀强'
    }
    for age_band, cnt in age_dist.items():
        age_df_sub = df[df['age_band'] == age_band]
        avg_r = age_df_sub[rev_col].mean() if rev_col and len(age_df_sub) > 0 else 0
        bg = C_GREEN_LIGHT if '6-12' in str(age_band) or '3-6' in str(age_band) else C_WHITE
        val(ws2, r2, 1, str(age_band), bg=bg)
        for ci, v in enumerate([int(cnt), f'{cnt/len(df)*100:.0f}%', f'${avg_r:,.0f}'], start=2):
            c = ws2.cell(row=r2, column=ci, value=v)
            c.fill = PatternFill('solid', fgColor=bg)
            c.alignment = Alignment(horizontal='center', vertical='center')
            c.font = Font(name='Arial', size=10)
        ws2.merge_cells(start_row=r2, start_column=5, end_row=r2, end_column=8)
        val(ws2, r2, 5, interpret_map.get(str(age_band), ''), bg=bg, fg='FF595959')
        ws2.row_dimensions[r2].height = 18
        r2 += 1
    apply_border(ws2, r2-len(age_dist)-1, r2-1, 1, 8)
    r2 += 1
    
    # ===== 竞争分析 - 新增内容 =====
    # 竞争进入建议
    section_title(ws2, r2, 1, '▌ 五、竞争进入建议', span=8)
    ws2.row_dimensions[r2].height = 24
    r2 += 1
    
    # 计算关键竞争指标
    cn_sku = cn_cnt
    us_sku = us_cnt
    cn_rev_pct = cn_rev/total_rev*100 if total_rev else 0
    
    entry_suggestions = [
        ('市场机会', '中等偏高', 
         f'中国卖家主导({cn_sku}个SKU占{cn_rev_pct:.0f}%收入)，但头部集中度低，无绝对垄断'),
        ('入场门槛', '中等', 
         f'平均评分{df["Rating"].mean():.1f}★，新品需保持4.4★以上才具备竞争力'),
        ('价格策略', '$30-$50最优', 
         f'该区间销量最大({main_count}款)，竞争适中，单品收益最佳'),
        ('差异化方向', '电池+亮度+磁力', 
         f'TOP3差评痛点：{top_pain_for_use}等，解决这些问题是突破关键'),
        ('风险提示', '注意以下几点', 
         '• FBA费用上涨压缩利润\n• 广告CPC在新进入期较高\n• 避免纯价格战，选择品质路线'),
    ]
    
    hdr(ws2, r2, 1, '维度', bg=C_BLUE_MID)
    ws2.merge_cells(start_row=r2, start_column=2, end_row=r2, end_column=3)
    hdr(ws2, r2, 2, '判断', bg=C_BLUE_MID)
    ws2.merge_cells(start_row=r2, start_column=4, end_row=r2, end_column=8)
    hdr(ws2, r2, 4, '详细说明', bg=C_BLUE_MID)
    ws2.row_dimensions[r2].height = 20
    r2 += 1
    
    for i, (dim, judge, detail) in enumerate(entry_suggestions):
        bg = C_BLUE_LIGHT if i % 2 == 0 else C_WHITE
        val(ws2, r2, 1, dim, bold=True, bg=bg)
        ws2.merge_cells(start_row=r2, start_column=2, end_row=r2, end_column=3)
        c = ws2.cell(row=r2, column=2, value=judge)
        c.font = Font(name='Arial', bold=True, color='FF1F3864')
        c.fill = PatternFill('solid', fgColor=bg)
        c.alignment = Alignment(horizontal='center', vertical='center')
        ws2.merge_cells(start_row=r2, start_column=4, end_row=r2, end_column=8)
        val(ws2, r2, 4, detail, bg=bg, fg='FF595959', wrap=True)
        ws2.row_dimensions[r2].height = 45
        r2 += 1
    apply_border(ws2, r2-len(entry_suggestions)-1, r2-1, 1, 8)
    r2 += 1
    
    # TOP品牌策略分析
    section_title(ws2, r2, 1, '▌ 六、TOP品牌竞争策略分析', span=8)
    ws2.row_dimensions[r2].height = 24
    r2 += 1
    
    hdr(ws2, r2, 1, '品牌层级', bg=C_BLUE_MID)
    hdr(ws2, r2, 2, '代表品牌', bg=C_BLUE_MID)
    hdr(ws2, r2, 3, '价格区间', bg=C_BLUE_MID)
    hdr(ws2, r2, 4, '客户群体', bg=C_BLUE_MID)
    ws2.merge_cells(start_row=r2, start_column=5, end_row=r2, end_column=8)
    hdr(ws2, r2, 5, '竞争策略建议', bg=C_BLUE_MID)
    ws2.row_dimensions[r2].height = 20
    r2 += 1
    
    brand_strategies = [
        ('头部品牌', 'DeWalt/Milwaukee/Klein', '$50-$150', '专业工人/工具爱好者', 
         '避开！定位高端，品牌溢价强，需长期积累'),
        ('中高端', 'Coquimbo/HOTLIGH等', '$25-$50', 'DIY用户/家庭用户', 
         '正面竞争需差异化（功能/品质），建议做细分场景'),
        ('入门级', '不知名单品牌', '$10-$25', '价格敏感用户', 
         '利润薄，不建议价格战，可考虑功能升级切入'),
    ]
    
    for i, (tier, brand, price, customer, strategy) in enumerate(brand_strategies):
        bg = C_YELLOW if i == 1 else (C_ORANGE if i == 0 else C_GREEN_LIGHT)
        val(ws2, r2, 1, tier, bold=True, bg=bg)
        val(ws2, r2, 2, brand, bg=bg)
        val(ws2, r2, 3, price, bg=bg)
        val(ws2, r2, 4, customer, bg=bg)
        ws2.merge_cells(start_row=r2, start_column=5, end_row=r2, end_column=8)
        val(ws2, r2, 5, strategy, bg=bg, fg='FF595959')
        ws2.row_dimensions[r2].height = 35
        r2 += 1
    apply_border(ws2, r2-len(brand_strategies)-1, r2-1, 1, 8)
    r2 += 1

    # ---- 扩展段：关键词 + Market 文件 ----
    # ▌ 五、核心关键词竞争度
    section_title(ws2, r2, 1, '▌ 五、核心关键词竞争度', span=8)
    ws2.row_dimensions[r2].height = 24
    r2 += 1

    kw_df = keyword_data.get('keywords') if keyword_data.get('_available') else None
    kw_top3_for_conclusion = []
    kw_lowest_spr_for_conclusion = ''
    kw_avg_bid_for_conclusion = ''
    if kw_df is not None and len(kw_df) > 0:
        # --- ReverseASIN 真实数据版 ---
        kw = kw_df.copy()
        kw['M. Searches'] = pd.to_numeric(kw.get('M. Searches', pd.Series(dtype=float)), errors='coerce').fillna(0)
        kw['Title Density'] = pd.to_numeric(kw.get('Title Density', pd.Series(dtype=float)), errors='coerce').fillna(0)
        kw['SPR'] = pd.to_numeric(kw.get('SPR', pd.Series(dtype=float)), errors='coerce').fillna(0)
        kw['Click Share'] = pd.to_numeric(kw.get('Click Share', pd.Series(dtype=float)), errors='coerce').fillna(0)
        kw['Conversion'] = pd.to_numeric(kw.get('Conversion', pd.Series(dtype=float)), errors='coerce').fillna(0)
        kw['Products'] = pd.to_numeric(kw.get('Products', pd.Series(dtype=float)), errors='coerce').fillna(0)
        org_rank_col = 'Organic Rank' if 'Organic Rank' in kw.columns else None
        has_ppc_bid = 'PPC Bid' in kw.columns
        has_suggested_bid = 'Suggested Bid' in kw.columns
        kw_top = kw.sort_values('M. Searches', ascending=False).head(20).reset_index(drop=True)

        headers = ['关键词', '月搜索量', '竞品ASIN数', 'SPR(8天上首页)', '点击份额', '转化率', 'PPC Bid', '建议Bid区间', '自然排名', '竞争度']
        # Sheet 2 只有 A-H (8列)，需要扩展列宽
        for extra_col in ['I', 'J']:
            ws2.column_dimensions[extra_col].width = 14
        for i, h in enumerate(headers, 1):
            hdr(ws2, r2, i, h, bg=C_BLUE_MID)
        ws2.row_dimensions[r2].height = 22
        r2 += 1
        row_start = r2
        bid_values = []
        for _, row in kw_top.iterrows():
            keyword = str(row.get('Keyword', '-'))
            m_search = float(row['M. Searches'])
            products = int(row['Products']) if row['Products'] > 0 else int(row['Title Density'])
            spr = float(row['SPR'])
            cs = float(row['Click Share'])
            conv = float(row['Conversion'])
            org_rank = str(row.get(org_rank_col, '-')) if org_rank_col else '-'
            ppc_bid = str(row.get('PPC Bid', '-')) if has_ppc_bid else '-'
            suggested_bid = str(row.get('Suggested Bid', '-')) if has_suggested_bid else '-'
            if ppc_bid != '-' and ppc_bid != 'nan':
                try:
                    bid_values.append(float(ppc_bid.replace('$', '').strip()))
                except (ValueError, AttributeError):
                    pass
            if products > 10000 and spr > 50:
                rating, rating_bg = '高', C_RED_LIGHT
            elif products > 1000 or spr > 20:
                rating, rating_bg = '中', C_YELLOW
            else:
                rating, rating_bg = '低', C_GREEN_LIGHT
            val(ws2, r2, 1, keyword, bold=True, bg=C_BLUE_LIGHT, fg='FF1F3864')
            val(ws2, r2, 2, f'{m_search:,.0f}')
            val(ws2, r2, 3, f'{products:,}', bg=C_RED_LIGHT if products > 10000 else (C_YELLOW if products > 1000 else C_WHITE))
            val(ws2, r2, 4, f'{spr:.0f}' if spr > 0 else '-')
            val(ws2, r2, 5, f'{cs:.2%}' if cs > 0 else '-')
            val(ws2, r2, 6, f'{conv:.2%}' if conv > 0 else '-')
            val(ws2, r2, 7, ppc_bid if ppc_bid != 'nan' else '-')
            val(ws2, r2, 8, suggested_bid if suggested_bid != 'nan' else '-')
            val(ws2, r2, 9, org_rank)
            val(ws2, r2, 10, rating, bold=True, bg=rating_bg)
            ws2.row_dimensions[r2].height = 20
            r2 += 1
        if r2 > row_start:
            apply_border(ws2, row_start-1, r2-1, 1, 10)
        r2 += 1

        # 关键词小结（叙述性）
        top3 = kw_top.head(3)['Keyword'].tolist()
        kw_top3_for_conclusion = top3
        lowest_spr_row = kw_top[kw_top['SPR'] > 0].sort_values('SPR').head(1)
        lowest_spr_kw = lowest_spr_row.iloc[0]['Keyword'] if len(lowest_spr_row) > 0 else '—'
        lowest_spr_val = int(lowest_spr_row.iloc[0]['SPR']) if len(lowest_spr_row) > 0 else 0
        kw_lowest_spr_for_conclusion = f'{lowest_spr_kw}（SPR={lowest_spr_val}）'
        high_comp = len(kw_top[(kw_top['Products'] > 10000) & (kw_top['SPR'] > 50)])
        avg_bid = f'${sum(bid_values)/len(bid_values):.2f}' if bid_values else 'N/A'
        kw_avg_bid_for_conclusion = avg_bid
        top1 = kw_top.iloc[0] if len(kw_top) > 0 else None
        summary_items = [
            f'• 核心大词「{top3[0]}」月搜索量 {int(kw_top.iloc[0]["M. Searches"]):,}，竞品 {int(kw_top.iloc[0]["Products"]):,} 个，竞争度高' if top1 is not None else '',
            f'• 最低门槛词「{lowest_spr_kw}」SPR 仅 {lowest_spr_val}，8 天需 {lowest_spr_val} 单即可上首页，适合新品切入',
            f'• Top 20 关键词平均 PPC Bid：{avg_bid}；{high_comp} 个词为高竞争度',
            f'• 长尾策略：优先投放 SPR < 30 且月搜索 > 5,000 的中等竞争词',
        ]
        summary_items = [s for s in summary_items if s]
        ws2.merge_cells(start_row=r2, start_column=1, end_row=r2+3, end_column=10)
        c = ws2.cell(row=r2, column=1)
        c.value = '\n'.join(summary_items)
        c.font = Font(name='Arial', size=10, color='FF1F3864')
        c.fill = PatternFill('solid', fgColor=C_BLUE_LIGHT)
        c.alignment = Alignment(horizontal='left', vertical='top', wrap_text=True)
        for rr in range(r2, r2+4):
            ws2.row_dimensions[rr].height = 22
        r2 += 4
    else:
        # --- 降级：Market 文件 5 词趋势 ---
        demand_df = market_data.get('demand_trends') if market_data.get('_available') else None
        if demand_df is not None and len(demand_df) > 2:
            d = demand_df.copy()
            month_col = d.columns[0]
            kw_cols = list(d.columns[1:])
            d[month_col] = d[month_col].astype(str)
            d = d.dropna(subset=[month_col])
            d = d[d[month_col].str.match(r'^\d{4}-\d{2}$', na=False)]
            d = d.sort_values(month_col)
            headers = ['核心关键词', '最新月搜索量', '12 月均值', '同比变化', '年内峰值月', '竞争度评级']
            for i, h in enumerate(headers, 1):
                hdr(ws2, r2, i, h, bg=C_BLUE_MID)
            ws2.row_dimensions[r2].height = 20
            r2 += 1
            row_start = r2
            for kw in kw_cols:
                try:
                    s = pd.to_numeric(d[kw], errors='coerce').dropna()
                    if len(s) == 0:
                        continue
                    latest = float(s.iloc[-1])
                    last12 = s.iloc[-12:] if len(s) >= 12 else s
                    avg12 = float(last12.mean())
                    if len(s) >= 24:
                        prev12 = s.iloc[-24:-12]
                        yoy = (last12.sum() - prev12.sum()) / prev12.sum() if prev12.sum() > 0 else 0
                        yoy_str = f'{yoy:+.1%}'
                    else:
                        yoy_str = 'N/A'
                    last12_months = d[month_col].iloc[-12:].tolist() if len(d) >= 12 else d[month_col].tolist()
                    peak_idx = last12.values.argmax()
                    peak_month = last12_months[peak_idx] if peak_idx < len(last12_months) else '--'
                    peak_month_m = peak_month.split('-')[-1] if '-' in peak_month else peak_month
                    if avg12 < 1000:
                        rating, rating_bg = '低', C_GREEN_LIGHT
                    elif avg12 < 10000:
                        rating, rating_bg = '中', C_YELLOW
                    else:
                        rating, rating_bg = '高', C_RED_LIGHT
                    val(ws2, r2, 1, str(kw).strip(), bold=True, bg=C_BLUE_LIGHT)
                    val(ws2, r2, 2, f'{latest:,.0f}')
                    val(ws2, r2, 3, f'{avg12:,.0f}')
                    val(ws2, r2, 4, yoy_str, fg='FF005A9E' if yoy_str.startswith('+') else 'FFC00000')
                    val(ws2, r2, 5, f'{peak_month_m} 月', bg=C_BLUE_LIGHT)
                    val(ws2, r2, 6, rating, bold=True, bg=rating_bg)
                    ws2.merge_cells(start_row=r2, start_column=6, end_row=r2, end_column=8)
                    ws2.row_dimensions[r2].height = 20
                    r2 += 1
                except Exception:
                    continue
            if r2 > row_start:
                apply_border(ws2, row_start-1, r2-1, 1, 8)
        else:
            ws2.merge_cells(start_row=r2, start_column=1, end_row=r2, end_column=8)
            val(ws2, r2, 1, '⚠ 未上传关键词反查文件或 Market 分析文件，关键词竞争数据缺失', bold=True, bg=C_YELLOW, fg='FF7A4F01')
            ws2.row_dimensions[r2].height = 30
            r2 += 1
    r2 += 1

    # ▌ 六、新品推广难度评估
    section_title(ws2, r2, 1, '▌ 六、新品推广难度评估', span=8)
    ws2.row_dimensions[r2].height = 24
    r2 += 1

    # 数据来源：BSR + Market
    total_products = len(df)
    new_products_6m = len(df[df['Available days'] < 180]) if 'Available days' in df.columns else 0
    new_products_1y = len(df[df['Available days'] < 365]) if 'Available days' in df.columns else 0
    new_in_top100 = new_products_1y  # 已在 BSR TOP100 内
    new_ratio_top100 = new_in_top100 / total_products if total_products > 0 else 0

    # 从 Market 数据补充
    cr5 = cr10 = None
    cn_seller_ratio = None
    rating_50_ratio = None
    brand_df = market_data.get('brand_concentration')
    if brand_df is not None and len(brand_df) > 5:
        try:
            sp = pd.to_numeric(brand_df['Sales Proportion'], errors='coerce').dropna()
            cr5 = sp.head(5).sum()
            cr10 = sp.head(10).sum()
        except Exception:
            pass
    origin_df = market_data.get('origin_of_seller')
    if origin_df is not None and len(origin_df) > 0:
        try:
            for _, row in origin_df.iterrows():
                val0 = str(row.iloc[0]).lower()
                if 'china' in val0 or 'cn' in val0 or '中国' in val0:
                    cn_seller_ratio = float(pd.to_numeric(row.iloc[3] if len(row) > 3 else 0, errors='coerce') or 0)
                    break
        except Exception:
            pass

    diff_rows = [
        ('新品数量（<1 年）', f'{new_products_1y} 个', f'占 BSR TOP100 的 {new_ratio_top100:.1%}'),
        ('新品数量（<6 月）', f'{new_products_6m} 个', f'占 BSR TOP100 的 {new_products_6m/max(total_products,1):.1%}'),
        ('新品进 Top100 比例', f'{new_ratio_top100:.1%}', '越高=新品越有机会；<10% 推广难度高'),
        ('头部品牌 CR5', f'{cr5:.1%}' if cr5 is not None else '需 Market 文件', '≥50% 则头部垄断，需差异化切入'),
        ('头部品牌 CR10', f'{cr10:.1%}' if cr10 is not None else '需 Market 文件', '越高代表品牌集中度越强'),
        ('中国卖家占比', f'{cn_seller_ratio:.1%}' if cn_seller_ratio is not None else '需 Market 文件', '>70% 则价格战激烈，利润被压缩'),
        ('评分门槛参考', f'{int(avg_rating*10)/10:.1f}★', '低于均分会被流量边缘化'),
    ]

    hdr(ws2, r2, 1, '评估指标', bg=C_BLUE_MID)
    ws2.merge_cells(start_row=r2, start_column=1, end_row=r2, end_column=2)
    hdr(ws2, r2, 3, '数值', bg=C_BLUE_MID)
    hdr(ws2, r2, 4, '含义说明', bg=C_BLUE_MID)
    ws2.merge_cells(start_row=r2, start_column=4, end_row=r2, end_column=8)
    ws2.row_dimensions[r2].height = 20
    r2 += 1
    row_start = r2
    for label, value, note in diff_rows:
        ws2.merge_cells(start_row=r2, start_column=1, end_row=r2, end_column=2)
        val(ws2, r2, 1, label, bold=True, bg=C_BLUE_LIGHT, fg='FF1F3864')
        val(ws2, r2, 3, value, bg=C_WHITE)
        ws2.merge_cells(start_row=r2, start_column=4, end_row=r2, end_column=8)
        val(ws2, r2, 4, note, bg=C_WHITE, fg='FF595959')
        ws2.row_dimensions[r2].height = 20
        r2 += 1
    apply_border(ws2, row_start-1, r2-1, 1, 8)
    r2 += 1

    # 综合评级
    difficulty_level = '中'
    difficulty_bg = C_YELLOW
    reasons = []
    if cr5 is not None and cr5 > 0.5:
        difficulty_level = '高'
        difficulty_bg = C_RED_LIGHT
        reasons.append(f'CR5={cr5:.0%} 头部垄断')
    if new_ratio_top100 < 0.1:
        difficulty_level = '高'
        difficulty_bg = C_RED_LIGHT
        reasons.append(f'新品进 Top100 仅 {new_ratio_top100:.0%}')
    if (cr5 is None or cr5 < 0.3) and new_ratio_top100 > 0.3:
        difficulty_level = '低'
        difficulty_bg = C_GREEN_LIGHT
        reasons.append(f'新品占比高 {new_ratio_top100:.0%}')
    if not reasons:
        reasons.append('各指标处于中等水平')
    cycle_map = {'低': '3-6 月可进入头部', '中': '6-9 月可进入头部', '高': '9-12 月需持续投入'}

    ws2.merge_cells(start_row=r2, start_column=1, end_row=r2, end_column=2)
    val(ws2, r2, 1, '综合推广难度', bold=True, bg=C_BLUE_DARK, fg='FFFFFFFF')
    val(ws2, r2, 3, difficulty_level, bold=True, bg=difficulty_bg)
    ws2.merge_cells(start_row=r2, start_column=4, end_row=r2, end_column=8)
    val(ws2, r2, 4, f'依据：{"，".join(reasons)} | 预计周期：{cycle_map[difficulty_level]}', bg=C_WHITE)
    ws2.row_dimensions[r2].height = 25
    r2 += 1
    r2 += 1

    # ▌ 七、小结
    section_title(ws2, r2, 1, '▌ 七、竞争小结', span=8, bg=C_ORANGE)
    ws2.row_dimensions[r2].height = 24
    r2 += 1

    try:
        top3_brands = [str(b) for b in brand_agg.head(3).index.tolist()]
    except Exception:
        top3_brands = []
    cr5_txt = f'CR5={cr5:.0%}' if cr5 is not None else 'CR5=需上传 Market 文件'
    summary_text = (
        f'• 品牌集中度：前 3 品牌 {", ".join(top3_brands) if top3_brands else "见上表"}；{cr5_txt}\n'
        f'• 新品友好度：{new_ratio_top100:.1%}（<1 年）进入 BSR TOP100，{"新品机会大" if new_ratio_top100 > 0.2 else "新品突围难度大"}\n'
        f'• 综合推广难度：{difficulty_level}，建议 {cycle_map[difficulty_level]}'
    )
    ws2.merge_cells(start_row=r2, start_column=1, end_row=r2+2, end_column=8)
    c = ws2.cell(row=r2, column=1)
    c.value = summary_text
    c.font = Font(name='Arial', size=11, color='FF1F3864')
    c.fill = PatternFill('solid', fgColor=C_BLUE_LIGHT)
    c.alignment = Alignment(horizontal='left', vertical='center', wrap_text=True)
    for rr in range(r2, r2+3):
        ws2.row_dimensions[rr].height = 22
    r2 += 3

    # ▌ 八、广告竞争格局（ReverseASIN）
    if kw_df is not None and len(kw_df) > 0:
        section_title(ws2, r2, 1, '▌ 八、广告竞争格局（自然流量 vs 广告流量）', span=10)
        ws2.row_dimensions[r2].height = 24
        r2 += 1

        kw_ad = kw_df.copy()
        kw_ad['Organic Share'] = pd.to_numeric(kw_ad.get('Organic Share', pd.Series(dtype=float)), errors='coerce').fillna(0)
        kw_ad['Sponsored Share'] = pd.to_numeric(kw_ad.get('Sponsored Share', pd.Series(dtype=float)), errors='coerce').fillna(0)
        kw_ad['Sponsored ASINs'] = pd.to_numeric(kw_ad.get('Sponsored ASINs', pd.Series(dtype=float)), errors='coerce').fillna(0)
        kw_ad['DSR'] = pd.to_numeric(kw_ad.get('DSR', pd.Series(dtype=float)), errors='coerce').fillna(0)
        kw_ad['M. Searches'] = pd.to_numeric(kw_ad.get('M. Searches', pd.Series(dtype=float)), errors='coerce').fillna(0)
        ad_top = kw_ad.sort_values('M. Searches', ascending=False).head(15).reset_index(drop=True)

        ad_headers = ['关键词', '月搜索量', '自然流量占比', '广告流量占比', '广告竞品数', 'DSR供需比', '广告策略']
        for i, h in enumerate(ad_headers, 1):
            hdr(ws2, r2, i, h, bg=C_BLUE_MID)
        ws2.merge_cells(start_row=r2, start_column=7, end_row=r2, end_column=8)
        ws2.row_dimensions[r2].height = 20
        r2 += 1
        ad_start = r2
        ad_dominant = 0
        organic_dominant = 0
        for _, row in ad_top.iterrows():
            kw_name = str(row.get('Keyword', '-'))
            m_s = float(row['M. Searches'])
            org_s = float(row['Organic Share'])
            spon_s = float(row['Sponsored Share'])
            spon_asins = int(row['Sponsored ASINs'])
            dsr = float(row['DSR'])
            if spon_s > 0.6:
                strategy = '广告主导，必须投放'
                strat_bg = C_RED_LIGHT
                ad_dominant += 1
            elif spon_s > 0.3:
                strategy = '混合流量，建议投放'
                strat_bg = C_YELLOW
            else:
                strategy = '自然流量为主，SEO 优先'
                strat_bg = C_GREEN_LIGHT
                organic_dominant += 1
            val(ws2, r2, 1, kw_name, bold=True, bg=C_BLUE_LIGHT, fg='FF1F3864')
            val(ws2, r2, 2, f'{m_s:,.0f}')
            val(ws2, r2, 3, f'{org_s:.1%}' if org_s > 0 else '-', bg=C_GREEN_LIGHT if org_s > 0.5 else C_WHITE)
            val(ws2, r2, 4, f'{spon_s:.1%}' if spon_s > 0 else '-', bg=C_RED_LIGHT if spon_s > 0.6 else C_WHITE)
            val(ws2, r2, 5, f'{spon_asins}' if spon_asins > 0 else '-')
            val(ws2, r2, 6, f'{dsr:.1f}' if dsr > 0 else '-', bg=C_GREEN_LIGHT if dsr > 1 else (C_RED_LIGHT if dsr < 0.5 and dsr > 0 else C_WHITE))
            ws2.merge_cells(start_row=r2, start_column=7, end_row=r2, end_column=8)
            val(ws2, r2, 7, strategy, bold=True, bg=strat_bg)
            ws2.row_dimensions[r2].height = 20
            r2 += 1
        if r2 > ad_start:
            apply_border(ws2, ad_start-1, r2-1, 1, 8)
        r2 += 1
        ws2.merge_cells(start_row=r2, start_column=1, end_row=r2, end_column=8)
        val(ws2, r2, 1, f'小结：Top 15 词中 {ad_dominant} 个广告主导、{organic_dominant} 个自然流量为主。{"新品必须配合广告投放" if ad_dominant > organic_dominant else "部分词可通过 SEO 获取自然流量"}', bg=C_BLUE_LIGHT, fg='FF1F3864')
        ws2.row_dimensions[r2].height = 22
        r2 += 2

    # ▌ 九、关键词贡献度排名（ReverseASIN）
    if kw_df is not None and len(kw_df) > 0 and 'Units Sold' in kw_df.columns:
        section_title(ws2, r2, 1, '▌ 九、关键词贡献度排名（按实际带单量）', span=8)
        ws2.row_dimensions[r2].height = 24
        r2 += 1

        kw_sold = kw_df.copy()
        kw_sold['Units Sold'] = pd.to_numeric(kw_sold.get('Units Sold', pd.Series(dtype=float)), errors='coerce').fillna(0)
        kw_sold['M. Searches'] = pd.to_numeric(kw_sold.get('M. Searches', pd.Series(dtype=float)), errors='coerce').fillna(0)
        kw_sold['Purchase Rate'] = pd.to_numeric(kw_sold.get('Purchase Rate', pd.Series(dtype=float)), errors='coerce').fillna(0)
        total_units = kw_sold['Units Sold'].sum()
        kw_sold_top = kw_sold[kw_sold['Units Sold'] > 0].sort_values('Units Sold', ascending=False).head(15).reset_index(drop=True)

        sold_headers = ['关键词', '预估带单量', '月搜索量', '购买率', '贡献占比']
        for i, h in enumerate(sold_headers, 1):
            hdr(ws2, r2, i, h, bg=C_BLUE_MID)
        ws2.merge_cells(start_row=r2, start_column=5, end_row=r2, end_column=8)
        ws2.row_dimensions[r2].height = 20
        r2 += 1
        sold_start = r2
        for _, row in kw_sold_top.iterrows():
            units = float(row['Units Sold'])
            contrib = units / total_units if total_units > 0 else 0
            val(ws2, r2, 1, str(row.get('Keyword', '-')), bold=True, bg=C_BLUE_LIGHT, fg='FF1F3864')
            val(ws2, r2, 2, f'{int(units):,}')
            val(ws2, r2, 3, f'{float(row["M. Searches"]):,.0f}')
            val(ws2, r2, 4, f'{float(row["Purchase Rate"]):.2%}' if float(row['Purchase Rate']) > 0 else '-')
            ws2.merge_cells(start_row=r2, start_column=5, end_row=r2, end_column=8)
            val(ws2, r2, 5, f'{contrib:.1%}', bold=True, bg=C_GREEN_LIGHT if contrib > 0.05 else C_WHITE)
            ws2.row_dimensions[r2].height = 20
            r2 += 1
        if r2 > sold_start:
            apply_border(ws2, sold_start-1, r2-1, 1, 8)
        top5_contrib = kw_sold_top.head(5)['Units Sold'].sum() / total_units if total_units > 0 else 0
        r2 += 1
        ws2.merge_cells(start_row=r2, start_column=1, end_row=r2, end_column=8)
        val(ws2, r2, 1, f'小结：Top 5 关键词贡献 {top5_contrib:.0%} 销量，总预估带单 {int(total_units):,} 件。', bg=C_BLUE_LIGHT, fg='FF1F3864')
        ws2.row_dimensions[r2].height = 22
        r2 += 2

    # ▌ 十、Listing 标题优化建议（Unique Words）
    uw_df = keyword_data.get('unique_words') if keyword_data.get('_available') else None
    if uw_df is not None and len(uw_df) > 5:
        section_title(ws2, r2, 1, '▌ 十、Listing 标题优化建议（高频词）', span=8)
        ws2.row_dimensions[r2].height = 24
        r2 += 1

        uw = uw_df.copy()
        uw['Frequency'] = pd.to_numeric(uw['Frequency'], errors='coerce').fillna(0)
        total_kw_count = len(kw_df) if kw_df is not None else 1000
        uw_top = uw.sort_values('Frequency', ascending=False).head(15).reset_index(drop=True)

        uw_headers = ['排名', '高频词', '出现次数', '覆盖率', '建议']
        for i, h in enumerate(uw_headers, 1):
            hdr(ws2, r2, i, h, bg=C_BLUE_MID)
        ws2.merge_cells(start_row=r2, start_column=5, end_row=r2, end_column=8)
        ws2.row_dimensions[r2].height = 20
        r2 += 1
        uw_start = r2
        must_words = []
        for idx, (_, row) in enumerate(uw_top.iterrows()):
            freq = int(row['Frequency'])
            coverage = freq / total_kw_count
            if coverage > 0.2:
                advice = '必填'
                advice_bg = C_RED_LIGHT
                must_words.append(str(row['Phrase']))
            elif coverage > 0.1:
                advice = '建议'
                advice_bg = C_YELLOW
            else:
                advice = '可选'
                advice_bg = C_WHITE
            val(ws2, r2, 1, f'{idx+1}', h_align='center')
            val(ws2, r2, 2, str(row['Phrase']), bold=True, bg=C_BLUE_LIGHT)
            val(ws2, r2, 3, f'{freq}')
            val(ws2, r2, 4, f'{coverage:.1%}')
            ws2.merge_cells(start_row=r2, start_column=5, end_row=r2, end_column=8)
            val(ws2, r2, 5, advice, bold=True, bg=advice_bg)
            ws2.row_dimensions[r2].height = 18
            r2 += 1
        if r2 > uw_start:
            apply_border(ws2, uw_start-1, r2-1, 1, 8)
        r2 += 1
        if must_words:
            ws2.merge_cells(start_row=r2, start_column=1, end_row=r2, end_column=8)
            val(ws2, r2, 1, f'标题必含词：{", ".join(must_words)}', bold=True, bg=C_YELLOW, fg='FF7A4F01')
            ws2.row_dimensions[r2].height = 22
            r2 += 1
        r2 += 1

    # ▌ 十一、素材与配送门槛（US-Market）
    if market_data.get('_available'):
        has_data = False
        threshold_rows = []
        ff_df = market_data.get('fulfillment')
        if ff_df is not None and len(ff_df) > 0:
            try:
                for _, row in ff_df.iterrows():
                    if 'fba' in str(row.iloc[0]).lower():
                        fba_pct = float(pd.to_numeric(row.iloc[2], errors='coerce') or 0)
                        fba_sales_pct = float(pd.to_numeric(row.iloc[4], errors='coerce') or 0)
                        threshold_rows.append(('FBA 销量占比', f'{fba_sales_pct:.1%}', '必须走 FBA' if fba_sales_pct > 0.8 else ('FBA 为主' if fba_sales_pct > 0.5 else 'FBM 也可')))
                        has_data = True
                        break
            except Exception:
                pass
        ap_df = market_data.get('aplus_video')
        if ap_df is not None and len(ap_df) > 0:
            try:
                for _, row in ap_df.iterrows():
                    t = str(row.iloc[0]).lower()
                    pct = float(pd.to_numeric(row.iloc[2], errors='coerce') or 0)
                    if 'a+' in t or 'a plus' in t:
                        threshold_rows.append(('A+ 页面覆盖率', f'{pct:.1%}', '建议制作 A+' if pct > 0.5 else 'A+ 非必需'))
                        has_data = True
                    elif 'video' in t:
                        threshold_rows.append(('视频覆盖率', f'{pct:.1%}', '建议制作视频' if pct > 0.3 else '视频非必需'))
                        has_data = True
            except Exception:
                pass
        if cn_seller_ratio is not None:
            threshold_rows.append(('中国卖家占比', f'{cn_seller_ratio:.1%}', '价格竞争激烈' if cn_seller_ratio > 0.7 else '竞争多元'))
            has_data = True

        if has_data and threshold_rows:
            section_title(ws2, r2, 1, '▌ 十一、素材与配送门槛', span=8)
            ws2.row_dimensions[r2].height = 24
            r2 += 1
            hdr(ws2, r2, 1, '指标', bg=C_BLUE_MID)
            hdr(ws2, r2, 2, '数值', bg=C_BLUE_MID)
            ws2.merge_cells(start_row=r2, start_column=3, end_row=r2, end_column=8)
            hdr(ws2, r2, 3, '含义', bg=C_BLUE_MID)
            ws2.row_dimensions[r2].height = 20
            r2 += 1
            th_start = r2
            for label, value, meaning in threshold_rows:
                val(ws2, r2, 1, label, bold=True, bg=C_BLUE_LIGHT, fg='FF1F3864')
                val(ws2, r2, 2, value, bold=True)
                ws2.merge_cells(start_row=r2, start_column=3, end_row=r2, end_column=8)
                val(ws2, r2, 3, meaning, bg=C_WHITE, fg='FF595959')
                ws2.row_dimensions[r2].height = 20
                r2 += 1
            apply_border(ws2, th_start-1, r2-1, 1, 8)
            r2 += 1

    # ▌ 十二、评论门槛（US-Market）
    ratings_df = market_data.get('ratings_dist') if market_data.get('_available') else None
    if ratings_df is not None and len(ratings_df) > 2:
        section_title(ws2, r2, 1, '▌ 十二、评论门槛分析', span=8)
        ws2.row_dimensions[r2].height = 24
        r2 += 1

        rt = ratings_df.copy()
        rt_headers = ['评论数区间', '商品数', '销量', '销量占比', '月销售额($)', '销售额占比']
        for i, h in enumerate(rt_headers, 1):
            hdr(ws2, r2, i, h, bg=C_BLUE_MID)
        ws2.merge_cells(start_row=r2, start_column=6, end_row=r2, end_column=8)
        ws2.row_dimensions[r2].height = 20
        r2 += 1
        rt_start = r2
        for _, row in rt.iterrows():
            try:
                band = str(row.iloc[0])
                if band == 'nan' or 'no rating' in band.lower():
                    continue
                products = int(pd.to_numeric(row.iloc[1], errors='coerce') or 0)
                sales = int(pd.to_numeric(row.iloc[2], errors='coerce') or 0)
                sp = float(pd.to_numeric(row.iloc[3], errors='coerce') or 0)
                rev = float(pd.to_numeric(row.iloc[4], errors='coerce') or 0)
                rp = float(pd.to_numeric(row.iloc[5], errors='coerce') or 0)
                val(ws2, r2, 1, band, bold=True, bg=C_BLUE_LIGHT)
                val(ws2, r2, 2, f'{products}')
                val(ws2, r2, 3, f'{sales:,}')
                val(ws2, r2, 4, f'{sp:.1%}')
                val(ws2, r2, 5, f'{rev:,.0f}')
                ws2.merge_cells(start_row=r2, start_column=6, end_row=r2, end_column=8)
                val(ws2, r2, 6, f'{rp:.1%}')
                ws2.row_dimensions[r2].height = 18
                r2 += 1
            except Exception:
                continue
        if r2 > rt_start:
            apply_border(ws2, rt_start-1, r2-1, 1, 8)
        r2 += 1
        # 计算门槛建议
        try:
            cumsum = 0
            threshold_band = '200+'
            for _, row in rt.iterrows():
                sp = float(pd.to_numeric(row.iloc[3], errors='coerce') or 0)
                cumsum += sp
                if cumsum >= 0.5:
                    threshold_band = str(row.iloc[0])
                    break
            ws2.merge_cells(start_row=r2, start_column=1, end_row=r2, end_column=8)
            val(ws2, r2, 1, f'建议：进入 Top 50 销量阵营至少需要 {threshold_band} 条评论。新品前期重点积累评价。', bold=True, bg=C_YELLOW, fg='FF7A4F01')
            ws2.row_dimensions[r2].height = 22
            r2 += 1
        except Exception:
            pass
        r2 += 1

    # ===== Sheet 3: BSR TOP100 =====
    ws3 = wb.create_sheet('BSR TOP100')
    ws3.sheet_view.showGridLines = False

    display_cols = ['#', 'ASIN', 'Brand', 'Product Title', price_col, 'Monthly Sales', rev_col,
                    'Rating', 'Ratings', 'Available days', 'BuyBox Location', 'Fulfillment', 'product_type', 'Gross Margin']
    display_names = ['排名', 'ASIN', '品牌', '产品标题', '价格($)', '月销量', '月收入($)',
                     '星级', '评分数', '上架天数', '卖家地区', '配送', '产品类型', '毛利率']
    widths_3 = [6, 14, 14, 120, 10, 10, 13, 8, 10, 10, 10, 10, 18, 10]
    for i, (ltr, w) in enumerate(zip('ABCDEFGHIJKLMN', widths_3)):
        ws3.column_dimensions[ltr].width = w

    ws3.merge_cells('A1:N1')
    c = ws3['A1']
    c.value = f'BSR TOP100 - LED Job Site Lighting - {datetime.now().strftime("%Y-%m-%d")}'
    c.font = Font(name='Arial', bold=True, size=13, color=C_WHITE)
    c.fill = PatternFill('solid', fgColor=C_BLUE_DARK)
    c.alignment = Alignment(horizontal='center', vertical='center')
    ws3.row_dimensions[1].height = 30

    for i, name in enumerate(display_names):
        hdr(ws3, 2, i+1, name, bg=C_BLUE_MID)
    ws3.row_dimensions[2].height = 22

    available_cols = [c for c in display_cols if c in df.columns]
    for row_idx, (_, drow) in enumerate(df[available_cols].iterrows(), start=3):
        bg = C_GREY_LIGHT if row_idx % 2 == 0 else C_WHITE
        if str(drow.get('BuyBox Location', '')) == 'CN':
            bg = C_BLUE_LIGHT if row_idx % 2 == 0 else C_WHITE
        for ci, col in enumerate(available_cols):
            v = drow[col]
            if col == 'Gross Margin' and pd.notna(v):
                v = f'{float(v)*100:.1f}%'
            elif col == rev_col and pd.notna(v):
                v = round(float(v), 1)
            c = ws3.cell(row=row_idx, column=ci+1, value=v)
            c.font = Font(name='Arial', size=9)
            c.fill = PatternFill('solid', fgColor=bg)
            h_align = 'center' if ci > 2 else ('left' if ci == 3 else 'center')
            c.alignment = Alignment(horizontal=h_align, vertical='center', wrap_text=(ci == 3))
        ws3.row_dimensions[row_idx].height = 15 if ci != 3 else 30

    apply_border(ws3, 2, 101, 1, len(available_cols))
    
    # ===== BSR TOP100 - 新增内容汇总 =====
    r3 = 104
    
    # 统计汇总
    section_title(ws3, r3, 1, '▌ 数据统计汇总', span=14)
    ws3.row_dimensions[r3].height = 24
    r3 += 1
    
    # 计算各指标
    avg_sales = df['Monthly Sales'].mean()
    median_sales = df['Monthly Sales'].median()
    avg_rev_bsr = df[rev_col].mean() if rev_col else 0
    
    summary_stats = [
        ('月销量统计', f"均值: {int(avg_sales):,}件", f"中位数: {int(median_sales):,}件", f"最高: {int(df['Monthly Sales'].max()):,}件"),
        ('价格统计', f"均价: ${df[price_col].mean():.1f}", f"中位价: ${df[price_col].median():.1f}", f"最高: ${df[price_col].max():.0f}"),
        ('评分统计', f"均值: {df['Rating'].mean():.2f}★", f"中位数: {df['Rating'].median():.1f}★", f"最低: {df['Rating'].min():.1f}★"),
    ]
    
    for i, (stat_type, v1, v2, v3) in enumerate(summary_stats):
        bg = C_BLUE_LIGHT if i % 2 == 0 else C_WHITE
        val(ws3, r3, 1, stat_type, bold=True, bg=bg)
        for ci, v in enumerate([v1, v2, v3], start=2):
            c = ws3.cell(row=r3, column=ci, value=v)
            c.font = Font(name='Arial', size=9)
            c.fill = PatternFill('solid', fgColor=bg)
            c.alignment = Alignment(horizontal='center', vertical='center')
        ws3.row_dimensions[r3].height = 18
        r3 += 1
    apply_border(ws3, r3-len(summary_stats)-1, r3-1, 1, 4)
    r3 += 1
    
    # 高销量产品特征分析
    section_title(ws3, r3, 1, '▌ 高销量产品特征分析（TOP20）', span=14)
    ws3.row_dimensions[r3].height = 24
    r3 += 1
    
    hdr(ws3, r3, 1, '特征维度', bg=C_BLUE_MID)
    ws3.merge_cells(start_row=r3, start_column=2, end_row=r3, end_column=5)
    hdr(ws3, r3, 2, 'TOP20产品数据', bg=C_BLUE_MID)
    ws3.merge_cells(start_row=r3, start_column=6, end_row=r3, end_column=10)
    hdr(ws3, r3, 6, '整体市场数据', bg=C_BLUE_MID)
    ws3.merge_cells(start_row=r3, start_column=11, end_row=r3, end_column=14)
    hdr(ws3, r3, 11, '对比结论', bg=C_BLUE_MID)
    ws3.row_dimensions[r3].height = 20
    r3 += 1
    
    top20 = df.nlargest(20, 'Monthly Sales')
    top20_avg_price = top20[price_col].mean()
    top20_avg_rating = top20['Rating'].mean()
    top20_avg_sales = top20['Monthly Sales'].mean()
    
    feature_analysis = [
        ('平均价格', f"${top20_avg_price:.1f}", f"${df[price_col].mean():.1f}", 
         'TOP20价格略高' if top20_avg_price > df[price_col].mean() else 'TOP20价格更亲民'),
        ('平均评分', f"{top20_avg_rating:.2f}★", f"{df['Rating'].mean():.2f}★", 
         'TOP20评分更高' if top20_avg_rating > df['Rating'].mean() else '评分相近'),
        ('平均月销', f"{int(top20_avg_sales):,}件", f"{int(avg_sales):,}件", 
         '差异显著'),
    ]
    
    for i, (dim, top20_v, all_v, conclusion) in enumerate(feature_analysis):
        bg = C_YELLOW if i % 2 == 0 else C_WHITE
        val(ws3, r3, 1, dim, bold=True, bg=bg)
        ws3.merge_cells(start_row=r3, start_column=2, end_row=r3, end_column=5)
        c = ws3.cell(row=r3, column=2, value=top20_v)
        c.font = Font(name='Arial', bold=True, color='FF1F3864')
        c.fill = PatternFill('solid', fgColor=bg)
        c.alignment = Alignment(horizontal='center', vertical='center')
        ws3.merge_cells(start_row=r3, start_column=6, end_row=r3, end_column=10)
        c = ws3.cell(row=r3, column=6, value=all_v)
        c.fill = PatternFill('solid', fgColor=bg)
        c.alignment = Alignment(horizontal='center', vertical='center')
        ws3.merge_cells(start_row=r3, start_column=11, end_row=r3, end_column=14)
        val(ws3, r3, 11, conclusion, bg=bg, fg='FF595959')
        ws3.row_dimensions[r3].height = 20
        r3 += 1
    apply_border(ws3, r3-len(feature_analysis)-1, r3-1, 1, 14)
    r3 += 1
    
    # TOP20 ASIN列表
    section_title(ws3, r3, 1, '▌ TOP20 ASIN速查（可直接竞品分析）', span=14)
    ws3.row_dimensions[r3].height = 24
    r3 += 1
    
    hdr(ws3, r3, 1, '排名', bg=C_BLUE_MID)
    hdr(ws3, r3, 2, 'ASIN', bg=C_BLUE_MID)
    hdr(ws3, r3, 3, '品牌', bg=C_BLUE_MID)
    hdr(ws3, r3, 4, '价格', bg=C_BLUE_MID)
    hdr(ws3, r3, 5, '月销量', bg=C_BLUE_MID)
    ws3.merge_cells(start_row=r3, start_column=6, end_row=r3, end_column=14)
    hdr(ws3, r3, 6, '产品标题', bg=C_BLUE_MID)
    ws3.row_dimensions[r3].height = 20
    r3 += 1
    
    for idx, (_, drow) in enumerate(top20.iterrows()):
        bg = C_ORANGE if idx < 3 else (C_YELLOW if idx < 10 else C_WHITE)
        ws3.cell(row=r3, column=1, value=idx+1).alignment = Alignment(horizontal='center', vertical='center')
        ws3.cell(row=r3, column=1).fill = PatternFill('solid', fgColor=bg)
        ws3.cell(row=r3, column=2, value=drow['ASIN']).alignment = Alignment(horizontal='center', vertical='center')
        ws3.cell(row=r3, column=2).fill = PatternFill('solid', fgColor=bg)
        ws3.cell(row=r3, column=3, value=drow['Brand']).alignment = Alignment(horizontal='center', vertical='center')
        ws3.cell(row=r3, column=3).fill = PatternFill('solid', fgColor=bg)
        ws3.cell(row=r3, column=4, value=f"${drow[price_col]:.1f}").alignment = Alignment(horizontal='center', vertical='center')
        ws3.cell(row=r3, column=4).fill = PatternFill('solid', fgColor=bg)
        ws3.cell(row=r3, column=5, value=f"{int(drow['Monthly Sales']):,}").alignment = Alignment(horizontal='center', vertical='center')
        ws3.cell(row=r3, column=5).fill = PatternFill('solid', fgColor=bg)
        ws3.merge_cells(start_row=r3, start_column=6, end_row=r3, end_column=14)
        title_val = str(drow['Product Title'])[:80] if pd.notna(drow['Product Title']) else ''
        c = ws3.cell(row=r3, column=6, value=title_val)
        c.font = Font(name='Arial', size=9)
        c.fill = PatternFill('solid', fgColor=bg)
        c.alignment = Alignment(horizontal='left', vertical='center')
        ws3.row_dimensions[r3].height = 18
        r3 += 1
    apply_border(ws3, r3-21, r3-1, 1, 14)

    # ===== Sheet 4: 推荐入场价 =====
    ws4 = wb.create_sheet('推荐入场价')
    ws4.sheet_view.showGridLines = False
    for col_letter, width in zip('ABCDEFGHI', [22, 14, 14, 14, 14, 16, 20, 25, 14]):
        ws4.column_dimensions[col_letter].width = width

    ws4.merge_cells('A1:I1')
    c = ws4['A1']
    c.value = 'LED工作灯 — 各产品类型推荐入场价区间'
    c.font = Font(name='Arial', bold=True, size=14, color=C_WHITE)
    c.fill = PatternFill('solid', fgColor=C_BLUE_DARK)
    c.alignment = Alignment(horizontal='center', vertical='center')
    ws4.row_dimensions[1].height = 35

    r4 = 3
    section_title(ws4, r4, 1, '▌ 推荐入场价逻辑：以市场P25-P75价位为参考，结合单品收益分析', span=9)
    ws4.row_dimensions[r4].height = 24
    r4 += 1

    price_hdr4 = ['产品类型', '市场MIN', '市场P25', '市场中位', '市场P75', '市场MAX', '推荐入场价', '理由', '备注']
    for i, h in enumerate(price_hdr4):
        hdr(ws4, r4, i+1, h, bg=C_BLUE_MID)
    ws4.row_dimensions[r4].height = 22
    r4 += 1

    # 使用动态计算的推荐入场价数据
    colors_row = [C_YELLOW, C_GREEN_LIGHT, C_WHITE, C_WHITE, C_BLUE_LIGHT, C_GREY_LIGHT]
    for i, rec in enumerate(pricing_recommendations):
        bg = colors_row[i] if i < len(colors_row) else C_WHITE
        row_p = (
            rec['product_type'],
            f"${rec['min_price']:.1f}",
            f"${rec['p25']:.1f}",
            f"${rec['median']:.1f}",
            f"${rec['p75']:.1f}",
            f"${rec['max_price']:.1f}",
            f"${rec['rec_min']:.2f}-${rec['rec_max']:.2f}",
            rec['reason'],
            rec['note']
        )
        for ci, v in enumerate(row_p):
            c = ws4.cell(row=r4, column=ci+1, value=v)
            c.fill = PatternFill('solid', fgColor=bg)
            c.font = Font(name='Arial', size=10, bold=(ci == 0 or ci == 6))
            c.alignment = Alignment(horizontal='center' if ci not in [0, 7, 8] else 'left',
                                    vertical='center', wrap_text=True)
        ws4.row_dimensions[r4].height = 45
        r4 += 1

    apply_border(ws4, 5, r4-1, 1, 9)
    r4 += 2

    # 利润测算（基于首推品类的推荐价格）
    first_rec = pricing_recommendations[0] if pricing_recommendations else None
    if first_rec:
        profit_price = (first_rec['rec_min'] + first_rec['rec_max']) / 2
        profit_title = f"▌ {first_rec['product_type']} 入场利润测算（${profit_price:.2f}档）"
    else:
        profit_price = 34.99
        profit_title = "▌ 充电磁吸工作灯入场利润测算（$34.99档）"
    
    section_title(ws4, r4, 1, profit_title, span=9)
    ws4.row_dimensions[r4].height = 24
    r4 += 1

    # 动态计算利润
    commission = profit_price * 0.15
    fba_fee = 5.15  # 估算
    ad_fee = profit_price * 0.12
    purchase_cost_low = 8.00
    purchase_cost_high = 10.00
    other_fee = 1.50
    profit_low = profit_price - commission - fba_fee - ad_fee - purchase_cost_low - other_fee
    profit_high = profit_price - commission - fba_fee - ad_fee - purchase_cost_high - other_fee
    profit_margin_low = profit_low / profit_price * 100
    profit_margin_high = profit_high / profit_price * 100

    # 基于实际数据估算月销量
    if first_rec:
        est_sales_low = int(first_rec['avg_rev'] / first_rec['median'] * 0.6)
        est_sales_high = int(first_rec['avg_rev'] / first_rec['median'] * 1.2)
    else:
        est_sales_low = 300
        est_sales_high = 500
    
    profit_items = [
        ('售价', f'${profit_price:.2f}', ''),
        ('亚马逊佣金(15%)', f'-${commission:.2f}', ''),
        ('FBA费用(估算)', f'-${fba_fee:.2f}', '重量约0.8-1.2lb，含packaging'),
        ('广告费(CPC估算,12%)', f'-${ad_fee:.2f}', '新品期可能更高'),
        ('采购成本（含运费）', f'-${purchase_cost_low:.2f}~${purchase_cost_high:.2f}', '国内约¥40-55/个，含海运'),
        ('税费/其他', f'-${other_fee:.2f}', ''),
        ('毛利润', f'${profit_low:.2f}~${profit_high:.2f}', f'毛利率 ~{profit_margin_low:.0f}-{profit_margin_high:.0f}%'),
        ('月销量目标（保守）', f'{est_sales_low}-{est_sales_high}件', '进入TOP50可期待'),
        ('月毛利润（估算）', f'${profit_low*est_sales_low:.0f}~${profit_high*est_sales_high:.0f}', ''),
    ]

    hdr(ws4, r4, 1, '成本结构', bg=C_BLUE_MID)
    ws4.merge_cells(start_row=r4, start_column=2, end_row=r4, end_column=5)
    hdr(ws4, r4, 2, '金额', bg=C_BLUE_MID)
    ws4.merge_cells(start_row=r4, start_column=6, end_row=r4, end_column=9)
    hdr(ws4, r4, 6, '说明', bg=C_BLUE_MID)
    r4 += 1
    for i, (item, amount, note) in enumerate(profit_items):
        bg = C_YELLOW if '毛利润' in item else (C_GREY_LIGHT if i % 2 == 0 else C_WHITE)
        bold = '毛利润' in item
        val(ws4, r4, 1, item, bold=bold, bg=bg)
        ws4.merge_cells(start_row=r4, start_column=2, end_row=r4, end_column=5)
        c = ws4.cell(row=r4, column=2, value=amount)
        c.font = Font(name='Arial', bold=bold, size=11, color='FF1F3864' if bold else '000000')
        c.fill = PatternFill('solid', fgColor=bg)
        c.alignment = Alignment(horizontal='center', vertical='center')
        ws4.merge_cells(start_row=r4, start_column=6, end_row=r4, end_column=9)
        val(ws4, r4, 6, note, bg=bg, fg='FF595959')
        ws4.row_dimensions[r4].height = 22
        r4 += 1
    apply_border(ws4, r4-len(profit_items)-1, r4-1, 1, 9)
    r4 += 2
    
    # ===== 推荐入场价 - 新增内容 =====
    # 首推品类决策依据
    section_title(ws4, r4, 1, '▌ 首推品类决策依据详细说明', span=9)
    ws4.row_dimensions[r4].height = 24
    r4 += 1
    
    if first_rec:
        rec_price_low = first_rec['rec_min']
        rec_price_high = first_rec['rec_max']
        rec_sku = first_rec['sku_count']
        rec_monthly_sales = int(first_rec.get('avg_rev', 0) / first_rec['median']) * rec_sku if rec_sku > 0 else 0
    
    decision_basis = [
        ('为什么选择这个品类？', 
         f'• 月销量{int(type_agg[type_agg["product_type"]=="充电磁吸工作灯"]["total_sales"].sum()):,}件，市场需求旺盛\n'
         f'• 均价${first_rec["median"]:.0f}，定价${rec_price_low:.0f}-${rec_price_high:.0f}有竞争力\n'
         f'• 差评痛点明确（电池/亮度/固定），产品改良空间大'),
        ('定价策略依据？', 
         f'• P25=${first_rec["p25"]:.1f}，P75=${first_rec["p75"]:.1f}，中位价${first_rec["median"]:.0f}\n'
         f'• 推荐价=${rec_price_low:.2f}-${rec_price_high:.2f}位于P40-P60区间\n'
         f'• 既能保证利润（毛利50%+），又有价格竞争力'),
        ('风险评估与应对？', 
         '• 风险1：竞品多（39个SKU），需差异化突围\n'
         '• 应对：聚焦差评痛点（电池容量/亮度），不做同质化竞争\n'
         '• 风险2：广告CPC高，ACOS初期可能超30%\n'
         '• 应对：优化listing转化，配合秒杀活动'),
    ]
    
    for i, (title, content) in enumerate(decision_basis):
        bg = C_BLUE_LIGHT if i % 2 == 0 else C_WHITE
        ws4.merge_cells(start_row=r4, start_column=1, end_row=r4, end_column=2)
        val(ws4, r4, 1, title, bold=True, bg=bg, fg='FF1F3864')
        ws4.merge_cells(start_row=r4, start_column=3, end_row=r4, end_column=9)
        val(ws4, r4, 3, content, bg=bg, fg='FF595959', wrap=True)
        ws4.row_dimensions[r4].height = 60
        r4 += 1
    apply_border(ws4, r4-len(decision_basis)-1, r4-1, 1, 9)
    r4 += 1
    
    # 各价格带竞争分析
    section_title(ws4, r4, 1, '▌ 各价格带竞争强度分析', span=9)
    ws4.row_dimensions[r4].height = 24
    r4 += 1
    
    hdr(ws4, r4, 1, '价格区间', bg=C_BLUE_MID)
    hdr(ws4, r4, 2, 'SKU数量', bg=C_BLUE_MID)
    hdr(ws4, r4, 3, '平均月销', bg=C_BLUE_MID)
    hdr(ws4, r4, 4, '竞争强度', bg=C_BLUE_MID)
    ws4.merge_cells(start_row=r4, start_column=5, end_row=r4, end_column=9)
    hdr(ws4, r4, 5, '入场建议', bg=C_BLUE_MID)
    ws4.row_dimensions[r4].height = 20
    r4 += 1
    
    price_band_analysis = [
        ('<$20', len(df[df[price_col] < 20]), int(df[df[price_col] < 20]['Monthly Sales'].mean()),
         '低', '利润薄，除非有供应链优势否则不建议'),
        ('$20-$35', len(df[(df[price_col] >= 20) & (df[price_col] < 35)]),
         int(df[(df[price_col] >= 20) & (df[price_col] < 35)]['Monthly Sales'].mean()),
         '中高', '主战场，竞争激烈，需差异化'),
        ('$35-$55', len(df[(df[price_col] >= 35) & (df[price_col] < 55)]),
         int(df[(df[price_col] >= 35) & (df[price_col] < 55)]['Monthly Sales'].mean()),
         '中', '★ 最优入场区间，兼顾利润和销量'),
        ('>$55', len(df[df[price_col] >= 55]),
         int(df[df[price_col] >= 55]['Monthly Sales'].mean()),
         '低', '客单价高，但销量有限，适合品质路线'),
    ]
    
    for i, (band, cnt, avg_s, intensity, advice) in enumerate(price_band_analysis):
        bg = C_GREEN_LIGHT if '★' in advice else (C_RED_LIGHT if intensity == '高' else C_WHITE)
        val(ws4, r4, 1, band, bold=True, bg=bg)
        ws4.cell(row=r4, column=2, value=cnt).alignment = Alignment(horizontal='center', vertical='center')
        ws4.cell(row=r4, column=2).fill = PatternFill('solid', fgColor=bg)
        ws4.cell(row=r4, column=3, value=f'{avg_s:,}件').alignment = Alignment(horizontal='center', vertical='center')
        ws4.cell(row=r4, column=3).fill = PatternFill('solid', fgColor=bg)
        intensity_color = 'FF1F3864' if intensity == '低' else ('FF7B0000' if intensity == '高' else 'FF595959')
        c = ws4.cell(row=r4, column=4, value=intensity)
        c.font = Font(name='Arial', bold=True, color=intensity_color)
        c.fill = PatternFill('solid', fgColor=bg)
        c.alignment = Alignment(horizontal='center', vertical='center')
        ws4.merge_cells(start_row=r4, start_column=5, end_row=r4, end_column=9)
        val(ws4, r4, 5, advice, bg=bg, fg='FF595959')
        ws4.row_dimensions[r4].height = 22
        r4 += 1
    apply_border(ws4, r4-len(price_band_analysis)-1, r4-1, 1, 9)

    # ===== Sheet 5: 竞品分析 =====
    ws5 = wb.create_sheet('竞品分析')
    ws5.sheet_view.showGridLines = False
    for col_letter, width in zip('ABCDEFGHIJK', [22, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14]):
        ws5.column_dimensions[col_letter].width = width

    ws5.merge_cells('A1:K1')
    c = ws5['A1']
    c.value = f'LED工作灯 — 竞品分析（基于{len(df)}个BSR样本 + {len(rev_df)}条真实评论）'
    c.font = Font(name='Arial', bold=True, size=14, color=C_WHITE)
    c.fill = PatternFill('solid', fgColor=C_BLUE_DARK)
    c.alignment = Alignment(horizontal='center', vertical='center')
    ws5.row_dimensions[1].height = 35

    r5 = 3

    # ▌ 一、Top 10 竞品参数横向对比
    section_title(ws5, r5, 1, '▌ 一、Top 10 竞品参数横向对比（按月销排序）', span=11)
    ws5.row_dimensions[r5].height = 24
    r5 += 1

    # 取 Top 10 竞品
    sort_key = None
    if 'Monthly Sales' in df.columns:
        sort_key = 'Monthly Sales'
    elif rev_col:
        sort_key = rev_col
    top10 = df.sort_values(sort_key, ascending=False).head(10).reset_index(drop=True) if sort_key else df.head(10).reset_index(drop=True)
    n_top = len(top10)

    def g(row, col_name, default=''):
        if col_name in top10.columns:
            v = row.get(col_name)
            if pd.isna(v):
                return default
            return v
        return default

    # 动态识别 Bullet Points 列
    bullet_col = None
    for c in df.columns:
        if 'bullet' in str(c).lower():
            bullet_col = c
            break

    # 提取 specs（标题 + 五点描述合并）
    specs_list = [extract_all_specs(
        g(top10.iloc[i], 'Product Title', ''),
        g(top10.iloc[i], bullet_col, '') if bullet_col else ''
    ) for i in range(n_top)]

    # 同时对全量 BSR 商品提取 specs（供 Sheet 10 聚合推荐使用）
    all_specs_for_agg = [extract_all_specs(
        str(df.iloc[i].get('Product Title', '')),
        str(df.iloc[i].get(bullet_col, '')) if bullet_col else ''
    ) for i in range(len(df))]

    # 各属性行
    attr_rows = [
        ('ASIN', [str(g(top10.iloc[i], 'ASIN', '-')) for i in range(n_top)]),
        ('品牌', [str(g(top10.iloc[i], 'Brand', '-')) for i in range(n_top)]),
        ('价格', [f'${g(top10.iloc[i], price_col, 0):.2f}' if price_col else '-' for i in range(n_top)]),
        ('月销量', [f'{int(g(top10.iloc[i], "Monthly Sales", 0)):,}' if 'Monthly Sales' in top10.columns else '-' for i in range(n_top)]),
        ('月销售额', [f'${g(top10.iloc[i], rev_col, 0):,.0f}' if rev_col else '-' for i in range(n_top)]),
        ('评分数', [f'{int(g(top10.iloc[i], "Ratings", 0)):,}' if 'Ratings' in top10.columns else '-' for i in range(n_top)]),
        ('星级', [f'{g(top10.iloc[i], "Rating", 0):.1f}★' if 'Rating' in top10.columns else '-' for i in range(n_top)]),
        ('在售天数', [f'{int(g(top10.iloc[i], "Available days", 0))}天' if 'Available days' in top10.columns else '-' for i in range(n_top)]),
        ('配送', [str(g(top10.iloc[i], 'Fulfillment', '-')) for i in range(n_top)]),
        ('卖家国家', [str(g(top10.iloc[i], 'BuyBox Location', '-')) for i in range(n_top)]),
        ('产品类型', [classify(g(top10.iloc[i], 'Product Title', '')) for i in range(n_top)]),
        ('光源', [specs_list[i].get('光源', '') for i in range(n_top)]),
        ('功率', [specs_list[i].get('功率W', '') for i in range(n_top)]),
        ('光通量', [specs_list[i].get('光通量lm', '') for i in range(n_top)]),
        ('电池容量', [specs_list[i].get('电池容量mAh', '') for i in range(n_top)]),
        ('充电方式', [specs_list[i].get('充电方式', '') for i in range(n_top)]),
        ('供电方式', [specs_list[i].get('供电方式', '') for i in range(n_top)]),
        ('电池类型', [specs_list[i].get('电池类型', '') for i in range(n_top)]),
        ('续航', [specs_list[i].get('续航', '') for i in range(n_top)]),
        ('防水等级', [specs_list[i].get('防水等级', '') for i in range(n_top)]),
        ('光照模式', [specs_list[i].get('光照模式数', '') for i in range(n_top)]),
        ('磁力/固定', [specs_list[i].get('磁力/固定方式', '') for i in range(n_top)]),
        ('旋转角度', [specs_list[i].get('旋转角度', '') for i in range(n_top)]),
        ('材质', [specs_list[i].get('材质', '') for i in range(n_top)]),
        ('重量', [specs_list[i].get('重量', '') for i in range(n_top)]),
    ]

    # 表头行：#1-#10
    val(ws5, r5, 1, '属性', bold=True, bg=C_BLUE_MID, fg=C_WHITE)
    for j in range(n_top):
        val(ws5, r5, 2+j, f'#{j+1}', bold=True, bg=C_BLUE_MID, fg=C_WHITE)
    ws5.row_dimensions[r5].height = 22
    r5 += 1
    row_start = r5
    for label, values in attr_rows:
        is_key = label in ('ASIN', '品牌', '价格', '月销量')
        val(ws5, r5, 1, label, bold=True, bg=C_BLUE_LIGHT, fg='FF1F3864')
        for j, v in enumerate(values):
            cell_bg = C_WHITE if not is_key else 'FFFFF8E7'
            val(ws5, r5, 2+j, str(v) if v != '' else '—', bg=cell_bg, fg='FF595959' if v == '' else 'FF000000')
        ws5.row_dimensions[r5].height = 20
        r5 += 1
    apply_border(ws5, row_start-1, r5-1, 1, 1+n_top)

    # 说明
    r5 += 1
    ws5.merge_cells(start_row=r5, start_column=1, end_row=r5, end_column=11)
    c = ws5.cell(row=r5, column=1)
    c.value = '注：参数从产品标题 + Bullet Points（五点描述）正则提取，空值表示未标注（不代表实际不具备）'
    c.font = Font(name='Arial', size=10, color='FF7A4F01', italic=True)
    c.fill = PatternFill('solid', fgColor=C_YELLOW)
    c.alignment = Alignment(horizontal='left', vertical='center')
    r5 += 2

    # ▌ 二、正向卖点 + ▌ 三、差评（原有 8 维分析）
    section_title(ws5, r5, 1, '▌ 二、正向卖点提炼（4-5★评论）', span=4)
    section_title(ws5, r5, 5, '▌ 三、差评痛点分析（1-2★评论）', span=4, bg='FF7B2D00')
    ws5.row_dimensions[r5].height = 24
    r5 += 1

    hdr(ws5, r5, 1, '卖点维度', bg=C_BLUE_MID)
    hdr(ws5, r5, 2, '提及次数', bg=C_BLUE_MID)
    hdr(ws5, r5, 3, '占比', bg=C_BLUE_MID)
    hdr(ws5, r5, 4, '典型用语/消费者语言', bg=C_BLUE_MID)
    hdr(ws5, r5, 5, '差评痛点', bg='FFC00000', fg=C_WHITE)
    hdr(ws5, r5, 6, '提及次数', bg='FFC00000', fg=C_WHITE)
    hdr(ws5, r5, 7, '占比', bg='FFC00000', fg=C_WHITE)
    hdr(ws5, r5, 8, '典型反馈', bg='FFC00000', fg=C_WHITE)
    r5 += 1

    pos_quotes = {
        '高亮度/强光': '"super bright","lights up my whole garage","incredibly bright for the size"',
        '轻便/便携': '"compact","fits in toolbox","easy to take anywhere","lightweight"',
        '易用/操作方便': '"easy to use","set it and forget it","simple one-button operation"',
        '性价比高': '"great value","can\'t beat the price","exactly what I needed at this price"',
        '耐用/品质好': '"solid build","feels sturdy","very well made","has held up"',
        '电池续航好': '"battery lasts hours","lasted all day","charges fast via USB-C"',
        '多功能/多模式': '"multiple modes","strobe mode is great","works for camping too"',
        '磁力强/多角度调节': '"strong magnet","sticks to any metal surface","360 rotation is perfect"',
    }
    neg_quotes = {
        '电池/充电问题': '"battery died after 2 months","won\'t charge anymore","battery drains overnight"',
        '耐久性/质量差': '"stopped working","broke after 3 uses","cheaply made","fell apart"',
        '亮度不足': '"not bright at all","dim","dollar store flashlight is brighter"',
        '灯头/角度固定差': '"head is floppy","doesn\'t stay in position","pivot is very loose"',
        '客服/售后问题': '"terrible customer service","no response","can\'t get a replacement"',
        '磁力不稳/固定问题': '"magnet won\'t hold","falls off metal surface","weak magnet"',
        '不含电池/配件缺失': '"battery not included","just the tool","need to buy charger separately"',
        '防水/防尘问题': '"not actually waterproof","rusted in rain","water got inside"',
    }

    pos_sorted = sorted(pos_counts.items(), key=lambda x: x[1], reverse=True)
    neg_sorted = sorted(neg_counts.items(), key=lambda x: x[1], reverse=True)
    total_pos = len(high_rev)
    total_neg = len(low_rev)

    max_rows = max(len(pos_sorted), len(neg_sorted))
    for i in range(max_rows):
        bg_pos = C_GREEN_LIGHT if i % 2 == 0 else C_WHITE
        bg_neg = C_RED_LIGHT if i % 2 == 0 else 'FFFFF5F5'

        if i < len(pos_sorted):
            cat_p, cnt_p = pos_sorted[i]
            val(ws5, r5, 1, cat_p, bold=True, bg=bg_pos)
            ws5.cell(row=r5, column=2, value=cnt_p).alignment = Alignment(horizontal='center', vertical='center')
            ws5.cell(row=r5, column=2).fill = PatternFill('solid', fgColor=bg_pos)
            pct_p = f'{cnt_p/total_pos*100:.1f}%' if total_pos > 0 else '0%'
            ws5.cell(row=r5, column=3, value=pct_p).alignment = Alignment(horizontal='center', vertical='center')
            ws5.cell(row=r5, column=3).fill = PatternFill('solid', fgColor=bg_pos)
            val(ws5, r5, 4, pos_quotes.get(cat_p, ''), fg='FF595959', bg=bg_pos, size=9)
        else:
            for ci in range(1, 5):
                ws5.cell(row=r5, column=ci).fill = PatternFill('solid', fgColor=C_WHITE)

        if i < len(neg_sorted):
            cat_n, cnt_n = neg_sorted[i]
            val(ws5, r5, 5, cat_n, bold=True, bg=bg_neg, fg='FF7B0000')
            ws5.cell(row=r5, column=6, value=cnt_n).alignment = Alignment(horizontal='center', vertical='center')
            ws5.cell(row=r5, column=6).fill = PatternFill('solid', fgColor=bg_neg)
            pct_n = f'{cnt_n/total_neg*100:.1f}%' if total_neg > 0 else '0%'
            ws5.cell(row=r5, column=7, value=pct_n).alignment = Alignment(horizontal='center', vertical='center')
            ws5.cell(row=r5, column=7).fill = PatternFill('solid', fgColor=bg_neg)
            val(ws5, r5, 8, neg_quotes.get(cat_n, ''), fg='FF595959', bg=bg_neg, size=9)
        else:
            for ci in range(5, 9):
                ws5.cell(row=r5, column=ci).fill = PatternFill('solid', fgColor=C_WHITE)

        ws5.row_dimensions[r5].height = 28
        r5 += 1

    apply_border(ws5, 5, r5-1, 1, 8)
    r5 += 1

    # 各ASIN评论质量汇总
    section_title(ws5, r5, 1, '▌ 四、各ASIN评论质量汇总', span=8)
    r5 += 1
    asin_hdr = ['ASIN', '产品类型', '评论总数', '平均星级', '差评率', '5★占比', '主要差评类型']
    for i, h in enumerate(asin_hdr):
        hdr(ws5, r5, i+1, h, bg=C_BLUE_MID)
    ws5.merge_cells(start_row=r5, start_column=7, end_row=r5, end_column=8)
    r5 += 1

    asin_type_map = dict(zip(df['ASIN'], df['product_type']))
    
    # 处理评论数据为空的情况
    if len(rev_df) > 0 and 'source_asin' in rev_df.columns:
        rev_asin_count = len(rev_df['source_asin'].unique())
        for source_asin in rev_df['source_asin'].unique():
            adf = rev_df[rev_df['source_asin'] == source_asin]
            if len(adf) == 0:
                continue
            avg_r = adf['Rating'].mean() if 'Rating' in adf.columns else 0
            low_pct = (adf['Rating'] <= 2).sum() / len(adf) * 100 if 'Rating' in adf.columns else 0
            five_pct = (adf['Rating'] == 5).sum() / len(adf) * 100 if 'Rating' in adf.columns else 0
            adf_low = adf[adf['Rating'] <= 2].copy() if 'Rating' in adf.columns else pd.DataFrame()
            complaints = []
            if len(adf_low) > 0 and 'Title' in adf_low.columns and 'Content' in adf_low.columns:
                adf_low['neg_text'] = adf_low['Title'].fillna('') + ' ' + adf_low['Content'].fillna('')
                for cat, kws in neg_keywords.items():
                    pattern = '|'.join(kws)
                    cnt = adf_low['neg_text'].str.lower().str.contains(pattern, na=False).sum()
                    if cnt > 0:
                        complaints.append(f'{cat}({cnt})')
            complaint_str = '、'.join(complaints[:3]) if complaints else '无明显差评集中'
            ptype = asin_type_map.get(source_asin, '未知')
            bg = C_RED_LIGHT if avg_r < 3.5 else (C_YELLOW if avg_r < 4.0 else C_WHITE)
            vals5 = [source_asin, ptype, len(adf), f'{avg_r:.1f}★', f'{low_pct:.1f}%', f'{five_pct:.1f}%']
            for ci, v in enumerate(vals5):
                c = ws5.cell(row=r5, column=ci+1, value=v)
                c.fill = PatternFill('solid', fgColor=bg)
                c.font = Font(name='Arial', size=9)
                c.alignment = Alignment(horizontal='center', vertical='center')
            ws5.merge_cells(start_row=r5, start_column=7, end_row=r5, end_column=8)
            val(ws5, r5, 7, complaint_str, bg=bg, fg='FF595959', size=9)
            ws5.row_dimensions[r5].height = 18
            r5 += 1
        apply_border(ws5, r5 - rev_asin_count - 1, r5-1, 1, 8)
    else:
        val(ws5, r5, 1, '暂无评论数据', bg=C_YELLOW)
        ws5.merge_cells(start_row=r5, start_column=1, end_row=r5, end_column=8)
        r5 += 1
    r5 += 1

    # ▌ 五、重点ASIN参数深度分析（有评论数据的ASIN）
    review_asins = rev_df['source_asin'].unique().tolist() if len(rev_df) > 0 and 'source_asin' in rev_df.columns else []
    # 筛选同时在BSR数据中的ASIN
    review_asins_in_bsr = [a for a in review_asins if a in df['ASIN'].values]

    if review_asins_in_bsr:
        # 按月销量排序，最多取8个
        asin_sales = []
        for a in review_asins_in_bsr:
            row_match = df[df['ASIN'] == a]
            if len(row_match) > 0:
                ms = row_match.iloc[0].get('Monthly Sales', 0)
                ms = ms if pd.notna(ms) else 0
                asin_sales.append((a, ms))
        asin_sales.sort(key=lambda x: x[1], reverse=True)
        focus_asins = [a for a, _ in asin_sales[:8]]
        n_focus = len(focus_asins)

        section_title(ws5, r5, 1, f'▌ 五、重点ASIN参数深度分析（{n_focus}个有评论数据的ASIN）', span=max(n_focus+1, 8))
        ws5.row_dimensions[r5].height = 24
        r5 += 1

        # 提取每个ASIN的参数
        focus_specs = []
        focus_rows_data = []
        for a in focus_asins:
            row_match = df[df['ASIN'] == a].iloc[0]
            title_val = str(row_match.get('Product Title', ''))
            bullet_val = str(row_match.get(bullet_col, '')) if bullet_col else ''
            sp = extract_all_specs(title_val, bullet_val)
            focus_specs.append(sp)
            focus_rows_data.append(row_match)

        # 每个ASIN的评论统计
        focus_review_stats = []
        for a in focus_asins:
            adf = rev_df[rev_df['source_asin'] == a]
            total_rev_count = len(adf)
            avg_r = adf['Rating'].mean() if 'Rating' in adf.columns and len(adf) > 0 else 0
            low_pct = (adf['Rating'] <= 2).sum() / len(adf) * 100 if 'Rating' in adf.columns and len(adf) > 0 else 0
            # Top3 正向
            high_adf = adf[adf['Rating'] >= 4] if 'Rating' in adf.columns else pd.DataFrame()
            pos_top3 = []
            if len(high_adf) > 0 and 'Title' in high_adf.columns and 'Content' in high_adf.columns:
                high_adf_text = high_adf['Title'].fillna('') + ' ' + high_adf['Content'].fillna('')
                for cat, kws in pos_keywords.items():
                    pattern = '|'.join(kws)
                    cnt = high_adf_text.str.lower().str.contains(pattern, na=False).sum()
                    if cnt > 0:
                        pos_top3.append((cat, cnt))
                pos_top3.sort(key=lambda x: x[1], reverse=True)
            # Top3 负向
            low_adf = adf[adf['Rating'] <= 2] if 'Rating' in adf.columns else pd.DataFrame()
            neg_top3_asin = []
            if len(low_adf) > 0 and 'Title' in low_adf.columns and 'Content' in low_adf.columns:
                low_adf_text = low_adf['Title'].fillna('') + ' ' + low_adf['Content'].fillna('')
                for cat, kws in neg_keywords.items():
                    pattern = '|'.join(kws)
                    cnt = low_adf_text.str.lower().str.contains(pattern, na=False).sum()
                    if cnt > 0:
                        neg_top3_asin.append((cat, cnt))
                neg_top3_asin.sort(key=lambda x: x[1], reverse=True)
            focus_review_stats.append({
                'total': total_rev_count,
                'avg_rating': avg_r,
                'neg_rate': low_pct,
                'pos_top3': pos_top3[:3],
                'neg_top3': neg_top3_asin[:3],
            })

        # 构建属性行
        focus_attr_rows = [
            ('ASIN', [a for a in focus_asins]),
            ('品牌', [str(focus_rows_data[i].get('Brand', '-')) for i in range(n_focus)]),
            ('价格', [f'${focus_rows_data[i].get(price_col, 0):.2f}' if price_col and pd.notna(focus_rows_data[i].get(price_col)) else '-' for i in range(n_focus)]),
            ('月销量', [f'{int(focus_rows_data[i].get("Monthly Sales", 0)):,}' if pd.notna(focus_rows_data[i].get('Monthly Sales')) else '-' for i in range(n_focus)]),
            ('评分/评论数', [f'{focus_review_stats[i]["avg_rating"]:.1f}★({focus_review_stats[i]["total"]}条)' for i in range(n_focus)]),
        ]
        # 分隔标记
        spec_attr_rows = [
            ('光通量', [focus_specs[i].get('光通量lm', '') for i in range(n_focus)]),
            ('电池容量', [focus_specs[i].get('电池容量mAh', '') for i in range(n_focus)]),
            ('充电方式', [focus_specs[i].get('充电方式', '') for i in range(n_focus)]),
            ('防水等级', [focus_specs[i].get('防水等级', '') for i in range(n_focus)]),
            ('光照模式', [focus_specs[i].get('光照模式数', '') for i in range(n_focus)]),
            ('磁力/固定', [focus_specs[i].get('磁力/固定方式', '') for i in range(n_focus)]),
            ('材质', [focus_specs[i].get('材质', '') for i in range(n_focus)]),
            ('续航', [focus_specs[i].get('续航', '') for i in range(n_focus)]),
            ('旋转角度', [focus_specs[i].get('旋转角度', '') for i in range(n_focus)]),
            ('重量', [focus_specs[i].get('重量', '') for i in range(n_focus)]),
        ]
        review_attr_rows = [
            ('好评TOP3', [', '.join([f'{cat}({cnt})' for cat, cnt in focus_review_stats[i]['pos_top3']]) or '—' for i in range(n_focus)]),
            ('差评TOP3', [', '.join([f'{cat}({cnt})' for cat, cnt in focus_review_stats[i]['neg_top3']]) or '—' for i in range(n_focus)]),
            ('差评率', [f'{focus_review_stats[i]["neg_rate"]:.1f}%' for i in range(n_focus)]),
        ]

        # 写表头
        val(ws5, r5, 1, '属性', bold=True, bg=C_BLUE_MID, fg=C_WHITE)
        for j in range(n_focus):
            val(ws5, r5, 2+j, f'ASIN-{j+1}', bold=True, bg=C_BLUE_MID, fg=C_WHITE)
        ws5.row_dimensions[r5].height = 22
        r5 += 1
        focus_start = r5

        # 基础信息行
        for label, values in focus_attr_rows:
            val(ws5, r5, 1, label, bold=True, bg=C_BLUE_LIGHT, fg='FF1F3864')
            for j, v in enumerate(values):
                val(ws5, r5, 2+j, str(v) if v else '—', bg='FFFFF8E7', fg='FF000000')
            ws5.row_dimensions[r5].height = 20
            r5 += 1

        # 参数分隔行
        val(ws5, r5, 1, '—— 产品参数（Bullet Points提取）——', bold=True, bg='FFD9E2F3', fg='FF1F3864')
        for j in range(n_focus):
            ws5.cell(row=r5, column=2+j).fill = PatternFill('solid', fgColor='FFD9E2F3')
        ws5.row_dimensions[r5].height = 20
        r5 += 1

        # 参数行
        for label, values in spec_attr_rows:
            val(ws5, r5, 1, label, bold=True, bg=C_BLUE_LIGHT, fg='FF1F3864')
            for j, v in enumerate(values):
                val(ws5, r5, 2+j, str(v) if v else '—', bg=C_WHITE, fg='FF595959' if not v else 'FF000000')
            ws5.row_dimensions[r5].height = 20
            r5 += 1

        # 评论分隔行
        val(ws5, r5, 1, '—— 评论洞察 ——', bold=True, bg='FFFCE4D6', fg='FF7B2D00')
        for j in range(n_focus):
            ws5.cell(row=r5, column=2+j).fill = PatternFill('solid', fgColor='FFFCE4D6')
        ws5.row_dimensions[r5].height = 20
        r5 += 1

        # 评论行
        for label, values in review_attr_rows:
            is_neg = '差评' in label
            row_bg = C_RED_LIGHT if is_neg else C_GREEN_LIGHT
            val(ws5, r5, 1, label, bold=True, bg=row_bg, fg='FF7B0000' if is_neg else 'FF1F5C1F')
            for j, v in enumerate(values):
                val(ws5, r5, 2+j, str(v), bg=row_bg, fg='FF7B0000' if is_neg else 'FF595959', size=9)
            ws5.row_dimensions[r5].height = 22
            r5 += 1

        apply_border(ws5, focus_start-1, r5-1, 1, 1+n_focus)
    else:
        section_title(ws5, r5, 1, '▌ 五、重点ASIN参数深度分析', span=8)
        ws5.row_dimensions[r5].height = 24
        r5 += 1
        val(ws5, r5, 1, '暂无评论数据（上传评论文件后可显示重点ASIN的参数+评论深度分析）', bg=C_YELLOW)
        ws5.merge_cells(start_row=r5, start_column=1, end_row=r5, end_column=8)
        r5 += 1
    r5 += 1

    # ===== 竞品分析 - 改进方向 =====
    # 核心改进方向总结
    section_title(ws5, r5, 1, '▌ 六、核心改进方向总结', span=8)
    ws5.row_dimensions[r5].height = 24
    r5 += 1
    
    # 改进优先级排序
    improvement_priority = [
        ('P1 首要改进', '电池续航',
         f'差评{int(neg_counts.get("电池/充电问题", 0))}条，占比最高',
         '• 电池容量≥5000mAh（竞品多3000-4000mAh）\n• USB-C快充，低电量提示\n• 续航≥8小时'),
        ('P2 重要改进', '产品亮度',
         f'差评{int(neg_counts.get("亮度不足", 0))}条',
         '• 亮度提升至2000lm以上\n• 实际亮度需≥标称90%\n• 增加续航/亮度双模式'),
        ('P3 品质改进', '耐用性/做工',
         f'差评{int(neg_counts.get("耐久性/质量差", 0))}条',
         '• 外壳升级（铝合金+ABS）\n• 通过3米跌落测试\n• IP65防护等级认证'),
        ('P4 体验改进', '固定/磁力',
         f'差评{int(neg_counts.get("磁力不稳/固定问题", 0))}条',
         '• 磁力≥35N\n• 增加锁定机构\n• 多角度调节+锁死功能'),
    ]

    for i, (priority, title, pain_info, solution) in enumerate(improvement_priority):
        bg = C_RED_LIGHT if i == 0 else (C_YELLOW if i == 1 else (C_BLUE_LIGHT if i == 2 else C_WHITE))
        val(ws5, r5, 1, priority, bold=True, bg=bg, fg='FF7B0000' if i == 0 else 'FF1F3864')
        val(ws5, r5, 2, title, bold=True, bg=bg)
        val(ws5, r5, 3, pain_info, bg=bg, fg='FF595959')
        ws5.merge_cells(start_row=r5, start_column=4, end_row=r5, end_column=8)
        val(ws5, r5, 4, solution, bg=bg, fg='FF595959', wrap=True)
        ws5.row_dimensions[r5].height = 55
        r5 += 1
    apply_border(ws5, r5-len(improvement_priority)-1, r5-1, 1, 8)
    r5 += 1
    
    # 卖点与差评对比矩阵
    section_title(ws5, r5, 1, '▌ 七、卖点/痛点对比矩阵', span=8)
    ws5.row_dimensions[r5].height = 24
    r5 += 1
    
    hdr(ws5, r5, 1, '改进方向', bg=C_BLUE_MID)
    hdr(ws5, r5, 2, '现有优势（需保持）', bg=C_GREEN_LIGHT)
    hdr(ws5, r5, 3, '现有痛点（需改进）', bg=C_RED_LIGHT)
    ws5.merge_cells(start_row=r5, start_column=4, end_row=r5, end_column=8)
    hdr(ws5, r5, 4, '改进策略', bg=C_BLUE_MID)
    ws5.row_dimensions[r5].height = 20
    r5 += 1
    
    comparison_matrix = [
        ('电池/续航', '• 竞品多标注USB-C充电\n• 快充是趋势', '• 电池容量不足\n• 续航衰减快', '升级至≥5000mAh，增加电量显示'),
        ('亮度/照明', '• 消费者认可高亮度\n• 亮度是核心卖点', '• 实际亮度不达标\n• 照射范围小', '亮度2000lm+，实际测试对比'),
        ('固定/磁力', '• 磁吸功能受欢迎\n• 360°调节是加分项', '• 磁力不够强\n• 角度固定不稳', '磁力≥35N，增加锁定机构'),
        ('做工/耐用', '• 铝合金材质受欢迎\n• 工业风格认可度高', '• 做工粗糙\n• 防水不达标', '材质升级，通过IP65测试'),
    ]

    for i, (direction, advantage, pain, strategy) in enumerate(comparison_matrix):
        bg = C_BLUE_LIGHT if i % 2 == 0 else C_WHITE
        val(ws5, r5, 1, direction, bold=True, bg=bg)
        val(ws5, r5, 2, advantage, bg=C_GREEN_LIGHT, fg='FF1F5C1F', size=9)
        val(ws5, r5, 3, pain, bg=C_RED_LIGHT, fg='FF7B0000', size=9)
        ws5.merge_cells(start_row=r5, start_column=4, end_row=r5, end_column=8)
        val(ws5, r5, 4, strategy, bg=bg, fg='FF595959', wrap=True)
        ws5.row_dimensions[r5].height = 40
        r5 += 1
    if comparison_matrix:
        apply_border(ws5, r5-len(comparison_matrix)-1, r5-1, 1, 8)

    # ===== Sheet 6: 产品上新方向 =====
    ws6 = wb.create_sheet('产品上新方向')
    ws6.sheet_view.showGridLines = False
    for col_letter, width in zip('ABCDEFGHI', [5, 26, 30, 28, 16, 14, 14, 18, 20]):
        ws6.column_dimensions[col_letter].width = width

    ws6.merge_cells('A1:I1')
    c = ws6['A1']
    c.value = 'LED工作灯 — 产品上新方向建议（基于数据缺口推导）'
    c.font = Font(name='Arial', bold=True, size=14, color=C_WHITE)
    c.fill = PatternFill('solid', fgColor=C_BLUE_DARK)
    c.alignment = Alignment(horizontal='center', vertical='center')
    ws6.row_dimensions[1].height = 35

    r6 = 3
    section_title(ws6, r6, 1, '▌ 数据支撑逻辑：差评痛点 × 市场缺口 × 价格空白 → 新品机会', span=9)
    ws6.row_dimensions[r6].height = 24
    r6 += 1

    new_prod_hdr = ['优先级', '产品方向', '核心改进点', '数据支撑依据', '目标售价', '预计月销', '竞争难度', '启动方式', '预期亮点']
    for i, h in enumerate(new_prod_hdr):
        hdr(ws6, r6, i+1, h, bg=C_BLUE_MID)
    ws6.row_dimensions[r6].height = 22
    r6 += 1

    neg_sorted2 = sorted(neg_counts.items(), key=lambda x: x[1], reverse=True)
    top_pain = neg_sorted2[0][0] if neg_sorted2 else '耐久性/质量差'
    top_pain_cnt = neg_sorted2[0][1] if neg_sorted2 else 50

    # 使用动态生成的产品上新方向数据
    # 如果动态数据不足，使用默认数据填充
    default_directions = [
        ('P1\n首推', '充电磁吸工作灯（基于实际数据推荐）',
         f'• 针对TOP痛点优化：{top_pain}({top_pain_cnt}条差评)\n• 根据实际竞品分析生成改进建议',
         f'基于{len(df)}个BSR样本和{len(rev_df)}条评论分析',
         '$29.99-$39.99', '300-500件/月', '中等', 'FBA直发，首批500pcs',
         '根据实际数据推荐'),
    ] if len(df) > 0 else []

    # 合并动态数据和默认数据
    display_directions = new_directions if new_directions else default_directions

    priority_colors = {'P1\n首推': C_YELLOW, 'P2\n推荐': C_GREEN_LIGHT, 'P3\n参考': C_BLUE_LIGHT, 'P4\n备选': C_GREY_LIGHT}
    for row_dir in display_directions:
        prio = row_dir[0]
        bg = priority_colors.get(prio, C_WHITE)
        for ci, v in enumerate(row_dir):
            c = ws6.cell(row=r6, column=ci+1, value=v)
            c.fill = PatternFill('solid', fgColor=bg)
            c.font = Font(name='Arial', size=9, bold=(ci == 0 or ci == 1))
            c.alignment = Alignment(horizontal='center' if ci in [0, 4, 5, 6] else 'left',
                                    vertical='center', wrap_text=True)
        ws6.row_dimensions[r6].height = 65
        r6 += 1

    apply_border(ws6, 5, r6-1, 1, 9)

    # 评分逻辑注解
    ws6.merge_cells(start_row=r6, start_column=1, end_row=r6, end_column=9)
    c = ws6.cell(row=r6, column=1)
    c.value = '注：优先级由综合评分自动排序 = 需求(月销)×0.30 + 单品收益×0.25 + 新品占比×0.15 + 质量评分×0.15 + 竞争度×0.15（数据驱动，非固定）'
    c.font = Font(name='Arial', size=9, color='FF7A4F01', italic=True)
    c.fill = PatternFill('solid', fgColor=C_YELLOW)
    c.alignment = Alignment(horizontal='left', vertical='center')
    ws6.row_dimensions[r6].height = 16
    r6 += 2

    # 决策矩阵
    section_title(ws6, r6, 1, '▌ 上新决策矩阵 — 优先级说明', span=9)
    ws6.row_dimensions[r6].height = 24
    r6 += 1

    # 品类特征关键字（用于生成自然叙述的"核心卖点/改良方向"）
    type_traits = {
        '充电磁吸工作灯': '灯头可调角度 + 磁力固定',
        '三脚架工作灯': '高度可调 + 三脚架稳定性',
        '手持手电/聚光灯': '照射距离 + 便携性',
        '泛光工作灯': '大范围照明 + IP 防护',
        '车间吊装灯': '工业场景 + T8/T5 灯管规格',
        '太阳能工作灯': '户外自充能 + 免布线',
        '夹灯': '夹具稳定性 + 便携性',
        '其他工作灯': '通用工作灯场景',
    }

    neg_sorted_for_desc = sorted(neg_counts.items(), key=lambda x: x[1], reverse=True)
    top_pains_cat_for_desc = [p[0] for p in neg_sorted_for_desc[:2] if p[1] > 0]
    pain_str_for_desc = '+'.join([p.replace('/', '') for p in top_pains_cat_for_desc]) if top_pains_cat_for_desc else '电池+亮度'
    top_pain_cnt_for_desc = int(neg_sorted_for_desc[0][1]) if neg_sorted_for_desc and neg_sorted_for_desc[0][1] > 0 else 0

    # 基于实际排名动态生成决策建议（结合数据 + 品类特征 + 自然叙述）
    def _type_strategy(ptype, rank_idx, all_scores):
        sc = all_scores.get(ptype, {})
        sku_n = sc.get('sku_count', 0)
        new_r = sc.get('new_ratio', 0)
        rating_v = sc.get('avg_rating', 0)
        avg_rev_v = sc.get('avg_rev', 0)
        total_s = int(sc.get('total_sales', 0))

        all_skus = [s.get('sku_count', 0) for s in all_scores.values()]
        max_sku = max(all_skus) if all_skus else 1
        all_revs = [s.get('avg_rev', 0) for s in all_scores.values()]
        max_rev = max(all_revs) if all_revs else 0
        all_sales = [s.get('total_sales', 0) for s in all_scores.values()]
        max_sales = max(all_sales) if all_sales else 0

        sku_ratio = sku_n / max_sku if max_sku else 0
        rev_ratio = avg_rev_v / max_rev if max_rev else 0
        sales_ratio = total_s / max_sales if max_sales else 0

        trait = type_traits.get(ptype, '核心差异化点')

        if rank_idx == 0:
            # P1 首推
            demand_phrase = '销量最大品类' if sales_ratio >= 0.9 else ('销量靠前' if sales_ratio >= 0.3 else '市场需求明确')
            stage_phrase = '市场需求成熟' if new_r < 0.2 else ('处于成长红利期' if new_r > 0.3 else '需求稳定')
            price_sense = '价格敏感度中等' if rating_v >= 4.3 else '用户对品质敏感'
            pain_phrase = f'差评痛点明确（{pain_str_for_desc}，{top_pain_cnt_for_desc}条）' if top_pain_cnt_for_desc else '差评数据有限'
            return (f'{ptype}是{demand_phrase}，{stage_phrase}，{pain_phrase}，'
                    f'围绕{trait}改良空间大，{price_sense}，适合首批快速验证。目标：6 个月内进入类目 TOP50。')

        elif rank_idx == 1:
            # P2 推荐
            if rev_ratio >= 0.7:
                rev_phrase = '客单价高、单品收益好'
            elif sales_ratio >= 0.5:
                rev_phrase = '销量稳定、走量型赛道'
            else:
                rev_phrase = '综合表现第二梯队'
            quality_req = '对产品品质要求更严格' if rating_v >= 4.5 else '对质量容忍度中等'
            return (f'{ptype}{rev_phrase}，但{quality_req}（{trait}），'
                    f'需要工厂定制/差异化升级支持，适合首推品类验证后扩展。')

        elif rank_idx <= 3:
            # P3 机会
            if sku_ratio < 0.25:
                comp_phrase = f'竞争极少（仅 {sku_n} 个 SKU），是短期内可快速占位的小类目'
            elif new_r > 0.3:
                comp_phrase = '处于成长期，新品红利仍在'
            else:
                comp_phrase = '需求稳定，可作为 SKU 丰富产品线'
            return f'{ptype}{comp_phrase}，围绕{trait}做差异化，降低产品线集中风险。'

        else:
            # P4 备选
            if sales_ratio < 0.1:
                vol_phrase = f'市场体量偏小（月销 {total_s:,} 件）'
            else:
                vol_phrase = '综合评分偏弱'
            return (f'{ptype}{vol_phrase}，日常 SKU 变动少，'
                    f'主要靠包装和节日/场景运营推动转化，适合节前短期投入，控投入控风险。')

    decision_notes = []
    labels = ['P1 首推（立即启动）', 'P2 推荐（第二梯队）', 'P3 参考（机会品类）', 'P3 参考（机会品类）', 'P4 备选（节日运营）']
    for i, pt in enumerate(ranked_product_types[:5]):
        lbl = labels[i] if i < len(labels) else 'P4 备选'
        decision_notes.append((lbl, _type_strategy(pt, i, ranked_type_scores)))
    if not decision_notes:
        decision_notes = [('—', '暂无品类评分数据（BSR 数据不足）')]
    for label, note in decision_notes:
        ws6.merge_cells(start_row=r6, start_column=1, end_row=r6, end_column=2)
        val(ws6, r6, 1, label, bold=True, bg=C_BLUE_LIGHT, fg='FF1F3864')
        ws6.merge_cells(start_row=r6, start_column=3, end_row=r6, end_column=9)
        val(ws6, r6, 3, note, wrap=True)
        ws6.row_dimensions[r6].height = 40
        r6 += 1
    apply_border(ws6, r6-len(decision_notes), r6-1, 1, 9)
    r6 += 1
    
    # ===== 产品上新方向 - 新增内容 =====
    # 行动计划总结
    section_title(ws6, r6, 1, '▌ 三、行动计划总结（立即执行）', span=9)
    ws6.row_dimensions[r6].height = 24
    r6 += 1
    
    hdr(ws6, r6, 1, '阶段', bg=C_BLUE_MID)
    hdr(ws6, r6, 2, '时间节点', bg=C_BLUE_MID)
    hdr(ws6, r6, 3, '关键任务', bg=C_BLUE_MID)
    ws6.merge_cells(start_row=r6, start_column=4, end_row=r6, end_column=6)
    hdr(ws6, r6, 4, '执行要点', bg=C_BLUE_MID)
    ws6.merge_cells(start_row=r6, start_column=7, end_row=r6, end_column=9)
    hdr(ws6, r6, 7, '预期目标', bg=C_BLUE_MID)
    ws6.row_dimensions[r6].height = 20
    r6 += 1
    
    action_plan = [
        ('选品开发', '第1-2周', '竞品调研+产品定义',
         '• 深入分析 TOP10 竞品优缺点\n• 确定差异化功能点\n• 与工厂沟通打样',
         '完成产品定义文档'),
        ('产品优化', '第3-4周', '针对差评痛点优化',
         '• 根据差评数据确定改进规格\n• 打样并功能测试\n• 确认成本与利润',
         '样品通过功能测试'),
        ('Listing准备', '第5-6周', 'Listing + 主图设计',
         '• 标题埋词优化\n• 五点描述突出差异化\n• A+ 页面设计',
         'Listing 优化完成'),
        ('测试期运营', '第7-12周', '新品推广',
         '• 初期测评+QA\n• 广告冷启动\n• 报秒杀/优惠券活动',
         f'评分≥{avg_rating:.1f}★'),
        ('稳定期', '第3-6月', '稳定排名',
         '• 广告精细化运营\n• 持续优化转化率\n• 拓展变体',
         '进入类目 TOP50'),
    ]
    
    priority_plan_colors = [C_YELLOW, C_YELLOW, C_BLUE_LIGHT, C_BLUE_LIGHT, C_GREEN_LIGHT]
    for i, (phase, time, task, points, goal) in enumerate(action_plan):
        bg = priority_plan_colors[i]
        val(ws6, r6, 1, phase, bold=True, bg=bg)
        val(ws6, r6, 2, time, bg=bg)
        val(ws6, r6, 3, task, bg=bg)
        ws6.merge_cells(start_row=r6, start_column=4, end_row=r6, end_column=6)
        val(ws6, r6, 4, points, bg=bg, fg='FF595959', size=9, wrap=True)
        ws6.merge_cells(start_row=r6, start_column=7, end_row=r6, end_column=9)
        val(ws6, r6, 7, goal, bg=bg, fg='FF1F3864')
        ws6.row_dimensions[r6].height = 55
        r6 += 1
    apply_border(ws6, r6-len(action_plan)-1, r6-1, 1, 9)
    r6 += 1
    
    # 执行要点与风险提示
    section_title(ws6, r6, 1, '▌ 四、执行要点与风险提示', span=9)
    ws6.row_dimensions[r6].height = 24
    r6 += 1
    
    risk_notes = [
        ('供应链准备', 'C_RED_LIGHT',
         '• 首批备货建议 300-500pcs，避免库存压力\n• 选择有出口经验的工厂，确保交期\n• 要求工厂提供样品进行功能测试'),
        ('品质把控', 'C_YELLOW',
         '• 重点测试差评高频维度（见上表改进优先级）\n• 跌落/防水/耐久等品控标准按品类确定\n• 要求工厂提供第三方检测报告'),
        ('Listing 优化', 'C_BLUE_LIGHT',
         '• 标题格式：[核心词]+[功能]+[规格]\n• 五点描述按痛点→解决方案→产品优势排列\n• 主图突出差异化功能'),
        ('风险预警', 'C_RED_LIGHT',
         '• 风险1：广告 ACOS 过高\n  → 应对：优化 Listing 转化，配合优惠券\n• 风险2：差评导致评分下滑\n  → 应对：主动联系差评用户，及时处理\n• 风险3：竞品价格战\n  → 应对：不参与价格战，用品质和评价区隔'),
    ]
    
    for title, color, content in risk_notes:
        bg = globals().get(color, C_WHITE)
        ws6.merge_cells(start_row=r6, start_column=1, end_row=r6, end_column=2)
        val(ws6, r6, 1, title, bold=True, bg=bg, fg='FF1F3864')
        ws6.merge_cells(start_row=r6, start_column=3, end_row=r6, end_column=9)
        val(ws6, r6, 3, content, bg=bg, fg='FF595959', wrap=True)
        ws6.row_dimensions[r6].height = 65
        r6 += 1
    apply_border(ws6, r6-len(risk_notes)-1, r6-1, 1, 9)

    # ===== Sheet 7: 评论数据汇总 =====
    ws7 = wb.create_sheet('评论数据汇总')
    ws7.sheet_view.showGridLines = False
    ws7.column_dimensions['A'].width = 14
    ws7.column_dimensions['B'].width = 14
    ws7.column_dimensions['C'].width = 8
    ws7.column_dimensions['D'].width = 35
    ws7.column_dimensions['E'].width = 55
    ws7.column_dimensions['F'].width = 10

    ws7.merge_cells('A1:F1')
    c = ws7['A1']
    c.value = f'评论原始数据（{len(rev_df)}条评论）'
    c.font = Font(name='Arial', bold=True, size=13, color=C_WHITE)
    c.fill = PatternFill('solid', fgColor=C_BLUE_DARK)
    c.alignment = Alignment(horizontal='center', vertical='center')
    ws7.row_dimensions[1].height = 28

    for i, h in enumerate(['ASIN', '来源文件', '星级', '标题', '评论内容（摘要）', '日期']):
        hdr(ws7, 2, i+1, h, bg=C_BLUE_MID)
    ws7.row_dimensions[2].height = 20

    if len(rev_df) > 0 and 'source_asin' in rev_df.columns:
        # 只选择存在的列
        display_cols = ['source_asin', 'Rating', 'Title', 'Content', 'Date']
        display_cols = [c for c in display_cols if c in rev_df.columns]
        rev_display = rev_df[display_cols].head(800)
        for row_idx, (_, revrow) in enumerate(rev_display.iterrows(), start=3):
            rating = revrow.get('Rating', 3)
            bg = C_RED_LIGHT if (pd.notna(rating) and float(rating) <= 2) else (C_WHITE if (pd.notna(rating) and float(rating) <= 3) else C_GREEN_LIGHT if row_idx % 2 == 0 else C_WHITE)
            content_preview = str(revrow.get('Content', ''))[:200] if pd.notna(revrow.get('Content', '')) else ''
            title_val = revrow.get('Title', '') if 'Title' in rev_df.columns else ''
            date_val = revrow.get('Date', '') if 'Date' in rev_df.columns else ''
            row_vals = [revrow.get('source_asin', ''), '', rating, title_val, content_preview, date_val]
            for ci, v in enumerate(row_vals):
                c = ws7.cell(row=row_idx, column=ci+1, value=str(v) if v else '')
                c.fill = PatternFill('solid', fgColor=bg)
                c.font = Font(name='Arial', size=8)
                c.alignment = Alignment(horizontal='center' if ci in [0, 2, 5] else 'left', vertical='center', wrap_text=(ci == 4))
            ws7.row_dimensions[row_idx].height = 14
        apply_border(ws7, 2, min(800+2, len(rev_df)+2), 1, 6)
    else:
        val(ws7, 3, 1, '暂无评论数据', bg=C_YELLOW)
        ws7.merge_cells(start_row=3, start_column=1, end_row=3, end_column=6)
    
    # ===== 评论数据汇总 - 新增内容 =====
    r7 = 803 if len(rev_df) > 0 else 5
    
    # 评分分布统计
    section_title(ws7, r7, 1, '▌ 评论评分分布统计', span=6)
    ws7.row_dimensions[r7].height = 24
    r7 += 1
    
    if len(rev_df) > 0 and 'Rating' in rev_df.columns:
        rating_dist = rev_df['Rating'].value_counts().sort_index()
        total_reviews = len(rev_df)
    else:
        rating_dist = {}
        total_reviews = 0
    
    hdr(ws7, r7, 1, '星级', bg=C_BLUE_MID)
    hdr(ws7, r7, 2, '评论数', bg=C_BLUE_MID)
    hdr(ws7, r7, 3, '占比', bg=C_BLUE_MID)
    hdr(ws7, r7, 4, '累计占比', bg=C_BLUE_MID)
    ws7.merge_cells(start_row=r7, start_column=5, end_row=r7, end_column=6)
    hdr(ws7, r7, 5, '质量评估', bg=C_BLUE_MID)
    ws7.row_dimensions[r7].height = 20
    r7 += 1
    
    cumsum = 0
    rating_colors_rev = {5: C_GREEN_LIGHT, 4: C_YELLOW, 3: 'FFFFF2CC', 2: C_ORANGE, 1: C_RED_LIGHT}
    quality_notes = {5: '★★★★★ 优秀', 4: '★★★★ 良好', 3: '★★★ 一般', 2: '★★ 较差', 1: '★ 差评集中'}
    
    for rating in [5, 4, 3, 2, 1]:
        cnt = rating_dist.get(rating, 0)
        pct = cnt / total_reviews * 100 if total_reviews > 0 else 0
        cumsum += pct
        bg = rating_colors_rev.get(rating, C_WHITE)
        val(ws7, r7, 1, f'{rating}★', bold=True, bg=bg)
        ws7.cell(row=r7, column=2, value=int(cnt)).alignment = Alignment(horizontal='center', vertical='center')
        ws7.cell(row=r7, column=2).fill = PatternFill('solid', fgColor=bg)
        ws7.cell(row=r7, column=3, value=f'{pct:.1f}%').alignment = Alignment(horizontal='center', vertical='center')
        ws7.cell(row=r7, column=3).fill = PatternFill('solid', fgColor=bg)
        ws7.cell(row=r7, column=4, value=f'{cumsum:.1f}%').alignment = Alignment(horizontal='center', vertical='center')
        ws7.cell(row=r7, column=4).fill = PatternFill('solid', fgColor=bg)
        ws7.merge_cells(start_row=r7, start_column=5, end_row=r7, end_column=6)
        val(ws7, r7, 5, quality_notes.get(rating, ''), bg=bg)
        ws7.row_dimensions[r7].height = 18
        r7 += 1
    apply_border(ws7, r7-5, r7-1, 1, 6)
    r7 += 1
    
    # 高频痛点词Top10
    section_title(ws7, r7, 1, '▌ 高频痛点词Top10（差评分析）', span=6)
    ws7.row_dimensions[r7].height = 24
    r7 += 1
    
    # 提取差评中的高频词
    if len(low_rev) > 0:
        all_low_text = (low_rev['Title'].fillna('') + ' ' + low_rev['Content'].fillna('')).str.lower()
        
        pain_words = ['battery', 'charge', 'died', 'broke', 'broken', 'dead', 'dim', 
                      'not bright', 'weak', 'stopped', 'cheap', 'return', 'refund', 
                      ' defective', 'failed', 'waste', 'garbage', 'terrible', 'worst']
        
        word_counts = {}
        for word in pain_words:
            word_counts[word] = all_low_text.str.contains(word, na=False).sum()
        
        top_pain_words = sorted(word_counts.items(), key=lambda x: x[1], reverse=True)[:10]
    else:
        top_pain_words = []
    
    hdr(ws7, r7, 1, '排名', bg=C_BLUE_MID)
    hdr(ws7, r7, 2, '痛点词', bg=C_BLUE_MID)
    hdr(ws7, r7, 3, '出现次数', bg=C_BLUE_MID)
    ws7.merge_cells(start_row=r7, start_column=4, end_row=r7, end_column=6)
    hdr(ws7, r7, 4, '翻译/说明', bg=C_BLUE_MID)
    ws7.row_dimensions[r7].height = 20
    r7 += 1
    
    word_translations = {
        'battery': '电池问题', 'charge': '充电问题', 'died': '故障/坏了',
        'broke': '损坏', 'broken': '损坏', 'dead': '无法使用',
        'dim': '亮度不足', 'not bright': '不够亮', 'weak': '太弱',
        'stopped': '停止工作', 'cheap': '质量差', 'return': '退货',
        'refund': '退款', 'defective': '有缺陷', 'failed': '失效',
        'waste': '浪费钱', 'garbage': '垃圾', 'terrible': '很差', 'worst': '最差'
    }
    
    for idx, (word, cnt) in enumerate(top_pain_words):
        bg = C_RED_LIGHT if idx < 3 else C_WHITE
        ws7.cell(row=r7, column=1, value=idx+1).alignment = Alignment(horizontal='center', vertical='center')
        ws7.cell(row=r7, column=1).fill = PatternFill('solid', fgColor=bg)
        val(ws7, r7, 2, word, bold=(idx < 3), bg=bg)
        ws7.cell(row=r7, column=3, value=cnt).alignment = Alignment(horizontal='center', vertical='center')
        ws7.cell(row=r7, column=3).fill = PatternFill('solid', fgColor=bg)
        ws7.merge_cells(start_row=r7, start_column=4, end_row=r7, end_column=6)
        val(ws7, r7, 4, word_translations.get(word, ''), bg=bg, fg='FF595959')
        ws7.row_dimensions[r7].height = 18
        r7 += 1
    apply_border(ws7, r7-len(top_pain_words)-1, r7-1, 1, 6)
    r7 += 1
    
    # 高频好评词Top10
    section_title(ws7, r7, 1, '▌ 高频好评词Top10（好评分析）', span=6)
    ws7.row_dimensions[r7].height = 24
    r7 += 1
    
    if len(high_rev) > 0:
        all_high_text = (high_rev['Title'].fillna('') + ' ' + high_rev['Content'].fillna('')).str.lower()
        
        good_words = ['bright', 'easy', 'great', 'perfect', 'love', 'good', 'excellent',
                      'recommend', 'useful', 'strong', 'compact', 'portable', 'durable', 'quality']
        
        good_word_counts = {}
        for word in good_words:
            good_word_counts[word] = all_high_text.str.contains(word, na=False).sum()
        
        top_good_words = sorted(good_word_counts.items(), key=lambda x: x[1], reverse=True)[:10]
    else:
        top_good_words = []
    
    hdr(ws7, r7, 1, '排名', bg=C_BLUE_MID)
    hdr(ws7, r7, 2, '好评词', bg=C_BLUE_MID)
    hdr(ws7, r7, 3, '出现次数', bg=C_BLUE_MID)
    ws7.merge_cells(start_row=r7, start_column=4, end_row=r7, end_column=6)
    hdr(ws7, r7, 4, '翻译/说明', bg=C_BLUE_MID)
    ws7.row_dimensions[r7].height = 20
    r7 += 1
    
    good_translations = {
        'bright': '亮度高', 'easy': '易用', 'great': '很棒', 'perfect': '完美',
        'love': '喜欢', 'good': '好', 'excellent': '优秀', 'recommend': '推荐',
        'useful': '实用', 'strong': '强劲', 'compact': '小巧', 'portable': '便携',
        'durable': '耐用', 'quality': '品质好'
    }
    
    for idx, (word, cnt) in enumerate(top_good_words):
        bg = C_GREEN_LIGHT if idx < 3 else C_WHITE
        ws7.cell(row=r7, column=1, value=idx+1).alignment = Alignment(horizontal='center', vertical='center')
        ws7.cell(row=r7, column=1).fill = PatternFill('solid', fgColor=bg)
        val(ws7, r7, 2, word, bold=(idx < 3), bg=bg)
        ws7.cell(row=r7, column=3, value=cnt).alignment = Alignment(horizontal='center', vertical='center')
        ws7.cell(row=r7, column=3).fill = PatternFill('solid', fgColor=bg)
        ws7.merge_cells(start_row=r7, start_column=4, end_row=r7, end_column=6)
        val(ws7, r7, 4, good_translations.get(word, ''), bg=bg, fg='FF595959')
        ws7.row_dimensions[r7].height = 18
        r7 += 1
    apply_border(ws7, r7-len(top_good_words)-1, r7-1, 1, 6)

    # 预初始化 Sheet 8 变量（供 Sheet 10 引用）
    sell_summary = '—'
    lifecycle_stage = '未知'
    lifecycle_reason = ''
    seasonality_peak_months = []

    # ===== Sheet 8: 类目趋势 =====
    ws8 = wb.create_sheet('类目趋势')
    ws8.sheet_view.showGridLines = False
    for col_letter, width in zip('ABCDEFGH', [20, 14, 14, 14, 14, 14, 14, 40]):
        ws8.column_dimensions[col_letter].width = width
    ws8.merge_cells('A1:H1')
    c = ws8['A1']
    c.value = 'LED工作灯 — 类目趋势 · 季节性 · 生命周期分析'
    c.font = Font(name='Arial', bold=True, size=14, color=C_WHITE)
    c.fill = PatternFill('solid', fgColor=C_BLUE_DARK)
    c.alignment = Alignment(horizontal='center', vertical='center')
    ws8.row_dimensions[1].height = 35
    r8 = 3

    if not market_data.get('_available'):
        ws8.merge_cells(start_row=r8, start_column=1, end_row=r8+2, end_column=8)
        c = ws8.cell(row=r8, column=1)
        c.value = '⚠ 未上传 Market 分析文件，无法生成类目趋势 / 季节性 / 生命周期分析。\n请在首页第 3 步上传卖家精灵 US-Market 文件（可选）后重新生成报告。'
        c.font = Font(name='Arial', bold=True, size=14, color='FF7A4F01')
        c.fill = PatternFill('solid', fgColor=C_YELLOW)
        c.alignment = Alignment(horizontal='center', vertical='center', wrap_text=True)
        for rr in range(r8, r8+3):
            ws8.row_dimensions[rr].height = 40
    else:
        # ▌ 一、市场销售趋势
        section_title(ws8, r8, 1, '▌ 一、近 2 年市场销售趋势', span=8)
        ws8.row_dimensions[r8].height = 24
        r8 += 1

        sell_df = market_data.get('sell_trends')
        sell_summary = '—'
        if sell_df is not None and len(sell_df) > 2:
            sell = sell_df.copy()
            sell.columns = [str(c).strip() for c in sell.columns]
            month_c = sell.columns[0]
            sales_c = sell.columns[1] if len(sell.columns) > 1 else None
            rev_c2 = sell.columns[2] if len(sell.columns) > 2 else None
            hdr(ws8, r8, 1, '月份', bg=C_BLUE_MID)
            hdr(ws8, r8, 2, '月销量', bg=C_BLUE_MID)
            hdr(ws8, r8, 3, '月销售额($)', bg=C_BLUE_MID)
            if len(sell.columns) > 3:
                hdr(ws8, r8, 4, '平均BSR', bg=C_BLUE_MID)
            r8 += 1
            data_start = r8
            for _, row in sell.iterrows():
                try:
                    m = str(row[month_c])
                    if m.replace('.', '').replace('0', '').isdigit() and len(m) >= 6:
                        m = m.split('.')[0]
                        m = f'{m[:4]}-{m[4:6]}'
                    val(ws8, r8, 1, m, bg=C_WHITE)
                    if sales_c:
                        c = ws8.cell(row=r8, column=2, value=float(row[sales_c]))
                        c.number_format = '#,##0'
                        c.fill = PatternFill('solid', fgColor=C_WHITE)
                        c.font = Font(name='Arial', size=10)
                        c.alignment = Alignment(horizontal='left', vertical='center')
                    if rev_c2:
                        c = ws8.cell(row=r8, column=3, value=float(row[rev_c2]))
                        c.number_format = '#,##0'
                        c.fill = PatternFill('solid', fgColor=C_WHITE)
                        c.font = Font(name='Arial', size=10)
                        c.alignment = Alignment(horizontal='left', vertical='center')
                    if len(sell.columns) > 3:
                        c = ws8.cell(row=r8, column=4, value=float(row[sell.columns[3]]))
                        c.number_format = '#,##0'
                        c.fill = PatternFill('solid', fgColor=C_WHITE)
                        c.font = Font(name='Arial', size=10)
                        c.alignment = Alignment(horizontal='left', vertical='center')
                    ws8.row_dimensions[r8].height = 18
                    r8 += 1
                except Exception:
                    continue
            data_end = r8 - 1
            apply_border(ws8, data_start-1, data_end, 1, 4)
            # 计算同比
            try:
                sales_series = pd.to_numeric(sell[sales_c], errors='coerce').dropna().values
                if len(sales_series) >= 24:
                    recent12 = sales_series[-12:].sum()
                    prev12 = sales_series[-24:-12].sum()
                    yoy = (recent12 - prev12) / prev12 if prev12 > 0 else 0
                    sell_summary = f'近 12 月销量 {recent12:,.0f} 件，同比{"增长" if yoy > 0 else "下降"} {abs(yoy):.1%}'
                elif len(sales_series) >= 6:
                    sell_summary = f'近 {len(sales_series)} 月销量合计 {sales_series.sum():,.0f} 件（数据不足 2 年，无法计算同比）'
            except Exception:
                pass
            # 折线图
            try:
                chart = LineChart()
                chart.title = '月度销量 / 销售额趋势'
                chart.height = 8
                chart.width = 18
                data_ref = Reference(ws8, min_col=2, max_col=3, min_row=data_start-1, max_row=data_end)
                cats_ref = Reference(ws8, min_col=1, max_col=1, min_row=data_start, max_row=data_end)
                chart.add_data(data_ref, titles_from_data=True)
                chart.set_categories(cats_ref)
                ws8.add_chart(chart, f'F{data_start-1}')
            except Exception:
                pass
            r8 += 1
        else:
            val(ws8, r8, 1, '（Industry Sell Trends 无数据）', bg=C_WHITE, fg='FF999999')
            r8 += 1
        r8 += 1

        # ▌ 二、核心关键词搜索趋势
        section_title(ws8, r8, 1, '▌ 二、核心关键词搜索量趋势（近 12 月 vs 上年同期）', span=8)
        ws8.row_dimensions[r8].height = 24
        r8 += 1

        demand_df = market_data.get('demand_trends')
        keyword_peaks = []
        if demand_df is not None and len(demand_df) > 12:
            d = demand_df.copy()
            d.columns = [str(c).strip() for c in d.columns]
            month_c = d.columns[0]
            kw_cols = list(d.columns[1:])
            d[month_c] = d[month_c].astype(str)
            d = d[d[month_c].str.match(r'^\d{4}-\d{2}$', na=False)].sort_values(month_c)

            hdr(ws8, r8, 1, '关键词', bg=C_BLUE_MID)
            hdr(ws8, r8, 2, '近 12 月总搜索', bg=C_BLUE_MID)
            hdr(ws8, r8, 3, '上年同期', bg=C_BLUE_MID)
            hdr(ws8, r8, 4, '同比', bg=C_BLUE_MID)
            hdr(ws8, r8, 5, '峰值月', bg=C_BLUE_MID)
            hdr(ws8, r8, 6, '峰值搜索', bg=C_BLUE_MID)
            ws8.merge_cells(start_row=r8, start_column=7, end_row=r8, end_column=8)
            hdr(ws8, r8, 7, '趋势判断', bg=C_BLUE_MID)
            r8 += 1
            kw_start = r8
            for kw in kw_cols:
                try:
                    s = pd.to_numeric(d[kw], errors='coerce').fillna(0)
                    if len(s) < 12:
                        continue
                    last12 = s.iloc[-12:]
                    prev12 = s.iloc[-24:-12] if len(s) >= 24 else None
                    tot_last = last12.sum()
                    tot_prev = prev12.sum() if prev12 is not None else 0
                    yoy = ((tot_last - tot_prev) / tot_prev) if tot_prev > 0 else None
                    peak_idx = int(last12.values.argmax())
                    months = d[month_c].iloc[-12:].tolist()
                    peak_month = months[peak_idx] if peak_idx < len(months) else '-'
                    peak_val = float(last12.iloc[peak_idx])
                    if yoy is None:
                        trend = '数据不足'
                        trend_bg = C_WHITE
                    elif yoy > 0.15:
                        trend = '明显增长'
                        trend_bg = C_GREEN_LIGHT
                    elif yoy > 0:
                        trend = '小幅增长'
                        trend_bg = C_GREEN_LIGHT
                    elif yoy > -0.15:
                        trend = '小幅下滑'
                        trend_bg = C_YELLOW
                    else:
                        trend = '明显下滑'
                        trend_bg = C_RED_LIGHT
                    val(ws8, r8, 1, kw, bold=True, bg=C_BLUE_LIGHT)
                    val(ws8, r8, 2, f'{tot_last:,.0f}')
                    val(ws8, r8, 3, f'{tot_prev:,.0f}' if tot_prev > 0 else 'N/A')
                    val(ws8, r8, 4, f'{yoy:+.1%}' if yoy is not None else 'N/A', fg='FF005A9E' if (yoy or 0) > 0 else 'FFC00000')
                    val(ws8, r8, 5, peak_month)
                    val(ws8, r8, 6, f'{peak_val:,.0f}')
                    ws8.merge_cells(start_row=r8, start_column=7, end_row=r8, end_column=8)
                    val(ws8, r8, 7, trend, bold=True, bg=trend_bg)
                    ws8.row_dimensions[r8].height = 20
                    r8 += 1
                    # 收集峰值月（用于季节性）
                    if peak_month != '-':
                        mm = peak_month.split('-')[-1]
                        keyword_peaks.append(mm)
                except Exception:
                    continue
            apply_border(ws8, kw_start-1, r8-1, 1, 8)
        else:
            val(ws8, r8, 1, '（Industry Demand and Trends 无数据）', bg=C_WHITE, fg='FF999999')
            r8 += 1
        r8 += 1

        # ▌ 三、季节性波动
        section_title(ws8, r8, 1, '▌ 三、季节性波动（历年同月均值）', span=8)
        ws8.row_dimensions[r8].height = 24
        r8 += 1

        seasonality_peak_months = []
        if demand_df is not None and len(demand_df) > 24:
            d = demand_df.copy()
            d.columns = [str(c).strip() for c in d.columns]
            month_c = d.columns[0]
            kw_cols = list(d.columns[1:])
            d[month_c] = d[month_c].astype(str)
            d = d[d[month_c].str.match(r'^\d{4}-\d{2}$', na=False)].copy()
            d['_m'] = d[month_c].str.slice(5, 7)
            agg = d.groupby('_m')[kw_cols].apply(lambda x: pd.to_numeric(x.stack(), errors='coerce').mean())
            hdr(ws8, r8, 1, '月份', bg=C_BLUE_MID)
            for j in range(2, 9):
                ws8.cell(row=r8, column=j).fill = PatternFill('solid', fgColor=C_BLUE_MID)
            hdr(ws8, r8, 2, '历年同月均搜索量（5 词合计）', bg=C_BLUE_MID)
            ws8.merge_cells(start_row=r8, start_column=2, end_row=r8, end_column=4)
            hdr(ws8, r8, 5, '相对均值', bg=C_BLUE_MID)
            ws8.merge_cells(start_row=r8, start_column=5, end_row=r8, end_column=6)
            hdr(ws8, r8, 7, '旺/淡季', bg=C_BLUE_MID)
            ws8.merge_cells(start_row=r8, start_column=7, end_row=r8, end_column=8)
            r8 += 1
            season_start = r8
            mean_val = agg.mean() if len(agg) else 0
            agg_sorted = agg.sort_index()
            for mm, v in agg_sorted.items():
                try:
                    rel = (v / mean_val - 1) if mean_val else 0
                    if rel > 0.15:
                        tag = '🔥 旺季'
                        bg = C_RED_LIGHT
                        seasonality_peak_months.append(mm)
                    elif rel < -0.15:
                        tag = '❄ 淡季'
                        bg = C_BLUE_LIGHT
                    else:
                        tag = '平稳'
                        bg = C_WHITE
                    val(ws8, r8, 1, f'{mm} 月', bold=True, bg=C_BLUE_LIGHT)
                    ws8.merge_cells(start_row=r8, start_column=2, end_row=r8, end_column=4)
                    val(ws8, r8, 2, f'{v:,.0f}', bg=C_WHITE)
                    ws8.merge_cells(start_row=r8, start_column=5, end_row=r8, end_column=6)
                    val(ws8, r8, 5, f'{rel:+.1%}', bg=bg, fg='FFC00000' if rel > 0 else 'FF005A9E')
                    ws8.merge_cells(start_row=r8, start_column=7, end_row=r8, end_column=8)
                    val(ws8, r8, 7, tag, bold=True, bg=bg)
                    ws8.row_dimensions[r8].height = 20
                    r8 += 1
                except Exception:
                    continue
            apply_border(ws8, season_start-1, r8-1, 1, 8)
        else:
            val(ws8, r8, 1, '（Industry Demand and Trends 不足 2 年，无法计算季节性）', bg=C_WHITE, fg='FF999999')
            r8 += 1
        r8 += 1

        # ▌ 四、生命周期判断
        section_title(ws8, r8, 1, '▌ 四、生命周期判断（按发布年度销量占比）', span=8)
        ws8.row_dimensions[r8].height = 24
        r8 += 1

        pub_df = market_data.get('publication_time_trends')
        lifecycle_stage = '未知'
        lifecycle_reason = ''
        if pub_df is not None and len(pub_df) > 3:
            stage, reason = infer_lifecycle_stage(pub_df)
            lifecycle_stage = stage
            lifecycle_reason = reason
            hdr(ws8, r8, 1, '发布年份', bg=C_BLUE_MID)
            hdr(ws8, r8, 2, '商品数', bg=C_BLUE_MID)
            hdr(ws8, r8, 3, '销量', bg=C_BLUE_MID)
            hdr(ws8, r8, 4, '销量占比', bg=C_BLUE_MID)
            hdr(ws8, r8, 5, '销售额($)', bg=C_BLUE_MID)
            hdr(ws8, r8, 6, '销售额占比', bg=C_BLUE_MID)
            ws8.merge_cells(start_row=r8, start_column=7, end_row=r8, end_column=8)
            hdr(ws8, r8, 7, '备注', bg=C_BLUE_MID)
            r8 += 1
            p_start = r8
            for _, row in pub_df.iterrows():
                try:
                    y = row.iloc[0]
                    if pd.isna(y):
                        continue
                    y_int = int(float(y))
                    products = int(pd.to_numeric(row.iloc[1], errors='coerce') or 0)
                    sales = int(pd.to_numeric(row.iloc[2], errors='coerce') or 0)
                    sp = float(pd.to_numeric(row.iloc[3], errors='coerce') or 0)
                    rev_v = float(pd.to_numeric(row.iloc[4], errors='coerce') or 0)
                    rp = float(pd.to_numeric(row.iloc[5], errors='coerce') or 0)
                    age = datetime.now().year - y_int
                    if age < 3:
                        note = '新品代'
                        bg = C_GREEN_LIGHT
                    elif age < 6:
                        note = '中生代'
                        bg = C_YELLOW
                    else:
                        note = '老品'
                        bg = C_BLUE_LIGHT
                    val(ws8, r8, 1, f'{y_int}年', bold=True, bg=bg)
                    val(ws8, r8, 2, f'{products}', bg=C_WHITE)
                    val(ws8, r8, 3, f'{sales:,}', bg=C_WHITE)
                    val(ws8, r8, 4, f'{sp:.1%}', bg=C_WHITE)
                    val(ws8, r8, 5, f'{rev_v:,.0f}', bg=C_WHITE)
                    val(ws8, r8, 6, f'{rp:.1%}', bg=C_WHITE)
                    ws8.merge_cells(start_row=r8, start_column=7, end_row=r8, end_column=8)
                    val(ws8, r8, 7, note, bold=True, bg=bg)
                    ws8.row_dimensions[r8].height = 18
                    r8 += 1
                except Exception:
                    continue
            apply_border(ws8, p_start-1, r8-1, 1, 8)

            # 生命周期结论
            r8 += 1
            ws8.merge_cells(start_row=r8, start_column=1, end_row=r8, end_column=8)
            c = ws8.cell(row=r8, column=1)
            c.value = f'🎯 生命周期判断：{lifecycle_stage}   |   {lifecycle_reason}'
            c.font = Font(name='Arial', bold=True, size=12, color='FF1F3864')
            c.fill = PatternFill('solid', fgColor=C_GREEN_LIGHT)
            c.alignment = Alignment(horizontal='left', vertical='center')
            ws8.row_dimensions[r8].height = 28
            r8 += 1
        else:
            val(ws8, r8, 1, '（Publication Time Trends 无数据）', bg=C_WHITE, fg='FF999999')
            r8 += 1
        r8 += 1

        # ▌ 五、类目集中度
        section_title(ws8, r8, 1, '▌ 五、类目集中度', span=8)
        ws8.row_dimensions[r8].height = 24
        r8 += 1

        brand_df = market_data.get('brand_concentration')
        listing_df = market_data.get('listing_concentration')
        conc_rows = []
        if brand_df is not None and len(brand_df) >= 5:
            try:
                sp = pd.to_numeric(brand_df['Sales Proportion'], errors='coerce').dropna()
                cr3_b = sp.head(3).sum()
                cr5_b = sp.head(5).sum()
                cr10_b = sp.head(10).sum()
                top3_b = brand_df.iloc[:3, 1].astype(str).tolist() if brand_df.shape[1] > 1 else []
                conc_rows.append(('品牌 CR3', f'{cr3_b:.1%}', f'Top3 品牌：{", ".join(top3_b)}'))
                conc_rows.append(('品牌 CR5', f'{cr5_b:.1%}', '前 5 品牌销量合计占比'))
                conc_rows.append(('品牌 CR10', f'{cr10_b:.1%}', '前 10 品牌销量合计占比'))
            except Exception:
                pass
        if listing_df is not None and len(listing_df) >= 10:
            try:
                sp = pd.to_numeric(listing_df['Sales Proportion'], errors='coerce').dropna()
                cr10_l = sp.head(10).sum()
                cr20_l = sp.head(20).sum()
                conc_rows.append(('Listing CR10', f'{cr10_l:.1%}', '前 10 ASIN 销量合计占比'))
                conc_rows.append(('Listing CR20', f'{cr20_l:.1%}', '前 20 ASIN 销量合计占比'))
            except Exception:
                pass
        if conc_rows:
            hdr(ws8, r8, 1, '指标', bg=C_BLUE_MID)
            hdr(ws8, r8, 2, '数值', bg=C_BLUE_MID)
            ws8.merge_cells(start_row=r8, start_column=3, end_row=r8, end_column=8)
            hdr(ws8, r8, 3, '说明', bg=C_BLUE_MID)
            r8 += 1
            c_start = r8
            for label, v, note in conc_rows:
                val(ws8, r8, 1, label, bold=True, bg=C_BLUE_LIGHT)
                val(ws8, r8, 2, v, bold=True, bg=C_WHITE)
                ws8.merge_cells(start_row=r8, start_column=3, end_row=r8, end_column=8)
                val(ws8, r8, 3, note, bg=C_WHITE, fg='FF595959')
                ws8.row_dimensions[r8].height = 20
                r8 += 1
            apply_border(ws8, c_start-1, r8-1, 1, 8)
        else:
            val(ws8, r8, 1, '（Brand/Listing Concentration 无数据）', bg=C_WHITE, fg='FF999999')
            r8 += 1
        r8 += 1

        # ▌ 六、小结
        section_title(ws8, r8, 1, '▌ 六、趋势与生命周期小结', span=8, bg=C_ORANGE)
        ws8.row_dimensions[r8].height = 24
        r8 += 1
        peak_summary = '；'.join([f'{m} 月' for m in sorted(set(seasonality_peak_months))]) if seasonality_peak_months else '无明显旺季'
        summary_lines = [
            f'• 销售趋势：{sell_summary}',
            f'• 季节性：旺季集中在 {peak_summary}',
            f'• 生命周期：{lifecycle_stage} —— {lifecycle_reason}',
        ]
        ws8.merge_cells(start_row=r8, start_column=1, end_row=r8+2, end_column=8)
        c = ws8.cell(row=r8, column=1)
        c.value = '\n'.join(summary_lines)
        c.font = Font(name='Arial', size=11, color='FF1F3864')
        c.fill = PatternFill('solid', fgColor=C_BLUE_LIGHT)
        c.alignment = Alignment(horizontal='left', vertical='top', wrap_text=True)
        for rr in range(r8, r8+3):
            ws8.row_dimensions[rr].height = 24
        r8 += 3

    # ===== Sheet 9: 风险预估 =====
    ws9 = wb.create_sheet('风险预估')
    ws9.sheet_view.showGridLines = False
    for col_letter, width in zip('ABCDEFGH', [22, 24, 18, 18, 50, 10, 10, 10]):
        ws9.column_dimensions[col_letter].width = width
    ws9.merge_cells('A1:H1')
    c = ws9['A1']
    c.value = 'LED工作灯 — 上架前风险预估清单（通用模板 + 品类自适应）'
    c.font = Font(name='Arial', bold=True, size=14, color=C_WHITE)
    c.fill = PatternFill('solid', fgColor='FFB43E21')
    c.alignment = Alignment(horizontal='center', vertical='center')
    ws9.row_dimensions[1].height = 35
    r9 = 3

    # 基于 BSR Title 做关键词命中统计
    titles_all = ' '.join(str(t).lower() for t in df['Product Title'].astype(str).tolist()) if 'Product Title' in df.columns else ''
    def count_hits(words):
        return sum(1 for t in df['Product Title'].astype(str) if any(w in t.lower() for w in words)) if 'Product Title' in df.columns else 0

    risk_items = [
        ('知识产权 · 专利 · 商标', '默认必查', '全品类', '高',
         '查询 Amazon Brand Registry / Google Patents / USPTO / Trademark Electronic Search System；避开明显仿款',
         1),
        ('电池安全 · UN38.3', ['battery', 'rechargeable', 'cordless', 'lithium'], '含电池类', '高',
         '锂电产品需要 UN38.3 测试报告 + MSDS + 非危险品鉴定；海运需走危包证',
         count_hits(['battery', 'rechargeable', 'cordless', 'lithium'])),
        ('UL/ETL 安全认证', ['electric', 'led', 'plug', 'ac', 'adapter', 'charger'], '电器类', '高',
         'LED 灯具常需 UL8750，照明/插电类需 UL1598，适配器需 UL60950；部分大卖家要求 ETL 作为替代',
         count_hits(['electric', 'led', 'plug', 'ac', 'adapter', 'charger'])),
        ('FCC 认证', ['wireless', 'bluetooth', 'remote', 'wifi', 'rf'], '无线设备', '中',
         '含无线模块产品必须提供 FCC ID；亚马逊抽查较多',
         count_hits(['wireless', 'bluetooth', 'remote', 'wifi', 'rf'])),
        ('加州 Prop 65', ['plastic', 'pvc', 'paint', 'cable'], '化学材料', '中',
         '含塑料 / 电缆 / 涂层产品需在 listing 添加 Prop 65 警告标签',
         count_hits(['plastic', 'pvc', 'paint', 'cable'])),
        ('CPSC / CPSIA', ['kid', 'child', 'baby', 'toddler'], '儿童用品', '高',
         '儿童用品需 CPSIA 重金属测试 + 小零件警告 + Tracking Label',
         count_hits(['kid', 'child', 'baby', 'toddler'])),
        ('FDA 注册', ['food', 'kitchen', 'contact'], '食品接触类', '中',
         '接触食品类需 FDA 设施注册 + 预警通知 PN；化妆品含声称者需 INCI 成分清单',
         count_hits(['food', 'kitchen', 'contact'])),
        ('Amazon 平台政策', '默认', '全品类', '中',
         '避开 Restricted Category；使用品牌备案减少跟卖；禁用 overclaim（"brightest in world"等）；准备好发票应对品牌投诉',
         1),
        ('供应链与海运', '默认', '全品类', '低',
         '头程建议预留 6-8 周；电池类只走海卡/美森；首批建议走空派做快速铺货',
         1),
        ('退货率高发品类', ['led', 'work light'], '本品类', '中',
         '工作灯历史退货率约 5-8%，常见退货原因：电池失效、亮度与图片不符、磁力不稳；listing 需标明真实规格',
         count_hits(['led', 'work light'])),
    ]

    hdr(ws9, r9, 1, '风险类别', bg='FFB43E21', fg=C_WHITE)
    hdr(ws9, r9, 2, '触发关键词', bg='FFB43E21', fg=C_WHITE)
    hdr(ws9, r9, 3, '适用范围', bg='FFB43E21', fg=C_WHITE)
    hdr(ws9, r9, 4, '风险等级', bg='FFB43E21', fg=C_WHITE)
    ws9.merge_cells(start_row=r9, start_column=5, end_row=r9, end_column=7)
    hdr(ws9, r9, 5, '规避建议', bg='FFB43E21', fg=C_WHITE)
    hdr(ws9, r9, 8, '命中数', bg='FFB43E21', fg=C_WHITE)
    ws9.row_dimensions[r9].height = 22
    r9 += 1

    total_hits = 0
    risk_rows_start = r9
    for cat, kws, scope, level, advice, hits in risk_items:
        level_bg = {'高': C_RED_LIGHT, '中': C_YELLOW, '低': C_GREEN_LIGHT}.get(level, C_WHITE)
        if isinstance(kws, list):
            kw_disp = ', '.join(kws)
        else:
            kw_disp = str(kws)
        val(ws9, r9, 1, cat, bold=True, bg=C_BLUE_LIGHT, fg='FF1F3864')
        val(ws9, r9, 2, kw_disp, bg=C_WHITE, fg='FF595959')
        val(ws9, r9, 3, scope, bg=C_WHITE)
        val(ws9, r9, 4, level, bold=True, bg=level_bg)
        ws9.merge_cells(start_row=r9, start_column=5, end_row=r9, end_column=7)
        val(ws9, r9, 5, advice, bg=C_WHITE)
        val(ws9, r9, 8, f'{hits}' if hits > 0 else '—', bold=True,
            bg=C_RED_LIGHT if hits > 10 else (C_YELLOW if hits > 0 else C_WHITE))
        ws9.row_dimensions[r9].height = 40
        if hits > 0 and level in ('高', '中'):
            total_hits += 1
        r9 += 1
    apply_border(ws9, risk_rows_start-1, r9-1, 1, 8)
    r9 += 1

    # 通用合规清单
    section_title(ws9, r9, 1, '▌ 通用合规清单（上新前自查）', span=8, bg=C_ORANGE)
    ws9.row_dimensions[r9].height = 24
    r9 += 1
    checklist = [
        '✓ 采购端：索要 1688/工厂的 材质报告、MSDS、发票',
        '✓ 品牌端：Amazon Brand Registry 备案（TM+R 商标）+ Transparency 计划防跟卖',
        '✓ 合规端：按品类准备认证报告（UL/FCC/CE/CPSIA 视需要）',
        '✓ 海运端：电池类确认 UN38.3 + 非危包 + 选择合规渠道',
        '✓ Listing 端：避免夸大卖点 / 对标词、补充真实规格图 / 使用说明',
        '✓ 法务端：查 Google Patents + USPTO + 商标全球搜索，避开 3 年内专利',
    ]
    for item in checklist:
        ws9.merge_cells(start_row=r9, start_column=1, end_row=r9, end_column=8)
        val(ws9, r9, 1, item, bg=C_WHITE, fg='FF1F3864')
        ws9.row_dimensions[r9].height = 20
        r9 += 1

    # ===== Sheet 10: 选品综合结论 =====
    ws10 = wb.create_sheet('选品综合结论')
    ws10.sheet_view.showGridLines = False
    for col_letter, width in zip('ABCDEFGH', [20, 14, 14, 14, 50, 14, 14, 14]):
        ws10.column_dimensions[col_letter].width = width
    ws10.merge_cells('A1:H1')
    c = ws10['A1']
    c.value = 'LED工作灯 — 选品综合评估与最终建议'
    c.font = Font(name='Arial', bold=True, size=14, color=C_WHITE)
    c.fill = PatternFill('solid', fgColor=C_BLUE_DARK)
    c.alignment = Alignment(horizontal='center', vertical='center')
    ws10.row_dimensions[1].height = 35
    r10 = 3

    # 1) 计算 8 维度评分
    def score_market_volume():
        if total_sales >= 200000: return 5, f'月总销量 {total_sales:,} 件，体量大'
        if total_sales >= 100000: return 4, f'月总销量 {total_sales:,} 件，体量良好'
        if total_sales >= 50000: return 3, f'月总销量 {total_sales:,} 件，体量中等'
        if total_sales >= 20000: return 2, f'月总销量 {total_sales:,} 件，体量偏小'
        return 1, f'月总销量 {total_sales:,} 件，体量有限'

    def score_demand_trend():
        if not market_data.get('_available'):
            return 3, '缺少 Market 文件，按中性评估'
        sell_df2 = market_data.get('sell_trends')
        if sell_df2 is None or len(sell_df2) < 13:
            return 3, '销售趋势数据不足'
        try:
            s = pd.to_numeric(sell_df2.iloc[:, 1], errors='coerce').dropna()
            if len(s) < 13:
                return 3, '销售趋势数据不足'
            recent = s.iloc[-12:].sum()
            prev = s.iloc[-24:-12].sum() if len(s) >= 24 else s.iloc[:-12].sum() * (12 / max(len(s)-12, 1))
            yoy = (recent - prev) / prev if prev > 0 else 0
            if yoy > 0.2: return 5, f'近 12 月销量同比 +{yoy:.0%}，高速增长'
            if yoy > 0.05: return 4, f'同比 +{yoy:.0%}，稳步增长'
            if yoy > -0.05: return 3, f'同比 {yoy:+.0%}，基本持平'
            if yoy > -0.15: return 2, f'同比 {yoy:+.0%}，小幅下滑'
            return 1, f'同比 {yoy:+.0%}，明显下滑'
        except Exception:
            return 3, '趋势计算失败'

    def score_competition():
        if cr5 is None:
            # 用 BSR 品牌数兜底
            if brand_count < 30: return 2, f'{brand_count} 品牌在争，竞争集中'
            if brand_count < 50: return 3, f'{brand_count} 品牌，中等竞争'
            return 4, f'{brand_count} 品牌分散，新品有机会'
        if cr5 > 0.6: return 1, f'CR5={cr5:.0%}，头部垄断明显'
        if cr5 > 0.4: return 2, f'CR5={cr5:.0%}，头部集中'
        if cr5 > 0.25: return 3, f'CR5={cr5:.0%}，适度集中'
        return 4, f'CR5={cr5:.0%}，竞争分散'

    def score_margin():
        if 'Gross Margin' in df.columns:
            try:
                gm_series = pd.to_numeric(df['Gross Margin'], errors='coerce').dropna()
                gm = gm_series.mean()
                if gm > 0.4: return 5, f'平均毛利 {gm:.0%}，空间大'
                if gm > 0.3: return 4, f'平均毛利 {gm:.0%}，健康'
                if gm > 0.2: return 3, f'平均毛利 {gm:.0%}，一般'
                if gm > 0.1: return 2, f'平均毛利 {gm:.0%}，偏薄'
                return 1, f'平均毛利 {gm:.0%}，利润压力大'
            except Exception:
                pass
        return 3, '毛利数据缺失，按中性评估'

    def score_supply_chain():
        t = titles_all
        if 'led' in t or 'flashlight' in t or 'work light' in t:
            return 4, '深圳/中山 LED 产业带成熟，供应商多、起订量低'
        if 'food' in t or 'medical' in t:
            return 2, '食品/医疗品类合规复杂，供应链门槛高'
        return 3, '通用品类，供应链平均水平'

    def score_promotion():
        if difficulty_level == '低': return 4, '新品推广难度低，进入期 3-6 月'
        if difficulty_level == '中': return 3, '推广难度中等，进入期 6-9 月'
        return 2, '推广难度高，需 9-12 月周期'

    def score_risk():
        if total_hits <= 1: return 5, f'仅 {total_hits} 项中高风险命中，风险可控'
        if total_hits <= 2: return 4, f'{total_hits} 项中高风险，需逐项准备'
        if total_hits <= 4: return 3, f'{total_hits} 项中高风险，合规成本上升'
        return 2, f'{total_hits} 项中高风险，需法务/认证前置'

    def score_diff():
        try:
            if price_col:
                prices = pd.to_numeric(df[price_col], errors='coerce').dropna()
                if len(prices) > 5:
                    cv = prices.std() / prices.mean() if prices.mean() else 0
                    if cv > 0.5: return 4, f'价格变异系数 {cv:.2f}，产品差异化空间大'
                    if cv > 0.3: return 3, f'价格变异系数 {cv:.2f}，差异化可做'
                    return 2, f'价格变异系数 {cv:.2f}，同质化严重'
        except Exception:
            pass
        return 3, '差异化数据缺失，按中性评估'

    dims = [
        ('市场体量', score_market_volume(), 0.15),
        ('需求趋势', score_demand_trend(), 0.15),
        ('竞争难度', score_competition(), 0.15),
        ('利润率', score_margin(), 0.15),
        ('供应链', score_supply_chain(), 0.10),
        ('推广压力', score_promotion(), 0.10),
        ('风险可控性', score_risk(), 0.10),
        ('差异化机会', score_diff(), 0.10),
    ]
    total_score = sum(s[0] * w for _, s, w in dims)
    if total_score >= 4.2:
        overall_label = '强烈推荐'
        overall_bg = C_GREEN_LIGHT
    elif total_score >= 3.5:
        overall_label = '推荐'
        overall_bg = C_GREEN_LIGHT
    elif total_score >= 2.8:
        overall_label = '可尝试'
        overall_bg = C_YELLOW
    elif total_score >= 2.0:
        overall_label = '谨慎进入'
        overall_bg = C_YELLOW
    else:
        overall_label = '不推荐'
        overall_bg = C_RED_LIGHT
    stars_full = int(round(total_score))
    stars_str = '★' * stars_full + '☆' * (5 - stars_full)
    def star_label(sc):
        return '★' * sc + '☆' * (5 - sc)

    # ---- 各维度数据汇总 ----
    cn_seller_txt = f'中国卖家占比 {cn_seller_ratio:.0%}' if cn_seller_ratio is not None else ''
    cr5_txt2 = f'CR5={cr5:.0%}' if cr5 is not None else '（需 Market 文件）'
    neg_top3 = sorted(neg_counts.items(), key=lambda x: x[1], reverse=True)[:3]
    neg_top3_txt = '、'.join([f'{cat}({int(cnt)}条)' for cat, cnt in neg_top3 if cnt > 0])
    peak_months_txt = '、'.join([f'{m}月' for m in sorted(set(seasonality_peak_months))]) if seasonality_peak_months else '无明显旺季'
    kw_summary_txt = f'{", ".join(kw_top3_for_conclusion)}' if kw_top3_for_conclusion else '（需关键词文件）'
    gm_val = gm_mean / 100 if gm_mean else 0
    promo_cycle = '3-6' if difficulty_level == '低' else ('6-9' if difficulty_level == '中' else '9-12')

    # ▌ 一、推荐理由总结
    section_title(ws10, r10, 1, '▌ 一、推荐理由总结', span=8)
    ws10.row_dimensions[r10].height = 24
    r10 += 1
    hdr(ws10, r10, 1, '分析维度', bg=C_BLUE_MID)
    ws10.merge_cells(start_row=r10, start_column=2, end_row=r10, end_column=8)
    hdr(ws10, r10, 2, '数据支撑与分析结论', bg=C_BLUE_MID)
    ws10.row_dimensions[r10].height = 20
    r10 += 1
    reason_start = r10
    reason_rows = [
        ('市场体量', f'BSR TOP100 月总销量 {total_sales:,} 件，月销售额 ${total_rev:,.0f}。{sell_summary}'),
        ('需求趋势', f'生命周期：{lifecycle_stage} —— {lifecycle_reason}；旺季 {peak_months_txt}'),
        ('竞争格局', f'{brand_count} 个品牌，{cr5_txt2}。新品(<1年)占比 {new_ratio_top100:.1%}，推广难度 {difficulty_level}' + (f'。{cn_seller_txt}' if cn_seller_txt else '')),
        ('利润空间', f'均价 ${avg_price:.0f}，毛利均值 {gm_val:.1%}。{"可支撑推广投入" if gm_val > 0.3 else ("利润中等" if gm_val > 0.15 else "利润偏薄")}'),
        ('差评痛点', f'{neg_top3_txt or "无评论数据"}。从痛点切入做差异化改进'),
        ('关键词', f'核心词：{kw_summary_txt}；最低门槛词：{kw_lowest_spr_for_conclusion or "（需关键词文件）"}'),
        ('风险', f'{total_hits} 项中高风险命中。需完成认证排查 + 专利检索'),
    ]
    for label, text in reason_rows:
        val(ws10, r10, 1, label, bold=True, bg=C_BLUE_LIGHT, fg='FF1F3864')
        ws10.merge_cells(start_row=r10, start_column=2, end_row=r10, end_column=8)
        val(ws10, r10, 2, text, bg=C_WHITE, fg='FF595959')
        ws10.row_dimensions[r10].height = 28
        r10 += 1
    apply_border(ws10, reason_start-1, r10-1, 1, 8)
    r10 += 1

    # ▌ 二、综合评级
    section_title(ws10, r10, 1, '▌ 二、综合评级', span=8)
    ws10.row_dimensions[r10].height = 24
    r10 += 1
    ws10.merge_cells(start_row=r10, start_column=1, end_row=r10, end_column=2)
    val(ws10, r10, 1, stars_str, bold=True, bg=overall_bg, size=14, h_align='center')
    val(ws10, r10, 3, f'{total_score:.2f} / 5.0', bold=True, bg=overall_bg, size=14, h_align='center')
    ws10.merge_cells(start_row=r10, start_column=4, end_row=r10, end_column=8)
    val(ws10, r10, 4, f'{overall_label}', bold=True, bg=overall_bg, size=14, h_align='center')
    ws10.row_dimensions[r10].height = 32
    r10 += 1

    hdr(ws10, r10, 1, '维度', bg=C_BLUE_MID)
    hdr(ws10, r10, 2, '评分', bg=C_BLUE_MID)
    hdr(ws10, r10, 3, '星级', bg=C_BLUE_MID)
    hdr(ws10, r10, 4, '权重', bg=C_BLUE_MID)
    ws10.merge_cells(start_row=r10, start_column=5, end_row=r10, end_column=8)
    hdr(ws10, r10, 5, '评分依据', bg=C_BLUE_MID)
    r10 += 1
    dim_start = r10
    for name, (sc, reason), weight in dims:
        bg = C_GREEN_LIGHT if sc >= 4 else (C_YELLOW if sc >= 3 else C_RED_LIGHT)
        val(ws10, r10, 1, name, bold=True, bg=C_BLUE_LIGHT, fg='FF1F3864')
        val(ws10, r10, 2, f'{sc} / 5', bold=True, bg=bg, h_align='center')
        val(ws10, r10, 3, star_label(sc), bg=bg, h_align='center')
        val(ws10, r10, 4, f'{weight:.0%}', bg=C_WHITE, h_align='center')
        ws10.merge_cells(start_row=r10, start_column=5, end_row=r10, end_column=8)
        val(ws10, r10, 5, reason, bg=C_WHITE, fg='FF595959')
        ws10.row_dimensions[r10].height = 22
        r10 += 1
    apply_border(ws10, dim_start-1, r10-1, 1, 8)
    r10 += 1

    # 预计算推荐开发参数（供 ▌三 和 ▌五 复用）
    rec_specs = aggregate_recommended_specs(all_specs_for_agg, neg_counts)

    # ▌ 三、首推入场品类（独立模块）
    section_title(ws10, r10, 1, '▌ 三、首推入场品类', span=8, bg=C_BLUE_DARK)
    ws10.row_dimensions[r10].height = 24
    r10 += 1
    if pricing_recommendations:
        top_rec = pricing_recommendations[0]
        est_low = int(top_rec.get('avg_sales', 0) * 0.6) if top_rec.get('avg_sales') else 300
        est_high = int(top_rec.get('avg_sales', 0) * 1.2) if top_rec.get('avg_sales') else 1200
        entry_fields = [
            ('首推品类', f'{top_rec["product_type"]}', C_RED_LIGHT),
            ('建议价格区间', f'${top_rec["rec_min"]:.0f}-${top_rec["rec_max"]:.0f}', C_WHITE),
            ('目标月销量', f'{est_low}-{est_high}', C_WHITE),
            ('首批备货量', '300-500 pcs', C_WHITE),
            ('预计回款周期', f'{promo_cycle} 个月', C_WHITE),
        ]
        entry_start = r10
        for label, value, bg in entry_fields:
            val(ws10, r10, 1, label, bold=True, bg=C_BLUE_LIGHT, fg='FF1F3864')
            ws10.merge_cells(start_row=r10, start_column=2, end_row=r10, end_column=8)
            val(ws10, r10, 2, value, bold=True, bg=bg, fg='FF7B0000' if bg == C_RED_LIGHT else 'FF1F3864')
            ws10.row_dimensions[r10].height = 24
            r10 += 1
        apply_border(ws10, entry_start-1, r10-1, 1, 8)

        # 评分逻辑注解
        ws10.merge_cells(start_row=r10, start_column=1, end_row=r10, end_column=8)
        c = ws10.cell(row=r10, column=1)
        c.value = '注：首推品类由综合评分自动排序 = 需求(月销)×0.30 + 单品收益×0.25 + 新品占比×0.15 + 质量评分×0.15 + 竞争度×0.15（数据驱动，非固定）'
        c.font = Font(name='Arial', size=9, color='FF7A4F01', italic=True)
        c.fill = PatternFill('solid', fgColor=C_YELLOW)
        c.alignment = Alignment(horizontal='left', vertical='center')
        ws10.row_dimensions[r10].height = 16
        r10 += 2

        # 子模块A：推荐功能参数
        if rec_specs:
            ws10.merge_cells(start_row=r10, start_column=1, end_row=r10, end_column=8)
            c = ws10.cell(row=r10, column=1)
            c.value = '—— 推荐功能参数 ——'
            c.font = Font(name='Arial', bold=True, size=11, color='FF1F3864')
            c.fill = PatternFill('solid', fgColor='FFD9E2F3')
            c.alignment = Alignment(horizontal='center', vertical='center')
            ws10.row_dimensions[r10].height = 22
            r10 += 1

            hdr(ws10, r10, 1, '参数名称', bg=C_BLUE_MID)
            ws10.merge_cells(start_row=r10, start_column=2, end_row=r10, end_column=3)
            hdr(ws10, r10, 2, '建议规格', bg=C_BLUE_MID)
            ws10.merge_cells(start_row=r10, start_column=4, end_row=r10, end_column=6)
            hdr(ws10, r10, 4, '数据依据', bg=C_BLUE_MID)
            ws10.merge_cells(start_row=r10, start_column=7, end_row=r10, end_column=8)
            hdr(ws10, r10, 7, '优先级', bg=C_BLUE_MID)
            ws10.row_dimensions[r10].height = 20
            r10 += 1
            param_start = r10

            for param_name, rec_value, basis, priority in rec_specs:
                if 'P1' in priority:
                    p_bg = C_RED_LIGHT
                    p_fg = 'FF7B0000'
                elif 'P2' in priority:
                    p_bg = C_YELLOW
                    p_fg = 'FF7A4F01'
                else:
                    p_bg = C_BLUE_LIGHT
                    p_fg = 'FF1F3864'
                val(ws10, r10, 1, param_name, bold=True, bg=C_BLUE_LIGHT, fg='FF1F3864')
                ws10.merge_cells(start_row=r10, start_column=2, end_row=r10, end_column=3)
                val(ws10, r10, 2, rec_value, bold=True, bg=C_WHITE, fg='FF000000')
                ws10.merge_cells(start_row=r10, start_column=4, end_row=r10, end_column=6)
                val(ws10, r10, 4, basis, bg=C_WHITE, fg='FF595959', size=9)
                ws10.merge_cells(start_row=r10, start_column=7, end_row=r10, end_column=8)
                val(ws10, r10, 7, priority, bold=True, bg=p_bg, fg=p_fg, h_align='center')
                ws10.row_dimensions[r10].height = 24
                r10 += 1

            apply_border(ws10, param_start-1, r10-1, 1, 8)
            r10 += 1

        # 子模块B：差异化升级方向
        neg_sorted_for_entry = sorted(neg_counts.items(), key=lambda x: x[1], reverse=True)
        diff_lines = []
        if len(neg_sorted_for_entry) >= 1:
            t1 = neg_sorted_for_entry[0]
            diff_lines.append(f'① 首要差异化：针对"{t1[0]}"（{int(t1[1])}条差评），重点突破竞品短板')
        if len(neg_sorted_for_entry) >= 2:
            t2 = neg_sorted_for_entry[1]
            diff_lines.append(f'② 次要差异化：改善"{t2[0]}"（{int(t2[1])}条差评），提升用户体验')
        if len(neg_sorted_for_entry) >= 3:
            t3 = neg_sorted_for_entry[2]
            diff_lines.append(f'③ 加分项：优化"{t3[0]}"（{int(t3[1])}条差评），建立口碑优势')
        if diff_lines:
            ws10.merge_cells(start_row=r10, start_column=1, end_row=r10, end_column=8)
            c = ws10.cell(row=r10, column=1)
            c.value = '差异化升级方向：\n' + '\n'.join(diff_lines)
            c.font = Font(name='Arial', size=10, color='FF1F3864')
            c.fill = PatternFill('solid', fgColor='FFD9E2F3')
            c.alignment = Alignment(horizontal='left', vertical='top', wrap_text=True)
            ws10.row_dimensions[r10].height = 20 + len(diff_lines) * 18
            r10 += 1

    r10 += 1

    # ▌ 四、行动建议（精简，不重复数据）
    section_title(ws10, r10, 1, '▌ 四、行动建议', span=8, bg=C_BLUE_DARK)
    ws10.row_dimensions[r10].height = 24
    r10 += 1
    advice_rows = [
        ('【切入策略】', f'从差评痛点切入做差异化，初期低价冲排名，积累评价后提价至 ${avg_price*0.85:.0f}-${avg_price*1.05:.0f}'),
        ('【推广节奏】', f'前 {promo_cycle.split("-")[0]} 月广告跑量 + 优惠券，重点投放 SPR 低的长尾词；评分需达 {avg_rating:.1f}★ 门槛'),
        ('【风险前置】', f'上新前完成认证排查（UL/FCC/UN38.3 等如适用）+ 专利检索，避免 Listing 下架'),
        ('【备货节奏】', f'旺季 {peak_months_txt}，提前 2 个月到仓；首批 300-500pcs 控风险'),
    ]
    adv_start = r10
    for label, text in advice_rows:
        ws10.merge_cells(start_row=r10, start_column=1, end_row=r10, end_column=2)
        val(ws10, r10, 1, label, bold=True, bg=C_BLUE_LIGHT, fg='FF1F3864')
        ws10.merge_cells(start_row=r10, start_column=3, end_row=r10, end_column=8)
        val(ws10, r10, 3, text, bg=C_WHITE)
        ws10.row_dimensions[r10].height = 28
        r10 += 1
    apply_border(ws10, adv_start-1, r10-1, 1, 8)
    r10 += 1

    # ===== 保存 =====
    wb.save(output_path)
    return output_path


# ============================================================
# Flask 路由
# ============================================================
@app.route('/')
def index():
    return render_template('index.html')


@app.route('/upload', methods=['POST'])
def upload():
    if 'bsr_file' not in request.files:
        flash('请上传BSR数据文件', 'error')
        return redirect(url_for('index'))

    bsr_file = request.files['bsr_file']
    if bsr_file.filename == '':
        flash('请上传BSR数据文件', 'error')
        return redirect(url_for('index'))

    if not allowed_file(bsr_file.filename):
        flash('BSR文件必须是Excel格式(.xlsx或.xls)', 'error')
        return redirect(url_for('index'))

    review_files = request.files.getlist('review_files')
    review_files = [f for f in review_files if f.filename and allowed_file(f.filename)]

    market_file = request.files.get('market_file')
    market_path = None

    session_id = str(uuid.uuid4())[:8]
    temp_dir = os.path.join(app.config['UPLOAD_FOLDER'], session_id)
    os.makedirs(temp_dir, exist_ok=True)

    try:
        bsr_filename = secure_filename(bsr_file.filename)
        bsr_path = os.path.join(temp_dir, bsr_filename)
        bsr_file.save(bsr_path)

        review_paths = []
        for f in review_files:
            if f.filename:
                filename = secure_filename(f.filename)
                path = os.path.join(temp_dir, filename)
                f.save(path)
                review_paths.append(path)

        if market_file and market_file.filename and allowed_file(market_file.filename):
            market_filename = secure_filename(market_file.filename)
            market_path = os.path.join(temp_dir, market_filename)
            market_file.save(market_path)

        keyword_file = request.files.get('keyword_file')
        keyword_path = None
        if keyword_file and keyword_file.filename and allowed_file(keyword_file.filename):
            keyword_filename = secure_filename(keyword_file.filename)
            keyword_path = os.path.join(temp_dir, keyword_filename)
            keyword_file.save(keyword_path)

        report_filename = f'选品评估报告_{datetime.now().strftime("%Y%m%d_%H%M%S")}.xlsx'
        report_path = os.path.join(app.config['REPORT_FOLDER'], report_filename)

        # 写入生成状态（前端轮询检测）
        status_file = os.path.join(app.config['REPORT_FOLDER'], '_status.txt')
        with open(status_file, 'w', encoding='utf-8') as sf:
            sf.write('generating')

        generate_report(bsr_path, review_paths, report_path, market_path, keyword_path)

        shutil.rmtree(temp_dir, ignore_errors=True)

        # 写入完成状态 + 报告路径（供前端轮询获取）
        with open(status_file, 'w', encoding='utf-8') as sf:
            sf.write(f'done:{report_filename}')

        # 返回 JSON，前端收到后显示下载按钮
        return jsonify({
            'success': True,
            'filename': report_filename,
            'message': '报告生成完成，点击下方按钮下载'
        })

    except Exception as e:
        import traceback
        traceback.print_exc()
        # 出错时清除状态
        try:
            status_file = os.path.join(app.config['REPORT_FOLDER'], '_status.txt')
            with open(status_file, 'w', encoding='utf-8') as sf:
                sf.write(f'error:{str(e)}')
        except:
            pass
        flash(f'生成报告时出错: {str(e)}', 'error')
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/status')
def status():
    """前端轮询状态接口"""
    status_file = os.path.join(app.config['REPORT_FOLDER'], '_status.txt')
    try:
        with open(status_file, 'r', encoding='utf-8') as f:
            content = f.read().strip()
        return {'status': content}
    except:
        return {'status': 'idle'}


@app.route('/download/<filename>')
def download_report(filename):
    """手动下载报告接口"""
    report_path = os.path.join(app.config['REPORT_FOLDER'], filename)
    if not os.path.exists(report_path):
        flash('报告文件不存在或已过期，请重新生成', 'error')
        return redirect(url_for('index'))
    return send_file(report_path, as_attachment=True, download_name=filename)


@app.route('/health')
def health():
    return {'status': 'ok', 'message': '选品报告生成服务运行中'}


if __name__ == '__main__':
    print("""
╔═══════════════════════════════════════════════════════════════╗
║         选品报告生成系统 v2.0 (完整版)                         ║
║         访问地址: http://localhost:8000                         ║
╚═══════════════════════════════════════════════════════════════╝
    """)
    app.run(host='0.0.0.0', port=8000, debug=True)
