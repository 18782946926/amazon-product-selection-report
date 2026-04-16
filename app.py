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
# 基于实际数据动态计算推荐入场价
# ============================================================
def calculate_pricing_recommendations(df, price_col, type_agg):
    """
    根据实际BSR数据计算各产品类型的推荐入场价
    返回动态生成的价格建议列表
    """
    pricing_recommendations = []
    
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
        
        # 判断是否是首推品类
        is_top_type = ptype == type_agg.iloc[0]['product_type'] if len(type_agg) > 0 else False
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
    
    # 按收益排序
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
        
        # 基于产品类型生成上新方向
        type_priority = {'充电磁吸工作灯': 'P1', '三脚架工作灯': 'P2', 
                         '泛光工作灯': 'P3', '夹灯': 'P3', 
                         '手持手电/聚光灯': 'P3', '太阳能工作灯': 'P3'}
        
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
# 核心报告生成函数
# ============================================================
def generate_report(bsr_path, review_paths, output_path):
    """
    完整版报告生成 - 100%移植原版分析逻辑
    """
    # --- 1. 读取BSR数据 ---
    df = pd.read_excel(bsr_path, sheet_name='US')

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
    rev_df['Rating'] = pd.to_numeric(rev_df['Rating'], errors='coerce')
    low_rev = rev_df[rev_df['Rating'] <= 2].copy() if len(rev_df) > 0 else pd.DataFrame()
    high_rev = rev_df[rev_df['Rating'] >= 4].copy() if len(rev_df) > 0 else pd.DataFrame()

    # --- 3. 统计汇总 ---
    total_rev = df[rev_col].sum() if rev_col else 0
    total_sales = df['Monthly Sales'].sum() if 'Monthly Sales' in df.columns else 0
    avg_price = df[price_col].mean() if price_col else 0
    median_price = df[price_col].median() if price_col else 0
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
    neg_text = (low_rev['Title'].fillna('') + ' ' + low_rev['Content'].fillna('')).str.lower()
    for cat, kws in neg_keywords.items():
        pattern = '|'.join(kws)
        neg_counts[cat] = neg_text.str.contains(pattern, na=False).sum()

    pos_counts = {}
    pos_text = (high_rev['Title'].fillna('') + ' ' + high_rev['Content'].fillna('')).str.lower()
    for cat, kws in pos_keywords.items():
        pattern = '|'.join(kws)
        pos_counts[cat] = pos_text.str.contains(pattern, na=False).sum()

    # --- 5.1 动态计算推荐入场价（基于实际BSR数据分析） ---
    pricing_recommendations = calculate_pricing_recommendations(df, price_col, type_agg)

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

    # ===== Sheet 5: 竞品卖点与差评 =====
    ws5 = wb.create_sheet('竞品卖点与差评')
    ws5.sheet_view.showGridLines = False
    for col_letter, width in zip('ABCDEFGH', [25, 12, 12, 40, 25, 12, 12, 35]):
        ws5.column_dimensions[col_letter].width = width

    ws5.merge_cells('A1:H1')
    c = ws5['A1']
    c.value = f'LED工作灯 — 竞品核心卖点 & 差评痛点分析（基于{len(rev_df)}条真实评论）'
    c.font = Font(name='Arial', bold=True, size=14, color=C_WHITE)
    c.fill = PatternFill('solid', fgColor=C_BLUE_DARK)
    c.alignment = Alignment(horizontal='center', vertical='center')
    ws5.row_dimensions[1].height = 35

    r5 = 3
    section_title(ws5, r5, 1, '▌ 一、正向卖点提炼（4-5★评论）', span=4)
    section_title(ws5, r5, 5, '▌ 二、差评痛点分析（1-2★评论）', span=4, bg='FF7B2D00')
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
    section_title(ws5, r5, 1, '▌ 三、各ASIN评论质量汇总', span=8)
    r5 += 1
    asin_hdr = ['ASIN', '产品类型', '评论总数', '平均星级', '差评率', '5★占比', '主要差评类型']
    for i, h in enumerate(asin_hdr):
        hdr(ws5, r5, i+1, h, bg=C_BLUE_MID)
    ws5.merge_cells(start_row=r5, start_column=7, end_row=r5, end_column=8)
    r5 += 1

    asin_type_map = dict(zip(df['ASIN'], df['product_type']))
    for source_asin in rev_df['source_asin'].unique():
        adf = rev_df[rev_df['source_asin'] == source_asin]
        if len(adf) == 0:
            continue
        avg_r = adf['Rating'].mean()
        low_pct = (adf['Rating'] <= 2).sum() / len(adf) * 100
        five_pct = (adf['Rating'] == 5).sum() / len(adf) * 100
        adf_low = adf[adf['Rating'] <= 2].copy()
        adf_low['neg_text'] = adf_low['Title'].fillna('') + ' ' + adf_low['Content'].fillna('')
        complaints = []
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

    apply_border(ws5, r5 - len(rev_df['source_asin'].unique()) - 1, r5-1, 1, 8)
    r5 += 1
    
    # ===== 竞品卖点与差评 - 新增内容 =====
    # 核心改进方向总结
    section_title(ws5, r5, 1, '▌ 四、核心改进方向总结', span=8)
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
    section_title(ws5, r5, 1, '▌ 五、卖点/痛点对比矩阵', span=8)
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
    r6 += 1

    # 决策矩阵
    section_title(ws6, r6, 1, '▌ 上新决策矩阵 — 优先级说明', span=9)
    ws6.row_dimensions[r6].height = 24
    r6 += 1

    decision_notes = [
        ('P1 首推（立即启动）', '充电磁吸工作灯是销量最大品类，市场需求成熟，差评痛点明确（灯头+电池），改良空间大，价格敏感度中等，适合首批快速验证。目标：6个月内进入类目TOP50。'),
        ('P2 推荐（第二梯队）', '三脚架工作灯客单价高、单品收益好，但对产品品质要求更严格（防水、三脚架稳定性），需要工厂定制支持，适合已有供应链后扩展。'),
        ('P3 参考（机会品类）', '太阳能灯竞争极少，是短期内可快速占位的小类目；T8吊装灯需求稳定，可作为SKU丰富产品线，降低集中风险。'),
        ('P4 备选（节日运营）', '礼品套装对日常SKU改动少，主要靠包装和节日运营推动转化，适合节前短期投入，ROI可观。'),
    ]
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
         '• 深入分析TOP10竞品优缺点\n• 确定差异化功能点\n• 与工厂沟通打样', 
         '完成产品定义文档'),
        ('产品优化', '第3-4周', '痛点针对优化',
         '• 电池≥5000mAh\n• 亮度2000lm+\n• 磁力≥35N+锁定', 
         '样品通过功能测试'),
        ('Listing准备', '第5-6周', 'listing+主图设计',
         '• 标题埋词优化\n• 五点描述突出痛点\n• A+页面设计', 
         'listing优化完成'),
        ('测试期运营', '第7-12周', '新品推广',
         '• 初期测评+QA\n• 广告冷启动\n• 报秒杀活动', 
         '评分≥4.4★'),
        ('稳定期', '第3-6月', '稳定排名',
         '• 广告精细化运营\n• 持续优化转化率\n• 拓展变体', 
         '进入类目TOP50'),
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
         '• 首批备货建议300-500pcs，避免库存压力\n• 选择有出口经验的工厂，确保交期\n• 要求工厂提供样品进行功能测试'),
        ('品质把控', 'C_YELLOW',
         '• 重点测试：电池续航、亮度、磁力强度\n• 跌落测试：3米跌落不开裂\n• 防水测试：IP65等级验证'),
        ('listing优化', 'C_BLUE_LIGHT',
         '• 标题格式：[核心词]+[功能]+[规格]\n• 五点描述按痛点-解决方案-产品优势排列\n• 主图突出差异化功能（磁力/亮度模式）'),
        ('风险预警', 'C_RED_LIGHT',
         '• 风险1：广告ACOS过高（可能超35%）\n  → 应对：优化listing转化，配合优惠券\n• 风险2：差评导致评分下滑\n  → 应对：主动联系差评用户，及时处理\n• 风险3：竞品价格战\n  → 应对：不参与价格战，用品质和评价区隔'),
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

    rev_display = rev_df[['source_asin', 'Rating', 'Title', 'Content', 'Date']].head(800)
    for row_idx, (_, revrow) in enumerate(rev_display.iterrows(), start=3):
        rating = revrow.get('Rating', 3)
        bg = C_RED_LIGHT if (pd.notna(rating) and float(rating) <= 2) else (C_WHITE if (pd.notna(rating) and float(rating) <= 3) else C_GREEN_LIGHT if row_idx % 2 == 0 else C_WHITE)
        content_preview = str(revrow.get('Content', ''))[:200] if pd.notna(revrow.get('Content', '')) else ''
        row_vals = [revrow.get('source_asin', ''), '', rating, revrow.get('Title', ''), content_preview, revrow.get('Date', '')]
        for ci, v in enumerate(row_vals):
            c = ws7.cell(row=row_idx, column=ci+1, value=str(v) if v else '')
            c.fill = PatternFill('solid', fgColor=bg)
            c.font = Font(name='Arial', size=8)
            c.alignment = Alignment(horizontal='center' if ci in [0, 2, 5] else 'left', vertical='center', wrap_text=(ci == 4))
        ws7.row_dimensions[row_idx].height = 14

    apply_border(ws7, 2, min(800+2, len(rev_df)+2), 1, 6)
    
    # ===== 评论数据汇总 - 新增内容 =====
    r7 = min(800+3, len(rev_df)+5)
    
    # 评分分布统计
    section_title(ws7, r7, 1, '▌ 评论评分分布统计', span=6)
    ws7.row_dimensions[r7].height = 24
    r7 += 1
    
    rating_dist = rev_df['Rating'].value_counts().sort_index()
    total_reviews = len(rev_df)
    
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

        report_filename = f'选品评估报告_{datetime.now().strftime("%Y%m%d_%H%M%S")}.xlsx'
        report_path = os.path.join(app.config['REPORT_FOLDER'], report_filename)

        # 写入生成状态（前端轮询检测）
        status_file = os.path.join(app.config['REPORT_FOLDER'], '_status.txt')
        with open(status_file, 'w', encoding='utf-8') as sf:
            sf.write('generating')

        generate_report(bsr_path, review_paths, report_path)

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
║         访问地址: http://localhost:5000                         ║
╚═══════════════════════════════════════════════════════════════╝
    """)
    app.run(host='0.0.0.0', port=5000, debug=True)
