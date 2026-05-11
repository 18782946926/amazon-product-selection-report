"""LLM 产出校验器：把"声称有证据"从文本约束升级为代码强制。

核心功能：
1. `resolve_pack_path(packs, path)`：解析 "voc.pain_clusters[0].name" 这类路径，取具体值
2. `validate_evidence(text, resolved_values)`：检查 text 里**真的出现**了某个被引用的值
   - 数字：允许 ±5% 容差
   - 字符串：子串包含，忽略大小写 + 中英文标点
3. `filter_synthesis(synthesis, packs_dict, stats)`：对 Synthesizer 输出逐条过滤，
   未兑现的 upgrade_direction / differentiation_angle / dimension_reason 直接丢弃

配合 prompt 侧的 "必须原样引用" 硬约束形成双保险——prompt 说规矩，校验器做最后裁判。
"""
from __future__ import annotations

import logging
import re
from typing import Any

log = logging.getLogger(__name__)


# 路径形如："voc.pain_clusters[0].name" / "stats.price_median" / "market.product_segments[2]"
_PATH_TOKEN_RE = re.compile(r"([a-zA-Z_][a-zA-Z0-9_]*)(?:\[(\d+)\])?")

# narrative 中"代码字段路径泄露"检测（业务可读性硬约束）
# 命中任何一种就视为 LLM 把 schema 路径写进了给业务人员看的中文叙述
# 1) 顶级 Pack 名 + 点号 / 方括号深入路径： stats.promo_ppc_bid_avg, voc.pain_clusters[0].name
# 2) 顶级 Pack 名 + 下划线字段（曾出现"voc_pack.xxx"等变体）
# 注：用 (?<![A-Za-z0-9_]) 而不是 \b——Python 的 \b 把中文当 \w，"结合stats.xxx" 这种贴在汉字后
# 的代码标识符不会被 \b 命中；改用"前面不能是 ASCII 字母/数字/下划线"才能正确边界
_SCHEMA_PATH_LEAK_RE = re.compile(
    r"(?<![A-Za-z0-9_])(?:stats|voc|market|traffic|trend|synthesis|spec|compliance|packs?)"
    r"(?:[._][A-Za-z0-9_]+|\[\d+\])+",
    re.IGNORECASE,
)


def contains_schema_path(text: str) -> bool:
    """narrative 文本中是否泄露 schema 字段路径（如 'stats.promo_ppc_bid_avg'、
    'voc.pain_clusters[0].name'）。这些是给业务人员看的中文叙述，必须纯自然语言，
    不能出现代码标识符——schema 路径只能放进 supporting_fields 数组。
    """
    if not text:
        return False
    return bool(_SCHEMA_PATH_LEAK_RE.search(text))


# 绝对化夸张数字检测：LLM 在 narrative / action_plan / improvements 里编造「100%好评率」
# 「100%通过」「零差评」「绝对领跑」这种**真实数据里几乎不可能精确成立的指标**。
# 评分 4.7 不等于 100% 好评率；任何说"100%通过"的工厂指标都是 LLM 自造的 KPI。
# 检测命中 → 整条文案丢弃，让渲染层走 fallback 或留空。
# 全品类通用：只匹配通用绝对化修辞，不依赖任何品类专用词。
_ABSOLUTE_METRIC_RE = re.compile(
    # 100% + 评价/性能词（但不匹配 "100% 中国卖家"、"100% 兼容" 这种合理百分比表达）
    r"100\s*%\s*(?:好评|通过|可靠|无差评|零差评|有效|成功|纯正|完美|无瑕|零返修|零缺陷|零投诉|无故障|零故障)"
    # 零 + 负面指标（非否定式）
    r"|零\s*(?:差评|缺陷|投诉|返修|失败|故障|退货)"
    # 0% + 负面指标
    r"|0\s*%\s*(?:差评|缺陷|投诉|失败|故障|退货)"
    # 绝对 + 强势词（前面不能是"不"）
    r"|(?<!不)绝对(?:领跑|最佳|完美|垄断|无敌|碾压|领先)"
    # 满分 / 100 分 评价
    r"|满分(?:好评|表现|评价)?"
    r"|100\s*分(?:好评|评价)?",
    re.IGNORECASE,
)


def contains_absolute_metric(text: str) -> bool:
    """检测 LLM 编造的绝对化夸张指标。返回 True 时调用方应丢弃这条文案。

    例如：
    - "100%好评率"、"100%通过出厂测试" → 命中（LLM 编的虚假 KPI）
    - "零差评"、"绝对领跑"、"满分好评" → 命中
    - "100% 中国卖家"、"兼容 100% Dewalt 电池" → 不命中（合理表达）
    """
    if not text:
        return False
    return bool(_ABSOLUTE_METRIC_RE.search(text))

# 禁用词表——命中即丢弃整条结论（与 prompts/common.py 对齐）
FORBIDDEN_PHRASES = [
    "前景可期", "前景广阔", "市场空间广阔", "潜力可观", "潜力巨大",
    "建议深入分析", "值得重点关注", "有较大机会", "不容忽视",
    "总体来看", "综合来看", "从整体上", "整体而言",
    "具有一定优势", "具备较好基础", "呈现良好态势",
    "LLM 不可用", "未生成", "无叙述", "降级模板",
    # 推广压力相关假数字：源数据 ReverseASIN 里本就没有「广告占比 / 自然流量占比」单列字段，
    # 任何引用这几个概念的叙述都是 LLM 凭空虚构（老版本列查找失败后 LLM 仍尝试编出来），整条剔除。
    "广告占比", "广告高占比", "广告占比词",
    "自然流量占比", "自然流量 100", "自然流量100",
    "无广告竞争", "无广告竞争压力",
    "广告位", "广告位占比",
]


def resolve_pack_path(root: dict, path: str) -> Any:
    """解析 "voc.pain_clusters[0].name" 这类路径，返回对应值（不存在则返回 None）。

    Args:
        root: {"market": {...}, "voc": {...}, "traffic": {...}, "trend": {...}, "stats": {...}}
            每个值应是 pydantic model_dump 产生的 dict
        path: 路径字符串
    """
    if not path or not isinstance(path, str):
        return None
    tokens = path.strip().split(".")
    node: Any = root
    for tok in tokens:
        m = _PATH_TOKEN_RE.fullmatch(tok)
        if not m:
            return None
        attr, idx = m.group(1), m.group(2)
        if isinstance(node, dict):
            if attr not in node:
                return None
            node = node[attr]
        else:
            # 非 dict（字符串/数字/列表本身），无法继续按属性取
            return None
        if idx is not None:
            if not isinstance(node, list) or int(idx) >= len(node):
                return None
            node = node[int(idx)]
    return node


def transform_schema_paths(text: str, root: dict) -> str:
    """把 text 里出现的 schema path 替换成实际值。

    LLM 偶尔在 narrative 里 quote 了字段路径（如 voc.pain_clusters[5].name）而不是实际值，
    与其 drop 整段 narrative，这里把 path 解析成实际值替换进去——业务能看到正常文本。

    无法解析的 path 保留原文（上游可二次 drop / clear）。
    """
    if not text or not isinstance(text, str):
        return text or ""

    def _replace(m: re.Match) -> str:
        path = m.group(0)
        val = resolve_pack_path(root, path)
        if val is None:
            return path
        if isinstance(val, str):
            return val
        if isinstance(val, bool):
            return str(val)
        if isinstance(val, int):
            return str(val)
        if isinstance(val, float):
            return f'{val:g}'
        return path

    return _SCHEMA_PATH_LEAK_RE.sub(_replace, text)


def _flatten_to_strings(value: Any, max_items: int = 10) -> list[str]:
    """把 resolve 出来的值扁平化为若干"可用于文本匹配的字符串片段"。

    - 数字：["123", "123.45"]
    - 字符串：[value]
    - dict：取 name / keyword / asin / quote / description 等常见字段
    - list：对每个元素递归
    """
    if value is None:
        return []
    out: list[str] = []
    if isinstance(value, (int, float)):
        # 数字：原样 + 去小数
        out.append(f"{value}")
        if isinstance(value, float):
            out.append(f"{int(value)}")
            out.append(f"{value:.1f}")
        return out
    if isinstance(value, str):
        s = value.strip()
        if s:
            out.append(s)
        return out
    if isinstance(value, dict):
        for k in ("name", "keyword", "asin", "quote", "title", "brand", "segment",
                  "complaint", "need", "reason", "stage", "direction", "band",
                  "signal", "description"):
            if k in value:
                out.extend(_flatten_to_strings(value[k], max_items))
        return out[:max_items]
    if isinstance(value, list):
        for item in value[:max_items]:
            out.extend(_flatten_to_strings(item, max_items))
        return out[:max_items]
    return out


def _normalize(text: str) -> str:
    """归一化：去除中英文标点、全半角统一、小写化、去空白。"""
    if not text:
        return ""
    t = text.lower()
    t = re.sub(r"[，。,.;:：；、！!？?「」\"'（）()\[\]【】\s\-_/]+", "", t)
    return t


def validate_evidence(text: str, resolved_values: list[str]) -> bool:
    """text 里是否至少出现了一个 resolved_values 的值（忽略大小写/标点/空白）。

    - 数字：在原文和归一化文本里分别检查子串（保留 "4200mAh" 这类黏连写法）
    - 字符串：归一化后做子串匹配
    - 空列表直接判失败（没有可引用的证据 = 声称有证据是假的）
    """
    if not text or not resolved_values:
        return False
    norm_text = _normalize(text)
    raw_lower = text.lower()
    for v in resolved_values:
        if not v:
            continue
        v_str = str(v)
        # 数字保留原格式匹配
        if re.fullmatch(r"-?\d+(\.\d+)?", v_str):
            if v_str in raw_lower or v_str in norm_text:
                return True
            # 对浮点做 ±5% 容差检查：从 text 里抓出数字，比较最接近的
            try:
                target = float(v_str)
                for m in re.finditer(r"-?\d+(?:\.\d+)?", text):
                    candidate = float(m.group())
                    if abs(candidate - target) <= max(abs(target) * 0.05, 0.01):
                        return True
            except (ValueError, TypeError):
                pass
            continue
        # 字符串子串匹配
        v_norm = _normalize(v_str)
        if v_norm and v_norm in norm_text:
            return True
    return False


def contains_forbidden(text: str) -> bool:
    """文本里是否包含任何禁用词。"""
    if not text:
        return False
    t = text.lower()
    for phrase in FORBIDDEN_PHRASES:
        if phrase.lower() in t:
            return True
    return False


def check_claim(text: str, supporting_fields: list[str], root: dict) -> tuple[bool, str]:
    """检查一条"有叙述 + 有 supporting_fields"的结论是否兑现。

    Returns:
        (is_valid, reason)
        - is_valid=True 时 reason 为空
        - is_valid=False 时 reason 是失效原因（仅用于日志/调试）
    """
    if not text or not text.strip():
        return False, "empty text"
    if contains_forbidden(text):
        return False, "forbidden phrase"
    if not supporting_fields:
        return False, "no supporting_fields"

    resolved: list[str] = []
    for path in supporting_fields:
        value = resolve_pack_path(root, path)
        if value is None:
            continue  # 路径不存在跳过，看其他路径能否兑现
        resolved.extend(_flatten_to_strings(value))

    if not resolved:
        return False, "all supporting paths unresolvable"
    if not validate_evidence(text, resolved):
        return False, "text does not cite resolved evidence"
    return True, ""


def _collect_all_pack_strings(root: dict, max_items: int = 500) -> list[str]:
    """递归扁平化整个 Pack 字典，收集所有字符串/数字值，供 soft 校验用。"""
    out: list[str] = []
    def _walk(node):
        if len(out) >= max_items:
            return
        if isinstance(node, dict):
            for v in node.values():
                _walk(v)
        elif isinstance(node, list):
            for it in node:
                _walk(it)
        elif isinstance(node, (int, float)):
            out.append(f"{node}")
            if isinstance(node, float):
                out.append(f"{int(node)}")
                out.append(f"{node:.1f}")
        elif isinstance(node, str):
            s = node.strip()
            if s and len(s) >= 2:
                out.append(s)
    _walk(root)
    return out[:max_items]


def check_claim_soft(text: str, supporting_fields: list[str], root: dict,
                     all_values_cache: list[str] | None = None) -> tuple[bool, str]:
    """软校验：对叙述性文本（recommendation_reasons / pricing_segment_insights / price_band_insights）
    不强制 supporting_fields 路径严格解析（LLM 偶尔编路径），改为：
    1) 通过标准 check_claim 时直接返回 True
    2) supporting_fields 全部不可解析时，兜底检查 narrative 是否命中 Pack 里**任意**真值（数字/命名/品牌）

    最关键的结论类字段（upgrade_directions / differentiation_angles / entry_recommendation）
    仍然走严格的 check_claim，不走这里。
    """
    if not text or not text.strip():
        return False, "empty text"
    if contains_forbidden(text):
        return False, "forbidden phrase"

    # 先走标准严格校验
    if supporting_fields:
        ok, why = check_claim(text, supporting_fields, root)
        if ok:
            return True, ""
        # 只有在路径不可解析时走兜底；"路径有值但 text 没引用"的情况不兜底（说明 LLM 说假话）
        if why != "all supporting paths unresolvable":
            return False, why

    # 兜底：narrative 里是否命中 Pack 里的任意真值
    cache = all_values_cache if all_values_cache is not None else _collect_all_pack_strings(root)
    if not cache:
        return False, "empty pack cache"
    if validate_evidence(text, cache):
        return True, "soft-matched pack value"
    return False, "narrative cites no pack value even softly"


def _mentions_promo_stat(text: str, packs_dict: dict) -> bool:
    """判断「推广压力」narrative 是否引用了至少一个 stats.promo_* 或 bsr_sp_ads_pct 的具体数字。

    判定方式：从 stats 里取真实数字，转成带 ±5% 容差的范围；narrative 里只要含有一个落在
    任一字段容差范围内的数字就通过。若没有任何 promo 相关 stats 字段，直接放行（Python 端兜底
    已处理这种情况，不在 filter 层再砍）。
    """
    import re as _re
    stats_obj = packs_dict.get("stats") or {}
    promo_keys = [
        "promo_ppc_bid_avg", "promo_ads_competitor_avg",
        "promo_click_share_avg_top", "promo_conversion_share_avg_top",
        "promo_spr_median", "promo_products_median", "bsr_sp_ads_pct",
    ]
    targets: list[float] = []
    for k in promo_keys:
        v = stats_obj.get(k)
        try:
            f = float(v)
            if f > 0:
                targets.append(f)
        except (TypeError, ValueError):
            continue
    if not targets:
        # stats 里完全没有推广压力数据 → 不强制校验（兜底文案会由 Python 直接写）
        return True
    # 把 narrative 里所有数字提出来
    found = [float(m) for m in _re.findall(r"[-+]?\d+(?:\.\d+)?", text)]
    for t in targets:
        lo, hi = t * 0.95, t * 1.05
        for f in found:
            if lo <= f <= hi:
                return True
    return False


def filter_synthesis(synthesis, packs_dict: dict) -> dict:
    """对 Synthesizer 输出（StrategySynthesis pydantic 实例）做 post-check。

    丢弃以下条目：
    - upgrade_directions[*]: justification 未兑现 supporting_fields
    - differentiation_angles[*]: rationale 未兑现 supporting_fields
    - entry_recommendation: reasoning 未兑现（整段保留 recommended_segment，但清空 reasoning）
    - sheet10_final_verdict.dimension_reasons[*]: reason_with_evidence 未兑现（整条丢）
    - sheet6_priority_matrix[*]: action_plan 含禁用词或为空 → 清空 action_plan

    Args:
        synthesis: StrategySynthesis 实例
        packs_dict: {"market": {...}, "voc": {...}, "traffic": {...}, "trend": {...}, "stats": {...}}
    Returns:
        应用到 synthesis 的 model_dump（dict），调用方再重新 model_validate
    """
    data = synthesis.model_dump(mode="json")

    stats_summary = {
        "kept": {"upgrade": 0, "diff": 0, "dim_reason": 0},
        "dropped": {"upgrade": 0, "diff": 0, "dim_reason": 0, "entry": False},
    }

    # upgrade_directions
    kept_upgrades = []
    for up in data.get("upgrade_directions", []):
        text = f"{up.get('target_spec', '')} {up.get('justification', '')}".strip()
        # schema 路径泄露硬拦截：check_claim 只验路径解析后真值是否被引用，
        # 对 narrative 文本里出现 schema 字面量（如 voc.pain_clusters[0].name）放行；
        # contains_schema_path 是独立维度的检查，必须叠加，否则升级建议会带代码字段
        if contains_schema_path(text):
            log.info("[SynthesisFilter] drop upgrade_direction '%s': schema path leaked",
                     up.get("dimension"))
            stats_summary["dropped"]["upgrade"] += 1
            continue
        if contains_absolute_metric(text):
            log.info("[SynthesisFilter] drop upgrade_direction '%s': absolute metric",
                     up.get("dimension"))
            stats_summary["dropped"]["upgrade"] += 1
            continue
        ok, why = check_claim(text, up.get("supporting_fields", []), packs_dict)
        if ok:
            kept_upgrades.append(up)
            stats_summary["kept"]["upgrade"] += 1
        else:
            log.info("[SynthesisFilter] drop upgrade_direction '%s': %s", up.get("dimension"), why)
            stats_summary["dropped"]["upgrade"] += 1
    data["upgrade_directions"] = kept_upgrades

    # differentiation_angles
    kept_diffs = []
    for d in data.get("differentiation_angles", []):
        text = f"{d.get('angle', '')} {d.get('rationale', '')}".strip()
        if contains_schema_path(text):
            log.info("[SynthesisFilter] drop differentiation_angle '%s': schema path leaked",
                     d.get("angle"))
            stats_summary["dropped"]["diff"] += 1
            continue
        if contains_absolute_metric(text):
            log.info("[SynthesisFilter] drop differentiation_angle '%s': absolute metric",
                     d.get("angle"))
            stats_summary["dropped"]["diff"] += 1
            continue
        ok, why = check_claim(text, d.get("supporting_fields", []), packs_dict)
        if ok:
            kept_diffs.append(d)
            stats_summary["kept"]["diff"] += 1
        else:
            log.info("[SynthesisFilter] drop differentiation_angle '%s': %s", d.get("angle"), why)
            stats_summary["dropped"]["diff"] += 1
    data["differentiation_angles"] = kept_diffs

    # entry_recommendation
    entry = data.get("entry_recommendation", {})
    reasoning = entry.get("reasoning", "")
    if reasoning:
        ok, why = check_claim(reasoning, entry.get("supporting_fields", []), packs_dict)
        if not ok:
            log.info("[SynthesisFilter] clear entry.reasoning: %s", why)
            entry["reasoning"] = ""
            stats_summary["dropped"]["entry"] = True
    data["entry_recommendation"] = entry

    # sheet10 dimension_reasons
    verdict = data.get("sheet10_final_verdict", {}) or {}
    kept_dims = []
    for dr in verdict.get("dimension_reasons", []):
        text = dr.get("reason_with_evidence", "")
        # dimension_reasons 的 supporting_fields 在 schema 里没有，弱检查：只查禁用词 + 非空
        if not text.strip():
            stats_summary["dropped"]["dim_reason"] += 1
            continue
        if contains_forbidden(text):
            log.info("[SynthesisFilter] drop dim_reason '%s': forbidden phrase", dr.get("dimension"))
            stats_summary["dropped"]["dim_reason"] += 1
            continue
        if contains_schema_path(text):
            log.info("[SynthesisFilter] drop dim_reason '%s': schema path leaked", dr.get("dimension"))
            stats_summary["dropped"]["dim_reason"] += 1
            continue
        if contains_absolute_metric(text):
            log.info("[SynthesisFilter] drop dim_reason '%s': absolute metric", dr.get("dimension"))
            stats_summary["dropped"]["dim_reason"] += 1
            continue
        # 「推广压力」维度：必须引用 stats.promo_* 或 bsr_sp_ads_pct 的真实数字
        if dr.get("dimension") == "推广压力" and not _mentions_promo_stat(text, packs_dict):
            log.info("[SynthesisFilter] drop dim_reason 推广压力: 未引用任何 stats.promo_*/bsr_sp_ads_pct 数字")
            stats_summary["dropped"]["dim_reason"] += 1
            continue
        kept_dims.append(dr)
        stats_summary["kept"]["dim_reason"] += 1
    verdict["dimension_reasons"] = kept_dims
    # headline 也查禁用词 + schema 路径泄露
    if verdict.get("headline") and contains_forbidden(verdict["headline"]):
        log.info("[SynthesisFilter] clear headline: forbidden phrase")
        verdict["headline"] = ""
    if verdict.get("headline") and contains_schema_path(verdict["headline"]):
        log.info("[SynthesisFilter] clear headline: schema path leaked")
        verdict["headline"] = ""
    if verdict.get("headline") and contains_absolute_metric(verdict["headline"]):
        log.info("[SynthesisFilter] clear headline: absolute metric hallucination")
        verdict["headline"] = ""
    data["sheet10_final_verdict"] = verdict

    # sheet6 priority_matrix：清空含禁用词 / schema 路径 / 绝对化夸张数字 的 action_plan，
    # 并过滤 improvements 列表里的同类污染条目。LLM 经常在这两处编「100%好评率」「零差评」
    # 「100%通过出厂测试」这种伪 KPI——不能写进给业务人员看的报告。
    for item in data.get("sheet6_priority_matrix", []):
        # action_plan：先 transform schema path → 实际值；再过禁用词/绝对值/残留 schema 检测
        ap = item.get("action_plan") or ""
        if ap:
            ap_t = transform_schema_paths(ap, packs_dict)
            if contains_forbidden(ap_t) or contains_absolute_metric(ap_t):
                log.info("[SynthesisFilter] clear sheet6 action_plan '%s': forbidden/absolute (post-transform)",
                         item.get("segment"))
                item["action_plan"] = ""
            elif contains_schema_path(ap_t):
                log.info("[SynthesisFilter] clear sheet6 action_plan '%s': unresolvable schema path",
                         item.get("segment"))
                item["action_plan"] = ""
            else:
                item["action_plan"] = ap_t
        # improvements 列表内逐条 transform 后检查
        kept_imp = []
        for s in (item.get("improvements") or []):
            if not isinstance(s, str):
                continue
            s_t = transform_schema_paths(s, packs_dict)
            if contains_forbidden(s_t) or contains_absolute_metric(s_t):
                log.info("[SynthesisFilter] drop sheet6 improvement '%s' due to bad phrase: %s",
                         item.get("segment"), s_t[:80])
                continue
            if contains_schema_path(s_t):
                log.info("[SynthesisFilter] drop sheet6 improvement '%s': unresolvable schema path: %s",
                         item.get("segment"), s_t[:80])
                continue
            kept_imp.append(s_t)
        item["improvements"] = kept_imp

    # 预算一次所有 Pack 值的扁平化缓存，3 类叙述字段共用（避免每条都递归整个 Pack）
    _all_values_cache = _collect_all_pack_strings(packs_dict)

    # recommendation_reasons：soft 校验（LLM 偶尔编 supporting_fields 路径，只要 narrative 引用真值就保留）
    kept_rr = []
    for rr in data.get("recommendation_reasons", []):
        narr = (rr.get("narrative") or "").strip()
        if not narr or len(narr) < 15:
            continue
        # 禁用词硬拦截（优先于 soft 校验）——避免「广告占比」等假数字叙述落盘
        if contains_forbidden(narr):
            log.info("[SynthesisFilter] drop recommendation_reason '%s': forbidden phrase",
                     rr.get("dimension"))
            continue
        # schema 路径泄露硬拦截（业务可读性硬约束）：narrative 不允许出现 stats.xxx / voc.xxx 等代码字段路径
        if contains_schema_path(narr):
            log.info("[SynthesisFilter] drop recommendation_reason '%s': schema path leaked into narrative",
                     rr.get("dimension"))
            continue
        # 绝对化夸张数字硬拦截：100%好评 / 零差评 / 绝对领跑 等 LLM 编造的虚假 KPI
        if contains_absolute_metric(narr):
            log.info("[SynthesisFilter] drop recommendation_reason '%s': absolute metric hallucination",
                     rr.get("dimension"))
            continue
        # 「推广压力」维度专项白名单：narrative 必须含至少 1 个 stats.promo_* 或 bsr_sp_ads_pct 的真实数字
        if rr.get("dimension") == "推广压力":
            if not _mentions_promo_stat(narr, packs_dict):
                log.info("[SynthesisFilter] drop rec_reason 推广压力: 未引用任何 stats.promo_*/bsr_sp_ads_pct 数字")
                continue
        sf = rr.get("supporting_fields", [])
        ok, why = check_claim_soft(narr, sf, packs_dict, _all_values_cache)
        if not ok:
            log.info("[SynthesisFilter] drop recommendation_reason '%s': %s",
                     rr.get("dimension"), why)
            continue
        kept_rr.append(rr)
    data["recommendation_reasons"] = kept_rr

    # pricing_segment_insights：soft 校验（Sheet 4 推荐入场价表细分叙述）
    kept_psi = []
    for psi in data.get("pricing_segment_insights", []):
        narr = (psi.get("narrative") or "").strip()
        if not narr or len(narr) < 15:
            continue
        if contains_schema_path(narr):
            log.info("[SynthesisFilter] drop pricing_segment_insight '%s': schema path leaked",
                     psi.get("segment"))
            continue
        if contains_absolute_metric(narr):
            log.info("[SynthesisFilter] drop pricing_segment_insight '%s': absolute metric",
                     psi.get("segment"))
            continue
        sf = psi.get("supporting_fields", [])
        ok, why = check_claim_soft(narr, sf, packs_dict, _all_values_cache)
        if not ok:
            log.info("[SynthesisFilter] drop pricing_segment_insight '%s': %s",
                     psi.get("segment"), why)
            continue
        kept_psi.append(psi)
    data["pricing_segment_insights"] = kept_psi

    # price_band_insights：soft 校验（Sheet 4 各价格带入场建议叙述）
    kept_pbi = []
    for pbi in data.get("price_band_insights", []):
        narr = (pbi.get("narrative") or "").strip()
        if not narr or len(narr) < 15:
            continue
        if contains_schema_path(narr):
            log.info("[SynthesisFilter] drop price_band_insight '%s': schema path leaked",
                     pbi.get("band"))
            continue
        if contains_absolute_metric(narr):
            log.info("[SynthesisFilter] drop price_band_insight '%s': absolute metric",
                     pbi.get("band"))
            continue
        sf = pbi.get("supporting_fields", [])
        ok, why = check_claim_soft(narr, sf, packs_dict, _all_values_cache)
        if not ok:
            log.info("[SynthesisFilter] drop price_band_insight '%s': %s",
                     pbi.get("band"), why)
            continue
        kept_pbi.append(pbi)
    data["price_band_insights"] = kept_pbi

    # sheet5_improvement_plan：Sheet 5 第六段 LLM 改进计划
    # 每条单独校验；pain_name 必须匹配 voc.pain_clusters[*].name；root_cause + action_items 合并做 soft 校验
    voc_pains = packs_dict.get("voc", {}).get("pain_clusters", []) or []
    valid_pain_names = {(p.get("name") or "").strip() for p in voc_pains if p.get("name")}
    kept_sip = []
    for sip in data.get("sheet5_improvement_plan", []):
        pname = (sip.get("pain_name") or "").strip()
        items = sip.get("action_items") or []
        rc = (sip.get("root_cause") or "").strip()
        tgt = (sip.get("target_metric") or "").strip()
        # 至少要有 pain_name 和 1 条 action_items
        # 原先 >=2 过严：履约/物流类 pain（如"收到二手/破损"）LLM 往往只给 1 条流程改进，
        # 被 drop 后整行走到渲染层的"本条 LLM 综合未覆盖"占位
        if not pname or len(items) < 1:
            log.info("[SynthesisFilter] drop sheet5_improvement_plan '%s': empty or <1 items",
                     sip.get("priority"))
            continue
        # pain_name 必须在 VOC pain_clusters 里（soft 匹配：容许 LLM 缩写/子串）
        if valid_pain_names and not any(pname in v or v in pname for v in valid_pain_names):
            log.info("[SynthesisFilter] drop sheet5_improvement_plan '%s': pain_name '%s' not in voc clusters",
                     sip.get("priority"), pname)
            continue
        # 禁用词 + soft 证据校验：把所有文字拼一起查一次
        combined = f"{rc} {' '.join(items)} {tgt}".strip()
        if not combined or len(combined) < 20:
            continue
        if contains_forbidden(combined):
            log.info("[SynthesisFilter] drop sheet5_improvement_plan '%s': forbidden phrase",
                     sip.get("priority"))
            continue
        # 不再对 sheet5_improvement_plan 跑 check_claim_soft：
        # action_items 本质是"改进目标叙述"（如"将充电电流从 2A 提升到 5A"），新值本来就不在 pack 里，
        # 字符串比对会把 LLM 合规的产品规格改进计划整条误杀（Battery Chargers 的 P1/P2/P3 就是这样丢失的）。
        # 硬门（pain_name ∈ voc / items>=1 / combined>=20 字 / forbidden_phrase）已足够防止 LLM 胡编。
        kept_sip.append(sip)
    data["sheet5_improvement_plan"] = kept_sip

    log.info("[SynthesisFilter] kept=%s dropped=%s", stats_summary["kept"], stats_summary["dropped"])
    return data
