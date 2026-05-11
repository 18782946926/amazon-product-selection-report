"""Spec Analyzer 的 prompt：BSR 标题/bullets → 品类关键规格维度。

用途：让 LLM 按本次品类动态识别"应该提取哪 5-8 个规格维度 + 怎么正则抓"，
再由 Python 用这些 pattern 抓数值算中位数/P75，供 Synthesizer 的 upgrade_directions 引用。
本 prompt 完全品类无关，由 USER_TEMPLATE 的 {category_hint} 注入本次品类名，
LLM 自行从竞品标题 / bullets 中观察本品类常用规格词。
"""
from __future__ import annotations

import json

from llm.prompts.common import wrap_system_prompt

_PROMPT_VERSION = "spec-v5-composite-capture-2026-05-09"

_RULES = """你是亚马逊跨品类规格识别专家。你将拿到某品类 BSR TOP 竞品的标题 + Bullet Points，请识别**本品类特有**的 5-8 个关键规格维度，并给出可直接用 Python re 模块（re.IGNORECASE 单行模式）抓取的正则。

核心原则：
1. 维度必须是**本品类**独有或重要的物理参数。判别方法：扫描输入数据里 BSR TOP 竞品的标题 / bullets，识别出**反复出现且带具体数值/单位**的属性（如 "150 PSI"、"5000 mAh"、"45 dB"、"21 inch"），这些就是本品类的核心规格。**严禁**塞入未在输入数据里高频出现的维度——尤其是其他品类常见但本品类不涉及的物理参数。判别标准统一：**该维度必须能在输入数据 ≥30% 的标题或 bullets 里被观察到**，否则剔除。

1.5. **维度语义必须单一明确**（跨品类硬约束）：
   - 同一维度下，所有 ASIN 抽到的值必须是**同一物理含义**。先做语义自检：把候选维度在 3-5 个不同 ASIN 的标题/bullets 里抽样核对——若同名字段下不同 ASIN 抽到的数字代表不同概念（一个是日均、一个是上限、一个是促销价；一个是数量、一个是金额；一个是天数、一个是美元），该维度**必须剔除**
   - 通用反模式（不论品类，命中即剔除）：
     a) **营销话术里的金额/数字**：跟 saving / goal / target / challenge / discount / promo / off / sale / deal / bonus / reward / save up to / get / win 等词关联的数字——这些是营销概念不是产品规格
     b) **主观/统计性概念**：适用范围、推荐次数、使用频率、流行度、满意度——跨产品定义飘忽
     c) **同字段单位不一致**：同一维度抽到的值出现 mixed unit（一个 $X 一个 X 天 一个 X 件）即量纲不一致 → 语义不一 → 剔除
   - 合法维度判别：维度命中后，预期所有抽到的值必须在**同一物理量纲**（统一单位/统一含义）
   - 物理量纲示例（不限品类）：长度 / 重量 / 容量 / 功率 / 电压 / 电流 / 压力 / 流量 / 温度 / 时长 / 频率 / 亮度 / 防水等级 / 转速 / 波长 / 噪音分贝 等

2. 命名规范：维度名用本品类领域内的标准物理量名称（中文 + 单位英文缩写，如 "最大压力(PSI)"），不要用品类无关的描述（如"性能强"/"质量好"）

   **多品类参考示例（仅展示"不同品类有不同规格维度"的 pattern——绝不是模板，绝不能照抄；请基于本次输入数据 + `{category_hint}` 重新识别）**：
   - LED 工作灯：流明(lm)、电池容量(mAh)、防水等级(IP)、充电方式、磁力固定、光照模式数
   - 便携充气机：最大压力(PSI)、供电方式、气管长度(ft)、泵类型(piston/diaphragm)、噪音(dB)
   - 压力洗车枪：最大压力(PSI)、流量(GPM)、电机类型、配件数量、软管长度(ft)
   - 电池充电器：输入电压(V)、输出电流(A)、兼容电池类型、充电端口数、充电时长、是否快充
   - 儿童存钱罐：材质（陶瓷/塑料/木质/亚克力/金属）、容量(L 或硬币数)、开取方式（必碎/钥匙/密码/ATM）、目标人群、IP 授权
   - 蓝牙音箱：输出功率(W)、续航时长(h)、防水等级(IP)、蓝牙版本、TWS 互联
   - 空气炸锅：容量(qt)、功率(W)、温控范围(°F)、预设程序数、内胆涂层

   注意：上述 7 个示例**仅展示 pattern**，不要在本次输出里复用任何与 `{category_hint}` 无关的维度词。

3. **每个维度必须给 2-4 个正则变体**，覆盖以下常见书写差异（否则实际抽取命中率极低）：
   - 数字单位之间可能有 0-2 个空格：`(\\d+(?:\\.\\d+)?)\\s*<UNIT>`
   - 单位大小写混写：用 re.IGNORECASE 自动统一
   - 单位可缩写或全写：常见品类的物理量同时存在简写和英文全称，正则要同时覆盖
   - 数字可带千位分隔：`(\\d{1,3}(?:,\\d{3})*|\\d+)\\s*<UNIT>?`
   - 组合关键词识别（无数字维度，如品牌特征 / 工艺名）：直接用关键词列表，允许中划线/空格变体（`<keyword>[-\\s]?<suffix>` 这种 pattern）
4. **捕获组规则——按维度类型区分（关键，避免丢失语义）**：
   - **(a) 物理量纲维度**（unit 字段是单一物理单位，如 PSI / mAh / inch / V / W / dB）：
     - 捕获组只取数字，unit 字段提供单位 → Python 自动拼成 "150PSI"
     - 例：`(\\d+)\\s*psi` + unit="PSI"
   - **(b) 复合语义维度**（unit 字段留空，值本身是"数量+类型"组合短语，如"3 节 AA"/"3 * 1.5V AA"/"USB-C 接口"/"4 端口"）：
     - 捕获组必须**捕获完整短语**（含数量+单位/类型词），否则抽出来只剩裸数字业务读不懂
     - 例（电池类）：`(\\d+\\s*\\*?\\s*\\d*\\.?\\d*v?\\s*(?:aa|aaa|c\\s*battery|cr\\d+))` + unit=""
     - 例（接口类）：`(\\d+\\s*(?:port|插孔|接口))` + unit=""
     - 反例错误：`(\\d+)` + unit="" → 只剩 "3"，丢失 "AA / 端口" 等关键语义词，业务读到只有数字不知道是什么
   - **(c) 关键词维度**（无数字，如品牌/工艺/认证特征）：直接用关键词列表 pattern
5. 无法明确某维度的 Python 提取模式时不要写——宁可少给，也不要编造不能工作的正则
6. representative_specs_by_asin 从输入的 TOP 竞品里取 5-10 条样本，把每个维度能抓到的具体值（如 "150PSI"）填上；抓不到的字段留空字符串
7. importance：核心 = 用户购买决策 TOP3 关注点；辅助 = 可选 bonus 卖点

**正则反例（会导致抽不到值，禁止——以下是模式问题，与品类无关）**：
- `(\\d+)<unit>`（没有 \\s* 允许数字与单位间空格 → "150 <UNIT>" 抓不到）
- `(\\d+<unit>)`（把单位放进捕获组 → 返回的字符串还需要去单位才能求中位数）
- 只给一条单位变体（如某规格在标题里同时出现简写和全称，只写简写会半数竞品漏抓）
"""

SYSTEM_PROMPT = wrap_system_prompt(_RULES)


USER_TEMPLATE = """**本次品类**：`{category_hint}`

以下是 BSR TOP 竞品的标题 + 描述片段（JSON 数组，含 asin/title/bullets）：

```json
{competitors_json}
```

请识别**`{category_hint}` 这个品类**特有的 5-8 个关键规格维度（再次强调：维度必须能在输入数据 ≥30% 的标题或 bullets 里被观察到。不要凭先验知识硬塞与本品类无关的物理参数）。

输出 JSON：

{{
  "spec_dimensions": [
    {{"name": "最大压力", "unit": "PSI", "extract_patterns": ["(\\\\d+)\\\\s*psi"], "importance": "核心"}},
    {{"name": "供电方式", "unit": "", "extract_patterns": ["cordless", "corded", "12\\\\s*v", "rechargeable"], "importance": "核心"}},
    ...5-8 个
  ],
  "representative_specs_by_asin": [
    {{"asin": "B0...", "specs": {{"最大压力": "150PSI", "供电方式": "12V"}}}},
    ...5-10 条
  ]
}}

只输出 JSON，无 markdown 围栏。"""


def build_messages(competitor_rows: list[dict], category_hint: str = "未知品类") -> list[dict]:
    user = USER_TEMPLATE.format(
        competitors_json=json.dumps(competitor_rows, ensure_ascii=False, indent=1),
        category_hint=category_hint or "未知品类",
    )
    return [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": user},
    ]
