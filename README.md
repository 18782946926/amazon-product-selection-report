# 亚马逊选品报告生成系统

基于 Flask + 通义千问（DashScope）的 Web 服务，把亚马逊任意品类的选品评估**自动化**：上传卖家精灵导出的 4 份数据，5 分钟出一份 10-sheet 完整 Excel 报告。**品类无关**——LED 灯、储钱罐、便携发电机、电池充电器、洗车枪、割草机均验证可跑。

## 核心特性

- 🖼️ **视觉 LLM 二审**：调用 Qwen-VL-Max 对每个 ASIN 的主图 + 标题 + bullet points 做形态/材质/动作三标签分析，比纯文本聚类更准
- 🧩 **14 个 LLM 分析器**：BSR / VOC / 关键词 / Market / Spec / LabelMerger / Sheet5 改进 / Lifecycle 等并发跑，单条慢一点不阻塞整体
- 🔢 **数值算法 + LLM 文案**：八维评分、结构型净利率、盈亏平衡采购上限等数值由代码算（不让 LLM 编造）；定性结论由 LLM 写
- 🛡️ **每分析器独立降级**：单一 LLM 调用失败回退本地兜底模板，报告永远能生成
- 🪟 **多窗口并发**：业务员同时开 2-3 个浏览器标签跑不同品类报告，会话隔离、互不串扰（文件名 uuid + 状态文件 sid 隔离 + 视觉 LLM per-key semaphore 钳）
- 🔑 **LLM Key Pool**：支持 `DASHSCOPE_API_KEYS=key1,key2,key3` 多 key round-robin，配 N 个 key → N 个标签真并发不抢配额
- 📦 **零安装打包版**：内部分发提供 `选品报告生成系统.zip`（含嵌入式 Python），同事解压双击 `启动.bat` 即用

## 10-Sheet 报告内容

| # | Sheet | 内容 |
|---|---|---|
| 1 | 市场分析 | 类目规模、价格分布、产品类型收益对比、市场结论 |
| 2 | 竞争分析 | 品牌集中度、中国卖家占比、新品存活窗口、BuyBox 美卖头部品牌 |
| 3 | BSR TOP100 | 完整产品数据 + LLM 视觉标签（O/P 列：形态/材质归类） |
| 4 | 入场利润测算 | 推荐入场售价、5 项成本扣减、结构型净利、成本结构优化空间、盈亏平衡采购上限（5 档） |
| 5 | 重点 ASIN 参数 | Top10 表 + 重点 ASIN 参数深度、卖点/痛点矩阵、改进方向 |
| 6 | 产品上新方向 | P1-P4 优先级矩阵 + 数据驱动的上新叙述 |
| 7 | 评论汇总 | 原始评论数据 |
| 8 | 关键词分析 | 关键词竞争度 / PPC Bid / 贡献度排名（需上传 ExpandKeywords 文件） |
| 9 | US 市场品类 | 品类级 Top 产品与价格区间（需上传 US-Market 文件） |
| 10 | 综合结论 | 首推入场子品类（推荐功能参数 + 差异化升级方向 + 结构型净利率验证） |

## 快速开始（开发环境）

### 1. 克隆

```bash
git clone https://github.com/18782946926/amazon-product-selection-report.git
cd amazon-product-selection-report
```

### 2. 装环境

```bash
python -m venv venv
# Windows
venv\Scripts\activate
# macOS/Linux
source venv/bin/activate

pip install -r requirements.txt
```

### 3. 配 API key

```bash
cp .env.example .env
# 编辑 .env，填入 DASHSCOPE_API_KEY=sk-xxx（阿里云百炼）
```

多窗口并发想真隔离，填多 key（用逗号分隔）：

```bash
DASHSCOPE_API_KEYS=sk-aaa,sk-bbb,sk-ccc
```

### 4. 启动

```bash
python app.py
```

浏览器打开 `http://localhost:8002`（端口可在 `.env` 的 `FLASK_PORT` 调）。

## 输入数据规范

| 文件 | 是否必填 | 来源 | 命名约定 |
|---|---|---|---|
| BSR Top100 | ✅ 必填 | 卖家精灵 BSR 导出（含 "US" sheet） | `BSR(品类名)-100-US-日期.xlsx` |
| 评论 | 可选（建议） | 卖家精灵 Reviews 导出 | `ASIN-US-Reviews-日期.xlsx` |
| ExpandKeywords 关键词 | 可选 | 卖家精灵 ExpandKeywords 导出 | `ExpandKeywords-US-...日期.xlsx` |
| US-Market 品类 | 可选 | 卖家精灵 Last-30-days 导出 | `US-Market-<品类>-Last-30-days-...xlsx` |

品类由 BSR 文件名自动识别，无需手动选择。

## 操作流程

1. 网站首页上传 BSR（必填）+ 其他 3 份（可选）
2. 点「生成选品评估报告」
3. 等 **2-5 分钟**（首次冷启动需调视觉 LLM 逐 ASIN 分析；同份数据二次跑约 30-60s 命中缓存）
4. 完成后浏览器自动下载 .xlsx

## 给内部同事用的零安装版

打包好的 `D:\选品报告生成系统\选品报告生成系统.zip`（约 120MB）发给同事：解压 → 双击根目录 `启动.bat` → 浏览器自动开 `http://localhost:8002` → 直接用。内含独立 Python + 全部依赖，零环境配置。

## 目录结构

```
web_report_generator/
├── app.py                    # Flask 主程序（路由 + Excel 生成）
├── requirements.txt
├── .env.example              # 配置模板（复制为 .env 填 key）
├── README.md
├── core/
│   ├── packs_runtime.py      # 5 个并发 analyzer 编排 + 视觉聚类
│   └── asin_collection_*.py  # ASIN 采集清单生成
├── llm/
│   ├── client.py             # LLM client（含 cache、retry）
│   ├── key_pool.py           # 多 key round-robin 池
│   ├── cache.py              # SHA256 内容哈希缓存
│   ├── analyzers/            # 14 个分析器（BSR/VOC/视觉/Spec 等）
│   ├── prompts/              # 各分析器对应的 prompt
│   ├── providers/            # qwen / doubao provider 适配
│   └── schemas.py            # Pydantic 输出 schema
├── config/llm_config.py      # LLM provider 配置
├── utils/                    # 文件名解析、日志、批处理工具
├── templates/index.html      # 前端表单页面
├── llm_cache/                # 运行时缓存（gitignored，可删）
├── reports/                  # 生成的报告（gitignored）
└── uploads/                  # 临时上传文件（gitignored）
```

## 常见问题

**Q: 报告生成 5 分钟太慢，能加速吗？**
单跑 5 分钟里视觉 LLM（100 ASIN × 8 路并发）占大头，DashScope 默认配额下基本是物理极限。如果业务员有真并发需求，**不是缩短单跑时间**，而是让多窗口并发跑——配多 key + 在阿里云后台申请提额。

**Q: 「out of quota」或 429 报错？**
阿里云百炼控制台「限流提额」菜单申请提高 RPM/TPM/QPS。需要申请的模型：`qwen-plus`、`qwen-max`（或 `qwen3-max`）、`qwen-vl-max`（或 `qwen3-vl-plus`）。

**Q: 端口 8002 被占用？**
编辑 `.env` 的 `FLASK_PORT` 改成其他端口，重启 `python app.py`。

**Q: 想本地测试不烧 LLM 钱？**
设置环境变量 `LLM_CACHE_REUSE=1`，所有 LLM 缓存复用（仅 dev 调样式用，不要用于真实业务）。

**Q: 文件名/状态文件冲突（多人同时跑）？**
新版已修：每个 /upload 请求生成独立 session_id，文件名加 uuid 后缀，状态文件按 sid 隔离，前端轮询带 sid 参数。多窗口并发不会再撞。

## 技术支持

GitHub issues：https://github.com/18782946926/amazon-product-selection-report/issues

## 许可证

MIT License
