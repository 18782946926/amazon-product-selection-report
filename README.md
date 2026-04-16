# 选品报告生成系统

一个基于 Flask 的 Web 服务，将亚马逊选品分析脚本封装为网页工具，支持多人同时使用。

## 功能特性

- 📊 **一键生成报告**：上传 BSR 数据和评论文件，自动生成完整的选品评估报告
- 📋 **6 大报告模块**：市场分析、竞争分析、BSR TOP100、竞品卖点与差评、推荐入场价、评论汇总
- 👥 **多人共享**：无需每个人安装 Python，通过浏览器即可使用
- 📱 **响应式设计**：支持电脑和手机访问
- 🔒 **数据安全**：文件仅在服务器临时处理，不持久化存储

## 系统要求

- Python 3.8 或更高版本
- 内存建议 2GB 以上
- 硬盘空间 1GB 以上

## 本地安装与运行

### 1. 安装依赖

```bash
# 进入项目目录
cd web_report_generator

# 创建虚拟环境（推荐）
python -m venv venv

# 激活虚拟环境
# Windows:
venv\Scripts\activate
# macOS/Linux:
source venv/bin/activate

# 安装依赖
pip install -r requirements.txt
```

### 2. 运行服务

```bash
python app.py
```

### 3. 访问服务

打开浏览器，访问：`http://localhost:5000`

## 部署到云服务器

### Linux 服务器部署（Nginx + Gunicorn）

#### 1. 安装系统依赖

```bash
# Ubuntu/Debian
sudo apt update
sudo apt install python3 python3-pip python3-venv nginx

# CentOS/RHEL
sudo yum install python3 python3-pip nginx
```

#### 2. 上传代码并安装

```bash
# 创建项目目录
sudo mkdir -p /var/www/report_generator
sudo chown -R $USER:$USER /var/www/report_generator

# 上传代码到该目录
# 然后进入目录
cd /var/www/report_generator

# 创建虚拟环境
python3 -m venv venv
source venv/bin/activate

# 安装依赖
pip install -r requirements.txt
pip install gunicorn
```

#### 3. 配置 Gunicorn

创建 `gunicorn_config.py`：

```python
bind = "127.0.0.1:5000"
workers = 4
worker_class = "sync"
max_requests = 1000
timeout = 120
```

#### 4. 启动 Gunicorn

```bash
# 启动服务
gunicorn -c gunicorn_config.py app:app

# 设置开机自启（使用 systemd）
sudo nano /etc/systemd/system/report-generator.service
```

写入以下内容：

```ini
[Unit]
Description=Report Generator Service
After=network.target

[Service]
User=www-data
Group=www-data
WorkingDirectory=/var/www/report_generator
Environment="PATH=/var/www/report_generator/venv/bin"
ExecStart=/var/www/report_generator/venv/bin/gunicorn -c /var/www/report_generator/gunicorn_config.py app:app
Restart=always

[Install]
WantedBy=multi-user.target
```

启用服务：

```bash
sudo systemctl enable report-generator
sudo systemctl start report-generator
```

#### 5. 配置 Nginx 反向代理

```bash
sudo nano /etc/nginx/sites-available/report-generator
```

写入以下内容：

```nginx
server {
    listen 80;
    server_name your-domain.com;  # 替换为你的域名或IP

    location / {
        proxy_pass http://127.0.0.1:5000;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_read_timeout 120s;
        client_max_body_size 100M;
    }
}
```

启用站点：

```bash
sudo ln -s /etc/nginx/sites-available/report-generator /etc/nginx/sites-enabled/
sudo nginx -t
sudo systemctl restart nginx
```

#### 6. 配置 HTTPS（可选但推荐）

使用 Let's Encrypt 免费证书：

```bash
sudo apt install certbot python3-certbot-nginx
sudo certbot --nginx -d your-domain.com
```

### Windows Server 部署（IIS）

1. 安装 Python via [Python.org](https://www.python.org/downloads/)
2. 安装 IIS URL Rewrite 模块
3. 使用 FastCGI 部署 Flask 应用
4. 配置反向代理到 Flask 端口

### Docker 部署（推荐）

创建 `Dockerfile`：

```dockerfile
FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .

EXPOSE 5000
CMD ["python", "app.py"]
```

创建 `docker-compose.yml`：

```yaml
version: '3.8'
services:
  report-generator:
    build: .
    ports:
      - "5000:5000"
    volumes:
      - ./uploads:/app/uploads
      - ./reports:/app/reports
    restart: unless-stopped
```

运行：

```bash
docker-compose up -d
```

## 使用方法

### 1. 准备数据文件

**BSR 数据文件：**
- 使用卖家精灵导出 BSR Excel 文件
- 文件需包含 "US" 工作表
- 建议命名格式：`BSR(...)-100-US-日期.xlsx`

**评论数据文件：**
- 使用卖家精灵导出评论 Excel 文件
- 文件名需包含 "Reviews"
- 建议命名格式：`ASIN-US-Reviews-日期.xlsx`
- 可上传多个评论文件

### 2. 上传文件

1. 打开网站首页
2. 点击"选择文件"上传 BSR 数据文件（必填）
3. 可选择上传多个评论文件
4. 点击"生成选品评估报告"

### 3. 下载报告

等待报告生成完成后，浏览器会自动下载 Excel 文件。

## 报告包含内容

| Sheet | 内容说明 |
|-------|---------|
| 市场分析 | 类目规模、价格分布、产品类型收益对比 |
| 竞争分析 | 品牌集中度、中国卖家占比、新品存活率 |
| BSR TOP100 | 完整产品数据表 |
| 竞品卖点与差评 | 正向卖点与差评痛点分析 |
| 推荐入场价 | 各产品类型定价建议 |
| 评论汇总 | 原始评论数据 |

## 目录结构

```
web_report_generator/
├── app.py                 # Flask 主程序
├── requirements.txt       # Python 依赖
├── README.md              # 说明文档
├── templates/
│   └── index.html         # 前端页面
├── uploads/               # 临时上传目录（自动创建）
└── reports/               # 生成的报告目录（自动创建）
```

## 常见问题

### Q: 上传文件时报错"文件太大"
A: 默认限制 100MB，如需调整，修改 `app.py` 中的 `MAX_CONTENT_LENGTH`

### Q: 报告生成很慢
A: 这是正常的，500条评论大约需要10-30秒处理时间

### Q: 如何添加更多产品类型分类？
A: 修改 `app.py` 中的 `classify_product()` 函数

### Q: 如何支持其他数据源格式？
A: 修改 `generate_report()` 函数中的列名映射逻辑

## 技术支持

如有问题，请联系开发者。

## 许可证

MIT License
