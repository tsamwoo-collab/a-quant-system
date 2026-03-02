# GitHub Actions 配置指南

## 📋 需要配置的 Secrets

在 GitHub 仓库中设置以下 Secrets（Settings → Secrets and variables → Actions）：

### 1. TUSHARE_TOKEN
你的 Tushare API Token
- 来源: https://tushare.pro/user/token
- 用途: 获取股票数据

### 2. FEISHU_WEBHOOK
飞书机器人的 Webhook URL
- 来源: 飞书群 → 设置 → 群机器人 → 自定义机器人
- 格式: `https://open.feishu.cn/open-apis/bot/v2/hook/...`

## 🚀 部署步骤

### 1. 初始化 Git 仓库
```bash
cd /Users/candyhu/a-quant-system
git init
git add .
git commit -m "Initial commit: 量化交易系统"
```

### 2. 在 GitHub 创建仓库
- 访问 https://github.com/new
- 创建新仓库（可以命名为 `a-quant-system`）
- 不要初始化 README

### 3. 推送代码
```bash
git remote add origin https://github.com/你的用户名/a-quant-system.git
git branch -M main
git push -u origin main
```

### 4. 配置 Secrets
1. 进入仓库页面：Settings → Secrets and variables → Actions
2. 点击 "New repository secret"
3. 添加以下两个 secret：
   - Name: `TUSHARE_TOKEN`, Value: 你的 Tushare Token
   - Name: `FEISHU_WEBHOOK`, Value: 你的飞书 Webhook URL

### 5. 启用 Actions
1. 进入 Actions 页面
2. 点击 "I understand my workflows, go ahead and enable them"
3. 在左侧选择 "每日量化信号生成" workflow
4. 点击 "Run workflow" 手动测试

## 📅 运行时间

- **自动运行**: 北京时间 19:30（工作日）
- **手动运行**: 在 Actions 页面点击 "Run workflow"

## 📊 轻量级版本说明

这个版本不保存本地数据库，每次运行时：
1. 从 Tushare API 获取最近 120 天的数据
2. 计算信号并推送到飞书
3. 不持久化任何数据文件

优点：
- 不需要本地数据库
- 运行快速（约 1-2 分钟）
- 不占用存储空间

缺点：
- 每次都需要下载数据
- 没有历史数据记录
- 受 Tushare API 频率限制

## 🔧 本地与 GitHub 共存

本地机器和 GitHub Actions 可以同时运行：
- 本地: 19:30 通过 crontab 运行 `daily_signals.py`（使用本地数据库）
- GitHub: 19:30 通过 workflow 运行 `daily_signals_lite.py`（实时数据）

如果本地电脑关机，GitHub Actions 会继续工作，确保你每天都能收到信号推送。
