# 飞书推送配置指南

## 📱 配置飞书Webhook推送

### 第一步：创建飞书机器人

1. **打开飞书群聊**（需要推送信号的群）

2. **添加群机器人**
   - 点击群聊右上角 "..."
   - 选择 "设置"
   - 选择 "群机器人"
   - 点击 "添加机器人"

3. **选择自定义机器人**
   - 选择 "自定义"
   - 点击 "添加"

4. **配置机器人**
   - 机器人名称: "量化信号推送"
   - 描述: "每日交易信号自动推送"

5. **获取Webhook地址**
   - 添加成功后会显示 Webhook 地址
   - 格式类似: `https://open.feishu.cn/open-apis/bot/v2/hook/xxxxxxxxxxxxxxxxx`

### 第二步：配置Webhook地址

#### 方法1: 使用配置文件（推荐）

1. 创建配置目录
```bash
mkdir -p config
```

2. 保存webhook地址
```bash
echo "https://open.feishu.cn/open-apis/bot/v2/hook/你的地址" > config/feishu_webhook.txt
```

3. 验证配置
```bash
cat config/feishu_webhook.txt
```

#### 方法2: 使用环境变量

```bash
export FEISHU_WEBHOOK_URL="https://open.feishu.cn/open-apis/bot/v2/hook/你的地址"
```

### 第三步：测试推送

```bash
cd /Users/candyhu/a-quant-system
python3 scripts/daily_signals.py
```

如果配置正确，你会收到飞书消息推送。

---

## 📅 设置定时任务

### 安装定时任务

```bash
cd /Users/candyhu/a-quant-system
chmod +x scripts/setup_scheduler.sh
./scripts/setup_scheduler.sh
```

### 定时任务说明

- **执行时间**: 每周一到周五 下午3点半
- **自动执行**: 无需手动操作
- **日志文件**: `logs/scheduler.log`

### 查看日志

```bash
# 实时查看日志
tail -f logs/scheduler.log

# 查看最近日志
tail -20 logs/scheduler.log
```

---

## 🔧 管理定时任务

### 查看当前定时任务
```bash
crontab -l
```

### 编辑定时任务
```bash
crontab -e
```

### 删除定时任务
```bash
crontab -e
# 然后删除所有相关行
```

### 重启定时任务
```bash
# macOS
sudo launchctl stop cron
sudo launchctl start cron

# Linux
sudo service cron restart
```

---

## 📊 推送消息格式

```
==================================================
📊 量化信号日报 - 2026-02-27
==================================================

📈 市场状态: 强趋势市 (ADX=28.5) → 建议持仓

🟢 买入信号 (3只):
  1. 600519 (信号: 0.65, 价格: 1820.00)
  2. 000858 (信号: 0.58, 价格: 158.00)
  3. 002415 (信号: 0.52, 价格: 48.50)

🔴 卖出信号: 无

==================================================
```

---

## ⚠️ 常见问题

### Q1: 没有收到推送？
- 检查webhook地址是否正确
- 查看日志文件确认是否执行
- 确认机器人是否在群聊中

### Q2: 推送消息格式错乱？
- 飞书不支持Markdown，使用纯文本格式

### Q3: 定时任务没有执行？
- 检查cron服务是否运行
- 查看日志文件确认
- 确认脚本路径正确

---

## 📞 需要帮助？

如果遇到问题，请提供以下信息：
1. 错误截图或日志
2. 定时任务列表 (`crontab -l`)
3. 日志文件内容 (`tail -20 logs/scheduler.log`)
