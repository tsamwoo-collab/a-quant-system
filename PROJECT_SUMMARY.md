# A股量化信号系统 - 项目总结

## 项目概述

基于**宏观-中观-微观**三层因子的A股自动化量化信号系统，具有**交易闭环**和**动能归因**功能。

## 项目结构

```
/Users/candyhu/a-quant-system/
├── config/
│   └── settings.py          # 配置文件（因子、权重、阈值）
│
├── data/                      # 数据模块
│   ├── __init__.py
│   ├── fetchers.py           # AkShare数据获取
│   └── storage.py            # DuckDB数据存储
│
├── factors/                   # 因子模块
│   ├── __init__.py
│   ├── macro.py              # 宏观因子（SHIBOR、北向资金、成交量）
│   ├── meso.py               # 中观因子（行业强度、估值、ROE）
│   └── micro.py              # 微观因子（动量、换手率、BIAS）
│
├── signals/                   # 信号模块
│   ├── __init__.py
│   └── combiner.py           # 因子合成与信号生成
│
├── dashboard/                 # 前端展示
│   ├── __init__.py
│   ├── app.py                # 原始版本（多文件导入）
│   ├── app_allinone.py       # ⭐ 单文件版本（推荐）
│   └── tracking.py           # 持仓追踪模块
│
├── scripts/                   # 脚本
│   ├── __init__.py
│   └── daily_run.py          # 每日自动运行
│
├── requirements.txt           # 依赖包
├── README.md                  # 项目说明
└── tracking.py               # 持仓追踪（根目录副本）
```

## 核心功能

### 📊 信号生成
- **宏观因子** (3个)：SHIBOR、北向资金净流入、市场成交量变化率
- **中观因子** (4个)：行业相对强度、行业排名、PE分位数、ROE趋势
- **微观因子** (5个)：换手率极值、20日动量、BIAS、龙虎榜净买入、两融净买入

### 🔄 交易闭环
- **持仓追踪**：自动记录买入/卖出历史
- **信号对比**：今日 vs 昨日信号变化
- **自动交易建议**：买入/清仓/减仓信号

### 🎯 跨越雷池逻辑
**只在临界突破时触发信号：**

| 触发条件 | 操作 |
|---------|------|
| 昨天 < 0.30 → 今天 ≥ 0.30 | 🚨 **买入** |
| 昨天 > -0.30 → 今天 ≤ -0.30 | ⚠️ **清仓** |

**系统忽略：**
- ❌ 昨天就高、今天仍高的（无跨越）
- ❌ 一直在谷底的（无跨越）

### 📊 动能归因分析
每条交易信号显示各层级变化：
```
🟢 中观行业: -0.10 ➡️ +0.65 (+0.75) (主驱动力)
🟡 微观量价: -0.30 ➡️ -0.05 (+0.25) (企稳)
⚪️ 宏观环境: -0.08 ➡️ -0.11 (-0.03) (无显著变化)
```

## 启动方式

### ⭐ 推荐方式（单文件版本）

```bash
cd /Users/candyhu/a-quant-system/dashboard
streamlit run app_allinone.py --server.port 8501
```

或者

```bash
cd /Users/candyhu/a-quant-system/dashboard
python3 -m streamlit run app_allinone.py --server.port 8501
```

然后浏览器访问：**http://localhost:8501**

### 备选方式

```bash
cd /Users/candyhu/a-quant-system
python3 -m streamlit run dashboard/app.py --server.port 8501
```

## Dashboard 功能

启动后你会看到：

### 📋 交易信号标签页
- 今日交易信号汇总
- 买入/卖出信号列表（带动能归因）
- 优先级标识

### 💼 持仓管理标签页
- 当前持仓列表
- 买入信号、当前信号
- 模拟盈亏

### 📊 信号总览标签页
- 信号分布直方图
- 信号等级饼图
- 完整信号列表

## 操作步骤

### 1. 启动 Dashboard
```bash
cd /Users/candyhu/a-quant-system/dashboard
streamlit run app_allinone.py --server.port 8501
```

### 2. 刷新页面
浏览器访问：http://localhost:8501

### 3. 测试功能
- 点击 **「🔄 刷新数据」** 按钮（多点击几次模拟不同天数）
- 查看 **「📋 交易信号」** 标签页
- 观察动能归因分析

## 已知问题

### 自动启动问题
streamlit 自动启动后立即退出，**需要手动启动**。

**解决方案**：在终端手动执行启动命令，可以看到错误信息。

### 模块导入问题
原始 `app.py` 依赖 `tracking.py` 导入，可能存在路径问题。

**解决方案**：使用 `app_allinone.py` 单文件版本，所有功能已内置。

## 技术栈

- **语言**：Python 3.14
- **数据源**：AkShare（免费）
- **数据库**：DuckDB
- **可视化**：Streamlit + Plotly
- **Web框架**：Streamlit

## 后续扩展方向

1. **真实数据接入**：解决AkShare网络连接问题
2. **回测模块**：验证策略历史表现
3. **风控模块**：仓位管理、止损止盈
4. **飞书推送**：每日自动推送信号
5. **实盘对接**：连接券商API

## 重要文件位置

- ⭐ **主文件**：`/Users/candyhu/a-quant-system/dashboard/app_allinone.py`
- **配置文件**：`/Users/candyhu/a-quant-system/config/settings.py`
- **启动脚本**：`/Users/candyhu/a-quant-system/start.sh`

## 明天继续工作的起点

```bash
# 1. 进入项目目录
cd /Users/candyhu/a-quant-system

# 2. 启动 Dashboard（选择一种方式）

# 方式A：单文件版本（推荐）
cd dashboard
streamlit run app_allinone.py --server.port 8501

# 方式B：完整版本
python3 -m streamlit run dashboard/app.py --server.port 8501

# 3. 浏览器访问
http://localhost:8501
```

## 今日完成的核心功能

✅ 宏观-中观-微观三层因子体系
✅ 持仓追踪模块
✅ 动能归因分析（macro/meso/micro层级）
✅ "跨越雷池"交易信号逻辑
✅ Streamlit Dashboard 可视化
✅ 买入阈值 0.30 / 卖出阈值 -0.30
✅ 自动忽略无跨越的股票

## 关键代码位置

- **因子计算**：`/Users/candyhu/a-quant-system/factors/`
- **持仓追踪**：`/Users/candyhu/a-quant-system/dashboard/tracking.py` 和 `/Users/candyhu/a-quant-system/tracking.py`
- **信号合成**：`/Users/candyhu/a-quant-system/signals/combiner.py`
- **Dashboard**：`/Users/candyhu/a-quant-system/dashboard/app_allinone.py`

---

**创建日期**：2026-02-27
**状态**：MVP 完成，可演示使用
