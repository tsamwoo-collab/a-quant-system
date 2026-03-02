# 数据适配器层架构说明

## 概述

数据适配器层使用**适配器模式**实现了统一的数据接口，支持多种数据源的灵活切换。

```
┌─────────────────────────────────────────────────────┐
│                    业务逻辑层                         │
│  (回测引擎、信号生成、风控模块、Dashboard等)          │
└──────────────────┬──────────────────────────────────┘
                   │
                   │ 统一接口 (IDataAdapter)
                   ▼
┌─────────────────────────────────────────────────────┐
│                  数据适配器层                         │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐ │
│  │ AkShare     │  │  Tushare    │  │  未来扩展... │ │
│  │  Adapter    │  │  Adapter    │  │             │ │
│  └─────────────┘  └─────────────┘  └─────────────┘ │
└──────────────────┬──────────────────────────────────┘
                   │
                   ▼
         ┌─────────────────┐
         │   数据源 API     │
         │ (AkShare/Tushare)│
         └─────────────────┘
```

## 核心优势

1. **解耦数据源与业务逻辑** - 切换数据源无需修改业务代码
2. **统一接口** - 所有数据源使用相同的方法签名
3. **易于测试** - 可以轻松mock适配器进行单元测试
4. **灵活扩展** - 添加新数据源只需实现接口

## 目录结构

```
data/adapters/
├── __init__.py          # 模块导出
├── base.py              # 抽象接口定义
├── akshare_adapter.py   # AkShare 实现（免费）
├── tushare_adapter.py   # Tushare 实现（付费）
└── factory.py           # 工厂模式
```

## 使用方法

### 基础使用

```python
from data.adapters import get_adapter, AdapterConfig

# 1. 使用默认适配器 (AkShare)
adapter = get_adapter()

# 2. 获取股票列表
stock_list = adapter.get_stock_list()
print(f"总股票数: {len(stock_list)}")

# 3. 获取指数成分股
cs300 = adapter.get_index_constituents(index_code="000300")
print(f"沪深300: {len(cs300)} 只")

# 4. 获取日线数据
quotes = adapter.get_daily_quotes(
    symbol="600519",
    start_date="20230101",
    end_date="20231231"
)
print(f"数据条数: {len(quotes)}")

# 5. 批量获取
results = adapter.batch_get_daily_quotes(
    symbols=["600519", "000858", "300750"],
    start_date="20230101",
    end_date="20231231"
)
```

### 切换到 Tushare

```python
config = AdapterConfig(
    adapter_type="tushare",
    tushare_config={
        "token": "your_tushare_token_here",
        "timeout": 30,
        "max_retries": 3
    }
)

adapter = get_adapter(config)

# 后续代码完全相同
stock_list = adapter.get_stock_list()
```

## 接口定义

### IDataAdapter 核心方法

| 方法 | 说明 | 返回值 |
|------|------|--------|
| `get_stock_list()` | 获取A股列表 | DataFrame (symbol, name, market, industry) |
| `get_index_constituents(code)` | 获取指数成分股 | DataFrame (symbol, name, weight) |
| `get_daily_quotes(symbol, start, end)` | 获取日线数据 | DataFrame (date, open, high, low, close, volume) |
| `batch_get_daily_quotes(symbols, ...)` | 批量获取日线 | Dict[symbol, DataFrame] |
| `get_financial(symbol)` | 获取财务数据 | DataFrame (pe, pb, ps, roe, roa) |
| `get_macro_shibor(days)` | 获取SHIBOR | DataFrame (date, 隔夜, 1周, ...) |
| `get_macro_north_flow(days)` | 获取北向资金 | DataFrame (date, net_flow_in) |
| `health_check()` | 健康检查 | Dict (status, message, latency) |

## 数据源对比

| 特性 | AkShare | Tushare |
|------|---------|---------|
| **费用** | 免费 | 付费（积分制） |
| **稳定性** | 中等 | 高 |
| **数据质量** | 良好 | 优秀 |
| **更新频率** | 日度 | 日度/分钟级 |
| **历史数据** | 有一定限制 | 完整 |
| **适用场景** | 开发测试 | 生产环境 |
| **获取Token** | 无需 | https://tushare.pro |

## 沪深300下载脚本

位置: `scripts/download_cs300.py`

```bash
# 交互式运行
python3 scripts/download_cs300.py

# 选择:
# - 数据源: AkShare (免费) / Tushare (付费)
# - 时间范围: 1年 / 2年 / 3年

# 数据保存到: data/cs300_2years.duckdb
```

## 扩展新数据源

如需添加新的数据源（如Wind、同花顺等）：

1. 创建新文件 `data/adapters/wind_adapter.py`
2. 实现 `IDataAdapter` 接口
3. 在 `factory.py` 中注册

```python
from data.adapters import IDataAdapter
from data.adapters.factory import register_adapter

class WindAdapter(IDataAdapter):
    # 实现所有接口方法
    pass

# 注册
register_adapter('wind', WindAdapter)
```

## 配置文件示例

`config/data_source.yaml`:

```yaml
# 默认数据源配置
adapter_type: akshare

# AkShare 配置
akshare_config:
  max_retries: 3
  retry_delay: 2.0
  proxy: null

# Tushare 配置（使用时取消注释）
# adapter_type: tushare
# tushare_config:
#   token: "your_token_here"
#   timeout: 30
#   max_retries: 3
```

## 常见问题

**Q: 为什么需要数据适配器层？**
A: 不同的数据源API差异很大，适配器层统一了接口，使得业务代码不需要关心底层使用的是哪个数据源。

**Q: AkShare够用吗？**
A: 对于开发和测试阶段完全够用。但如果需要更高数据质量和稳定性，建议切换到Tushare。

**Q: 如何批量下载全市场数据？**
A: 使用 `scripts/batch_download.py`，注意全市场下载可能需要较长时间。

**Q: 数据存储在哪里？**
A: 使用DuckDB存储，默认路径为 `data/real_market.duckdb` 或 `data/cs300_2years.duckdb`

## 更新日志

- **2026-02-28**: 初始版本，支持AkShare和Tushare
