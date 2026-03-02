# A股量化信号系统

基于宏观-中观-微观三层因子的A股自动化量化信号生成系统。

## 系统架构

```
┌─────────────────────────────────────────────────────────┐
│                    A股量化信号系统                         │
├─────────────────────────────────────────────────────────┤
│                                                          │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐  │
│  │    宏观      │    │    中观      │    │    微观      │  │
│  │  • SHIBOR   │    │  • 行业强度  │    │  • 换手率    │  │
│  │  • 北向资金  │    │  • 行业排名  │    │  • 动量      │  │
│  │  • 成交量    │    │  • 估值分位  │    │  • BIAS     │  │
│  │             │    │  • ROE趋势   │    │  • 龙虎榜    │  │
│  └──────┬──────┘    └──────┬──────┘    └──────┬──────┘  │
│         └──────────────────┼──────────────────┘         │
│                            ▼                            │
│                 ┌──────────────────┐                    │
│                 │   信号合成引擎    │                    │
│                 └────────┬─────────┘                    │
│                          ▼                              │
│                 ┌──────────────────┐                    │
│                 │   Web Dashboard  │                    │
│                 └──────────────────┘                    │
└─────────────────────────────────────────────────────────┘
```

## 因子库

### 宏观因子
| 因子名称 | 说明 |
|---------|------|
| shibor_on | SHIBOR隔夜利率，反映市场流动性 |
| north_flow_net | 北向资金净流入，反映外资态度 |
| market_volume_ratio | 市场成交量变化率，反映市场活跃度 |

### 中观因子
| 因子名称 | 说明 |
|---------|------|
| industry_relative_strength | 行业相对强度，行业vs市场表现 |
| industry_rank | 行业涨跌幅排名 |
| industry_pe_percentile | 行业PE分位数，反映行业估值水平 |
| industry_roe_trend | 行业ROE趋势，反映行业盈利能力 |

### 微观因子
| 因子名称 | 说明 |
|---------|------|
| turnover_extreme | 换手率极值 |
| momentum_20d | 20日动量因子 |
| bias | 均线偏离度 |
| longhubang_net_buy | 龙虎榜净买入 |
| margin_net_buy | 两融净买入 |

## 快速开始

### 1. 安装依赖

```bash
cd a-quant-system
pip install -r requirements.txt
```

### 2. 运行 Dashboard

```bash
streamlit run dashboard/app.py
```

浏览器访问 http://localhost:8501

### 3. 运行每日任务

```bash
python scripts/daily_run.py
```

## 项目结构

```
a-quant-system/
├── config/              # 配置文件
│   └── settings.py      # 系统配置
│
├── data/                # 数据模块
│   ├── fetchers.py      # AkShare数据获取
│   └── storage.py       # DuckDB数据存储
│
├── factors/             # 因子模块
│   ├── macro.py         # 宏观因子
│   ├── meso.py          # 中观因子
│   └── micro.py         # 微观因子
│
├── signals/             # 信号模块
│   └── combiner.py      # 因子合成与信号生成
│
├── dashboard/           # 前端展示
│   └── app.py           # Streamlit应用
│
├── scripts/             # 脚本
│   └── daily_run.py     # 每日自动运行
│
├── logs/                # 日志目录
├── data/                # 数据存储目录
└── requirements.txt     # 依赖包
```

## 配置说明

编辑 `config/settings.py` 修改系统配置：

```python
# 因子权重
FACTOR_CONFIG = {
    "macro": {"weight": 0.2},
    "meso": {"weight": 0.3},
    "micro": {"weight": 0.5},
}

# 信号阈值
SIGNAL_CONFIG = {
    "threshold": {
        "strong_buy": 0.7,
        "buy": 0.3,
        "sell": -0.3,
        "strong_sell": -0.7,
    }
}
```

## 后续扩展

- [ ] 飞书机器人推送
- [ ] 回测模块
- [ ] 风险控制模块
- [ ] 机器学习因子
- [ ] 实盘对接
