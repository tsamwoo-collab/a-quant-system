"""
配置文件 - 定义因子、参数、数据源等
"""
from pathlib import Path
from datetime import time
from typing import Dict, List

# ============= 项目路径 =============
BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / "data"
LOG_DIR = BASE_DIR / "logs"
DB_PATH = DATA_DIR / "quant.db"

# ============= 数据源配置 =============
DATA_SOURCE = "akshare"

# AkShare 数据接口映射
AKSHARE_ENDPOINTS = {
    # 宏观数据
    "shibor": "tool_shibor",              # SHIBOR利率
    "north_flow": "tool_money_market_hsgt",  # 北向资金
    "market_volume": "stock_zh_a_hist",    # 市场成交量（用指数代替）

    # 中观数据
    "industry_index": "stock_sector_spot",     # 行业指数
    "industry_flow": "tool_money_flow_hsgt",   # 行业资金流

    # 微观数据
    "stock_daily": "stock_zh_a_hist",          # 日线行情
    "stock_financial": "stock_financial_analysis_indicator",  # 财务指标
    "longhubang": "tool_stock_lhb_detail_daily",  # 龙虎榜
    "margin": "tool_stock_margin_detail_sz",      # 两融（深交所）
}

# ============= 因子定义 =============
FACTOR_CONFIG: Dict[str, Dict[str, List[str]]] = {
    "macro": {
        "factors": [
            "shibor_on",          # SHIBOR隔夜利率
            "north_flow_net",     # 北向资金净流入
            "market_volume_ratio", # 市场成交量变化率
        ],
        "weight": 0.2,  # 宏观因子权重
    },
    "meso": {
        "factors": [
            "industry_relative_strength",  # 行业相对强度
            "industry_rank",               # 行业涨跌幅排名
            "industry_pe_percentile",      # 行业PE分位数
            "industry_roe_trend",          # 行业ROE趋势
        ],
        "weight": 0.3,  # 中观因子权重
    },
    "micro": {
        "factors": [
            "turnover_extreme",       # 日均换手率极值
            "momentum_20d",           # 20日动量
            "bias",                   # 均线偏离度
            "longhubang_net_buy",     # 龙虎榜净买入
            "margin_net_buy",         # 两融净买入
        ],
        "weight": 0.5,  # 微观因子权重
    },
}

# ============= 信号配置 =============
SIGNAL_CONFIG = {
    "threshold": {
        "strong_buy": 0.7,   # 强买入阈值
        "buy": 0.3,          # 买入阈值
        "sell": -0.3,        # 卖出阈值
        "strong_sell": -0.7, # 强卖出阈值
    },
    "normalization": "zscore",  # 标准化方法: zscore, minmax, rank
    "combine_method": "weighted",  # 合成方法: weighted, equal, ic_weighted
}

# ============= 股票池配置 =============
STOCK_POOL = {
    "index": "000300",  # 沪深300作为基础池
    "exclude_st": True,  # 排除ST
    "exclude_new": True, # 排除新股（上市不足60天）
    "min_market_cap": 50,  # 最小市值（亿）
    "max_stocks": 100,   # 最大股票数量
}

# ============= 调度配置 =============
SCHEDULE_CONFIG = {
    "daily_run": {
        "time": time(18, 0),  # 每天18:00运行
        "timezone": "Asia/Shanghai",
    },
    "market_open": {
        "time": time(9, 30),
        "weekday": "1-5",  # 周一到周五
    },
}

# ============= Dashboard 配置 =============
DASHBOARD_CONFIG = {
    "title": "A股量化信号系统",
    "port": 8501,
    "refresh_interval": 300,  # 5分钟刷新
}

# ============= 日志配置 =============
LOG_CONFIG = {
    "level": "INFO",
    "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    "file": LOG_DIR / "quant_{date}.log",
}

# ============= 数据更新配置 =============
UPDATE_CONFIG = {
    "force_update": False,      # 是否强制更新
    "parallel": True,           # 是否并行下载
    "max_workers": 4,           # 最大工作线程
    "retry_times": 3,           # 失败重试次数
}
