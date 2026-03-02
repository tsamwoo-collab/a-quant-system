"""
下载2018年沪深300数据用于样本外盲测 (修复版)
"""
import pandas as pd
import numpy as np
import akshare as ak
from datetime import datetime
import time
import duckdb
import os

def get_cs300_stocks():
    """获取沪深300成分股列表"""
    print("正在获取沪深300成分股列表...")

    try:
        # 获取沪深300成分股
        cs300 = ak.index_stock_cons(symbol="000300")

        # 获取成分股代码
        stocks = cs300['品种代码'].tolist()

        print(f"✅ 获取到 {len(stocks)} 只沪深300成分股")

        return stocks

    except Exception as e:
        print(f"❌ 获取沪深300成分股失败: {e}")
        return []

def download_2018_data(stocks):
    """下载2018年数据"""
    print("\n开始下载2018年数据...")

    # 数据库路径
    db_path = "data/cs300_2018.duckdb"
    os.makedirs("data", exist_ok=True)

    # 创建数据库
    conn = duckdb.connect(db_path)

    # 创建表
    conn.execute("""
        CREATE TABLE IF NOT EXISTS quotes (
            date DATE,
            symbol VARCHAR,
            open FLOAT,
            high FLOAT,
            low FLOAT,
            close FLOAT,
            volume BIGINT,
            amount FLOAT,
            PRIMARY KEY (date, symbol)
        )
    """)

    stats = {'success': 0, 'failed': 0, 'failed_list': []}
    all_data = []

    for i, stock in enumerate(stocks):
        try:
            print(f"[{i+1}/{len(stocks)}] 下载 {stock}...")

            # 转换股票代码格式
            ak_symbol = f"sh{stock}" if stock.startswith('6') else f"sz{stock}"

            # 下载2018年数据
            df = ak.stock_zh_a_daily(
                symbol=ak_symbol,
                start_date="20180101",
                end_date="20181231",
                adjust="qfq"
            )

            if df.empty:
                print(f"  ⚠️ {stock} 2018年无数据")
                stats['failed'] += 1
                stats['failed_list'].append(stock)
                continue

            # 确保日期索引存在
            if 'date' not in df.columns:
                df = df.reset_index()

            # 检查列名
            print(f"  列名: {list(df.columns)}")

            # 选择需要的列（处理列名可能不同的情况）
            required_cols = ['date', 'open', 'high', 'low', 'close', 'volume']
            available_cols = [c for c in required_cols if c in df.columns]

            if len(available_cols) < 6:
                print(f"  ⚠️ {stock} 缺少必需列: {set(required_cols) - set(available_cols)}")
                stats['failed'] += 1
                stats['failed_list'].append(stock)
                continue

            # 选择需要的列
            df = df[available_cols].copy()
            df['symbol'] = stock
            df['date'] = pd.to_datetime(df['date'])

            # 添加缺失的可选列
            if 'amount' not in df.columns:
                df['amount'] = 0.0

            # 收集数据
            all_data.append(df)

            print(f"  ✅ {stock} 2018年数据: {len(df)} 条")
            stats['success'] += 1

            # 延迟避免请求过快
            time.sleep(0.3)

        except Exception as e:
            print(f"  ❌ {stock} 下载失败: {e}")
            stats['failed'] += 1
            stats['failed_list'].append(stock)

    # 批量插入数据
    if all_data:
        print(f"\n批量插入 {len(all_data)} 只股票的数据...")
        combined_df = pd.concat(all_data, ignore_index=True)

        conn.register('quotes_df', combined_df)
        conn.execute("INSERT INTO quotes SELECT * FROM quotes_df")

        print(f"✅ 成功插入 {len(combined_df)} 条记录")

    # 统计
    print(f"\n=== 下载完成 ===")
    print(f"成功: {stats['success']}")
    print(f"失败: {stats['failed']}")

    # 验证数据
    result = conn.execute("""
        SELECT
            COUNT(DISTINCT symbol) as stock_count,
            COUNT(DISTINCT date) as date_count,
            MIN(date) as min_date,
            MAX(date) as max_date,
            COUNT(*) as total_records
        FROM quotes
    """).fetchdf()

    print(f"\n=== 数据统计 ===")
    print(f"股票数量: {result['stock_count'].iloc[0]}")
    print(f"交易日期: {result['date_count'].iloc[0]} 天")
    print(f"日期范围: {result['min_date'].iloc[0]} 至 {result['max_date'].iloc[0]}")
    print(f"总记录数: {result['total_records'].iloc[0]}")

    conn.close()

    return stats

def verify_data():
    """验证下载数据"""
    db_path = "data/cs300_2018.duckdb"

    if not os.path.exists(db_path):
        print("❌ 数据库文件不存在")
        return

    conn = duckdb.connect(db_path)

    # 查看样本数据
    print("\n=== 样本数据 ===")
    sample = conn.execute("""
        SELECT * FROM quotes
        ORDER BY date, symbol
        LIMIT 10
    """).fetchdf()
    print(sample)

    # 查看每个股票的数据量
    print("\n=== 各股票数据量（前10）===")
    counts = conn.execute("""
        SELECT symbol, COUNT(*) as count
        FROM quotes
        GROUP BY symbol
        ORDER BY count DESC
        LIMIT 10
    """).fetchdf()
    print(counts)

    conn.close()

if __name__ == "__main__":
    print("=" * 50)
    print("下载2018年沪深300数据")
    print("=" * 50)

    # 获取成分股
    stocks = get_cs300_stocks()

    if stocks:
        # 下载数据
        download_2018_data(stocks)

        # 验证数据
        verify_data()

        print("\n✅ 完成！数据已保存到: data/cs300_2018.duckdb")
        print("   可以在Dashboard中选择「真实数据」进行2018年盲测")
    else:
        print("❌ 无法获取股票列表")
