"""
下载2018年沪深300数据用于样本外盲测
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

        # 备用方案：使用已知的主要成分股
        # 沪深300主要权重股
        major_stocks = [
            '600519', '601318', '601939', '600036', '000858',
            '600030', '601012', '000333', '600276', '601166',
            '600887', '000002', '600031', '600048', '600104',
            '600690', '601888', '000651', '600000', '601328'
        ]
        print(f"使用备用方案：{len(major_stocks)} 只主要成分股")
        return major_stocks

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

            # 重置索引并处理
            df = df.reset_index()

            # 选择需要的列
            df = df[['date', 'open', 'high', 'low', 'close', 'volume']]
            df['symbol'] = stock
            df['date'] = pd.to_datetime(df['date'])
            df['amount'] = 0.0

            # 删除旧数据
            conn.execute("DELETE FROM quotes WHERE symbol = ?", [stock])

            # 插入新数据
            conn.register('temp_df', df)
            conn.execute("INSERT INTO quotes SELECT * FROM temp_df")

            print(f"  ✅ {stock} 2018年数据: {len(df)} 条")
            stats['success'] += 1

            # 延迟避免请求过快
            time.sleep(0.5)

        except Exception as e:
            print(f"  ❌ {stock} 下载失败: {e}")
            stats['failed'] += 1
            stats['failed_list'].append(stock)

    # 统计
    print(f"\n=== 下载完成 ===")
    print(f"成功: {stats['success']}")
    print(f"失败: {stats['failed']}")

    if stats['failed_list']:
        print(f"失败列表: {stats['failed_list'][:10]}...")

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
    print("\n=== 各股票数据量 ===")
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

    # 下载数据
    download_2018_data(stocks)

    # 验证数据
    verify_data()

    print("\n✅ 完成！数据已保存到: data/cs300_2018.duckdb")
    print("   可以在Dashboard中选择「真实数据」进行2018年盲测")
