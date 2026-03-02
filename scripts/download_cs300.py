"""
下载沪深300成分股2年历史数据
使用数据适配器模式，方便切换数据源
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
import duckdb
from datetime import datetime, timedelta
from data.adapters import get_adapter, AdapterConfig, list_adapters


def progress_callback(current: int, total: int, symbol: str):
    """进度回调"""
    percent = current / total * 100
    print(f"\r  进度: {current}/{total} ({percent:.1f}%) - 最新: {symbol}", end='', flush=True)


def download_cs300_data(
    adapter_type: str = "akshare",
    years: int = 2,
    db_path: str = "data/cs300_2years.duckdb"
):
    """下载沪深300数据

    Args:
        adapter_type: 数据源类型 (akshare/tushare)
        years: 历史年数
        db_path: 数据库保存路径
    """
    print("=" * 60)
    print(f"🚀 下载沪深300成分股 {years}年历史数据")
    print("=" * 60)

    # 创建适配器
    config = AdapterConfig(adapter_type=adapter_type)
    adapter = get_adapter(config)

    print(f"\n📊 数据源: {adapter.name}")
    print(f"   类型: {'付费' if adapter.is_paid else '免费'}")

    # 健康检查
    print("\n🔍 健康检查...")
    health = adapter.health_check()
    print(f"   状态: {health['status']}")
    print(f"   消息: {health['message']}")
    if health['status'] != 'healthy':
        print(f"⚠️ 数据源连接异常，是否继续？")
        return

    # 初始化数据库
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    conn = duckdb.connect(db_path)

    # 创建表
    conn.execute("""
        CREATE TABLE IF NOT EXISTS stock_list (
            symbol VARCHAR PRIMARY KEY,
            name VARCHAR,
            market VARCHAR,
            industry VARCHAR,
            list_date DATE,
            update_time TIMESTAMP
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS daily_quotes (
            date DATE,
            symbol VARCHAR,
            open FLOAT,
            high FLOAT,
            low FLOAT,
            close FLOAT,
            volume BIGINT,
            amount FLOAT,
            turnover_rate FLOAT,
            PRIMARY KEY (date, symbol)
        )
    """)

    # 获取沪深300成分股
    print("\n📋 获取沪深300成分股...")
    stock_list = adapter.get_index_constituents(index_code="000300")

    if stock_list.empty:
        print("❌ 获取成分股失败")
        return

    print(f"✅ 成分股数量: {len(stock_list)}")

    # 去重并保存股票列表
    stock_list_df = stock_list[['symbol', 'name']].copy()
    stock_list_df = stock_list_df.drop_duplicates(subset=['symbol'], keep='first')
    stock_list_df['market'] = stock_list_df['symbol'].apply(
        lambda x: 'SH' if x.startswith('6') else 'SZ'
    )
    stock_list_df['industry'] = ''
    stock_list_df['list_date'] = None
    stock_list_df['update_time'] = datetime.now()

    print(f"   去重后: {len(stock_list_df)} 只")

    conn.register('stock_list_df', stock_list_df)
    conn.execute("DELETE FROM stock_list")
    conn.execute("INSERT INTO stock_list SELECT * FROM stock_list_df")
    conn.unregister('stock_list_df')

    # 计算日期范围
    end_date = datetime.now().strftime("%Y%m%d")
    start_date = (datetime.now() - timedelta(days=years*365)).strftime("%Y%m%d")

    print(f"\n⏰ 时间范围: {start_date} 至 {end_date}")
    print(f"   预计数据量: ~{len(stock_list_df) * years * 250} 条")

    # 批量下载日线数据
    print("\n📥 开始下载日线数据...")
    symbols = stock_list_df['symbol'].tolist()

    results = adapter.batch_get_daily_quotes(
        symbols=symbols,
        start_date=start_date,
        end_date=end_date,
        batch_size=20,
        callback=progress_callback
    )

    print(f"\n✅ 下载完成: {len(results)}/{len(symbols)} 只股票")

    # 保存数据到数据库
    print("\n💾 保存数据到数据库...")

    success_count = 0
    total_records = 0

    for symbol, df in results.items():
        if df.empty:
            continue

        try:
            # 删除旧数据
            conn.execute("DELETE FROM daily_quotes WHERE symbol = ?", [symbol])

            # 插入新数据
            conn.register('quotes_df', df)
            conn.execute("""
                INSERT INTO daily_quotes
                SELECT date, symbol, open, high, low, close, volume, amount, turnover_rate
                FROM quotes_df
            """)
            conn.unregister('quotes_df')

            success_count += 1
            total_records += len(df)
        except Exception as e:
            print(f"\n  保存 {symbol} 失败: {e}")

    # 数据库统计
    print("\n" + "=" * 60)
    print("📊 下载统计")
    print("=" * 60)
    print(f"  股票总数: {len(stock_list)}")
    print(f"  下载成功: {success_count}")
    print(f"  下载失败: {len(stock_list) - success_count}")
    print(f"  数据记录: {total_records:,} 条")

    # 查询数据范围
    date_range = conn.execute("""
        SELECT
            MIN(date) as start_date,
            MAX(date) as end_date,
            COUNT(DISTINCT symbol) as symbol_count
        FROM daily_quotes
    """).fetchone()

    print(f"\n  实际日期范围: {date_range[0]} 至 {date_range[1]}")
    print(f"  实际股票数: {date_range[2]}")

    conn.close()

    print(f"\n✅ 数据已保存到: {db_path}")


def interactive_download():
    """交互式下载"""
    # 显示可用数据源
    print("\n可用的数据源:")
    adapters = list_adapters()
    for i, (key, info) in enumerate(adapters.items(), 1):
        paid_status = "付费" if info['is_paid'] else "免费"
        print(f"  {i}. {info['name']} - {info['description']} ({paid_status})")

    # 选择数据源
    print("\n选择数据源:")
    print("  1. AkShare (推荐，免费)")
    print("  2. Tushare (需要 token)")

    choice = input("\n请选择 (1-2, 默认1): ").strip() or "1"

    adapter_map = {"1": "akshare", "2": "tushare"}
    adapter_type = adapter_map.get(choice, "akshare")

    # Tushare 需要输入 token
    config = None
    if adapter_type == "tushare":
        token = input("\n请输入 Tushare Token: ").strip()
        if not token:
            print("❌ Token 不能为空，切换到 AkShare")
            adapter_type = "akshare"
        else:
            config = AdapterConfig(
                adapter_type="tushare",
                tushare_config={"token": token}
            )

    # 选择时间范围
    print("\n选择时间范围:")
    print("  1. 1年 (快速测试)")
    print("  2. 2年 (推荐)")
    print("  3. 3年")

    years_choice = input("\n请选择 (1-3, 默认2): ").strip() or "2"
    years_map = {"1": 1, "2": 2, "3": 3}
    years = years_map.get(years_choice, 2)

    # 数据库名称
    db_name = f"data/cs300_{years}years.duckdb"

    print(f"\n配置:")
    print(f"  数据源: {adapters[adapter_type]['name']}")
    print(f"  时间范围: {years} 年")
    print(f"  数据库: {db_name}")

    confirm = input("\n确认开始下载? (y/n): ").strip().lower()
    if confirm != 'y':
        print("已取消")
        return

    # 开始下载
    if config:
        download_cs300_data(
            adapter_type=adapter_type,
            years=years,
            db_path=db_name
        )
    else:
        download_cs300_data(
            adapter_type=adapter_type,
            years=years,
            db_path=db_name
        )


if __name__ == "__main__":
    try:
        interactive_download()
    except KeyboardInterrupt:
        print("\n\n⚠️ 用户中断")
    except Exception as e:
        print(f"\n❌ 错误: {e}")
        import traceback
        traceback.print_exc()
