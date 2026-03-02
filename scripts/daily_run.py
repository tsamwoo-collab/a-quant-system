"""
每日自动运行脚本 - 数据更新和信号生成
"""
import sys
from pathlib import Path
from datetime import datetime
import logging

# 添加项目根目录到路径
sys.path.insert(0, str(Path(__file__).parent.parent))

from data import get_fetcher, get_storage
from factors import MacroFactors, MesoFactors, MicroFactors
from signals import MultiLevelSignalGenerator
from config.settings import STOCK_POOL

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def daily_run():
    """每日任务主函数"""
    logger.info("=" * 50)
    logger.info("开始执行每日任务")
    logger.info("=" * 50)

    start_time = datetime.now()

    try:
        # 初始化
        fetcher = get_fetcher()
        storage = get_storage()
        macro_calc = MacroFactors(fetcher, storage)
        meso_calc = MesoFactors(fetcher, storage)
        micro_calc = MicroFactors(fetcher, storage)
        signal_gen = MultiLevelSignalGenerator(macro_calc, meso_calc, micro_calc)

        today = datetime.now().strftime('%Y-%m-%d')

        # 1. 更新宏观因子数据
        logger.info("Step 1: 更新宏观数据...")
        update_macro_data(fetcher, storage)

        # 2. 获取股票池
        logger.info("Step 2: 获取股票池...")
        stock_list = get_stock_pool(fetcher)
        logger.info(f"股票池共 {len(stock_list)} 只股票")

        # 3. 批量生成信号
        logger.info("Step 3: 生成信号...")
        signals_df = generate_signals(signal_gen, stock_list, storage, today)

        if signals_df is not None and not signals_df.empty:
            # 4. 保存信号
            logger.info("Step 4: 保存信号...")
            save_signals(signals_df, storage, today)

            # 5. 输出摘要
            print_summary(signals_df)

        # 6. 获取行业映射
        logger.info("Step 5: 更新行业映射...")
        meso_calc.get_industry_mapping()

        elapsed = (datetime.now() - start_time).total_seconds()
        logger.info(f"每日任务完成，耗时 {elapsed:.2f} 秒")
        logger.info("=" * 50)

        return True

    except Exception as e:
        logger.error(f"每日任务执行失败: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False


def update_macro_data(fetcher, storage):
    """更新宏观因子数据"""
    # SHIBOR
    try:
        shibor = fetcher.get_shibor(days=30)
        if shibor is not None:
            storage.save_macro(shibor, 'shibor_on')
            logger.info("✓ SHIBOR数据更新成功")
    except Exception as e:
        logger.error(f"✗ SHIBOR数据更新失败: {e}")

    # 北向资金
    try:
        north = fetcher.get_north_flow(days=30)
        if north is not None:
            storage.save_macro(north, 'north_flow_net')
            logger.info("✓ 北向资金数据更新成功")
    except Exception as e:
        logger.error(f"✗ 北向资金数据更新失败: {e}")

    # 市场成交量
    try:
        volume = fetcher.get_market_volume(days=60)
        if volume is not None:
            storage.save_macro(volume, 'market_volume_ratio')
            logger.info("✓ 市场成交量数据更新成功")
    except Exception as e:
        logger.error(f"✗ 市场成交量数据更新失败: {e}")


def get_stock_pool(fetcher):
    """获取股票池"""
    try:
        # 获取沪深300成分股
        constituents = fetcher.get_index_constituents(STOCK_POOL['index'])
        if constituents is None or constituents.empty:
            logger.warning("无法获取指数成分股，使用默认股票池")
            return [
                ('000001', '平安银行'),
                ('600036', '招商银行'),
                ('600519', '贵州茅台'),
                ('000858', '五粮液'),
            ]

        stock_list = list(zip(
            constituents['symbol'].tolist()[:STOCK_POOL.get('max_stocks', 100)],
            constituents.get('name', pd.Series([''] * len(constituents))).tolist()
        ))

        return stock_list

    except Exception as e:
        logger.error(f"获取股票池失败: {e}")
        return []


def generate_signals(signal_gen, stock_list, storage, date):
    """批量生成信号"""
    signals_list = []

    total = len(stock_list)
    for i, (symbol, name) in enumerate(stock_list):
        try:
            if i % 10 == 0:
                logger.info(f"进度: {i}/{total}")

            signal_data = signal_gen.generate_signal(symbol, "")
            signal_data['symbol'] = symbol
            signal_data['name'] = name
            signals_list.append(signal_data)

        except Exception as e:
            logger.debug(f"股票 {symbol} 信号生成失败: {e}")
            continue

    if not signals_list:
        logger.warning("未能生成任何信号")
        return None

    import pandas as pd
    signals_df = pd.DataFrame(signals_list)

    # 排序
    signals_df = signals_df.sort_values('signal', ascending=False)

    return signals_df


def save_signals(signals_df, storage, date):
    """保存信号到数据库"""
    # 保存完整信号数据
    storage.save_signal(signals_df, date)

    # 保存因子数据
    factor_list = []
    for _, row in signals_df.iterrows():
        factors = row.get('factors', {})
        for factor_name, factor_value in factors.items():
            if pd.notna(factor_value):
                factor_list.append({
                    'symbol': row['symbol'],
                    'factor_name': factor_name,
                    'factor_value': factor_value
                })

    if factor_list:
        import pandas as pd
        factor_df = pd.DataFrame(factor_list)
        storage.save_factor(factor_df, date)


def print_summary(signals_df):
    """输出信号摘要"""
    print("\n" + "=" * 50)
    print("今日信号摘要")
    print("=" * 50)

    # 统计各信号等级数量
    level_counts = signals_df['signal_level'].value_counts()
    print("\n信号等级分布:")
    for level, count in level_counts.items():
        print(f"  {level}: {count}")

    # Top 10 买入信号
    strong_buy = signals_df[signals_df['signal'] >= 0.7].head(10)
    if not strong_buy.empty:
        print("\n强买入信号 Top 10:")
        for _, row in strong_buy.iterrows():
            print(f"  {row['symbol']} {row.get('name', '')}: {row['signal']:.3f}")

    # Top 10 卖出信号
    strong_sell = signals_df[signals_df['signal'] <= -0.7].tail(10)
    if not strong_sell.empty:
        print("\n强卖出信号 Bottom 10:")
        for _, row in strong_sell.iterrows():
            print(f"  {row['symbol']} {row.get('name', '')}: {row['signal']:.3f}")

    print("=" * 50 + "\n")


if __name__ == "__main__":
    import pandas as pd  # 确保pandas已导入
    success = daily_run()
    sys.exit(0 if success else 1)
