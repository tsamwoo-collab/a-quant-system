"""
每日信号生成和推送（含持仓管理）
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from backtest.signal_strategies import AdaptiveStrategyADX, get_strategy, list_strategies
from backtest.stop_loss import PositionTracker
from scripts.position_manager import PositionManager, process_trading_signals
from scripts.feishu_bitable import FeishuBitable
from scripts.tushare_local_db import TushareLocalDB
from scripts.tushare_downloader import TushareDataDownloader

# 配置
STRATEGY_NAME = "adaptive_adx_rest"  # 自适应ADX+空仓
BUY_THRESHOLD = 0.30
SELL_THRESHOLD = -0.30
MAX_POSITIONS = 5
POSITION_SIZE = 20000
INITIAL_CASH = 100000

# 止盈止损参数
INITIAL_STOP = 0.08
TRAILING_STOP = 0.08
TAKE_PROFIT = 0.30

# 持仓管理器
position_manager = PositionManager()

# 数据源配置
USE_TUSHARE_LOCAL = True  # 是否使用 Tushare 本地数据库
TUSHARE_DB_PATH = "data/tushare_db.duckdb"  # Tushare 本地数据库路径
LEGACY_DB_PATH = "data/cs300_2years.duckdb"  # 原始数据库路径
USE_ADJ_PRICE = True  # 是否使用前复权价格

# 买入过滤参数
MIN_ADX = 25  # 最小 ADX 值（趋势强度）
MIN_PE = 0  # 最小市盈率（剔除亏损股，pe_ttm > 0）
MIN_MARKET_CAP = 300000  # 最小市值（万元，剔除极小微盘股）
MIN_WINNER_RATE = 70  # 最小赢利比例%（筹码分布，上方套牢盘少）

# 日期修正（如系统时间错误可设置）
FORCE_YEAR = None  # 强制使用指定年份，None 表示使用系统时间

def get_market_data(db_path: str = None):
    """
    获取市场数据（支持多数据源）

    Args:
        db_path: 数据库路径（可选）

    Returns:
        dict: {
            'quotes': DataFrame(date, symbol, close, volume),
            'latest_date': str,
            'price_df': DataFrame,
            'volume_df': DataFrame
        }
    """
    import duckdb

    # 确定数据源
    if USE_TUSHARE_LOCAL:
        # 使用 Tushare 本地数据库
        db = TushareLocalDB(db_path or TUSHARE_DB_PATH)
        print("📊 使用 Tushare 本地数据库")

        # 获取股票列表
        stocks = db.get_stock_list()
        if stocks.empty:
            print("⚠️  Tushare 数据库为空，切换到备用数据源")
            return get_market_data_legacy()

        # 获取最新日期
        latest_date = db.get_latest_date()
        if not latest_date:
            print("⚠️  Tushare 数据库无数据，切换到备用数据源")
            return get_market_data_legacy()

        print(f"📅 最新数据日期: {latest_date}")

        # 获取沪深300成分股（或前300只）
        cs300_codes = db.get_cs300_stocks()[:300]
        print(f"📈 查询股票: {len(cs300_codes)} 只")

        # 获取最近数据（使用复权价格）
        quotes = db.get_daily_data(
            ts_codes=cs300_codes,
            start_date=None,  # 获取所有可用数据
            end_date=None,
            use_adj=USE_ADJ_PRICE  # 使用前复权价格
        )

        if quotes.empty:
            print("⚠️  未获取到数据，切换到备用数据源")
            return get_market_data_legacy()

        # 转换日期格式 (YYYYMMDD -> YYYY-MM-DD)
        quotes['date'] = pd.to_datetime(quotes['trade_date'], format='%Y%m%d').dt.strftime('%Y-%m-%d')

        # 按最新日期筛选
        latest_date = quotes['date'].max()
        latest_date_str = latest_date  # 保存字符串格式
        latest_data = quotes[quotes['date'] == latest_date].copy()

        # 使用复权价格（如果可用）
        price_col = 'close_qfq' if USE_ADJ_PRICE and 'close_qfq' in quotes.columns else 'close'
        print(f"💰 使用价格类型: {'前复权' if price_col == 'close_qfq' else '原始'}")

        # 转换为策略格式
        price_df = latest_data.pivot(index='date', columns='ts_code', values=price_col)
        volume_df = latest_data.pivot(index='date', columns='ts_code', values='vol')

        return {
            'quotes': quotes,
            'latest_date': latest_date_str,
            'price_df': price_df,
            'volume_df': volume_df,
            'data_source': f'Tushare本地数据库({"前复权价格" if price_col == "close_qfq" else "原始价格"})'
        }

    else:
        # 使用原始数据库
        return get_market_data_legacy(db_path)


def get_market_data_legacy(db_path: str = None):
    """使用原始数据库获取数据"""
    import duckdb

    if db_path is None:
        db_path = LEGACY_DB_PATH

    print(f"📊 使用原始数据库: {db_path}")
    conn = duckdb.connect(db_path)

    # 获取最新数据
    quotes = conn.execute("""
        SELECT date, symbol, close, volume
        FROM daily_quotes
        ORDER BY date DESC, symbol
    """).fetchdf()

    if quotes.empty:
        return {"error": "数据库为空"}

    # 获取最新日期的数据
    latest_date = quotes['date'].max()
    latest_data = quotes[quotes['date'] == latest_date].copy()

    # 转换为策略需要的格式
    price_df = latest_data.pivot(index='date', columns='symbol', values='close')
    volume_df = latest_data.pivot(index='date', columns='symbol', values='volume')

    return {
        'quotes': quotes,
        'latest_date': latest_date,
        'price_df': price_df,
        'volume_df': volume_df,
        'data_source': '原始数据库'
    }


def generate_daily_signals(db_path: str = None):
    """
    生成每日交易信号（含多层过滤和持仓管理）

    Args:
        db_path: 数据库路径（可选，默认使用 Tushare 本地数据库）

    Returns:
        dict: 信号和持仓信息
    """
    # 获取市场数据
    market_data = get_market_data(db_path)

    if "error" in market_data:
        return market_data

    quotes = market_data['quotes']
    latest_date = market_data['latest_date']
    price_df = market_data['price_df']
    volume_df = market_data['volume_df']

    print(f"✅ 数据加载完成 ({market_data['data_source']})")

    # 获取最新交易日数据（用于价格查询）
    price_col = 'close_qfq' if USE_ADJ_PRICE and 'close_qfq' in quotes.columns else 'close'
    latest_data = quotes[quotes['date'] == latest_date].copy()

    # 生成动能信号
    strategy = get_strategy(STRATEGY_NAME)
    signal_df = strategy.generate(price_df, volume_df)

    # 获取当前日期的信号
    current_signals = signal_df.iloc[-1] if len(signal_df) > 0 else pd.Series()

    # ===== 多层过滤 =====
    print("\n" + "="*50)
    print("🔍 开始多层过滤")
    print("="*50)

    # 获取过滤数据
    db = TushareLocalDB() if USE_TUSHARE_LOCAL else None
    trade_date_for_api = latest_date.replace('-', '')  # YYYYMMDD 格式

    # 1. ADX 过滤
    print(f"\n【过滤1/4】ADX 趋势过滤 (>{MIN_ADX})")
    adx_strategy = AdaptiveStrategyADX()
    latest_adx = 0
    adx_passed = False

    try:
        if USE_TUSHARE_LOCAL and db:
            # 获取完整的股票列表，而不是只用当前有的
            all_cs300 = db.get_cs300_stocks()[:200]  # 使用200只
            start_date = (datetime.now() - timedelta(days=120)).strftime("%Y-%m-%d")  # 延长到120天
            historical_quotes = db.get_daily_data(
                ts_codes=all_cs300,
                start_date=start_date,
                end_date=None,
                use_adj=USE_ADJ_PRICE
            )

            if not historical_quotes.empty:
                historical_quotes['date'] = pd.to_datetime(historical_quotes['trade_date'], format='%Y%m%d').dt.strftime('%Y-%m-%d')
                hist_price_col = 'close_qfq' if USE_ADJ_PRICE and 'close_qfq' in historical_quotes.columns else 'close'
                historical_price_df = historical_quotes.pivot(index='date', columns='ts_code', values=hist_price_col)

                # 过滤掉数据不足的股票
                historical_price_df = historical_price_df.dropna(axis=1, thresh=60)  # 至少60天数据

                if len(historical_price_df) > 0:
                    adx_regime = adx_strategy._calculate_market_adx(historical_price_df)
                    latest_adx = adx_regime.iloc[-1] if len(adx_regime) > 0 else 0
                else:
                    latest_adx = 0

        if latest_adx >= MIN_ADX:
            adx_passed = True
            print(f"  ✅ ADX={latest_adx:.1f} >= {MIN_ADX}")
        else:
            print(f"  ❌ ADX={latest_adx:.1f} < {MIN_ADX}（市场趋势不足，不建议买入）")
    except Exception as e:
        print(f"  ⚠️ ADX 计算失败: {e}")

    # 2. 停牌过滤
    print(f"\n【过滤2/4】停牌过滤")
    suspended_stocks = []
    if db:
        suspended_stocks = db.get_suspended_stocks(trade_date_for_api)
        print(f"  停牌股票: {len(suspended_stocks)} 只")

    # 3. 基本面过滤
    print(f"\n【过滤3/4】基本面过滤 (PE>{MIN_PE}, 市值>{MIN_MARKET_CAP}万)")
    basic_data = {}
    if db:
        daily_basic = db.get_daily_basic(trade_date=trade_date_for_api)
        if not daily_basic.empty:
            for _, row in daily_basic.iterrows():
                basic_data[row['ts_code']] = {
                    'pe_ttm': row['pe_ttm'],
                    'total_mv': row['total_mv']
                }
            print(f"  基本面数据: {len(basic_data)} 只股票")

    # 4. 筹码分布过滤
    print(f"\n【过滤4/4】筹码分布过滤 (winner_rate>{MIN_WINNER_RATE}%)")
    cyq_data = {}
    if db:
        cyq_perf = db.get_cyq_perf(
            ts_codes=list(price_df.columns),
            trade_date=trade_date_for_api
        )
        if not cyq_perf.empty:
            for _, row in cyq_perf.iterrows():
                cyq_data[row['ts_code']] = {
                    'winner_rate': row['winner_rate']
                }
            print(f"  筹码数据: {len(cyq_data)} 只股票")

    print("\n" + "="*50)

    # 应用多层过滤到买入信号
    buy_signals_raw = current_signals[current_signals >= BUY_THRESHOLD].sort_values(ascending=False)
    buy_signals = []

    print(f"\n原始买入信号: {len(buy_signals_raw)} 只 (动能得分 >= {BUY_THRESHOLD})")

    for symbol, signal_score in buy_signals_raw.items():
        # 获取价格
        price_val = latest_data[latest_data['ts_code'] == symbol][price_col].values
        if len(price_val) == 0:
            continue
        current_price = float(price_val[0])

        # 过滤链
        filter_reasons = []

        # 过滤1: ADX
        if not adx_passed:
            filter_reasons.append("ADX不足")
            continue

        # 过滤2: 停牌
        if symbol in suspended_stocks:
            filter_reasons.append("停牌")
            continue

        # 过滤3: 基本面
        if symbol in basic_data:
            basic = basic_data[symbol]
            if basic['pe_ttm'] <= MIN_PE:
                filter_reasons.append(f"PE={basic['pe_ttm']:.2f}")
                continue
            if basic['total_mv'] <= MIN_MARKET_CAP:
                filter_reasons.append(f"市值={basic['total_mv']:.0f}万")
                continue

        # 过滤4: 筹码分布
        if symbol in cyq_data:
            cyq = cyq_data[symbol]
            if cyq['winner_rate'] < MIN_WINNER_RATE:
                filter_reasons.append(f"赢利率={cyq['winner_rate']:.1f}%")
                continue

        # 通过所有过滤
        buy_signals.append({
            'symbol': symbol,
            'signal': signal_score,
            'price': current_price
        })

        if len(buy_signals) >= MAX_POSITIONS:
            break

    print(f"通过过滤: {len(buy_signals)} 只")
    print("="*50)

    # 找出卖出信号（不使用多层过滤）
    sell_signals_raw = current_signals[current_signals <= SELL_THRESHOLD]
    sell_signals = []

    for symbol in sell_signals_raw.index:
        price_val = latest_data[latest_data['ts_code'] == symbol][price_col].values
        if len(price_val) > 0:
            sell_signals.append({
                'symbol': symbol,
                'signal': sell_signals_raw[symbol],
                'price': float(price_val[0])
            })

    # 生成 ADX 状态描述
    if latest_adx > 25:
        adx_status = f"强趋势市 (ADX={latest_adx:.1f}) → 建议持仓"
    elif latest_adx < 20:
        adx_status = f"震荡市 (ADX={latest_adx:.1f}) → 建议空仓"
    else:
        adx_status = f"中性 (ADX={latest_adx:.1f}) → 观望"

    # 基础信号信息（修正年份）
    date_str = latest_date if isinstance(latest_date, str) else latest_date.strftime("%Y-%m-%d")
    if FORCE_YEAR and date_str:
        date_str = date_str.replace("2026", "2025")

    result = {
        "date": date_str,
        "adx_status": adx_status,
        "buy_signals": buy_signals,
        "sell_signals": sell_signals,
        "positions_summary": [],
        "close_signals": [],
        "filter_stats": {
            "adx": latest_adx,
            "suspended": len(suspended_stocks),
            "basic_data": len(basic_data),
            "cyq_data": len(cyq_data)
        }
    }

    # 处理持仓管理（检查止盈止损）
    if buy_signals or sell_signals or position_manager.positions:
        # 准备价格字典
        prices = {}
        for sig in buy_signals:
            prices[sig['symbol']] = sig['price']
        for sig in sell_signals:
            prices[sig['symbol']] = sig['price']

        # 检查现有持仓的止盈止损
        positions_to_close = []
        for symbol in list(position_manager.positions.keys()):
            if position_manager.positions[symbol]['status'] == 'open':
                current_price = prices.get(symbol)
                if current_price:
                    check = position_manager.check_stop_conditions(
                        symbol, current_price, INITIAL_STOP, TRAILING_STOP, TAKE_PROFIT
                    )
                    if check['should_close']:
                        positions_to_close.append({
                            'symbol': symbol,
                            'reason': check['reason'],
                            'pnl_pct': check['pnl_pct'],
                            'current_price': current_price
                        })

        # 更新返回的信号信息
        enhanced_signals = result.copy()
        enhanced_signals['positions_summary'] = []
        enhanced_signals['close_signals'] = []

        # 添加平仓信号
        for close_sig in positions_to_close:
            position_manager.close_position(close_sig['symbol'], close_sig['current_price'], close_sig['reason'])
            enhanced_signals['close_signals'].append({
                'symbol': close_sig['symbol'],
                'reason': close_sig['reason'],
                'pnl_pct': close_sig['pnl_pct']
            })

        # 获取当前持仓摘要
        if prices:
            enhanced_signals['positions_summary'] = position_manager.get_positions_summary(prices)

        result = enhanced_signals

    return result

def format_message(signals: dict) -> str:
    """格式化为可读的消息（含持仓评估）"""
    lines = []
    lines.append("=" * 50)
    lines.append(f"📊 量化信号日报 - {signals['date']}")
    lines.append("=" * 50)
    lines.append("")
    lines.append(f"📈 市场状态: {signals['adx_status']}")
    lines.append("")

    # 持仓评估
    if signals['positions_summary']:
        lines.append(f"💼 当前持仓 ({len(signals['positions_summary'])}只):")
        for i, pos in enumerate(signals['positions_summary'], 1):
            pnl_icon = "🟢" if pos['pnl_pct'] > 0 else "🔴"
            lines.append(f"  {i}. {pos['symbol']} {pnl_icon} {pos['pnl_pct']*100:+.1f}% "
                        f"(入场:{pos['entry_price']:.2f} → {pos['current_price']:.2f})")
    else:
        lines.append("💼 当前持仓: 无")

    lines.append("")

    # 今日平仓
    if signals['close_signals']:
        lines.append(f"📤 今日平仓 ({len(signals['close_signals'])}只):")
        for i, close_sig in enumerate(signals['close_signals'], 1):
            pnl_icon = "✅" if close_sig['pnl_pct'] > 0 else "❌"
            lines.append(f"  {i}. {close_sig['symbol']} {pnl_icon} "
                        f"{close_sig['reason']} ({close_sig['pnl_pct']*100:+.1f}%)")
    else:
        lines.append("📤 今日平仓: 无")

    lines.append("")

    # 买入信号
    if signals["buy_signals"]:
        lines.append(f"🟢 买入信号 ({len(signals['buy_signals'])}只):")
        for i, sig in enumerate(signals['buy_signals'][:5], 1):
            lines.append(f"  {i}. {sig['symbol']} (信号: {sig['signal']:.2f}, 价格: {sig['price']:.2f})")
        if len(signals['buy_signals']) > 5:
            lines.append(f"  ... 还有{len(signals['buy_signals'])-5}只")
    else:
        lines.append("🟢 买入信号: 无")

    lines.append("")

    # 卖出信号
    if signals["sell_signals"]:
        lines.append(f"🔴 卖出信号 ({len(signals['sell_signals'])}只):")
        for i, sig in enumerate(signals['sell_signals'][:10], 1):
            lines.append(f"  {i}. {sig['symbol']} (信号: {sig['signal']:.2f})")
    else:
        lines.append("🔴 卖出信号: 无")

    lines.append("")
    lines.append("=" * 50)

    return "\n".join(lines)

def send_feishu_message(message: str, webhook_url: str = None):
    """发送飞书消息"""
    # 从环境变量或配置文件读取webhook
    if not webhook_url:
        # 尝试从环境变量读取
        import os
        webhook_url = os.getenv("FEISHU_WEBHOOK_URL")

    if not webhook_url:
        # 尝试从配置文件读取
        config_file = "config/feishu_webhook.txt"
        if os.path.exists(config_file):
            with open(config_file, "r") as f:
                webhook_url = f.read().strip()

    if not webhook_url:
        print("⚠️  未配置飞书webhook，跳过推送")
        print("   配置方法:")
        print("   1. 在飞书群添加自定义机器人")
        print("   2. 选择Webhook类型，复制webhook地址")
        print("   3. 将webhook地址保存到: config/feishu_webhook.txt")
        print("   4. 或设置环境变量: export FEISHU_WEBHOOK_URL='你的webhook地址'")
        return False

    import requests

    data = {
        "msg_type": "text",
        "content": {
            "text": message
        }
    }

    try:
        response = requests.post(webhook_url, json=data, timeout=10)
        # 飞书返回 {"StatusCode":0,"StatusMessage":"success"}表示成功
        result = response.json()
        if result.get("StatusCode") == 0 and result.get("StatusMessage") == "success":
            print("✅ 飞书推送成功")
            return True
        else:
            print(f"❌ 飞书推送失败: {result}")
            return False
    except Exception as e:
        print(f"❌ 飞书推送异常: {e}")
        return False

def main():
    """主函数"""
    print("=" * 50)
    print("每日信号生成 (含持仓管理)")
    print("=" * 50)

    # 自动更新数据（确保使用最新数据）
    print("\n" + "=" * 50)
    print("🔄 数据更新")
    print("=" * 50)
    try:
        downloader = TushareDataDownloader()
        downloader.update_daily(use_today=True)  # 尝试更新今日数据
    except Exception as e:
        print(f"⚠️ 数据更新失败: {e}")
        print("   将使用本地已有数据生成报告...")

    # 生成信号（含持仓管理）
    signals = generate_daily_signals()

    if "error" in signals:
        print(f"❌ 错误: {signals['error']}")
        return

    # 格式化消息
    message = format_message(signals)

    # 打印到控制台
    print("\n" + message)

    # 保存到文件
    output_dir = "data/signals"
    os.makedirs(output_dir, exist_ok=True)

    today = datetime.now().strftime("%Y%m%d")
    output_file = f"{output_dir}/signal_{today}.txt"

    with open(output_file, "w", encoding="utf-8") as f:
        f.write(message)

    print(f"\n✅ 信号已保存到: {output_file}")

    # 推送到飞书
    print("\n" + "=" * 50)
    send_feishu_message(message)

    # 写入飞书多维表格
    print("\n" + "=" * 50)
    print("飞书多维表格写入...")
    bitable = FeishuBitable()
    if bitable.is_configured():
        bitable.write_daily_signal(signals)
    else:
        print("⚠️  飞书 Bitable 未配置，跳过写入")
        print("   配置文件: config/feishu_bitable.json")

if __name__ == "__main__":
    main()
