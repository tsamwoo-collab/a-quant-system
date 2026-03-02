"""
每日信号生成脚本 - GitHub Actions 轻量级版本
每次实时获取 Tushare 数据，不存储本地数据库
"""
import tushare as ts
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import requests
import json
import os
import sys


# ==================== 配置 ====================
# Tushare 配置
TUSHARE_TOKEN = os.environ.get('TUSHARE_TOKEN')
TUSHARE_PROXY = 'http://lianghua.nanyangqiankun.top'

# Feishu 配置
FEISHU_WEBHOOK = os.environ.get('FEISHU_WEBHOOK')

# 策略参数
STRATEGY_NAME = "adaptive_adx_rest"
BUY_THRESHOLD = 0.30
SELL_THRESHOLD = -0.30
MAX_POSITIONS = 5
USE_TUSHARE_LOCAL = False  # 轻量级版本不使用本地数据库
USE_ADJ_PRICE = True

# 过滤参数
MIN_ADX = 25  # ADX 趋势过滤
MIN_PE = 0  # PE > 0
MIN_MARKET_CAP = 300000  # 市值 > 300亿
MIN_WINNER_RATE = 70  # 获利比例 > 70%

# 测试股票数量（控制 API 调用量）
TEST_STOCK_COUNT = 200
HISTORY_DAYS = 120  # 历史天数


# ==================== Tushare API ====================
def init_tushare():
    """初始化 Tushare API"""
    if not TUSHARE_TOKEN:
        raise ValueError("TUSHARE_TOKEN 环境变量未设置")

    pro = ts.pro_api(TUSHARE_TOKEN)
    pro._DataApi__token = TUSHARE_TOKEN
    pro._DataApi__http_url = TUSHARE_PROXY
    return pro


def get_cs300_stocks(pro):
    """获取沪深300成分股"""
    try:
        # 获取最新的沪深300成分股
        end_date = datetime.now().strftime('%Y%m%d')
        start_date = (datetime.now() - timedelta(days=10)).strftime('%Y%m%d')

        df = pro.index_weight(index_code='000300.SH', start_date=start_date, end_date=end_date)

        if df.empty:
            print("  ⚠️ index_weight 返回空，尝试使用 stock_basic")
            # 备用方案：直接获取股票列表
            df_basic = pro.stock_basic(list_status='L', fields='ts_code,name')
            return df_basic['ts_code'].tolist()[:TEST_STOCK_COUNT]

        stocks = df['con_code'].unique().tolist()
        print(f"  ✅ 获取到 {len(stocks)} 只沪深300成分股")
        return stocks[:TEST_STOCK_COUNT]
    except Exception as e:
        print(f"  ❌ 获取沪深300失败: {e}")
        # 最后备用方案：获取所有上市股票
        try:
            df_basic = pro.stock_basic(list_status='L', fields='ts_code,name')
            return df_basic['ts_code'].tolist()[:TEST_STOCK_COUNT]
        except:
            return ['000001.SZ', '000002.SZ', '600000.SH', '600036.SH', '600519.SH']


def download_daily_data(pro, ts_codes, start_date, end_date=None):
    """下载日线数据"""
    if end_date is None:
        end_date = datetime.now().strftime('%Y%m%d')

    print(f"  开始下载 {len(ts_codes[:TEST_STOCK_COUNT])} 只股票的数据 ({start_date} - {end_date})")

    all_data = []
    success_count = 0
    for i, ts_code in enumerate(ts_codes[:TEST_STOCK_COUNT]):
        try:
            df = pro.daily(ts_code=ts_code, start_date=start_date, end_date=end_date)
            if not df.empty:
                all_data.append(df)
                success_count += 1
            if i % 50 == 0 and i > 0:
                print(f"  下载进度: {i}/{len(ts_codes[:TEST_STOCK_COUNT])}, 成功: {success_count}")
        except Exception as e:
            if i < 5:  # 只打印前5个错误
                print(f"  ❌ {ts_code} 下载失败: {e}")
            continue

    print(f"  下载完成: {success_count}/{len(ts_codes[:TEST_STOCK_COUNT])} 只股票成功")

    if all_data:
        result = pd.concat(all_data, ignore_index=True)
        result = result.sort_values(['ts_code', 'trade_date'])
        print(f"  ✅ 总共获取 {len(result)} 条数据")
        return result
    print(f"  ❌ 没有获取到任何数据")
    return pd.DataFrame()


def download_adj_factor(pro, ts_codes, start_date, end_date=None):
    """下载复权因子"""
    if end_date is None:
        end_date = datetime.now().strftime('%Y%m%d')

    all_data = []
    for ts_code in ts_codes[:TEST_STOCK_COUNT]:
        try:
            df = pro.adj_factor(ts_code=ts_code, start_date=start_date, end_date=end_date)
            if not df.empty:
                all_data.append(df)
        except:
            continue

    if all_data:
        return pd.concat(all_data, ignore_index=True)
    return pd.DataFrame()


def download_daily_basic(pro, ts_codes, trade_date):
    """下载每日基础指标"""
    all_data = []
    for ts_code in ts_codes[:TEST_STOCK_COUNT]:
        try:
            df = pro.daily_basic(ts_code=ts_code, trade_date=trade_date,
                                fields='ts_code,trade_date,pe_ttm,pb,total_mv')
            if not df.empty:
                all_data.append(df)
        except:
            continue

    if all_data:
        return pd.concat(all_data, ignore_index=True)
    return pd.DataFrame()


# ==================== 信号计算 ====================
class SimpleSignalGenerator:
    """简化的信号生成器"""

    def __init__(self):
        self.buy_threshold = BUY_THRESHOLD
        self.sell_threshold = SELL_THRESHOLD

    def calculate_momentum_score(self, df):
        """计算动量得分"""
        if len(df) < 20:
            return 0

        close = df['close_qfq'].values if 'close_qfq' in df.columns else df['close'].values

        # 短期动量 (5日)
        if len(close) >= 6:
            short_momentum = (close[-1] - close[-5]) / close[-5]
        else:
            short_momentum = 0

        # 中期动量 (20日)
        if len(close) >= 21:
            medium_momentum = (close[-1] - close[-20]) / close[-20]
        else:
            medium_momentum = 0

        # 波动率
        if len(close) >= 10:
            returns = pd.Series(close).pct_change().dropna()
            volatility = returns.std()
        else:
            volatility = 0.01

        # 综合得分
        score = (short_momentum * 0.6 + medium_momentum * 0.4) / (volatility + 0.01)

        # RSI 辅助
        if len(close) >= 14:
            rsi = self.calculate_rsi(close)
            if rsi > 70:
                score *= 0.8  # 超买，降低分数
            elif rsi < 30:
                score *= 1.2  # 超卖，提高分数

        return score

    def calculate_rsi(self, prices, period=14):
        """计算 RSI"""
        if len(prices) < period + 1:
            return 50

        deltas = pd.Series(prices).diff()
        gains = deltas.where(deltas > 0, 0)
        losses = -deltas.where(deltas < 0, 0)

        avg_gain = gains.rolling(window=period).mean()
        avg_loss = losses.rolling(window=period).mean()

        rs = avg_gain / (avg_loss + 1e-10)
        rsi = 100 - (100 / (1 + rs))

        return rsi.iloc[-1]

    def calculate_market_adx(self, price_df):
        """计算市场 ADX（简化版）"""
        if len(price_df) < 20:
            return 20.0

        adx_values = []
        for stock in price_df.columns:
            stock_adx = self._calculate_adx_single(price_df[stock])
            if stock_adx > 0:
                adx_values.append(stock_adx)

        if adx_values:
            return np.mean(adx_values)
        return 20.0

    def _calculate_adx_single(self, prices, period=14):
        """计算单个股票的 ADX"""
        if len(prices) < period * 2:
            return 0

        df = pd.DataFrame({'close': prices})

        # TR
        df['high'] = df['close']
        df['low'] = df['close']
        df['tr'] = np.maximum(df['high'] - df['low'],
                             np.maximum(abs(df['high'] - df['close'].shift(1)),
                                       abs(df['low'] - df['close'].shift(1))))

        # +DM, -DM
        df['+dm'] = np.where((df['high'] - df['high'].shift(1)) > (df['low'].shift(1) - df['low']),
                            df['high'] - df['high'].shift(1), 0)
        df['-dm'] = np.where((df['low'].shift(1) - df['low']) > (df['high'] - df['high'].shift(1)),
                            df['low'].shift(1) - df['low'], 0)

        # 平滑
        df['atr'] = df['tr'].rolling(window=period).mean()
        df['+di'] = (df['+dm'].rolling(window=period).mean() / df['atr'] * 100)
        df['-di'] = (df['-dm'].rolling(window=period).mean() / df['atr'] * 100)

        # DX
        df['dx'] = abs(df['+di'] - df['-di']) / (df['+di'] + df['-di'] + 1e-10) * 100

        # ADX
        df['adx'] = df['dx'].rolling(window=period).mean()

        return df['adx'].iloc[-1] if not pd.isna(df['adx'].iloc[-1]) else 0

    def generate_signals(self, price_df):
        """生成信号"""
        signals = {}

        for symbol in price_df.columns:
            prices = price_df[symbol].dropna()
            if len(prices) < 20:
                continue

            score = self.calculate_momentum_score(pd.DataFrame({'close': prices}))

            if score > self.buy_threshold:
                signals[symbol] = {'signal': score, 'type': 'buy'}
            elif score < self.sell_threshold:
                signals[symbol] = {'signal': score, 'type': 'sell'}

        return signals


# ==================== Feishu 推送 ====================
def send_feishu_notification(report):
    """发送飞书通知"""
    if not FEISHU_WEBHOOK:
        print("⚠️ 未配置 FEISHU_WEBHOOK，跳过推送")
        return False

    headers = {'Content-Type': 'application/json'}

    try:
        response = requests.post(FEISHU_WEBHOOK, headers=headers, data=json.dumps(report))
        if response.status_code == 200:
            print("✅ 飞书推送成功")
            return True
        else:
            print(f"❌ 飞书推送失败: {response.status_code} - {response.text}")
            return False
    except Exception as e:
        print(f"❌ 飞书推送异常: {e}")
        return False


# ==================== 主程序 ====================
def generate_daily_signals_lite():
    """生成每日信号（轻量级版本）"""
    print(f"=== 开始生成信号 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ===")

    # 初始化
    pro = init_tushare()
    generator = SimpleSignalGenerator()

    # 获取股票列表
    print("📊 获取股票列表...")
    cs300_stocks = get_cs300_stocks(pro)
    print(f"✅ 获取 {len(cs300_stocks)} 只股票")

    # 下载历史数据
    end_date = datetime.now().strftime('%Y%m%d')
    start_date = (datetime.now() - timedelta(days=HISTORY_DAYS)).strftime('%Y%m%d')

    print(f"📥 下载历史数据 ({start_date} - {end_date})...")
    daily_data = download_daily_data(pro, cs300_stocks, start_date, end_date)
    print(f"✅ 获取 {len(daily_data)} 条数据")

    if daily_data.empty:
        print("❌ 没有获取到数据")
        return None

    # 下载复权因子
    print("📥 下载复权因子...")
    adj_data = download_adj_factor(pro, cs300_stocks, start_date, end_date)

    # 应用复权
    if not adj_data.empty:
        daily_data = daily_data.merge(adj_data[['ts_code', 'trade_date', 'adj_factor']],
                                     on=['ts_code', 'trade_date'], how='left')
        daily_data['adj_factor'] = daily_data['adj_factor'].fillna(1)
        for col in ['open', 'high', 'low', 'close', 'pre_close']:
            if col in daily_data.columns:
                daily_data[f'{col}_qfq'] = daily_data[col] * daily_data['adj_factor']
        print("✅ 复权处理完成")

    # 整理价格数据
    price_col = 'close_qfq' if 'close_qfq' in daily_data.columns else 'close'
    daily_data['date'] = pd.to_datetime(daily_data['trade_date'], format='%Y%m%d')

    # 筛选有效股票
    stock_counts = daily_data.groupby('ts_code').size()
    valid_stocks = stock_counts[stock_counts >= 60].index.tolist()
    daily_data = daily_data[daily_data['ts_code'].isin(valid_stocks)]

    print(f"📊 有效股票: {len(valid_stocks)} 只")

    # 计算市场 ADX
    print("📊 计算市场 ADX...")
    price_df = daily_data.pivot(index='date', columns='ts_code', values=price_col)
    market_adx = generator.calculate_market_adx(price_df)
    print(f"✅ 市场 ADX: {market_adx:.2f}")

    # 判断市场状态
    if market_adx >= MIN_ADX:
        market_status = "趋势市"
        market_advice = "可交易"
        adx_passed = True
    else:
        market_status = "震荡市"
        market_advice = "建议空仓"
        adx_passed = False

    print(f"📈 市场状态: {market_status} (ADX={market_adx:.2f})")

    # 下载当日基础指标（用于过滤）
    print("📥 下载基础指标...")
    today_basic = download_daily_basic(pro, cs300_stocks, end_date)

    # 生成信号
    print("🔍 生成信号...")
    all_signals = generator.generate_signals(price_df)

    # 应用过滤
    buy_signals = []
    sell_signals = []

    for symbol, signal_info in all_signals.items():
        if signal_info['type'] == 'buy' and adx_passed:
            # 基础过滤
            if not today_basic.empty:
                basic = today_basic[today_basic['ts_code'] == symbol]
                if not basic.empty:
                    pe = basic['pe_ttm'].values[0]
                    mv = basic['total_mv'].values[0]

                    if pe <= MIN_PE or mv <= MIN_MARKET_CAP:
                        continue

            buy_signals.append({
                'symbol': symbol,
                'score': signal_info['signal']
            })
        elif signal_info['type'] == 'sell':
            sell_signals.append({
                'symbol': symbol,
                'score': signal_info['signal']
            })

    # 排序
    buy_signals.sort(key=lambda x: x['score'], reverse=True)
    buy_signals = buy_signals[:MAX_POSITIONS]

    print(f"🟢 买入信号: {len(buy_signals)} 个")
    print(f"🔴 卖出信号: {len(sell_signals)} 个")

    # 构建报告
    report = build_report(market_status, market_adx, buy_signals, sell_signals)

    return report


def build_report(market_status, market_adx, buy_signals, sell_signals):
    """构建飞书报告"""
    today = datetime.now().strftime('%Y-%m-%d')

    lines = [
        f"==================================================",
        f"📊 量化信号日报 - {today}",
        f"==================================================",
        f"",
        f"📈 市场状态: {market_status} (ADX={market_adx:.1f})",
        f"",
        f"💼 当前持仓: 无",
        f"",
        f"📤 今日平仓: 无",
        f"",
    ]

    if buy_signals:
        lines.append(f"🟢 买入信号 ({len(buy_signals)}):")
        for i, sig in enumerate(buy_signals, 1):
            lines.append(f"   {i}. {sig['symbol']} (得分: {sig['score']:.2f})")
    else:
        lines.append(f"🟢 买入信号: 无")

    lines.append("")

    if sell_signals:
        lines.append(f"🔴 卖出信号 ({len(sell_signals)}):")
        for i, sig in enumerate(sell_signals, 1):
            lines.append(f"   {i}. {sig['symbol']} (得分: {sig['score']:.2f})")
    else:
        lines.append(f"🔴 卖出信号: 无")

    lines.append("==================================================")

    text = "\n".join(lines)

    # 构建飞书卡片
    card = {
        "msg_type": "interactive",
        "card": {
            "header": {
                "title": {
                    "tag": "plain_text",
                    "content": f"📊 量化信号日报 - {today}"
                },
                "template": "blue" if market_status == "趋势市" else "red"
            },
            "elements": [
                {
                    "tag": "div",
                    "text": {
                        "tag": "lark_md",
                        "content": text
                    }
                }
            ]
        }
    }

    return card


def main():
    """主函数"""
    try:
        report = generate_daily_signals_lite()

        if report:
            # 推送到飞书
            send_feishu_notification(report)
            print("\n✅ 信号生成完成")
            return 0
        else:
            print("\n❌ 信号生成失败")
            return 1

    except Exception as e:
        print(f"\n❌ 程序异常: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
