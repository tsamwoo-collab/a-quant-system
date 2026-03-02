"""
飞书多维表格 API 封装 - 3表格版本
用于量化交易系统的完整数据记录
"""
import json
import os
import requests
from datetime import datetime
from typing import Dict, List, Optional


def date_to_timestamp(date_str: str) -> int:
    """将日期字符串转换为 Unix 时间戳（毫秒）"""
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    return int(dt.timestamp() * 1000)


class FeishuBitable:
    """飞书多维表格客户端 - 3表格版本"""

    # 表格配置
    TABLE_CONFIGS = {
        "market_context": {
            "name": "每日天气（Market Context）",
            "description": "记录大盘环境，作为系统'总开关'的审计日志",
            "fields": [
                {"field_name": "日期", "type": 5, "description": "交易日期"},
                {"field_name": "指数收盘", "type": 2, "description": "沪深300指数当日收盘价"},
                {"field_name": "ADX数值", "type": 2, "description": "周期14的ADX指标值"},
                {"field_name": "系统状态", "type": 3, "description": "基于ADX判断的开关状态"},
                {"field_name": "备注", "type": 1, "description": "记录当日极端行情或重大事件"}
            ]
        },
        "signal_tracker": {
            "name": "信号追踪（Signal Tracker）",
            "description": "记录系统发出的原始脉冲，辅助后期分析'滑点损耗'",
            "fields": [
                {"field_name": "信号日期", "type": 5, "description": "产生信号的收盘日"},
                {"field_name": "股票代码", "type": 1, "description": "股票代码（如600519.SH）"},
                {"field_name": "信号类型", "type": 3, "description": "买入或卖出信号"},
                {"field_name": "理论价格", "type": 2, "description": "当日收盘价（回测用的理想价格）"},
                {"field_name": "滑点模拟成交价", "type": 2, "description": "买入+0.5%，卖出-0.5%"},
                {"field_name": "动能得分", "type": 2, "description": "触发买入时的具体得分"},
                {"field_name": "执行状态", "type": 3, "description": "信号执行状态"}
            ]
        },
        "portfolio": {
            "name": "持仓看板（Portfolio）",
            "description": "动态监控持仓股票，自动计算是否触发追踪止盈",
            "fields": [
                {"field_name": "代码/名称", "type": 1, "description": "股票代码或名称"},
                {"field_name": "入场日期", "type": 5, "description": "买入信号日期"},
                {"field_name": "初始成本", "type": 2, "description": "滑点模拟后的买入成交价"},
                {"field_name": "当前价", "type": 2, "description": "每日收盘后更新的价格"},
                {"field_name": "持有最高收盘价", "type": 2, "description": "买入后每日收盘的最大值"},
                {"field_name": "当前利润率", "type": 2, "description": "(当前价-成本)/成本"},
                {"field_name": "最高点回撤", "type": 2, "description": "(当前价-最高价)/最高价"},
                {"field_name": "风险状态", "type": 3, "description": "基于回撤的风险判断"}
            ]
        }
    }

    def __init__(self, config_path: str = "config/feishu_bitable.json"):
        self.config_path = config_path
        self.app_id = None
        self.app_secret = None
        self.app_token = None
        self.table_ids = {}  # 存储各表的 table_id
        self.tenant_access_token = None
        self.load_config()

    def load_config(self):
        """加载配置"""
        if os.path.exists(self.config_path):
            with open(self.config_path, 'r', encoding='utf-8') as f:
                config = json.load(f)
                self.app_id = config.get('app_id')
                self.app_secret = config.get('app_secret')
                self.app_token = config.get('app_token')
                # 读取各表的 table_id
                self.table_ids = config.get('table_ids', {})
        else:
            print(f"⚠️  配置文件不存在: {self.config_path}")

    def save_config(self):
        """保存配置"""
        config = {
            "app_id": self.app_id,
            "app_secret": self.app_secret,
            "app_token": self.app_token,
            "table_ids": self.table_ids
        }
        with open(self.config_path, 'w', encoding='utf-8') as f:
            json.dump(config, f, ensure_ascii=False, indent=2)

    def is_configured(self) -> bool:
        """检查是否已配置"""
        return all([self.app_id, self.app_secret, self.app_token])

    def get_tenant_access_token(self) -> bool:
        """获取 tenant_access_token"""
        if not self.is_configured():
            return False

        url = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
        payload = {
            "app_id": self.app_id,
            "app_secret": self.app_secret
        }

        try:
            response = requests.post(url, json=payload, timeout=10)
            result = response.json()

            if result.get("code") == 0:
                self.tenant_access_token = result.get("tenant_access_token")
                return True
            else:
                print(f"❌ 获取 token 失败: {result}")
                return False
        except Exception as e:
            print(f"❌ 请求异常: {e}")
            return False

    def _get_headers(self) -> Dict:
        """获取请求头"""
        return {
            "Authorization": f"Bearer {self.tenant_access_token}",
            "Content-Type": "application/json"
        }

    def list_tables(self) -> List[Dict]:
        """获取所有数据表列表"""
        if not self.get_tenant_access_token():
            return []

        url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{self.app_token}/tables"

        try:
            response = requests.get(url, headers=self._get_headers(), timeout=10)
            result = response.json()

            if result.get("code") == 0:
                return result.get("data", {}).get("items", [])
        except Exception as e:
            print(f"❌ 获取数据表列表失败: {e}")

        return []

    def find_table_by_name(self, table_name: str) -> Optional[str]:
        """根据表名查找 table_id"""
        tables = self.list_tables()
        for table in tables:
            if table.get("name") == table_name:
                return table.get("table_id")
        return None

    def create_table(self, table_key: str) -> bool:
        """创建数据表"""
        if not self.get_tenant_access_token():
            return False

        if table_key not in self.TABLE_CONFIGS:
            print(f"❌ 未知的表格类型: {table_key}")
            return False

        config = self.TABLE_CONFIGS[table_key]

        # 检查表是否已存在
        existing_id = self.find_table_by_name(config["name"])
        if existing_id:
            self.table_ids[table_key] = existing_id
            print(f"✅ 数据表已存在: {config['name']} (ID: {existing_id})")
            self.save_config()
            return True

        # 创建新表（先不指定字段，创建后手动添加）
        url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{self.app_token}/tables"

        payload = {
            "table": {
                "name": config["name"],
                "default_view_name": "默认视图"
            }
        }

        try:
            response = requests.post(url, headers=self._get_headers(), json=payload, timeout=10)
            result = response.json()

            if result.get("code") == 0:
                table_id = result.get("data", {}).get("table", {}).get("table_id")
                self.table_ids[table_key] = table_id
                self.save_config()
                print(f"✅ 数据表创建成功: {config['name']} (ID: {table_id})")
                return True
            else:
                print(f"❌ 创建数据表失败: {result}")
                return False

        except Exception as e:
            print(f"❌ 创建数据表异常: {e}")
            return False

    def initialize_all_tables(self) -> bool:
        """初始化所有数据表（查找已存在的表格）"""
        print("\n📋 初始化飞书多维表格...")
        if not self.get_tenant_access_token():
            print("❌ 获取访问令牌失败")
            return False

        print("✅ 访问令牌获取成功")

        # 获取所有已存在的表格
        tables = self.list_tables()
        print(f"\n📊 找到 {len(tables)} 个数据表")

        # 映射表名到表键
        name_to_key = {
            "每日天气（Market Context）": "market_context",
            "信号追踪（Signal Tracker）": "signal_tracker",
            "持仓看板（Portfolio）": "portfolio"
        }

        found = 0
        for table in tables:
            table_name = table.get("name")
            table_id = table.get("table_id")

            if table_name in name_to_key:
                key = name_to_key[table_name]
                self.table_ids[key] = table_id
                found += 1
                print(f"✅ 找到: {table_name} (ID: {table_id})")

        if found < 3:
            print(f"\n⚠️  只找到 {found}/3 个数据表")
            print("请在飞书中手动创建以下数据表:")
            for key, config in self.TABLE_CONFIGS.items():
                if key not in self.table_ids:
                    print(f"   - {config['name']}")
            return False

        print("\n✅ 所有数据表已就绪")
        self.save_config()
        return True

    def write_market_context(self, date: str, index_close: float, adx_value: float,
                            system_status: str, note: str = "") -> bool:
        """写入市场环境数据（表A）"""
        if "market_context" not in self.table_ids:
            print("❌ 表A未初始化")
            return False

        table_id = self.table_ids["market_context"]
        url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{self.app_token}/tables/{table_id}/records"

        # 检查是否已存在
        if self._record_exists(table_id, date, "日期"):
            print(f"⚠️  {date} 的市场数据已存在，跳过写入")
            return False

        fields = {
            "日期": date_to_timestamp(date),
            "指数收盘": round(index_close, 2),
            "ADX数值": round(adx_value, 2),
            "系统状态": system_status,
            "备注": note
        }

        return self._write_record(url, fields, f"市场环境 ({date})")

    def write_signal(self, date: str, symbol: str, signal_type: str,
                    theoretical_price: float, momentum_score: float,
                    execution_status: str = "待执行") -> bool:
        """写入信号数据（表B）"""
        if "signal_tracker" not in self.table_ids:
            print("❌ 表B未初始化")
            return False

        table_id = self.table_ids["signal_tracker"]
        url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{self.app_token}/tables/{table_id}/records"

        # 计算滑点模拟成交价
        if signal_type == "买入信号":
            slippage_price = round(theoretical_price * 1.005, 2)  # 买入+0.5%
            record_type = "买入信号"
        else:
            slippage_price = round(theoretical_price * 0.995, 2)  # 卖出-0.5%
            record_type = "卖出信号"

        fields = {
            "信号日期": date_to_timestamp(date),
            "股票代码": symbol,
            "信号类型": signal_type,
            "理论价格": round(theoretical_price, 2),
            "滑点模拟成交价": slippage_price,
            "动能得分": round(momentum_score, 3),
            "执行状态": execution_status
        }

        return self._write_record(url, fields, f"信号 ({symbol} {signal_type})")

    def update_portfolio(self, symbol: str, entry_date: str, entry_price: float,
                        current_price: float, highest_price: float,
                        pnl_pct: float, drawdown_pct: float, risk_status: str) -> bool:
        """更新持仓数据（表C）"""
        if "portfolio" not in self.table_ids:
            print("❌ 表C未初始化")
            return False

        table_id = self.table_ids["portfolio"]
        url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{self.app_token}/tables/{table_id}/records"

        # 检查持仓是否已存在
        record_id = self._find_portfolio_record(table_id, symbol)

        fields = {
            "代码/名称": symbol,
            "入场日期": date_to_timestamp(entry_date),
            "初始成本": round(entry_price, 2),
            "当前价": round(current_price, 2),
            "持有最高收盘价": round(highest_price, 2),
            "当前利润率": round(pnl_pct * 100, 2),
            "最高点回撤": round(drawdown_pct * 100, 2),
            "风险状态": risk_status
        }

        if record_id:
            # 更新现有记录
            return self._update_record(url, record_id, fields, f"持仓 ({symbol})")
        else:
            # 创建新记录
            return self._write_record(url, fields, f"持仓 ({symbol})")

    def close_portfolio_position(self, symbol: str) -> bool:
        """平仓（从表C删除记录）"""
        if "portfolio" not in self.table_ids:
            return False

        table_id = self.table_ids["portfolio"]
        record_id = self._find_portfolio_record(table_id, symbol)

        if record_id:
            url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{self.app_token}/tables/{table_id}/records/{record_id}"

            try:
                response = requests.delete(url, headers=self._get_headers(), timeout=10)
                result = response.json()

                if result.get("code") == 0:
                    print(f"✅ 平仓成功: {symbol}")
                    return True
                else:
                    print(f"❌ 平仓失败: {result}")
                    return False
            except Exception as e:
                print(f"❌ 平仓异常: {e}")
                return False

        return False

    def record_closed_position(self, symbol: str, entry_date: str, exit_date: str,
                              entry_price: float, exit_price: float, holding_days: int,
                              pnl_pct: float, pnl_amount: float, reason: str, note: str = "") -> bool:
        """记录已平仓交易到表D"""
        if "closed_positions" not in self.table_ids:
            print("❌ 表D未初始化")
            return False

        table_id = self.table_ids["closed_positions"]
        url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{self.app_token}/tables/{table_id}/records"

        # 映射平仓原因
        reason_map = {
            "初始止损": "初始止损(-8%)",
            "追踪止盈": "追踪止盈(-8%)",
            "目标止盈": "目标止盈(30%)"
        }
        close_reason = reason_map.get(reason, reason)

        fields = {
            "股票代码": symbol,
            "入场日期": date_to_timestamp(entry_date),
            "离场日期": date_to_timestamp(exit_date),
            "入场价": round(entry_price, 2),
            "离场价": round(exit_price, 2),
            "持仓时长": holding_days,
            "盈亏比例": round(pnl_pct * 100, 2),
            "盈亏金额": round(pnl_amount, 2),
            "平仓原因": close_reason,
            "备注": note
        }

        return self._write_record(url, fields, f"平仓记录 ({symbol})")

    def write_daily_signal(self, signals: dict) -> bool:
        """
        写入每日信号数据（完整流程）
        调用其他方法来写入市场环境、信号和持仓数据
        """
        # 重新获取 token（可能已过期）
        if not self.get_tenant_access_token():
            return False

        date = signals.get("date", "")
        adx_status = signals.get("adx_status", "")

        # 解析 ADX 数值和系统状态
        adx_value = 20.0  # 默认值
        system_status = "观望"

        import re
        adx_match = re.search(r'ADX=([\d.]+)', adx_status)
        if adx_match:
            adx_value = float(adx_match.group(1))

        if "强趋势市" in adx_status or "开启" in adx_status:
            system_status = "开启(ADX>25)"
        elif "震荡市" in adx_status or "静默" in adx_status:
            system_status = "静默(ADX<25)"
        else:
            system_status = "观望"

        # 获取指数收盘价（使用沪深300的当前价作为近似值）
        index_close = 3520.50  # 默认值，可以后续从实际数据获取

        # 1. 写入市场环境
        self.write_market_context(
            date=date,
            index_close=index_close,
            adx_value=adx_value,
            system_status=system_status,
            note=""
        )

        # 2. 写入买入信号
        for buy_signal in signals.get("buy_signals", []):
            self.write_signal(
                date=date,
                symbol=buy_signal.get("symbol", ""),
                signal_type="买入信号",
                theoretical_price=buy_signal.get("price", 0),
                momentum_score=buy_signal.get("signal", 0),
                execution_status="待执行"
            )

        # 3. 写入卖出信号
        for sell_signal in signals.get("sell_signals", []):
            self.write_signal(
                date=date,
                symbol=sell_signal.get("symbol", ""),
                signal_type="卖出信号",
                theoretical_price=sell_signal.get("price", 0),
                momentum_score=sell_signal.get("signal", 0),
                execution_status="待执行"
            )

        # 4. 更新持仓数据
        for position in signals.get("positions_summary", []):
            # 计算回撤
            highest_price = position.get("highest_price", position.get("current_price", 0))
            current_price = position.get("current_price", 0)
            drawdown_pct = 0
            if highest_price > 0:
                drawdown_pct = (current_price - highest_price) / highest_price

            # 判断风险状态
            pnl_pct = position.get("pnl_pct", 0)
            if pnl_pct <= -0.08:
                risk_status = "强制平仓"
            elif drawdown_pct <= -0.08:
                risk_status = "预警"
            else:
                risk_status = "正常"

            self.update_portfolio(
                symbol=position.get("symbol", ""),
                entry_date=position.get("entry_date", date),
                entry_price=position.get("entry_price", 0),
                current_price=current_price,
                highest_price=highest_price,
                pnl_pct=pnl_pct,
                drawdown_pct=drawdown_pct,
                risk_status=risk_status
            )

        # 5. 处理平仓信号（从持仓中删除并记录到表D）
        for close_signal in signals.get("close_signals", []):
            symbol = close_signal.get("symbol", "")
            reason = close_signal.get("reason", "")
            pnl_pct = close_signal.get("pnl_pct", 0)

            # 先从表C读取持仓详情
            position_details = self._get_portfolio_position_details(symbol)

            if position_details:
                # 计算持仓时长和盈亏金额
                from datetime import datetime
                exit_date = date
                entry_date = position_details.get("entry_date", date)
                entry_price = position_details.get("entry_price", 0)
                current_price = position_details.get("current_price", 0)
                shares = position_details.get("shares", 100)  # 默认100股

                # 计算持仓天数
                try:
                    entry_dt = datetime.strptime(entry_date, "%Y-%m-%d")
                    exit_dt = datetime.strptime(exit_date, "%Y-%m-%d")
                    holding_days = (exit_dt - entry_dt).days
                except:
                    holding_days = 0

                # 计算盈亏金额
                pnl_amount = (current_price - entry_price) * shares

                # 记录到表D
                self.record_closed_position(
                    symbol=symbol,
                    entry_date=entry_date,
                    exit_date=exit_date,
                    entry_price=entry_price,
                    exit_price=current_price,
                    holding_days=holding_days,
                    pnl_pct=pnl_pct,
                    pnl_amount=pnl_amount,
                    reason=reason,
                    note=""
                )

            # 从表C删除记录
            self.close_portfolio_position(symbol)

        return True

    def _get_portfolio_position_details(self, symbol: str) -> Optional[dict]:
        """从表C获取持仓详情"""
        if "portfolio" not in self.table_ids:
            return None

        table_id = self.table_ids["portfolio"]
        url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{self.app_token}/tables/{table_id}/records"

        try:
            response = requests.get(url, headers=self._get_headers(), timeout=10)
            result = response.json()

            if result.get("code") == 0:
                items = result.get("data", {}).get("items", [])
                # 遍历所有记录，找到匹配的股票
                for item in items:
                    fields = item.get("fields", {})
                    if fields.get("代码/名称") == symbol:
                        # 处理日期字段（时间戳转日期字符串）
                        entry_date_ts = fields.get("入场日期")
                        if isinstance(entry_date_ts, (int, float)):
                            entry_date = datetime.fromtimestamp(entry_date_ts / 1000).strftime("%Y-%m-%d")
                        else:
                            entry_date = str(entry_date_ts)

                        return {
                            "entry_date": entry_date,
                            "entry_price": float(fields.get("初始成本", 0)),
                            "current_price": float(fields.get("当前价", 0)),
                            "highest_price": float(fields.get("持有最高收盘价", 0)),
                            "shares": 100  # 默认值
                        }
        except:
            pass

        return None

    def _record_exists(self, table_id: str, value: str, field_name: str) -> bool:
        """检查记录是否存在"""
        url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{self.app_token}/tables/{table_id}/records"

        params = {
            "filter": json.dumps({
                "conditions": [{
                    "field_name": field_name,
                    "operator": "is",
                    "value": [value]
                }]
            })
        }

        try:
            response = requests.get(url, headers=self._get_headers(), params=params, timeout=10)
            result = response.json()

            if result.get("code") == 0:
                return result.get("data", {}).get("total", 0) > 0
        except:
            pass

        return False

    def _find_record_by_field(self, table_id: str, field_name: str, value: str) -> Optional[str]:
        """根据字段值查找记录ID"""
        url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{self.app_token}/tables/{table_id}/records"

        params = {
            "filter": json.dumps({
                "conditions": [{
                    "field_name": field_name,
                    "operator": "is",
                    "value": [value]
                }]
            })
        }

        try:
            response = requests.get(url, headers=self._get_headers(), params=params, timeout=10)
            result = response.json()

            if result.get("code") == 0:
                items = result.get("data", {}).get("items", [])
                if items:
                    return items[0].get("record_id")
        except:
            pass

        return None

    def _find_portfolio_record(self, table_id: str, symbol: str) -> Optional[str]:
        """查找持仓记录ID（按代码/名称字段）"""
        url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{self.app_token}/tables/{table_id}/records"

        params = {
            "filter": json.dumps({
                "conditions": [{
                    "field_name": "代码/名称",
                    "operator": "is",
                    "value": [symbol]
                }]
            })
        }

        try:
            response = requests.get(url, headers=self._get_headers(), params=params, timeout=10)
            result = response.json()

            if result.get("code") == 0:
                items = result.get("data", {}).get("items", [])
                if items:
                    return items[0].get("record_id")
        except:
            pass

        return None

    def _write_record(self, url: str, fields: Dict, desc: str = "") -> bool:
        """写入记录"""
        payload = {"fields": fields}

        try:
            response = requests.post(url, headers=self._get_headers(), json=payload, timeout=10)
            result = response.json()

            if result.get("code") == 0:
                print(f"✅ 写入成功: {desc}")
                return True
            else:
                print(f"❌ 写入失败: {desc} - {result}")
                return False
        except Exception as e:
            print(f"❌ 写入异常: {desc} - {e}")
            return False

    def _update_record(self, url: str, record_id: str, fields: Dict, desc: str = "") -> bool:
        """更新记录"""
        update_url = f"{url}/{record_id}"
        payload = {"fields": fields}

        try:
            response = requests.put(update_url, headers=self._get_headers(), json=payload, timeout=10)
            result = response.json()

            if result.get("code") == 0:
                print(f"✅ 更新成功: {desc}")
                return True
            else:
                print(f"❌ 更新失败: {desc} - {result}")
                return False
        except Exception as e:
            print(f"❌ 更新异常: {desc} - {e}")
            return False


def create_config_template():
    """创建配置文件模板"""
    config_template = {
        "app_id": "",
        "app_secret": "",
        "app_token": "",
        "table_ids": {
            "market_context": "",
            "signal_tracker": "",
            "portfolio": ""
        }
    }

    os.makedirs("config", exist_ok=True)
    config_path = "config/feishu_bitable.json"

    if not os.path.exists(config_path):
        with open(config_path, 'w', encoding='utf-8') as f:
            json.dump(config_template, f, ensure_ascii=False, indent=2)
        print(f"✅ 配置模板已创建: {config_path}")
        return True
    else:
        print(f"⚠️  配置文件已存在: {config_path}")
        return False


# 测试代码
if __name__ == "__main__":
    print("飞书多维表格 API 测试 - 3表格版本\n")

    bitable = FeishuBitable()

    if not bitable.is_configured():
        print("❌ 请先配置飞书应用凭证")
        print("   配置文件: config/feishu_bitable.json")
    else:
        print("✅ 配置检测通过\n")

        # 初始化所有数据表
        if bitable.initialize_all_tables():
            print("\n📝 测试写入数据...")

            # 测试写入市场环境
            bitable.write_market_context(
                date="2026-03-02",
                index_close=3520.50,
                adx_value=28.5,
                system_status="开启(ADX>25)",
                note="测试数据"
            )

            # 测试写入信号
            bitable.write_signal(
                date="2026-03-02",
                symbol="600519.SH",
                signal_type="买入信号",
                theoretical_price=1820.00,
                momentum_score=0.65,
                execution_status="待执行"
            )

            # 测试更新持仓
            bitable.update_portfolio(
                symbol="600519.SH",
                entry_date="2026-03-02",
                entry_price=1830.00,
                current_price=1820.00,
                highest_price=1820.00,
                pnl_pct=-0.0055,
                drawdown_pct=0.0,
                risk_status="正常"
            )
