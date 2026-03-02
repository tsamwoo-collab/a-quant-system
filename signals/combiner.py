"""
因子合成模块 - 将多层级因子合成为最终信号
"""
import pandas as pd
import numpy as np
from typing import Dict, List, Optional
from scipy import stats
import logging

logger = logging.getLogger(__name__)


class FactorCombiner:
    """因子合成器"""

    def __init__(self, config: Dict = None):
        self.config = config or self._default_config()

    def _default_config(self) -> Dict:
        """默认配置"""
        return {
            'weights': {
                'macro': 0.2,
                'meso': 0.3,
                'micro': 0.5
            },
            'normalization': 'zscore',  # zscore, minmax, rank
            'combine_method': 'weighted',  # weighted, equal
        }

    def combine_factors(
        self,
        factors_df: pd.DataFrame,
        method: str = None
    ) -> pd.Series:
        """
        合成多个因子为单一信号

        Args:
            factors_df: 因子数据，列为因子名，行为股票
            method: 合成方法

        Returns:
            合成后的信号序列
        """
        if factors_df is None or factors_df.empty:
            return pd.Series()

        method = method or self.config['combine_method']

        # 去除NaN
        factors_df = factors_df.dropna(how='all')

        if factors_df.empty:
            return pd.Series()

        # 标准化
        normalized = self._normalize_factors(factors_df)

        # 合成
        if method == 'weighted':
            return self._weighted_combine(normalized)
        elif method == 'equal':
            return self._equal_combine(normalized)
        else:
            return self._equal_combine(normalized)

    def _normalize_factors(self, factors_df: pd.DataFrame) -> pd.DataFrame:
        """标准化因子"""
        method = self.config['normalization']
        normalized = pd.DataFrame(index=factors_df.index)

        for col in factors_df.columns:
            series = factors_df[col].dropna()

            if len(series) == 0:
                normalized[col] = np.nan
                continue

            if method == 'zscore':
                # Z-Score标准化
                mean, std = series.mean(), series.std()
                if std == 0:
                    normalized[col] = 0
                else:
                    normalized[col] = (factors_df[col] - mean) / std
                    # 限制极值
                    normalized[col] = normalized[col].clip(-3, 3)

            elif method == 'minmax':
                # Min-Max标准化到[-1, 1]
                min_val, max_val = series.min(), series.max()
                if max_val == min_val:
                    normalized[col] = 0
                else:
                    normalized[col] = 2 * (factors_df[col] - min_val) / (max_val - min_val) - 1

            elif method == 'rank':
                # 排名标准化到[-1, 1]
                ranks = series.rank(pct=True)
                normalized[col] = 2 * ranks - 1

        return normalized

    def _weighted_combine(self, normalized_df: pd.DataFrame) -> pd.Series:
        """加权合成"""
        # 按因子层级分组
        factor_groups = self._group_factors(normalized_df.columns)

        scores = {}
        for level, factors in factor_groups.items():
            available_factors = [f for f in factors if f in normalized_df.columns]
            if not available_factors:
                scores[level] = 0
                continue

            # 计算层级得分（等权平均）
            level_score = normalized_df[available_factors].mean(axis=1)
            scores[level] = level_score

        # 按配置权重合成
        weights = self.config['weights']
        final_score = (
            scores.get('macro', 0) * weights.get('macro', 0) +
            scores.get('meso', 0) * weights.get('meso', 0) +
            scores.get('micro', 0) * weights.get('micro', 0)
        )

        # 归一化到[-1, 1]
        if final_score.std() > 0:
            final_score = (final_score - final_score.mean()) / final_score.std()
            final_score = final_score.clip(-2, 2) / 2

        return final_score

    def _equal_combine(self, normalized_df: pd.DataFrame) -> pd.Series:
        """等权合成"""
        return normalized_df.mean(axis=1)

    def _group_factors(self, factor_names: List[str]) -> Dict[str, List[str]]:
        """按层级分组因子"""
        groups = {
            'macro': ['shibor_on', 'north_flow_net', 'market_volume_ratio'],
            'meso': ['industry_relative_strength', 'industry_rank',
                    'industry_pe_percentile', 'industry_roe_trend'],
            'micro': ['turnover_extreme', 'momentum_20d', 'bias',
                     'longhubang_net_buy', 'margin_net_buy']
        }

        matched = {}
        for level, factors in groups.items():
            matched[level] = [f for f in factors if f in factor_names]

        return matched

    def calculate_level_scores(
        self,
        macro_factors: pd.Series = None,
        meso_factors: pd.Series = None,
        micro_factors: pd.Series = None
    ) -> Dict[str, float]:
        """
        计算各层级得分

        Returns:
            {'macro': score, 'meso': score, 'micro': score}
        """
        scores = {'macro': 0, 'meso': 0, 'micro': 0}

        if macro_factors is not None and not macro_factors.empty:
            scores['macro'] = macro_factors.mean()

        if meso_factors is not None and not meso_factors.empty:
            scores['meso'] = meso_factors.mean()

        if micro_factors is not None and not micro_factors.empty:
            scores['micro'] = micro_factors.mean()

        return scores


class MultiLevelSignalGenerator:
    """多层级信号生成器"""

    def __init__(self, macro_calc, meso_calc, micro_calc):
        self.macro_calc = macro_calc
        self.meso_calc = meso_calc
        self.micro_calc = micro_calc
        self.combiner = FactorCombiner()

    def generate_signal(self, symbol: str, industry: str = None) -> Dict:
        """
        为单只股票生成信号

        Returns:
            {
                'signal': float,  # 最终信号 [-1, 1]
                'macro_score': float,
                'meso_score': float,
                'micro_score': float,
                'factors': dict,
                'signal_level': str
            }
        """
        # 计算各层级因子
        macro_factors = self.macro_calc.calculate_all()
        meso_factors = self.meso_calc.calculate_for_stock(symbol, industry) if industry else pd.Series()
        micro_factors = self.micro_calc.calculate_for_stock(symbol)

        # 计算层级得分
        scores = self.combiner.calculate_level_scores(
            macro_factors, meso_factors, micro_factors
        )

        # 合成所有因子
        all_factors = pd.concat([macro_factors, meso_factors, micro_factors])

        # 计算最终信号
        if not all_factors.empty:
            # 转换为DataFrame以便合成
            factors_df = pd.DataFrame(all_factors).T
            signal = self.combiner.combine_factors(factors_df).iloc[0] if len(factors_df) > 0 else 0
        else:
            signal = 0

        # 确定信号等级
        signal_level = self._get_signal_level(signal)

        return {
            'signal': signal,
            'macro_score': scores['macro'],
            'meso_score': scores['meso'],
            'micro_score': scores['micro'],
            'factors': all_factors.to_dict(),
            'signal_level': signal_level
        }

    def _get_signal_level(self, signal: float) -> str:
        """获取信号等级"""
        if signal >= 0.7:
            return '强买入'
        elif signal >= 0.3:
            return '买入'
        elif signal >= -0.3:
            return '持有'
        elif signal >= -0.7:
            return '卖出'
        else:
            return '强卖出'

    def batch_generate(self, stocks: List[tuple]) -> pd.DataFrame:
        """
        批量生成信号

        Args:
            stocks: [(symbol, industry), ...]

        Returns:
            信号DataFrame
        """
        results = []

        for symbol, industry in stocks:
            try:
                signal_data = self.generate_signal(symbol, industry)
                signal_data['symbol'] = symbol
                signal_data['industry'] = industry
                results.append(signal_data)
            except Exception as e:
                logger.error(f"生成股票 {symbol} 信号失败: {e}")
                continue

        if not results:
            return pd.DataFrame()

        df = pd.DataFrame(results)
        df = df.set_index('symbol').sort_values('signal', ascending=False)
        return df
