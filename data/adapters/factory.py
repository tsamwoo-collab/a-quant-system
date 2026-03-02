"""
数据适配器工厂
根据配置创建对应的数据适配器实例
"""
import logging
from typing import Optional, Dict

from .base import IDataAdapter, AdapterConfig
from .akshare_adapter import AkShareAdapter
from .tushare_adapter import TushareAdapter

logger = logging.getLogger(__name__)

# 适配器注册表
_ADAPTER_REGISTRY = {
    'akshare': AkShareAdapter,
    'tushare': TushareAdapter,
}


def get_adapter(config: Optional[AdapterConfig] = None) -> IDataAdapter:
    """获取数据适配器实例

    Args:
        config: 适配器配置，默认使用 AkShare

    Returns:
        数据适配器实例

    Raises:
        ValueError: 不支持的适配器类型
    """
    if config is None:
        config = AdapterConfig()

    adapter_type = config.adapter_type.lower()

    if adapter_type not in _ADAPTER_REGISTRY:
        raise ValueError(
            f"不支持的数据源: {adapter_type}，"
            f"支持的类型: {list(_ADAPTER_REGISTRY.keys())}"
        )

    adapter_class = _ADAPTER_REGISTRY[adapter_type]

    # 根据类型创建适配器
    if adapter_type == 'akshare':
        adapter = adapter_class(
            max_retries=config.akshare_config.get('max_retries', 3),
            retry_delay=config.akshare_config.get('retry_delay', 2.0),
            proxy=config.akshare_config.get('proxy')
        )
    elif adapter_type == 'tushare':
        token = config.tushare_config.get('token')
        if not token:
            raise ValueError("使用 Tushare 需要配置 token，请在 config.tushare_config 中设置")

        adapter = adapter_class(
            token=token,
            timeout=config.tushare_config.get('timeout', 30),
            max_retries=config.tushare_config.get('max_retries', 3)
        )
    else:
        adapter = adapter_class()

    logger.info(f"✅ 创建数据适配器: {adapter.name}")
    return adapter


def register_adapter(name: str, adapter_class: type):
    """注册新的适配器类型

    Args:
        name: 适配器名称
        adapter_class: 适配器类
    """
    _ADAPTER_REGISTRY[name.lower()] = adapter_class
    logger.info(f"注册数据适配器: {name}")


def list_adapters() -> Dict[str, Dict[str, any]]:
    """列出所有可用的适配器类型

    Returns:
        字典，键为适配器名称，值为适配器信息
    """
    return {
        'akshare': {
            'name': 'AkShare',
            'description': '免费开源的财经数据接口',
            'is_paid': False,
            'class': AkShareAdapter
        },
        'tushare': {
            'name': 'Tushare',
            'description': '专业的财经数据接口（需积分）',
            'is_paid': True,
            'class': TushareAdapter,
            'note': '需要配置 token'
        }
    }


def create_default_adapter() -> IDataAdapter:
    """创建默认适配器（AkShare）"""
    return get_adapter(AdapterConfig(adapter_type='akshare'))
