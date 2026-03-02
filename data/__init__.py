"""
数据模块初始化
"""
# 旧的接口（向后兼容）
from .fetchers import AkShareFetcher, get_fetcher
from .storage import DataStorage, get_storage

# 新的数据适配器接口（推荐使用）
from .adapters import (
    IDataAdapter,
    AdapterConfig,
    get_adapter,
    create_default_adapter,
    list_adapters
)

__all__ = [
    # 旧接口
    'AkShareFetcher', 'get_fetcher',
    'DataStorage', 'get_storage',
    # 新接口
    'IDataAdapter', 'AdapterConfig',
    'get_adapter', 'create_default_adapter', 'list_adapters',
]
