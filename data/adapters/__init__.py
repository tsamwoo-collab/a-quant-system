"""
数据适配器模块
提供统一的数据接口，支持多种数据源切换
"""
from .base import IDataAdapter, AdapterConfig
from .factory import (
    get_adapter,
    create_default_adapter,
    register_adapter,
    list_adapters
)

__all__ = [
    'IDataAdapter',
    'AdapterConfig',
    'get_adapter',
    'create_default_adapter',
    'register_adapter',
    'list_adapters',
]
