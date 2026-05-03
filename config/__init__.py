"""
Configuration package for wash trade detection system.

This package provides configuration management for:
- Blockchain network settings (chains, RPC endpoints)
- DEX integrations (ABIs, router/factory addresses)
- Application settings (database, logging, detection thresholds)
- Type definitions for type-safe configuration

Example:
    >>> from config import settings, CHAINS, get_chain_config, ChainConfig
    >>> ethereum = get_chain_config(chain_id=1)
    >>> print(f"Ethereum RPC: {ethereum.rpc_url}")
"""

from .settings import settings
from .chains import CHAINS, get_chain_config, get_dex_config
from .types import (
    ChainConfig,
    DEXConfig,
    BlockRange,
    PoolTokens,
    RPCEndpointConfig,
)

__all__ = [
    # Settings
    "settings",
    # Chain configuration
    "CHAINS",
    "get_chain_config",
    "get_dex_config",
    # Type definitions
    "ChainConfig",
    "DEXConfig",
    "BlockRange",
    "PoolTokens",
    "RPCEndpointConfig",
]

__version__ = "1.0.0"
