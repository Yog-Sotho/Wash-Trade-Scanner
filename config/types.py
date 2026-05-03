"""
Type definitions for blockchain and DEX configuration.

This module provides strongly-typed dataclasses for chain and DEX configuration,
replacing the dictionary-based approach for improved type safety and IDE support.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple


@dataclass(frozen=True, slots=True)
class DEXConfig:
    """
    Configuration for a decentralized exchange (DEX).

    Attributes:
        name: Human-readable DEX name (e.g., "UniswapV2", "PancakeSwap")
        router: Router contract address
        factory: Factory contract address
        event_sig: Event signature for swap events
        abi: Contract ABI for event decoding
        dex_type: DEX type for event processing ("v2", "v3", "curve", "balancer", "syncswap")
    """

    name: str
    router: str
    factory: str
    event_sig: str
    abi: List[Dict[str, Any]]
    dex_type: str = "v2"

    def __post_init__(self) -> None:
        """Validate DEX configuration after initialization."""
        if not self.name:
            raise ValueError("DEX name cannot be empty")
        if not self.router.startswith("0x") or len(self.router) != 42:
            raise ValueError(f"Invalid router address: {self.router}")
        if not self.factory.startswith("0x") or len(self.factory) != 42:
            raise ValueError(f"Invalid factory address: {self.factory}")
        if self.dex_type not in ("v2", "v3", "curve", "balancer", "syncswap"):
            raise ValueError(f"Invalid DEX type: {self.dex_type}")

    @property
    def event_signature_hash(self) -> str:
        """Compute the keccak256 hash of the event signature."""
        from web3 import Web3
        return Web3.keccak(text=self.event_sig).hex()

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "name": self.name,
            "router": self.router,
            "factory": self.factory,
            "event_sig": self.event_sig,
            "abi": self.abi,
            "type": self.dex_type,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "DEXConfig":
        """Create from dictionary."""
        return cls(
            name=data["name"],
            router=data["router"],
            factory=data["factory"],
            event_sig=data["event_sig"],
            abi=data["abi"],
            dex_type=data.get("type", "v2"),
        )


@dataclass(frozen=True, slots=True)
class ChainConfig:
    """
    Configuration for a blockchain network.

    Attributes:
        chain_id: Unique chain identifier (EIP-155 chain ID)
        name: Human-readable chain name (e.g., "Ethereum", "Polygon")
        rpc_url: HTTP RPC endpoint URL
        ws_url: WebSocket RPC endpoint URL (optional)
        native_token: Symbol of the chain's native token (e.g., "ETH", "MATIC")
        block_time: Average block time in seconds
        explorer_api: Block explorer API URL
        start_block: Starting block for historical sync
        dexes: List of DEX configurations on this chain
        chain_id_alias: Alternative chain ID references (optional)
    """

    chain_id: int
    name: str
    rpc_url: str
    ws_url: str
    native_token: str
    block_time: float
    explorer_api: str
    start_block: int
    dexes: List[DEXConfig] = field(default_factory=list)
    chain_id_aliases: Tuple[int, ...] = field(default_factory=(), repr=False)
    _rpc_url_placeholder: str = field(default="", repr=False, compare=False)

    def __post_init__(self) -> None:
        """Validate chain configuration after initialization."""
        if not self.name:
            raise ValueError("Chain name cannot be empty")
        if self.chain_id <= 0:
            raise ValueError(f"Invalid chain ID: {self.chain_id}")
        if not self.rpc_url:
            raise ValueError("RPC URL cannot be empty")
        if self.block_time <= 0:
            raise ValueError(f"Invalid block time: {self.block_time}")
        if self.start_block < 0:
            raise ValueError(f"Invalid start block: {self.start_block}")
        # Check for placeholder URLs
        if "YOUR_KEY" in self.rpc_url:
            object.__setattr__(self, "_rpc_url_placeholder", self.rpc_url)

    @property
    def has_placeholder_rpc(self) -> bool:
        """Check if RPC URL contains a placeholder requiring user configuration."""
        return bool(self._rpc_url_placeholder)

    @property
    def is_testnet(self) -> bool:
        """Infer if this is likely a testnet based on common patterns."""
        testnet_indicators = ("testnet", "test", "goerli", "sepolia", "mumbai", "rinkeby")
        name_lower = self.name.lower()
        return any(indicator in name_lower for indicator in testnet_indicators)

    def get_dex(self, dex_name: str) -> Optional[DEXConfig]:
        """Get DEX configuration by name (case-insensitive)."""
        for dex in self.dexes:
            if dex.name.lower() == dex_name.lower():
                return dex
        return None

    def get_dex_names(self) -> List[str]:
        """Get list of DEX names on this chain."""
        return [dex.name for dex in self.dexes]

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "chain_id": self.chain_id,
            "name": self.name,
            "rpc_url": self.rpc_url,
            "ws_url": self.ws_url,
            "native_token": self.native_token,
            "block_time": self.block_time,
            "explorer_api": self.explorer_api,
            "start_block": self.start_block,
            "dexes": [dex.to_dict() for dex in self.dexes],
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ChainConfig":
        """Create from dictionary."""
        dexes = [
            DEXConfig.from_dict(dex_data) if isinstance(dex_data, dict) else dex_data
            for dex_data in data.get("dexes", [])
        ]
        return cls(
            chain_id=data["chain_id"],
            name=data["name"],
            rpc_url=data["rpc_url"],
            ws_url=data.get("ws_url", ""),
            native_token=data["native_token"],
            block_time=data["block_time"],
            explorer_api=data["explorer_api"],
            start_block=data["start_block"],
            dexes=dexes,
        )


# Type aliases for common patterns
PoolTokens = Tuple[str, str]  # (token0, token1)
Address = str
BlockNumber = int
TransactionHash = str
USDValue = float


@dataclass(frozen=True)
class BlockRange:
    """
    Represents a range of blockchain blocks.

    Attributes:
        start: Starting block number (inclusive)
        end: Ending block number (inclusive)
    """

    start: BlockNumber
    end: BlockNumber

    def __post_init__(self) -> None:
        """Validate block range."""
        if self.start < 0:
            raise ValueError(f"Start block cannot be negative: {self.start}")
        if self.end < self.start:
            raise ValueError(f"End block {self.end} cannot be less than start block {self.start}")

    @property
    def size(self) -> int:
        """Return the number of blocks in this range."""
        return self.end - self.start + 1

    def contains(self, block: BlockNumber) -> bool:
        """Check if a block number is within this range."""
        return self.start <= block <= self.end

    def overlaps(self, other: "BlockRange") -> bool:
        """Check if this range overlaps with another range."""
        return self.start <= other.end and self.end >= other.start

    def split(self, chunk_size: int) -> List["BlockRange"]:
        """Split this range into smaller chunks."""
        if chunk_size <= 0:
            raise ValueError(f"Chunk size must be positive: {chunk_size}")
        chunks: List[BlockRange] = []
        current_start = self.start
        while current_start <= self.end:
            current_end = min(current_start + chunk_size - 1, self.end)
            chunks.append(BlockRange(start=current_start, end=current_end))
            current_start = current_end + 1
        return chunks


@dataclass
class RPCEndpointConfig:
    """
    Configuration for an RPC endpoint.

    Attributes:
        url: Full RPC endpoint URL
        chain_id: Associated chain ID
        is_websocket: Whether this is a WebSocket endpoint
        requires_auth: Whether the endpoint requires API key authentication
        rate_limit: Requests per second limit (0 = unlimited)
        timeout: Request timeout in seconds
    """

    url: str
    chain_id: int
    is_websocket: bool = False
    requires_auth: bool = True
    rate_limit: int = 10
    timeout: int = 60
    retries: int = 3

    def __post_init__(self) -> None:
        """Validate RPC endpoint configuration."""
        if not self.url:
            raise ValueError("RPC URL cannot be empty")
        if self.rate_limit < 0:
            raise ValueError(f"Rate limit cannot be negative: {self.rate_limit}")
        if self.timeout <= 0:
            raise ValueError(f"Timeout must be positive: {self.timeout}")
        if self.retries < 0:
            raise ValueError(f"Retries cannot be negative: {self.retries}")

    @property
    def base_url(self) -> str:
        """Extract base URL without path or query parameters."""
        from urllib.parse import urlparse
        parsed = urlparse(self.url)
        return f"{parsed.scheme}://{parsed.netloc}"
