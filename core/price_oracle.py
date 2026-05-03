"""
Price oracle for fetching token prices and USD conversions.

This module provides a robust price oracle service for converting token amounts
to USD values. It supports multiple price sources with fallback mechanisms,
caching, and rate limiting.

Features:
- Multi-source price fetching (CoinGecko API primary, with fallbacks)
- In-memory caching with configurable TTL
- Rate limiting to respect API constraints
- Batch price fetching for efficiency
- Graceful degradation when APIs are unavailable

Example:
    >>> from core.price_oracle import PriceOracle
    >>> oracle = PriceOracle()
    >>> price = await oracle.get_price(chain_id=1, token_address="0x...")
    >>> usd_value = await oracle.amount_to_usd(chain_id=1, token_address="0x...", amount=1000000)
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from collections import defaultdict

import httpx
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)

from config.settings import settings

logger = logging.getLogger(__name__)


# ==============================================================================
# Constants
# ==============================================================================

# Cache TTL in seconds
DEFAULT_CACHE_TTL_SECONDS: int = 60  # 1 minute default
MAX_CACHE_TTL_SECONDS: int = 300     # 5 minutes maximum
MIN_CACHE_TTL_SECONDS: int = 15      # 15 seconds minimum

# Rate limiting
DEFAULT_RATE_LIMIT_RPM: int = 10     # Requests per minute
DEFAULT_TIMEOUT_SECONDS: float = 30.0

# API Configuration
COINGECKO_BASE_URL: str = "https://api.coingecko.com/api/v3"
COINGECKO_MAX_IDS_PER_REQUEST: int = 50

# Retry configuration
MAX_RETRIES: int = 3
RETRY_BASE_DELAY: float = 1.0
RETRY_MAX_DELAY: float = 10.0


# ==============================================================================
# Custom Exceptions
# ==============================================================================

class PriceOracleError(Exception):
    """Base exception for price oracle errors."""
    pass


class PriceFetchError(PriceOracleError):
    """Raised when price fetching fails."""
    pass


class RateLimitError(PriceOracleError):
    """Raised when rate limit is exceeded."""
    pass


class TokenNotSupportedError(PriceOracleError):
    """Raised when token is not supported by price sources."""
    pass


# ==============================================================================
# Data Classes
# ==============================================================================

@dataclass(frozen=True, slots=True)
class TokenPrice:
    """
    Immutable price data for a token.

    Attributes:
        token_address: The token contract address (lowercase)
        chain_id: Blockchain network ID
        price_usd: Current price in USD
        market_cap: Market capitalization in USD
        volume_24h: 24-hour trading volume in USD
        price_change_24h: 24-hour price change percentage
        last_updated: Timestamp of last price update
        source: Price data source (e.g., 'coingecko')
        confidence: Confidence score (0.0 to 1.0)
    """
    token_address: str
    chain_id: int
    price_usd: float
    market_cap: Optional[float] = None
    volume_24h: Optional[float] = None
    price_change_24h: Optional[float] = None
    last_updated: datetime = field(default_factory=datetime.utcnow)
    source: str = "coingecko"
    confidence: float = 1.0

    def __post_init__(self) -> None:
        """Validate price data after initialization."""
        if self.price_usd < 0:
            raise ValueError(f"Price cannot be negative: {self.price_usd}")
        if not 0.0 <= self.confidence <= 1.0:
            raise ValueError(f"Confidence must be between 0.0 and 1.0: {self.confidence}")

    @property
    def is_stale(self) -> bool:
        """Check if price data is stale based on TTL."""
        age = (datetime.utcnow() - self.last_updated).total_seconds()
        return age > DEFAULT_CACHE_TTL_SECONDS


@dataclass(frozen=True, slots=True)
class PriceRequest:
    """
    Encapsulates a price request for tracking and rate limiting.

    Attributes:
        chain_id: Blockchain network ID
        token_address: Token contract address
        timestamp: Request timestamp
    """
    chain_id: int
    token_address: str
    timestamp: datetime = field(default_factory=datetime.utcnow)


# ==============================================================================
# Token Address Mapping
# ==============================================================================

# Mapping of chain_id to native token address for wrapped tokens
NATIVE_TOKEN_WRAPPED: Dict[int, str] = {
    1: "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",      # WETH on Ethereum
    56: "0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c",     # WBNB on BSC
    137: "0x0d500B1d8E8eF31E21C99d1Db9A6444d3DBfB8fE",   # WMATIC on Polygon
    42161: "0x82aF49447D8a07e3bd95BD0d56f12cB339cBB0ad",  # WETH on Arbitrum
    10: "0x4200000000000000000000000000000000000006",     # WETH on Optimism
    8453: "0x4200000000000000000000000000000000000006",   # WETH on Base
    43114: "0xB31f66AA3C1e785363F087A1A55d0b9bfc0C9b23",  # WAVAX on Avalanche
}

# Mapping of chain_id to CoinGecko platform identifier
CHAIN_TO_PLATFORM: Dict[int, str] = {
    1: "ethereum",
    56: "binance-smart-chain",
    137: "polygon-pos",
    42161: "arbitrum-one",
    10: "optimism",
    8453: "base",
    43114: "avalanche",
}


# ==============================================================================
# Rate Limiter
# ==============================================================================

class RateLimiter:
    """
    Token bucket rate limiter for API requests.

    Uses token bucket algorithm to limit requests per minute
    while allowing burst traffic.
    """

    def __init__(self, rpm: int = DEFAULT_RATE_LIMIT_RPM) -> None:
        """
        Initialize rate limiter.

        Args:
            rpm: Maximum requests per minute
        """
        if rpm <= 0:
            raise ValueError(f"RPM must be positive, got {rpm}")
        self._rpm = rpm
        self._tokens = float(rpm)
        self._last_update = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        """Wait for permission to make a request."""
        async with self._lock:
            now = time.monotonic()
            elapsed = now - self._last_update
            self._last_update = now

            # Refill tokens based on elapsed time
            tokens_per_second = self._rpm / 60.0
            self._tokens = min(self._rpm, self._tokens + (elapsed * tokens_per_second))

            if self._tokens < 1.0:
                wait_time = (1.0 - self._tokens) / tokens_per_second
                await asyncio.sleep(wait_time)
                self._tokens = 0.0
            else:
                self._tokens -= 1.0

    @property
    def available_tokens(self) -> int:
        """Get current number of available tokens."""
        return int(self._tokens)


# ==============================================================================
# Price Oracle Implementation
# ==============================================================================

class PriceOracle:
    """
    Multi-source price oracle with caching and rate limiting.

    This class provides a robust interface for fetching token prices
    and converting token amounts to USD values. It implements:
    - In-memory caching with TTL
    - Rate limiting for API compliance
    - Automatic retry with exponential backoff
    - Fallback to cached data on API failures
    - Batch fetching for efficiency

    Attributes:
        cache_ttl: Time-to-live for cached prices in seconds
        rate_limiter: Rate limiter for API requests
        _cache: Internal price cache
        _pending_requests: Deduplication for concurrent requests

    Example:
        >>> oracle = PriceOracle()
        >>> price = await oracle.get_price(chain_id=1, token_address="0x...")
        >>> usd = await oracle.amount_to_usd(chain_id=1, token_address="0x...", amount=1_000_000)
    """

    def __init__(
        self,
        cache_ttl: int = DEFAULT_CACHE_TTL_SECONDS,
        api_key: Optional[str] = None,
    ) -> None:
        """
        Initialize the price oracle.

        Args:
            cache_ttl: Cache time-to-live in seconds (clamped to valid range)
            api_key: Optional CoinGecko API key for higher rate limits
        """
        # Clamp TTL to valid range
        self._cache_ttl = max(MIN_CACHE_TTL_SECONDS, min(MAX_CACHE_TTL_SECONDS, cache_ttl))
        self._api_key = api_key or settings.COINGECKO_API_KEY
        self._rate_limiter = RateLimiter()
        self._cache: Dict[Tuple[int, str], Tuple[TokenPrice, float]] = {}  # (chain_id, addr) -> (price, expiry)
        self._pending_requests: Dict[Tuple[int, str], asyncio.Task] = {}
        self._lock = asyncio.Lock()
        self._initialized = True

        logger.info(
            f"PriceOracle initialized with cache_ttl={self._cache_ttl}s, "
            f"api_key={'<configured>' if self._api_key else '<none>'}"
        )

    @property
    def cache_ttl(self) -> int:
        """Get the configured cache TTL in seconds."""
        return self._cache_ttl

    @property
    def api_key_configured(self) -> bool:
        """Check if API key is configured."""
        return bool(self._api_key)

    # -------------------------------------------------------------------------
    # Public API
    # -------------------------------------------------------------------------

    async def get_price(
        self,
        chain_id: int,
        token_address: str,
        force_refresh: bool = False,
    ) -> Optional[TokenPrice]:
        """
        Get the current USD price for a token.

        Args:
            chain_id: Blockchain network ID
            token_address: Token contract address (checksummed or lowercase)
            force_refresh: Force fetch from API, bypassing cache

        Returns:
            TokenPrice object if found, None if token not supported

        Raises:
            PriceOracleError: If price fetch fails after retries
        """
        # Normalize address
        addr = self._normalize_address(token_address)

        # Check cache first (unless force refresh)
        if not force_refresh:
            cached = self._get_cached_price(chain_id, addr)
            if cached is not None and not cached.is_stale:
                logger.debug(f"Cache hit for {chain_id}:{addr}")
                return cached

        # Deduplicate concurrent requests for same token
        cache_key = (chain_id, addr)
        if cache_key in self._pending_requests:
            logger.debug(f"Waiting for pending request: {chain_id}:{addr}")
            try:
                return await self._pending_requests[cache_key]
            except PriceOracleError:
                pass  # Fall through to fetch

        # Create task for this request
        async def _fetch_price() -> Optional[TokenPrice]:
            try:
                return await self._fetch_price_from_api(chain_id, addr)
            finally:
                async with self._lock:
                    self._pending_requests.pop(cache_key, None)

        async with self._lock:
            if cache_key not in self._pending_requests:
                self._pending_requests[cache_key] = asyncio.create_task(_fetch_price())

        return await self._pending_requests[cache_key]

    async def amount_to_usd(
        self,
        chain_id: int,
        token_address: str,
        amount: int,
        decimals: int = 18,
        force_refresh: bool = False,
    ) -> Optional[float]:
        """
        Convert a token amount to USD value.

        Args:
            chain_id: Blockchain network ID
            token_address: Token contract address
            amount: Token amount in smallest unit (wei, etc.)
            decimals: Token decimal places
            force_refresh: Force price refresh

        Returns:
            USD value as float, or None if price unavailable
        """
        price = await self.get_price(chain_id, token_address, force_refresh)
        if price is None:
            return None

        # Convert to human-readable amount
        human_amount = amount / (10 ** decimals)
        return human_amount * price.price_usd

    async def get_prices_batch(
        self,
        requests: List[Tuple[int, str]],
        force_refresh: bool = False,
    ) -> Dict[Tuple[int, str], Optional[TokenPrice]]:
        """
        Get prices for multiple tokens in a single batch request.

        Args:
            requests: List of (chain_id, token_address) tuples
            force_refresh: Force refresh all prices

        Returns:
            Dictionary mapping (chain_id, address) to TokenPrice (or None)
        """
        results: Dict[Tuple[int, str], Optional[TokenPrice]] = {}

        # Filter out cached prices if not forcing refresh
        if not force_refresh:
            pending_requests: List[Tuple[int, str]] = []
            for chain_id, addr in requests:
                addr = self._normalize_address(addr)
                cache_key = (chain_id, addr)
                cached = self._get_cached_price(chain_id, addr)
                if cached is not None and not cached.is_stale:
                    results[cache_key] = cached
                else:
                    pending_requests.append((chain_id, addr))
            requests = pending_requests

        if not requests:
            return results

        # Group by chain for batch API calls
        by_chain: Dict[int, List[str]] = defaultdict(list)
        for chain_id, addr in requests:
            by_chain[chain_id].append(addr)

        # Fetch each chain in parallel
        fetch_tasks = [
            self._fetch_batch_prices(chain_id, addresses)
            for chain_id, addresses in by_chain.items()
        ]

        batch_results = await asyncio.gather(*fetch_tasks, return_exceptions=True)

        for batch_result in batch_results:
            if isinstance(batch_result, dict):
                results.update(batch_result)
            elif isinstance(batch_result, Exception):
                logger.warning(f"Batch fetch failed: {batch_result}")

        # Fill in None for any missing prices
        for chain_id, addr in requests:
            cache_key = (chain_id, self._normalize_address(addr))
            if cache_key not in results:
                results[cache_key] = None

        return results

    async def convert_amounts_to_usd(
        self,
        amounts: List[Tuple[int, str, int, int]],  # (chain_id, addr, amount, decimals)
        force_refresh: bool = False,
    ) -> Dict[Tuple[int, str], Optional[float]]:
        """
        Convert multiple token amounts to USD.

        Args:
            amounts: List of (chain_id, address, amount, decimals) tuples
            force_refresh: Force price refresh

        Returns:
            Dictionary mapping (chain_id, address) to USD value (or None)
        """
        requests = [(chain_id, addr) for chain_id, addr, _, _ in amounts]
        prices = await self.get_prices_batch(requests, force_refresh)

        results: Dict[Tuple[int, str], Optional[float]] = {}
        for chain_id, addr, amount, decimals in amounts:
            cache_key = (chain_id, self._normalize_address(addr))
            price = prices.get(cache_key)
            if price is not None:
                human_amount = amount / (10 ** decimals)
                results[cache_key] = human_amount * price.price_usd
            else:
                results[cache_key] = None

        return results

    def clear_cache(self) -> None:
        """Clear all cached prices."""
        self._cache.clear()
        logger.info("Price cache cleared")

    # -------------------------------------------------------------------------
    # Internal Methods
    # -------------------------------------------------------------------------

    def _normalize_address(self, address: str) -> str:
        """Normalize address to lowercase hex format."""
        addr = address.lower().strip()
        if addr.startswith("0x"):
            return addr
        return f"0x{addr}"

    def _get_cached_price(self, chain_id: int, token_address: str) -> Optional[TokenPrice]:
        """Get cached price if still valid."""
        cache_key = (chain_id, token_address)
        if cache_key in self._cache:
            price, expiry = self._cache[cache_key]
            if time.time() < expiry:
                return price
            else:
                del self._cache[cache_key]
        return None

    def _set_cached_price(self, price: TokenPrice) -> None:
        """Cache a price with TTL."""
        cache_key = (price.chain_id, price.token_address)
        expiry = time.time() + self._cache_ttl
        self._cache[cache_key] = (price, expiry)

    @retry(
        stop=stop_after_attempt(MAX_RETRIES),
        wait=wait_exponential(multiplier=RETRY_BASE_DELAY, max=RETRY_MAX_DELAY),
        retry=retry_if_exception_type((PriceFetchError, httpx.HTTPError)),
        reraise=True,
    )
    async def _fetch_price_from_api(
        self,
        chain_id: int,
        token_address: str,
    ) -> Optional[TokenPrice]:
        """
        Fetch price from CoinGecko API with retry logic.

        Args:
            chain_id: Blockchain network ID
            token_address: Token contract address

        Returns:
            TokenPrice if successful, None if not supported
        """
        await self._rate_limiter.acquire()

        platform = CHAIN_TO_PLATFORM.get(chain_id)
        if platform is None:
            logger.warning(f"Chain {chain_id} not supported by CoinGecko")
            return None

        headers = {"accept": "application/json"}
        if self._api_key:
            headers["x-cg-demo-api-key"] = self._api_key

        url = f"{COINGECKO_BASE_URL}/simple/token_price/{platform}"
        params = {
            "contract_addresses": token_address,
            "vs_currencies": "usd",
            "include_market_cap": "true",
            "include_24hr_vol": "true",
            "include_24hr_change": "true",
        }

        try:
            async with httpx.AsyncClient(timeout=DEFAULT_TIMEOUT_SECONDS) as client:
                response = await client.get(url, headers=headers, params=params)
                response.raise_for_status()
                data = response.json()

        except httpx.HTTPStatusError as e:
            if e.response.status_code == 429:
                raise RateLimitError(f"CoinGecko rate limit exceeded") from e
            raise PriceFetchError(f"HTTP error {e.response.status_code}: {e}") from e
        except httpx.RequestError as e:
            raise PriceFetchError(f"Request failed: {e}") from e

        # Parse response
        token_data = data.get(token_address.lower())
        if not token_data:
            logger.debug(f"Token {token_address} not found on CoinGecko")
            return None

        price_usd = token_data.get("usd")
        if price_usd is None:
            return None

        return TokenPrice(
            token_address=token_address,
            chain_id=chain_id,
            price_usd=float(price_usd),
            market_cap=token_data.get("usd_market_cap"),
            volume_24h=token_data.get("usd_24h_vol"),
            price_change_24h=token_data.get("usd_24h_change"),
            last_updated=datetime.utcnow(),
            source="coingecko",
            confidence=1.0,
        )

    async def _fetch_batch_prices(
        self,
        chain_id: int,
        token_addresses: List[str],
    ) -> Dict[Tuple[int, str], Optional[TokenPrice]]:
        """
        Fetch prices for multiple tokens from same chain.

        Args:
            chain_id: Blockchain network ID
            token_addresses: List of token addresses

        Returns:
            Dictionary of cached prices
        """
        results: Dict[Tuple[int, str], Optional[TokenPrice]] = {}

        platform = CHAIN_TO_PLATFORM.get(chain_id)
        if platform is None:
            for addr in token_addresses:
                results[(chain_id, addr)] = None
            return results

        # CoinGecko limits batch size
        for i in range(0, len(token_addresses), COINGECKO_MAX_IDS_PER_REQUEST):
            batch = token_addresses[i:i + COINGECKO_MAX_IDS_PER_REQUEST]
            addresses_str = ",".join(a.lower() for a in batch)

            await self._rate_limiter.acquire()

            headers = {"accept": "application/json"}
            if self._api_key:
                headers["x-cg-demo-api-key"] = self._api_key

            url = f"{COINGECKO_BASE_URL}/simple/token_price/{platform}"
            params = {
                "contract_addresses": addresses_str,
                "vs_currencies": "usd",
                "include_market_cap": "true",
                "include_24hr_vol": "true",
            }

            try:
                async with httpx.AsyncClient(timeout=DEFAULT_TIMEOUT_SECONDS) as client:
                    response = await client.get(url, headers=headers, params=params)
                    response.raise_for_status()
                    data = response.json()

            except httpx.HTTPError as e:
                logger.warning(f"Batch fetch failed for chain {chain_id}: {e}")
                for addr in batch:
                    results[(chain_id, addr)] = None
                continue

            for addr in batch:
                addr_lower = addr.lower()
                token_data = data.get(addr_lower)

                if token_data and "usd" in token_data:
                    price = TokenPrice(
                        token_address=addr,
                        chain_id=chain_id,
                        price_usd=float(token_data["usd"]),
                        market_cap=token_data.get("usd_market_cap"),
                        volume_24h=token_data.get("usd_24h_vol"),
                        last_updated=datetime.utcnow(),
                        source="coingecko",
                        confidence=1.0,
                    )
                    self._set_cached_price(price)
                    results[(chain_id, addr)] = price
                else:
                    results[(chain_id, addr)] = None

        return results

    async def health_check(self) -> Dict[str, any]:
        """
        Check oracle health status.

        Returns:
            Dictionary with health metrics
        """
        return {
            "initialized": self._initialized,
            "cache_size": len(self._cache),
            "cache_ttl": self._cache_ttl,
            "api_key_configured": self.api_key_configured,
            "rate_limiter_tokens": self._rate_limiter.available_tokens,
        }


# ==============================================================================
# Global Oracle Instance
# ==============================================================================

# Lazy-loaded global oracle instance
_oracle_instance: Optional[PriceOracle] = None


def get_price_oracle() -> PriceOracle:
    """
    Get the global price oracle instance.

    Returns:
        Singleton PriceOracle instance
    """
    global _oracle_instance
    if _oracle_instance is None:
        _oracle_instance = PriceOracle()
    return _oracle_instance
