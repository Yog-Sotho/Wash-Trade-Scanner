"""
Reference-asset registry for USD valuation of swaps.

A swap is valued from whichever leg is a *reference asset*: a token whose USD
price is known independently of the swap itself.

- STABLECOIN: USD stablecoins, valued at exactly $1.00.
- MARKET: wrapped or bridged market assets, valued at their CoinGecko coin's
  historical USD price (e.g. WETH on any chain -> "ethereum").

Provenance: every address/decimals pair below was verified on 2026-10-31
against three independent sources -- the issuer's own contract list where one
is published (Circle for USDC; Tether for USDT on Ethereum and Avalanche), the
Uniswap Labs Default token list (v22.33.0), and live on-chain calls confirming
contract code, symbol() and decimals() on each chain. Note BNB Smart Chain's
Binance-Peg USDC/USDT use 18 decimals, not 6.

Deliberately excluded:
- BUSD: Paxos ended issuance in February 2023, so a $1.00 assumption is unsafe.
- Chains without an entry here: their swaps stay unpriced (None) rather than
  being valued from an unverified source.

Tokens are identified by address only, never by symbol: symbols are
trivially spoofable, and a wash trader could deploy a token named "USDC".
Keys are lowercase, matching how the ingestor stores token addresses.
"""

from dataclasses import dataclass
from enum import StrEnum


class AssetKind(StrEnum):
    STABLECOIN = "stablecoin"
    MARKET = "market"


@dataclass(frozen=True)
class ReferenceAsset:
    symbol: str  # informational only; never used for matching
    decimals: int
    kind: AssetKind
    coingecko_id: str | None = None  # required for MARKET assets


REFERENCE_ASSETS: dict[int, dict[str, ReferenceAsset]] = {
    1: {  # Ethereum
        "0x6b175474e89094c44da98b954eedeac495271d0f": ReferenceAsset(
            "DAI", 18, AssetKind.STABLECOIN
        ),
        "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48": ReferenceAsset(
            "USDC", 6, AssetKind.STABLECOIN
        ),
        "0xdac17f958d2ee523a2206206994597c13d831ec7": ReferenceAsset(
            "USDT", 6, AssetKind.STABLECOIN
        ),
        "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2": ReferenceAsset(
            "WETH", 18, AssetKind.MARKET, "ethereum"
        ),
    },
    10: {  # OP Mainnet
        "0xda10009cbd5d07dd0cecc66161fc93d7c9000da1": ReferenceAsset(
            "DAI", 18, AssetKind.STABLECOIN
        ),
        "0x0b2c639c533813f4aa9d7837caf62653d097ff85": ReferenceAsset(
            "USDC", 6, AssetKind.STABLECOIN
        ),
        "0x7f5c764cbc14f9669b88837ca1490cca17c31607": ReferenceAsset(
            "USDC.e", 6, AssetKind.STABLECOIN
        ),
        "0x94b008aa00579c1307b0ef2c499ad98a8ce58e58": ReferenceAsset(
            "USDT", 6, AssetKind.STABLECOIN
        ),
        "0x01bff41798a0bcf287b996046ca68b395dbc1071": ReferenceAsset(
            "USDT0", 6, AssetKind.STABLECOIN
        ),
        "0x4200000000000000000000000000000000000006": ReferenceAsset(
            "WETH", 18, AssetKind.MARKET, "ethereum"
        ),
    },
    56: {  # BNB Smart Chain
        "0x1af3f329e8be154074d8769d1ffa4ee058b1dbc3": ReferenceAsset(
            "DAI", 18, AssetKind.STABLECOIN
        ),
        "0x8ac76a51cc950d9822d68b83fe1ad97b32cd580d": ReferenceAsset(
            "USDC", 18, AssetKind.STABLECOIN
        ),
        "0x55d398326f99059ff775485246999027b3197955": ReferenceAsset(
            "USDT", 18, AssetKind.STABLECOIN
        ),
        "0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c": ReferenceAsset(
            "WBNB", 18, AssetKind.MARKET, "binancecoin"
        ),
        "0x2170ed0880ac9a755fd29b2688956bd959f933f8": ReferenceAsset(
            "WETH", 18, AssetKind.MARKET, "ethereum"
        ),
    },
    137: {  # Polygon PoS
        "0x8f3cf7ad23cd3cadbd9735aff958023239c6a063": ReferenceAsset(
            "DAI", 18, AssetKind.STABLECOIN
        ),
        "0x3c499c542cef5e3811e1192ce70d8cc03d5c3359": ReferenceAsset(
            "USDC", 6, AssetKind.STABLECOIN
        ),
        "0x2791bca1f2de4661ed88a30c99a7a9449aa84174": ReferenceAsset(
            "USDC.e", 6, AssetKind.STABLECOIN
        ),
        "0xc2132d05d31c914a87c6611c10748aeb04b58e8f": ReferenceAsset(
            "USDT", 6, AssetKind.STABLECOIN
        ),
        "0x7ceb23fd6bc0add59e62ac25578270cff1b9f619": ReferenceAsset(
            "WETH", 18, AssetKind.MARKET, "ethereum"
        ),
        "0x0d500b1d8e8ef31e21c99d1db9a6444d3adf1270": ReferenceAsset(
            "WPOL", 18, AssetKind.MARKET, "polygon-ecosystem-token"
        ),
    },
    8453: {  # Base
        "0x50c5725949a6f0c72e6c4a641f24049a917db0cb": ReferenceAsset(
            "DAI", 18, AssetKind.STABLECOIN
        ),
        "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913": ReferenceAsset(
            "USDC", 6, AssetKind.STABLECOIN
        ),
        "0xfde4c96c8593536e31f229ea8f37b2ada2699bb2": ReferenceAsset(
            "USDT", 6, AssetKind.STABLECOIN
        ),
        "0xd9aaec86b65d86f6a7b5b1b0c42ffa531710b6ca": ReferenceAsset(
            "USDbC", 6, AssetKind.STABLECOIN
        ),
        "0x4200000000000000000000000000000000000006": ReferenceAsset(
            "WETH", 18, AssetKind.MARKET, "ethereum"
        ),
    },
    42161: {  # Arbitrum One
        "0xda10009cbd5d07dd0cecc66161fc93d7c9000da1": ReferenceAsset(
            "DAI", 18, AssetKind.STABLECOIN
        ),
        "0xaf88d065e77c8cc2239327c5edb3a432268e5831": ReferenceAsset(
            "USDC", 6, AssetKind.STABLECOIN
        ),
        "0xff970a61a04b1ca14834a43f5de4533ebddb5cc8": ReferenceAsset(
            "USDC.e", 6, AssetKind.STABLECOIN
        ),
        "0xfd086bc7cd5c481dcc9c85ebe478a1c0b69fcbb9": ReferenceAsset(
            "USDT0", 6, AssetKind.STABLECOIN
        ),
        "0x82af49447d8a07e3bd95bd0d56f35241523fbab1": ReferenceAsset(
            "WETH", 18, AssetKind.MARKET, "ethereum"
        ),
    },
    43114: {  # Avalanche C-Chain
        "0xb97ef9ef8734c71904d8002f8b6bc66dd9c48a6e": ReferenceAsset(
            "USDC", 6, AssetKind.STABLECOIN
        ),
        "0x9702230a8ea53601f5cd2dc00fdbc13d4df4a8c7": ReferenceAsset(
            "USDT", 6, AssetKind.STABLECOIN
        ),
        "0xb31f66aa3c1e785363f0875a1b74e27b85fd66c7": ReferenceAsset(
            "WAVAX", 18, AssetKind.MARKET, "avalanche-2"
        ),
        "0x49d5c2bdffac6ce2bfdb6640f4f80f226bc10bab": ReferenceAsset(
            "WETH", 18, AssetKind.MARKET, "ethereum"
        ),
    },
}


def get_reference_asset(chain_id: int, token_address: str) -> ReferenceAsset | None:
    """Return the reference asset for a token on a chain, or None if it is not one."""
    return REFERENCE_ASSETS.get(chain_id, {}).get(token_address.lower())
