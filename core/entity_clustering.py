"""
Entity clustering based on funding links (ETH transfers).
Uses node RPC tracing or fallback block scanning to identify addresses controlled by the same entity.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import asdict, dataclass
from typing import Any

import networkx as nx
from sqlalchemy import and_, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql import func
from web3 import AsyncWeb3, Web3
from web3.types import RPCEndpoint, TraceFilterParams

from config.chains import get_chain_config
from config.settings import settings
from core.circuit_breaker import CircuitBreaker
from core.exceptions import CircuitBreakerOpenError, RPCError
from core.ingestor import RateLimiter, chain_breaker, chain_rate_limiter
from core.rpc import InstrumentedHTTPProvider
from core.storage import Storage
from models.schemas import AddressCluster, SwapTrade

logger = logging.getLogger(__name__)

# Default window searched for funding links: about a year of Ethereum blocks.
FUNDING_WINDOW_BLOCKS = 2_000_000


@dataclass
class FundingCoverage:
    """What funding-link clustering examined, reported with the audit: a pool
    with no clusters must not be mistaken for a pool with no funding links
    when the links were never looked for.

    method: "trace_filter" (the whole window, one query per 20 addresses),
    "block_scan" (each block of `scanned`, one request per block), "skipped"
    (the node has no trace_filter and the scan is off) or "failed".
    """

    method: str
    window: tuple[int, int] | None = None  # blocks the links were wanted for
    scanned: tuple[int, int] | None = None  # blocks actually examined
    blocks_failed: int = 0  # of `scanned`, blocks that could not be fetched
    reason: str | None = None

    @property
    def complete(self) -> bool:
        return (
            self.method in ("trace_filter", "block_scan")
            and self.scanned == self.window
            and not self.blocks_failed
        )

    def as_dict(self) -> dict[str, Any]:
        return {**asdict(self), "complete": self.complete}


class EntityClusterer:
    """
    Clusters addresses that have received ETH from a common source.
    Uses on‑chain tracing to identify funding relationships.
    """

    def __init__(self, storage: Storage):
        self.storage = storage
        # Set by build_funding_graph: what the last clustering examined.
        self.coverage: FundingCoverage | None = None
        self._limiter: RateLimiter | None = None
        self._breaker: CircuitBreaker | None = None

    def _use_chain(self, chain_id: int) -> None:
        """Share the chain's process-wide rate limit and breaker with ingestion."""
        self._limiter = chain_rate_limiter(chain_id)
        self._breaker = chain_breaker(chain_id)

    async def _request(self, web3: AsyncWeb3, method: str, params: list[Any]) -> Any:
        """A raw JSON-RPC call under the chain's rate limit and breaker (when
        set); returns the response dict, JSON-RPC errors included."""

        async def call() -> Any:
            return await web3.provider.make_request(RPCEndpoint(method), params)

        if self._limiter is not None:
            await self._limiter.acquire()
        if self._breaker is not None:
            return await self._breaker.call(call)
        return await call()

    async def _node_supports_trace_filter(self, web3: AsyncWeb3) -> bool:
        """
        Check if the node supports the `trace_filter` RPC method.
        """
        try:
            # Test with a small range that is unlikely to return huge data
            test_params = TraceFilterParams(
                fromBlock=1,
                toBlock=1,
                fromAddress=[
                    Web3.to_checksum_address("0x0000000000000000000000000000000000000000")
                ],
            )
            response = await self._request(web3, "trace_filter", [test_params])
        except Exception:
            return False
        # make_request returns JSON-RPC error responses (e.g. "method not found")
        # instead of raising them.
        return "error" not in response

    async def _fetch_funding_edges_trace_filter(
        self,
        web3: AsyncWeb3,
        addresses: set[str],
        from_block: int,
        to_block: int,
    ) -> list[tuple[str, str | None, int]]:
        """
        Use trace_filter to find all ETH transfers (value > 0) involving any of the given addresses.
        Returns list of (from, to, value_in_wei) tuples.
        """
        edges: list[tuple[str, str | None, int]] = []
        # Convert addresses to checksum format
        checksum_addresses = [Web3.to_checksum_address(addr) for addr in addresses]

        # trace_filter can be heavy; we split addresses into batches if many
        batch_size = 20
        for i in range(0, len(checksum_addresses), batch_size):
            batch = checksum_addresses[i : i + batch_size]
            params = TraceFilterParams(
                fromBlock=from_block,
                toBlock=to_block,
                fromAddress=batch,
                toAddress=batch,
            )
            try:
                traces = await self._request(web3, "trace_filter", [params])
                if "error" in traces:
                    # Reading an error response as "no traces" would silently
                    # report zero funding links.
                    raise RPCError(f"trace_filter returned an error: {traces['error']}")
                for trace in traces.get("result", []):
                    action = trace.get("action", {})
                    from_addr = action.get("from")
                    to_addr = action.get("to")
                    value = int(action.get("value", "0x0"), 16)
                    if value > 0 and from_addr and to_addr:
                        edges.append((from_addr.lower(), to_addr.lower(), value))
            except Exception as e:
                logger.error(f"trace_filter failed for batch: {e}")
                # If trace_filter fails entirely, fall back to block scanning
                raise
        return edges

    async def _fetch_funding_edges_block_scan(
        self,
        web3: AsyncWeb3,
        addresses: set[str],
        from_block: int,
        to_block: int,
    ) -> tuple[list[tuple[str, str | None, int]], int]:
        """Fallback: fetch every block of the range with its transactions and
        keep the value transfers touching `addresses`. One request per block,
        RPC_CONCURRENCY at a time under the chain's rate limit.

        Returns (edges, blocks that failed every attempt): a failed block is
        counted, not silently skipped.
        """
        edges: list[tuple[str, str | None, int]] = []
        addresses_lower = {addr.lower() for addr in addresses}
        slots = asyncio.Semaphore(settings.RPC_CONCURRENCY)
        failed = 0

        async def scan(block_num: int) -> None:
            nonlocal failed
            block = None
            for attempt in range(settings.RPC_RETRIES + 1):
                try:
                    async with slots:
                        response = await self._request(
                            web3, "eth_getBlockByNumber", [hex(block_num), True]
                        )
                    block = response.get("result")
                    if isinstance(block, dict):
                        break
                    raise RPCError(str(response.get("error") or "block not found"))
                except CircuitBreakerOpenError:
                    raise
                except Exception as e:
                    block = None
                    logger.debug(f"Block {block_num} (attempt {attempt + 1}): {e}")
            if block is None:
                failed += 1
                return
            for tx in block.get("transactions") or []:
                if not isinstance(tx, dict):
                    continue
                tx_from = str(tx.get("from") or "").lower()
                tx_to = str(tx["to"]).lower() if tx.get("to") else None
                value = tx.get("value") or 0
                value = int(value, 16) if isinstance(value, str) else int(value)
                if value > 0 and (
                    tx_from in addresses_lower or (tx_to and tx_to in addresses_lower)
                ):
                    edges.append((tx_from, tx_to, value))

        await asyncio.gather(*(scan(n) for n in range(from_block, to_block + 1)))
        if failed:
            logger.warning(
                f"{failed} of {to_block - from_block + 1} blocks could not be fetched for "
                "funding links; clustering coverage is partial."
            )
        return edges, failed

    async def _block_scan_fallback(
        self,
        web3: AsyncWeb3,
        addresses: set[str],
        from_block: int,
        to_block: int,
        chain_id: int,
        reason: str,
    ) -> list[tuple[str, str | None, int]]:
        """Scan blocks one RPC call each, bounded by ENTITY_SCAN_MAX_BLOCKS
        (at most 50,000: about a week of Ethereum blocks).

        Unbounded, the default ~2M-block window means ~2M eth_getBlock calls:
        an audit that effectively never finishes. So the fallback is off by
        default and, when enabled, covers only the most recent
        ENTITY_SCAN_MAX_BLOCKS blocks of the window. self.coverage says which.
        """
        window = (from_block, to_block)
        cap = settings.ENTITY_SCAN_MAX_BLOCKS
        if cap == 0:
            logger.warning(
                f"Chain {chain_id}: skipping funding-link clustering because the node "
                "does not support trace_filter. Use a trace-enabled node, or set "
                "ENTITY_SCAN_MAX_BLOCKS to scan recent blocks instead (one RPC call per block)."
            )
            self.coverage = FundingCoverage(
                "skipped", window, reason=f"{reason}; ENTITY_SCAN_MAX_BLOCKS is 0"
            )
            return []
        size = to_block - from_block + 1
        if size > cap:
            logger.warning(
                f"Chain {chain_id}: scanning only the latest {cap} of {size} blocks for "
                "funding links (ENTITY_SCAN_MAX_BLOCKS); clustering coverage is partial."
            )
            from_block = to_block - cap + 1
        edges, failed = await self._fetch_funding_edges_block_scan(
            web3, addresses, from_block, to_block
        )
        self.coverage = FundingCoverage(
            "block_scan", window, (from_block, to_block), failed, reason=reason
        )
        return edges

    async def build_funding_graph(
        self,
        chain_id: int,
        addresses: list[str],
        session: AsyncSession,
        from_block_override: int | None = None,
        to_block_override: int | None = None,
    ) -> nx.DiGraph[str]:
        """
        Build a directed graph where an edge A -> B exists if A sent ETH to B.
        Uses the most efficient RPC method available.
        """
        G: nx.DiGraph[str] = nx.DiGraph()
        addresses_set = {addr.lower() for addr in addresses}
        if not addresses_set:
            self.coverage = FundingCoverage("skipped", reason="no addresses")
            return G

        chain_config = get_chain_config(chain_id)
        web3 = AsyncWeb3(InstrumentedHTTPProvider(chain_config["rpc_url"], chain_id))
        self._use_chain(chain_id)

        # Block range: the FUNDING_WINDOW_BLOCKS up to the head unless given.
        latest: int | None = None
        if from_block_override is None or to_block_override is None:
            latest = int(await web3.eth.block_number)
        to_block = to_block_override if to_block_override is not None else int(latest or 0)
        from_block = (
            from_block_override
            if from_block_override is not None
            else max(0, int(latest or 0) - FUNDING_WINDOW_BLOCKS)
        )

        # Check for tracing support
        supports_trace = await self._node_supports_trace_filter(web3)

        if supports_trace:
            logger.info(f"Using trace_filter for chain {chain_id}")
            try:
                edges = await self._fetch_funding_edges_trace_filter(
                    web3, addresses_set, from_block, to_block
                )
                window = (from_block, to_block)
                self.coverage = FundingCoverage("trace_filter", window, window)
            except Exception as exc:
                logger.warning(f"trace_filter failed on chain {chain_id}")
                edges = await self._block_scan_fallback(
                    web3,
                    addresses_set,
                    from_block,
                    to_block,
                    chain_id,
                    reason=f"trace_filter failed ({type(exc).__name__})",
                )
        else:
            logger.info(f"trace_filter not supported on chain {chain_id}")
            edges = await self._block_scan_fallback(
                web3,
                addresses_set,
                from_block,
                to_block,
                chain_id,
                reason="the node does not support trace_filter",
            )

        # Add nodes and edges
        for addr in addresses_set:
            G.add_node(addr)
        for from_addr, to_addr, value in edges:
            if from_addr and to_addr:
                G.add_edge(from_addr, to_addr, value=value)

        logger.info(
            f"Built funding graph with {G.number_of_nodes()} nodes and {G.number_of_edges()} edges"
        )
        return G

    async def find_connected_components(
        self,
        graph: nx.DiGraph[str],
    ) -> list[set[str]]:
        """
        Find weakly connected components in the funding graph.
        These represent potential entity clusters.
        """
        components = list(nx.weakly_connected_components(graph))
        return [set(c) for c in components if len(c) > 1]

    async def cluster_addresses(
        self,
        chain_id: int,
        pool_address: str,
        session: AsyncSession,
    ) -> list[AddressCluster]:
        """
        Cluster all addresses involved in a pool's trades based on funding relationships.
        """
        # Get all unique addresses from trades in this pool
        senders_stmt = (
            select(SwapTrade.sender)
            .where(
                and_(
                    SwapTrade.chain_id == chain_id,
                    SwapTrade.pool_address == pool_address,
                )
            )
            .distinct()
        )
        senders_result = await session.execute(senders_stmt)
        senders = [r[0] for r in senders_result.fetchall()]

        recipients_stmt = (
            select(SwapTrade.recipient)
            .where(
                and_(
                    SwapTrade.chain_id == chain_id,
                    SwapTrade.pool_address == pool_address,
                )
            )
            .distinct()
        )
        recipients_result = await session.execute(recipients_stmt)
        recipients = [r[0] for r in recipients_result.fetchall()]

        all_addresses = list(set(senders + recipients))

        # Build funding graph using the entire chain history relevant to these addresses
        graph = await self.build_funding_graph(
            chain_id,
            all_addresses,
            session,
        )

        # Find clusters
        components = await self.find_connected_components(graph)

        clusters: list[AddressCluster] = []
        ids = [f"{chain_id}:{pool_address}:{i}" for i in range(len(components))]
        stored: dict[str, AddressCluster] = {}
        if ids:
            # One query for every cluster already stored, not one per cluster.
            result = await session.execute(
                select(AddressCluster).where(AddressCluster.cluster_id.in_(ids))
            )
            stored = {cluster.cluster_id: cluster for cluster in result.scalars().all()}
        for cluster_id, component in zip(ids, components, strict=True):
            existing = stored.get(cluster_id)
            if existing:
                existing.addresses = list(component)
                existing.last_updated = func.now()
                clusters.append(existing)
            else:
                cluster = AddressCluster(
                    cluster_id=cluster_id,
                    addresses=list(component),
                    confidence_score=0.8,  # Based on funding link strength
                )
                session.add(cluster)
                clusters.append(cluster)

        await session.commit()
        logger.info(f"Clustered {len(all_addresses)} addresses into {len(clusters)} clusters")
        return clusters
