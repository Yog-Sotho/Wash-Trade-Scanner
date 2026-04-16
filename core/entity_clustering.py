"""
Entity clustering based on funding links (ETH transfers).
Uses node RPC tracing or fallback block scanning to identify addresses controlled by the same entity.
"""

import asyncio
import logging
from typing import List, Dict, Set, Optional, Tuple
from collections import defaultdict

import networkx as nx
from web3 import AsyncWeb3, Web3
from web3.types import TxData, TraceFilterParams
from sqlalchemy import select, and_
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql import func

from models.schemas import AddressCluster, SwapTrade
from core.storage import Storage
from config.chains import get_chain_config

logger = logging.getLogger(__name__)


class EntityClusterer:
    """
    Clusters addresses that have received ETH from a common source.
    Uses on‑chain tracing to identify funding relationships.
    """

    def __init__(self, storage: Storage):
        self.storage = storage

    async def _node_supports_trace_filter(self, web3: AsyncWeb3) -> bool:
        """
        Check if the node supports the `trace_filter` RPC method.
        """
        try:
            # Test with a small range that is unlikely to return huge data
            test_params = TraceFilterParams(
                fromBlock=hex(1),
                toBlock=hex(1),
                fromAddress=["0x0000000000000000000000000000000000000000"],
            )
            await web3.provider.make_request("trace_filter", [test_params])
            return True
        except Exception:
            return False

    async def _fetch_funding_edges_trace_filter(
        self,
        web3: AsyncWeb3,
        addresses: Set[str],
        from_block: int,
        to_block: int,
    ) -> List[Tuple[str, str, int]]:
        """
        Use trace_filter to find all ETH transfers (value > 0) involving any of the given addresses.
        Returns list of (from, to, value_in_wei) tuples.
        """
        edges = []
        # Convert addresses to checksum format
        checksum_addresses = [Web3.to_checksum_address(addr) for addr in addresses]

        # trace_filter can be heavy; we split addresses into batches if many
        batch_size = 20
        for i in range(0, len(checksum_addresses), batch_size):
            batch = checksum_addresses[i:i + batch_size]
            params = TraceFilterParams(
                fromBlock=hex(from_block),
                toBlock=hex(to_block),
                fromAddress=batch,
                toAddress=batch,
            )
            try:
                traces = await web3.provider.make_request("trace_filter", [params])
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
        addresses: Set[str],
        from_block: int,
        to_block: int,
    ) -> List[Tuple[str, str, int]]:
        """
        Fallback method: scan each block in the range and inspect transaction `value`.
        Much slower but works when tracing is unavailable.
        """
        edges = []
        addresses_lower = {addr.lower() for addr in addresses}

        # Scan block by block
        for block_num in range(from_block, to_block + 1):
            try:
                block = await web3.eth.get_block(block_num, full_transactions=True)
                for tx in block.transactions:
                    if not isinstance(tx, dict):
                        continue  # In full_transactions mode, it's a dict
                    tx_from = tx.get("from", "").lower()
                    tx_to = tx.get("to", "").lower() if tx.get("to") else None
                    value = tx.get("value", 0)
                    if value > 0:
                        if tx_from in addresses_lower or (tx_to and tx_to in addresses_lower):
                            edges.append((tx_from, tx_to, value))
            except Exception as e:
                logger.error(f"Failed to fetch block {block_num}: {e}")
                continue

            # Throttle to avoid hitting rate limits
            await asyncio.sleep(0.01)
        return edges

    async def build_funding_graph(
        self,
        chain_id: int,
        addresses: List[str],
        session: AsyncSession,
        from_block_override: Optional[int] = None,
        to_block_override: Optional[int] = None,
    ) -> nx.DiGraph:
        """
        Build a directed graph where an edge A -> B exists if A sent ETH to B.
        Uses the most efficient RPC method available.
        """
        G = nx.DiGraph()
        addresses_set = set(addr.lower() for addr in addresses)
        if not addresses_set:
            return G

        chain_config = get_chain_config(chain_id)
        web3 = AsyncWeb3(AsyncWeb3.AsyncHTTPProvider(chain_config.rpc_url))

        # Determine block range
        if from_block_override is None:
            # Use a reasonable starting block (e.g., 1 year ago or chain start)
            # For production, this would be configurable
            latest = await web3.eth.block_number
            from_block = max(0, latest - 2_000_000)  # Approx 1 year for Ethereum
        else:
            from_block = from_block_override

        if to_block_override is None:
            to_block = await web3.eth.block_number
        else:
            to_block = to_block_override

        # Check for tracing support
        supports_trace = await self._node_supports_trace_filter(web3)

        if supports_trace:
            logger.info(f"Using trace_filter for chain {chain_id}")
            try:
                edges = await self._fetch_funding_edges_trace_filter(
                    web3, addresses_set, from_block, to_block
                )
            except Exception:
                logger.warning("trace_filter failed, falling back to block scan")
                edges = await self._fetch_funding_edges_block_scan(
                    web3, addresses_set, from_block, to_block
                )
        else:
            logger.info(f"trace_filter not supported, using block scan for chain {chain_id}")
            edges = await self._fetch_funding_edges_block_scan(
                web3, addresses_set, from_block, to_block
            )

        # Add nodes and edges
        for addr in addresses_set:
            G.add_node(addr)
        for from_addr, to_addr, value in edges:
            if from_addr and to_addr:
                G.add_edge(from_addr, to_addr, value=value)

        logger.info(f"Built funding graph with {G.number_of_nodes()} nodes and {G.number_of_edges()} edges")
        return G

    async def find_connected_components(
        self,
        graph: nx.DiGraph,
    ) -> List[Set[str]]:
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
    ) -> List[AddressCluster]:
        """
        Cluster all addresses involved in a pool's trades based on funding relationships.
        """
        # Get all unique addresses from trades in this pool
        stmt = select(SwapTrade.sender).where(
            and_(
                SwapTrade.chain_id == chain_id,
                SwapTrade.pool_address == pool_address,
            )
        ).distinct()
        result = await session.execute(stmt)
        senders = [r[0] for r in result.fetchall()]

        stmt = select(SwapTrade.recipient).where(
            and_(
                SwapTrade.chain_id == chain_id,
                SwapTrade.pool_address == pool_address,
            )
        ).distinct()
        result = await session.execute(stmt)
        recipients = [r[0] for r in result.fetchall()]

        all_addresses = list(set(senders + recipients))

        # Build funding graph using the entire chain history relevant to these addresses
        graph = await self.build_funding_graph(
            chain_id,
            all_addresses,
            session,
        )

        # Find clusters
        components = await self.find_connected_components(graph)

        clusters = []
        for i, component in enumerate(components):
            cluster_id = f"{chain_id}:{pool_address}:{i}"

            # Check if cluster already exists
            stmt = select(AddressCluster).where(AddressCluster.cluster_id == cluster_id)
            result = await session.execute(stmt)
            existing = result.scalar_one_or_none()

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