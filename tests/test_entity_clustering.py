"""
Unit tests for entity clustering (funding-graph based address clustering).
"""

from unittest.mock import AsyncMock, MagicMock, patch

import networkx as nx
import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from core.entity_clustering import EntityClusterer
from core.storage import Storage
from models.schemas import AddressCluster


@pytest.fixture
def clusterer():
    return EntityClusterer(storage=AsyncMock(spec=Storage))


@pytest.mark.asyncio
async def test_find_connected_components_groups_linked_addresses(clusterer):
    graph: nx.DiGraph[str] = nx.DiGraph()
    graph.add_edge("0xa", "0xb")
    graph.add_edge("0xc", "0xd")
    graph.add_node("0xe")  # isolated, should be excluded (component size 1)

    components = await clusterer.find_connected_components(graph)

    assert len(components) == 2
    assert {"0xa", "0xb"} in components
    assert {"0xc", "0xd"} in components


@pytest.mark.asyncio
async def test_find_connected_components_empty_graph(clusterer):
    graph: nx.DiGraph[str] = nx.DiGraph()
    components = await clusterer.find_connected_components(graph)
    assert components == []


def _mock_session_with_scalars(rows_by_call):
    """Return a session whose session.execute(...) results yield rows_by_call in order."""
    session = AsyncMock(spec=AsyncSession)
    results = []
    for rows in rows_by_call:
        result = MagicMock()
        result.scalars.return_value.all.return_value = rows
        result.fetchall.return_value = [(r,) for r in rows]
        result.scalar_one_or_none.return_value = None
        results.append(result)
    session.execute = AsyncMock(side_effect=results)
    return session


@pytest.mark.asyncio
async def test_cluster_addresses_creates_new_clusters(clusterer):
    # senders query, recipients query, then one "does cluster already exist" query
    # per discovered component (here: a single alice<->bob component).
    session = _mock_session_with_scalars([["0xalice"], ["0xbob"], []])

    graph: nx.DiGraph[str] = nx.DiGraph()
    graph.add_edge("0xalice", "0xbob")

    with patch.object(clusterer, "build_funding_graph", AsyncMock(return_value=graph)):
        clusters = await clusterer.cluster_addresses(1, "0xpool", session)

    assert len(clusters) == 1
    assert isinstance(clusters[0], AddressCluster)
    assert set(clusters[0].addresses) == {"0xalice", "0xbob"}
    session.add.assert_called_once()
    session.commit.assert_awaited_once()


@pytest.mark.asyncio
async def test_cluster_addresses_no_edges_yields_no_clusters(clusterer):
    session = _mock_session_with_scalars([["0xalice"], ["0xbob"]])

    with patch.object(clusterer, "build_funding_graph", AsyncMock(return_value=nx.DiGraph())):
        clusters = await clusterer.cluster_addresses(1, "0xpool", session)

    assert clusters == []
    session.commit.assert_awaited_once()
