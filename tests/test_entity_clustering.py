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


@pytest.mark.asyncio
async def test_node_supports_trace_filter_true(clusterer):
    web3 = AsyncMock()
    web3.provider.make_request = AsyncMock(return_value={"result": []})
    assert await clusterer._node_supports_trace_filter(web3) is True


@pytest.mark.asyncio
async def test_node_supports_trace_filter_false(clusterer):
    web3 = AsyncMock()
    web3.provider.make_request = AsyncMock(side_effect=RuntimeError("method not found"))
    assert await clusterer._node_supports_trace_filter(web3) is False


ALICE = "0x" + "1" * 40
BOB = "0x" + "2" * 40
CAROL = "0x" + "3" * 40


@pytest.mark.asyncio
async def test_fetch_funding_edges_trace_filter(clusterer):
    web3 = AsyncMock()
    web3.provider.make_request = AsyncMock(
        return_value={
            "result": [
                {
                    "action": {
                        "from": ALICE,
                        "to": BOB,
                        "value": "0x64",  # 100 wei
                    }
                },
                {
                    "action": {
                        "from": ALICE,
                        "to": CAROL,
                        "value": "0x0",  # zero-value transfer, should be skipped
                    }
                },
            ]
        }
    )

    edges = await clusterer._fetch_funding_edges_trace_filter(
        web3, {ALICE, BOB}, from_block=1, to_block=100
    )

    assert edges == [(ALICE.lower(), BOB.lower(), 100)]


@pytest.mark.asyncio
async def test_fetch_funding_edges_trace_filter_raises_on_failure(clusterer):
    web3 = AsyncMock()
    web3.provider.make_request = AsyncMock(side_effect=RuntimeError("rpc error"))

    with pytest.raises(RuntimeError):
        await clusterer._fetch_funding_edges_trace_filter(web3, {ALICE}, from_block=1, to_block=100)


@pytest.mark.asyncio
async def test_fetch_funding_edges_block_scan(clusterer):
    web3 = AsyncMock()
    web3.eth.get_block = AsyncMock(
        return_value={
            "transactions": [
                {"from": "0xAlice", "to": "0xBob", "value": 500},
                {"from": "0xStranger", "to": "0xNobody", "value": 999},  # unrelated, skipped
                "0xnot-a-dict-should-be-skipped",
            ]
        }
    )

    edges = await clusterer._fetch_funding_edges_block_scan(
        web3, {"0xalice", "0xbob"}, from_block=1, to_block=1
    )

    assert edges == [("0xalice", "0xbob", 500)]


@pytest.mark.asyncio
async def test_fetch_funding_edges_block_scan_handles_block_error(clusterer):
    web3 = AsyncMock()
    web3.eth.get_block = AsyncMock(side_effect=RuntimeError("boom"))

    edges = await clusterer._fetch_funding_edges_block_scan(
        web3, {"0xalice"}, from_block=1, to_block=1
    )
    assert edges == []


@pytest.mark.asyncio
async def test_build_funding_graph_no_addresses(clusterer):
    graph = await clusterer.build_funding_graph(1, [], session=AsyncMock())
    assert graph.number_of_nodes() == 0


@pytest.mark.asyncio
async def test_build_funding_graph_uses_trace_filter(clusterer):
    with patch("core.entity_clustering.AsyncWeb3") as MockWeb3:
        web3 = AsyncMock()
        web3.eth.block_number = 1000
        MockWeb3.return_value = web3
        MockWeb3.AsyncHTTPProvider = MagicMock()

        with (
            patch.object(clusterer, "_node_supports_trace_filter", AsyncMock(return_value=True)),
            patch.object(
                clusterer,
                "_fetch_funding_edges_trace_filter",
                AsyncMock(return_value=[("0xalice", "0xbob", 100)]),
            ),
        ):
            graph = await clusterer.build_funding_graph(
                1,
                ["0xalice", "0xbob"],
                session=AsyncMock(),
                from_block_override=0,
                to_block_override=1000,
            )

    assert graph.has_edge("0xalice", "0xbob")
    assert graph.number_of_nodes() == 2


@pytest.mark.asyncio
async def test_build_funding_graph_falls_back_to_block_scan(clusterer):
    with patch("core.entity_clustering.AsyncWeb3") as MockWeb3:
        web3 = AsyncMock()
        MockWeb3.return_value = web3
        MockWeb3.AsyncHTTPProvider = MagicMock()

        with (
            patch.object(clusterer, "_node_supports_trace_filter", AsyncMock(return_value=True)),
            patch.object(
                clusterer,
                "_fetch_funding_edges_trace_filter",
                AsyncMock(side_effect=RuntimeError("trace_filter unsupported mid-scan")),
            ),
            patch.object(
                clusterer,
                "_fetch_funding_edges_block_scan",
                AsyncMock(return_value=[("0xalice", "0xbob", 50)]),
            ),
        ):
            graph = await clusterer.build_funding_graph(
                1,
                ["0xalice", "0xbob"],
                session=AsyncMock(),
                from_block_override=0,
                to_block_override=1,
            )

    assert graph.has_edge("0xalice", "0xbob")
