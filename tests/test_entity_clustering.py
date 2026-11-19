"""
Unit tests for entity clustering (funding-graph based address clustering).
"""

from unittest.mock import AsyncMock, MagicMock, patch

import networkx as nx
import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from core.entity_clustering import EntityClusterer
from core.exceptions import RPCError
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
    """Return a session whose session.execute(...) results yield rows_by_call
    in order. The first call is the traders query: each of its rows is a
    trader (tx_from) with a router as the swap's sender."""
    session = AsyncMock(spec=AsyncSession)
    results = []
    for i, rows in enumerate(rows_by_call):
        result = MagicMock()
        result.scalars.return_value.all.return_value = rows
        if i == 0:
            result.fetchall.return_value = [(r, "0xrouter") for r in rows]
        else:
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
                    "action": {"from": ALICE, "to": BOB, "value": "0x64"},  # 100 wei
                    "blockNumber": 42,
                    "transactionPosition": 7,
                    "traceAddress": [0, 1],  # an internal transfer
                },
                {
                    "action": {"from": ALICE, "to": CAROL, "value": "0x0"},  # no value
                    "blockNumber": 43,
                    "transactionPosition": 0,
                    "traceAddress": [],
                },
            ]
        }
    )

    edges = await clusterer._fetch_funding_edges_trace_filter(
        web3, {ALICE, BOB}, from_block=1, to_block=100
    )

    assert edges == [(ALICE.lower(), BOB.lower(), 100, (42, 7, 0))]
    # Transfers into the addresses: who funded them, wherever from.
    (_method, (params,)) = web3.provider.make_request.await_args.args
    assert "fromAddress" not in params
    assert {a.lower() for a in params["toAddress"]} == {ALICE.lower(), BOB.lower()}


@pytest.mark.asyncio
async def test_fetch_funding_edges_trace_filter_raises_on_failure(clusterer):
    web3 = AsyncMock()
    web3.provider.make_request = AsyncMock(side_effect=RuntimeError("rpc error"))

    with pytest.raises(RuntimeError):
        await clusterer._fetch_funding_edges_trace_filter(web3, {ALICE}, from_block=1, to_block=100)


@pytest.mark.asyncio
async def test_fetch_funding_edges_block_scan(clusterer):
    web3 = AsyncMock()
    web3.provider.make_request = AsyncMock(
        return_value={
            "result": {
                "transactions": [
                    {"from": "0xAlice", "to": "0xBob", "value": hex(500)},
                    {"from": "0xStranger", "to": "0xNobody", "value": "0x3e7"},  # unrelated
                    {"from": "0xBob", "to": "0xStranger", "value": "0x1"},  # outgoing
                    {"from": "0xBob", "to": None, "value": "0x1"},  # a deployment
                    "0xnot-a-dict-should-be-skipped",
                ]
            }
        }
    )

    edges, failed = await clusterer._fetch_funding_edges_block_scan(
        web3, {"0xalice", "0xbob"}, from_block=1, to_block=1
    )

    assert edges == [("0xalice", "0xbob", 500, (1, 0, 0))]
    assert failed == 0
    method, params = web3.provider.make_request.await_args.args
    assert (method, params) == ("eth_getBlockByNumber", ["0x1", True])


@pytest.mark.asyncio
async def test_fetch_funding_edges_block_scan_counts_failed_blocks(clusterer, monkeypatch):
    """A block that cannot be fetched used to be skipped silently."""
    from config.settings import settings

    monkeypatch.setattr(settings, "RPC_RETRIES", 1)
    web3 = AsyncMock()
    web3.provider.make_request = AsyncMock(side_effect=RuntimeError("boom"))

    edges, failed = await clusterer._fetch_funding_edges_block_scan(
        web3, {"0xalice"}, from_block=1, to_block=3
    )
    assert edges == []
    assert failed == 3
    assert web3.provider.make_request.await_count == 6  # RPC_RETRIES + 1 attempts each


@pytest.mark.asyncio
async def test_block_scan_runs_concurrently_within_the_cap(clusterer, monkeypatch):
    import asyncio

    from config.settings import settings

    monkeypatch.setattr(settings, "RPC_CONCURRENCY", 4)
    in_flight = peak = 0

    async def make_request(_method, _params):
        nonlocal in_flight, peak
        in_flight += 1
        peak = max(peak, in_flight)
        await asyncio.sleep(0.005)
        in_flight -= 1
        return {"result": {"transactions": []}}

    web3 = AsyncMock()
    web3.provider.make_request = AsyncMock(side_effect=make_request)
    _, failed = await clusterer._fetch_funding_edges_block_scan(web3, {"0xa"}, 1, 40)

    assert failed == 0
    assert web3.provider.make_request.await_count == 40
    assert peak == 4  # concurrent, and never more than RPC_CONCURRENCY


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
                AsyncMock(return_value=[("0xalice", "0xbob", 100, (5, 0, 0))]),
            ),
            patch.object(clusterer, "_contracts", AsyncMock(return_value=set())),
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
async def test_build_funding_graph_falls_back_to_block_scan(clusterer, monkeypatch):
    from config.settings import settings

    monkeypatch.setattr(settings, "ENTITY_SCAN_MAX_BLOCKS", 10)  # fallback is opt-in
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
                AsyncMock(return_value=([("0xalice", "0xbob", 50, (1, 0, 0))], 0)),
            ),
            patch.object(clusterer, "_contracts", AsyncMock(return_value=set())),
        ):
            graph = await clusterer.build_funding_graph(
                1,
                ["0xalice", "0xbob"],
                session=AsyncMock(),
                from_block_override=0,
                to_block_override=1,
            )

    assert graph.has_edge("0xalice", "0xbob")
    assert clusterer.coverage.method == "block_scan"
    assert clusterer.coverage.reason == "trace_filter failed (RuntimeError)"
    assert clusterer.coverage.complete  # the 2-block window, within the cap


@pytest.mark.asyncio
async def test_node_supports_trace_filter_false_on_error_response(clusterer):
    """make_request returns JSON-RPC errors instead of raising them; an error
    response used to be read as "trace_filter supported"."""
    web3 = AsyncMock()
    web3.provider.make_request = AsyncMock(
        return_value={"error": {"code": -32601, "message": "Method not found"}}
    )
    assert await clusterer._node_supports_trace_filter(web3) is False


@pytest.mark.asyncio
async def test_fetch_funding_edges_trace_filter_raises_on_error_response(clusterer):
    """An error response must not be read as "no funding links"."""
    web3 = AsyncMock()
    web3.provider.make_request = AsyncMock(return_value={"error": {"message": "range too large"}})

    with pytest.raises(RPCError):
        await clusterer._fetch_funding_edges_trace_filter(web3, {ALICE}, 1, 100)


async def _graph_without_trace_support(clusterer, from_block, to_block, failed=0):
    scan = AsyncMock(return_value=([], failed))
    with (
        patch("core.entity_clustering.AsyncWeb3") as MockWeb3,
        patch.object(clusterer, "_node_supports_trace_filter", AsyncMock(return_value=False)),
        patch.object(clusterer, "_fetch_funding_edges_block_scan", scan),
        patch.object(clusterer, "_contracts", AsyncMock(return_value=set())),
    ):
        MockWeb3.return_value = AsyncMock()
        graph = await clusterer.build_funding_graph(
            1,
            ["0xalice", "0xbob"],
            session=AsyncMock(),
            from_block_override=from_block,
            to_block_override=to_block,
        )
    return graph, scan


@pytest.mark.asyncio
async def test_block_scan_fallback_is_off_by_default(clusterer, caplog):
    """Regression: without trace_filter the audit fell back to scanning ~2M
    blocks one RPC call at a time, so it never finished on public nodes."""
    graph, scan = await _graph_without_trace_support(clusterer, 0, 2_000_000)

    scan.assert_not_awaited()
    assert graph.number_of_edges() == 0
    assert graph.number_of_nodes() == 2
    assert "ENTITY_SCAN_MAX_BLOCKS" in caplog.text
    # Reported with the audit, not only logged.
    assert clusterer.coverage.as_dict() == {
        "method": "skipped",
        "window": (0, 2_000_000),
        "scanned": None,
        "blocks_failed": 0,
        "reason": "the node does not support trace_filter; ENTITY_SCAN_MAX_BLOCKS is 0",
        "complete": False,
    }


@pytest.mark.asyncio
async def test_block_scan_fallback_covers_only_latest_blocks_up_to_cap(
    clusterer, monkeypatch, caplog
):
    from config.settings import settings

    monkeypatch.setattr(settings, "ENTITY_SCAN_MAX_BLOCKS", 100)
    _, scan = await _graph_without_trace_support(clusterer, 0, 1_000)

    _, _, from_block, to_block = scan.call_args.args
    assert (from_block, to_block) == (901, 1_000)
    assert "coverage is partial" in caplog.text
    assert clusterer.coverage.window == (0, 1_000)
    assert clusterer.coverage.scanned == (901, 1_000)
    assert not clusterer.coverage.complete


@pytest.mark.asyncio
async def test_block_scan_fallback_scans_whole_window_within_cap(clusterer, monkeypatch):
    from config.settings import settings

    monkeypatch.setattr(settings, "ENTITY_SCAN_MAX_BLOCKS", 100)
    _, scan = await _graph_without_trace_support(clusterer, 950, 1_000)

    _, _, from_block, to_block = scan.call_args.args
    assert (from_block, to_block) == (950, 1_000)


@pytest.mark.asyncio
async def test_failed_blocks_make_the_coverage_partial(clusterer, monkeypatch):
    from config.settings import settings

    monkeypatch.setattr(settings, "ENTITY_SCAN_MAX_BLOCKS", 100)
    await _graph_without_trace_support(clusterer, 950, 1_000, failed=2)

    assert clusterer.coverage.scanned == (950, 1_000)
    assert clusterer.coverage.blocks_failed == 2
    assert not clusterer.coverage.complete


@pytest.mark.asyncio
async def test_trace_filter_coverage_is_the_whole_window(clusterer):
    with (
        patch("core.entity_clustering.AsyncWeb3"),
        patch.object(clusterer, "_node_supports_trace_filter", AsyncMock(return_value=True)),
        patch.object(clusterer, "_fetch_funding_edges_trace_filter", AsyncMock(return_value=[])),
    ):
        await clusterer.build_funding_graph(
            1, ["0xalice"], AsyncMock(), from_block_override=10, to_block_override=20
        )

    assert clusterer.coverage.as_dict()["complete"] is True
    assert clusterer.coverage.window == clusterer.coverage.scanned == (10, 20)


def test_the_block_scan_is_hard_capped():
    """The scan is one request per block: 50,000 blocks is ~8 minutes at the
    default rate limit; the old 2,000,000 cap allowed ~6 hours."""
    from pydantic import ValidationError

    from config.settings import Settings

    assert Settings(ENTITY_SCAN_MAX_BLOCKS=50_000).ENTITY_SCAN_MAX_BLOCKS == 50_000
    with pytest.raises(ValidationError):
        Settings(ENTITY_SCAN_MAX_BLOCKS=50_001)


@pytest.mark.asyncio
async def test_stored_clusters_are_looked_up_in_one_query(clusterer):
    # senders, recipients, then a single lookup for all three components.
    session = _mock_session_with_scalars([["0xa", "0xc", "0xe"], ["0xb", "0xd", "0xf"], []])
    graph: nx.DiGraph[str] = nx.DiGraph()
    graph.add_edges_from([("0xa", "0xb"), ("0xc", "0xd"), ("0xe", "0xf")])

    with patch.object(clusterer, "build_funding_graph", AsyncMock(return_value=graph)):
        clusters = await clusterer.cluster_addresses(1, "0xpool", session)

    assert len(clusters) == 3
    assert session.execute.await_count == 3


@pytest.mark.asyncio
async def test_clustering_shares_the_chains_rate_limit_and_breaker(clusterer):
    from core.ingestor import chain_breaker, chain_rate_limiter

    with (
        patch("core.entity_clustering.AsyncWeb3"),
        patch.object(clusterer, "_node_supports_trace_filter", AsyncMock(return_value=True)),
        patch.object(clusterer, "_fetch_funding_edges_trace_filter", AsyncMock(return_value=[])),
    ):
        await clusterer.build_funding_graph(
            1, ["0xalice"], AsyncMock(), from_block_override=1, to_block_override=2
        )
    assert clusterer._limiter is chain_rate_limiter(1)
    assert clusterer._breaker is chain_breaker(1)

    limiter = MagicMock()
    limiter.acquire = AsyncMock()
    clusterer._limiter, clusterer._breaker = limiter, None
    web3 = AsyncMock()
    web3.provider.make_request = AsyncMock(return_value={"result": {}})
    await clusterer._request(web3, "eth_getBlockByNumber", ["0x1", True])
    limiter.acquire.assert_awaited_once()


# ------------------------------------------------- first funders and hubs


def _addr(n: int) -> str:
    return f"0x{n:040x}"


EXCHANGE, ROUTER, OPERATOR = _addr(0xE0), _addr(0xC0), _addr(0x0B)


async def _graph(clusterer, edges, addresses, contracts=(), eoas=None):
    """build_funding_graph over the given transfers, on a node whose
    eth_getCode answers code for `contracts` and none for the rest."""
    code_for = {c.lower() for c in contracts}

    async def make_request(method, params):
        assert method == "eth_getCode"
        return {"result": "0x6080" if params[0].lower() in code_for else "0x"}

    web3 = AsyncMock()
    web3.provider.make_request = AsyncMock(side_effect=make_request)
    with (
        patch("core.entity_clustering.AsyncWeb3", return_value=web3),
        patch.object(clusterer, "_node_supports_trace_filter", AsyncMock(return_value=True)),
        patch.object(clusterer, "_fetch_funding_edges_trace_filter", AsyncMock(return_value=edges)),
    ):
        graph = await clusterer.build_funding_graph(
            1, addresses, AsyncMock(), from_block_override=0, to_block_override=10**6, eoas=eoas
        )
    return graph, web3.provider.make_request


def _clusters_of(graph) -> list[set[str]]:
    return [set(c) for c in nx.weakly_connected_components(graph) if len(c) > 1]


async def test_wallets_first_funded_by_one_operator_are_one_entity(clusterer):
    wallets = [_addr(0x100 + i) for i in range(6)]
    edges = [(OPERATOR, w, 10**17, (100 + i, 0, 0)) for i, w in enumerate(wallets)]
    graph, _ = await _graph(clusterer, edges, wallets, eoas=wallets)
    assert _clusters_of(graph) == [{OPERATOR, *wallets}]


async def test_an_exchange_hot_wallet_funding_many_traders_links_none_of_them(
    clusterer, monkeypatch
):
    """The bug this rule fixes: everyone who withdrew from one exchange was
    one entity."""
    from config.settings import settings

    monkeypatch.setattr(settings, "FUNDING_MAX_FANOUT", 50)
    users = [_addr(0x1000 + i) for i in range(51)]
    edges = [(EXCHANGE, u, 10**18, (i, 0, 0)) for i, u in enumerate(users)]
    graph, _ = await _graph(clusterer, edges, users, eoas=users)
    assert _clusters_of(graph) == []

    graph, _ = await _graph(clusterer, edges[:50], users[:50], eoas=users[:50])
    assert len(_clusters_of(graph)) == 1  # at the cap it still links


async def test_a_contract_funder_links_nobody(clusterer):
    """A router refunding ETH to two of its users does not make them one."""
    a, b = _addr(0x201), _addr(0x202)
    edges = [(ROUTER, a, 5, (1, 0, 1)), (ROUTER, b, 5, (2, 0, 1))]
    graph, _ = await _graph(clusterer, edges, [a, b], contracts=[ROUTER], eoas=[a, b])
    assert _clusters_of(graph) == []


async def test_a_listed_funder_links_nobody(clusterer, monkeypatch):
    from config.settings import settings

    a, b = _addr(0x301), _addr(0x302)
    edges = [(EXCHANGE, a, 5, (1, 0, 0)), (EXCHANGE, b, 5, (2, 0, 0))]
    monkeypatch.setattr(settings, "FUNDING_EXCLUDED_FUNDERS", EXCHANGE.upper().replace("0X", "0x"))
    graph, _ = await _graph(clusterer, edges, [a, b], eoas=[a, b])
    assert _clusters_of(graph) == []


async def test_only_the_first_funding_links(clusterer):
    """A later payment between two unrelated wallets is not ownership."""
    a, b = _addr(0x401), _addr(0x402)
    edges = [
        (b, a, 10**16, (900, 3, 0)),  # b pays a, long after a was funded
        (_addr(0x4A), a, 10**18, (100, 0, 0)),  # a's first funder
        (_addr(0x4B), b, 10**18, (120, 0, 0)),  # b's first funder
    ]
    graph, _ = await _graph(clusterer, edges, [a, b], eoas=[a, b])
    assert not any({a, b} <= c for c in _clusters_of(graph))

    edges[0] = (b, a, 10**16, (50, 0, 0))  # b funded a before anyone else did
    graph, _ = await _graph(clusterer, edges, [a, b], eoas=[a, b])
    assert any({a, b} <= c for c in _clusters_of(graph))


async def test_audited_contracts_are_no_ones_wallet(clusterer):
    """A recipient that is a contract (a router unwrapping WETH) is left out,
    and only addresses in a link and not known to be wallets cost a code
    lookup: the bot contract nobody funded costs none."""
    trader = _addr(0x501)
    edges = [(trader, ROUTER, 1, (1, 0, 0))]  # the trader was the router's first funder
    bot = _addr(0x5B0)
    graph, lookups = await _graph(
        clusterer, edges, [trader, ROUTER, bot], contracts=[ROUTER, bot], eoas=[trader]
    )
    assert ROUTER not in graph and _clusters_of(graph) == []
    assert [call.args[1][0].lower() for call in lookups.await_args_list] == [ROUTER]


async def test_an_address_whose_code_cannot_be_fetched_links_nobody(clusterer):
    a, b, funder = _addr(0x601), _addr(0x602), _addr(0x6F)
    web3 = AsyncMock()
    web3.provider.make_request = AsyncMock(return_value={"error": {"message": "rate limited"}})
    assert await clusterer._contracts(web3, {funder}) == {funder}

    edges = [(funder, a, 1, (1, 0, 0)), (funder, b, 1, (2, 0, 0))]
    with (
        patch("core.entity_clustering.AsyncWeb3", return_value=web3),
        patch.object(clusterer, "_node_supports_trace_filter", AsyncMock(return_value=True)),
        patch.object(clusterer, "_fetch_funding_edges_trace_filter", AsyncMock(return_value=edges)),
    ):
        graph = await clusterer.build_funding_graph(
            1, [a, b], AsyncMock(), from_block_override=0, to_block_override=9, eoas=[a, b]
        )
    assert _clusters_of(graph) == []


async def test_clustering_uses_the_traders_not_the_routers_they_swap_through(clusterer):
    """Swap senders are routers; the pool's traders are the transactions'
    senders. A swap without one falls back to its sender, code-checked."""
    session = AsyncMock(spec=AsyncSession)
    traders, recipients = MagicMock(), MagicMock()
    traders.fetchall.return_value = [
        ("0xAlice", "0xRouter"),
        ("0xbob", "0xrouter"),
        (None, "0xBot"),
    ]
    recipients.fetchall.return_value = [("0xCarol",), ("0xrouter",)]
    session.execute = AsyncMock(side_effect=[traders, recipients])
    build = AsyncMock(return_value=nx.DiGraph())

    with patch.object(clusterer, "build_funding_graph", build):
        await clusterer.cluster_addresses(1, "0xpool", session)

    _chain, addresses, _session = build.await_args.args
    assert addresses == ["0xalice", "0xbob", "0xbot", "0xcarol", "0xrouter"]
    assert build.await_args.kwargs["eoas"] == {"0xalice", "0xbob"}


def test_excluded_funders_setting_is_validated():
    from pydantic import ValidationError

    from config.settings import Settings

    mixed_case = "0x" + EXCHANGE[2:].upper()
    listed = Settings(FUNDING_EXCLUDED_FUNDERS=f" {mixed_case} , {ROUTER}")
    assert listed.funding_excluded_funder_set == {EXCHANGE, ROUTER}
    with pytest.raises(ValidationError):
        Settings(FUNDING_EXCLUDED_FUNDERS="binance")


async def test_transfers_into_addresses_not_audited_link_nothing(clusterer):
    """A node returning more than was asked for (transfers into other
    addresses) must not add strangers to a cluster or count them as funded."""
    a, b, stranger = _addr(0x701), _addr(0x702), _addr(0x7FF)
    edges = [(OPERATOR, stranger, 1, (1, 0, 0)), (OPERATOR, a, 1, (2, 0, 0))]
    graph, _ = await _graph(clusterer, edges, [a, b], eoas=[a, b])
    assert stranger not in graph
    assert _clusters_of(graph) == [{OPERATOR, a}]
