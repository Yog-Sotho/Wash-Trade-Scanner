# Comparison with open-source and published work

How this scanner compares with the most used open-source projects and the
methods they publish, and what was taken from them. Surveyed in November 2026.

Three projects were read at the source:

- [flashbots/mev-inspect-py](https://github.com/flashbots/mev-inspect-py) at `ce8179f`
- [SorellaLabs/brontes](https://github.com/SorellaLabs/brontes) at `3576773`
- [duneanalytics/spellbook](https://github.com/duneanalytics/spellbook) at `5c872c9`

The rest are known from their papers and documentation.

## The landscape

There is no widely used open-source detector of wash trading on AMM DEXs. The
open-source work falls into three groups:

- **MEV inspectors** recognise the trading that looks most like wash trading
  but is not.
- **Dune's SQL filters** flag NFT wash trades.
- **Academic methods** (statistical, graph-based) mostly come without
  maintained code.

| Project | What it is | License | Scope |
|---|---|---|---|
| flashbots/mev-inspect-py | MEV inspector: arbitrage, sandwiches, liquidations, JIT liquidity, from transaction traces | MIT, archived | Ethereum, needs a tracing node |
| SorellaLabs/brontes | MEV inspector: sandwiches (incl. multi-front-run), atomic arbitrage, JIT, CEX-DEX arbitrage, liquidations | MIT / Apache-2.0 | Ethereum, needs a Reth node and CEX quote data |
| Dune spellbook `nft.wash_trades` | Five SQL filters marking NFT trades as wash | Business Source License 1.1 | NFTs, inside Dune |
| Cong, Li, Tang, Yang, "Crypto Wash Trading" (Management Science, 2023) | Benford's law, round-number clustering and tail tests on trade sizes | paper, no code | centralised exchanges |
| Victor and Weintraud, "Detecting and Quantifying Wash Trading on Decentralized Cryptocurrency Exchanges" (WWW 2021) | Strongly connected trader components whose trades net to zero | paper | order-book DEXs (IDEX, EtherDelta) |
| Bitquery Solana wash-trading detector | Rule labels (self-trades, repeated pairs, loops) and an XGBoost model trained on them | docs and demo repo | Solana DEX trades via Bitquery |
| Solidus Labs A-A detector | A trade is wash when its trader holds at least 1% of the pool's LP tokens | Dune dashboard | Uniswap V2 |
| Nethermind wash-trade bot (Forta) | NFT wash-trade alerts | open source | NFTs |

## Feature by feature

Legend: ✔ has it, ◐ partly, – no.

| Capability | This scanner | mev-inspect-py | brontes | Dune `nft.wash_trades` | Cong et al. |
|---|---|---|---|---|---|
| Wash-trading detection on AMM pools | ✔ nine heuristic detectors and an Isolation Forest, calibrated | – | – | – (NFTs) | – (CEX) |
| Labelled evaluation on real data, regression-gated in CI | ✔ [benchmarks](benchmarks.md) | – | ◐ test fixtures per case | – | ◐ regulated exchanges as the benchmark |
| Probability per flagged trade, with evidence | ✔ | – | ◐ profit per bundle | – (flags) | – |
| Funding-link clustering | ✔ first funder, hubs excluded (**borrowed**) | – | – | ✔ first funder, labelled hubs excluded | – |
| Sandwich detection | ✔ incl. multi-front-run (**borrowed**) | ✔ | ✔ incl. multi-front-run | – | – |
| Public routers kept out of attacker identity | ✔ (**borrowed**) | ✔ hard-coded routers | ✔ labelled contracts | – | – |
| Atomic (cyclic) arbitrage | ✔ net token flow per transaction | ✔ route search | ✔ typed | – | – |
| Trade-size forensics (Benford, round numbers) | ✔ per pool (**borrowed**) | – | – | – | ✔ |
| Cross-pool correlation | ✔ | – | ◐ multi-pool bundles | – | – |
| Flash-loan-funded trades | – | – | ✔ | ✔ | – |
| JIT liquidity, liquidations | – | ✔ | ✔ | – | – |
| CEX-DEX arbitrage | – | – | ✔ (needs CEX quotes) | – | – |
| LP-share self-dealing (Solidus) | – | – | – | – | – |
| Service: API, monitors, alerts, metrics | ✔ | ◐ DB and workers | ◐ DB and CLI | – (Dune) | – |

## What was borrowed

Each borrowing below fixed a problem found by measurement on this
repository's recorded pools, or one that a test reproduces.

### Funding links: the first funder, never a hub (from Dune's `nft.wash_trades`)

Dune's fourth filter marks a trade when buyer and seller were *first funded
by* the same wallet, or one first funded the other. It ignores funders that
are exchanges, bridges or Tornado Cash. This scanner joined *any* two
addresses connected by *any* value transfer over a year, transitively.

That rule merged unrelated wallets in three ways:

- Everyone who withdrew from the same exchange became one entity.
- Everyone who received an ETH refund from the same router became one
  entity.
- A single later payment joined two otherwise unrelated clusters.

It also clustered the swaps' *senders*, and those are mostly routers. On
the recorded USDC/WETH pool, 444 of 569 distinct swap senders never sent a
transaction: they are routers and other contracts. So the clustering ran
mostly over routers, while the detectors looked up wallets.

Clustering now takes the pool's traders (transaction senders) and the
recipients of their swaps. It links an address only to its first funder in
the window. A funder links nobody when it is:

- a contract;
- the first funder of more than `FUNDING_MAX_FANOUT` (50) of the audited
  addresses;
- listed in `FUNDING_EXCLUDED_FUNDERS`.

The SQL itself was not copied (Business Source License); the rule is
re-implemented. Tests reproduce each false merge. An end-to-end audit over a
fake node links an operator's two wallets but not a router's users
([tests](../tests/test_entity_clustering.py)).

### Public routers are no one's bot (from mev-inspect-py)

mev-inspect-py refuses to treat the Uniswap routers as a sandwich attacker.
This scanner identified a sandwich bot across its sending accounts by the
contract that swapped for itself. On the recorded USDC/WETH pools, that
contract matched 1 of 44 sandwiches, and the match was false. The two
"attacker" legs belonged to unrelated users who came in through different
public contracts: 0x Protocol and the ERC-4337 EntryPoint. Both were routed
through one executor that 278 accounts use.

Instead of a hard-coded list, a contract identifies a bot only when at most
`SANDWICH_BOT_MAX_SENDERS` (5) accounts use it in the audited trades. The
false sandwich is gone. A wash trader routing both legs through an aggregator
can no longer have them excluded as MEV.

### Multi-front-run ("Big Mac") sandwiches (from brontes)

brontes recognises an attacker that front-runs with several swaps and sells
their total back once. This scanner matched one front-run to one back-run, so
it missed them. Implemented in `core/mev.py`, it finds 4 more sandwiches in
the recorded clean pools, all by one account: 47 sandwiches instead of 44.
Their legs no longer reach the detectors.

### Trade-size forensics (from Cong, Li, Tang and Yang)

People trade round amounts, and their trade sizes follow Benford's law.
Scripts manufacturing volume do neither. Each audit now reports, per pool:

- the share of swaps with a round side;
- the first-digit deviation from Benford's law, read against Nigrini's
  thresholds.

Neither statistic labels a trade. On the recorded pools:

| Pool | Kind | Round share | Benford MAD | Conformity |
|---|---|---|---|---|
| `univ2_usdc_weth` | clean | 16.42% | 0.0117 | acceptable |
| `univ3_usdc_weth_005` | clean | 7.64% | 0.0044 | close |
| `univ3_weth_usdt_005` | clean | 9.74% | 0.0051 | close |
| `nexf_weth` | FBI-documented wash market | **0.14%** | **0.0349** | **nonconformity** |

Four pools show that the signal exists, not how often it errs. A pool where
wash trades are a small share of volume will look ordinary.

## What was not borrowed, and why

- **Flash-loan filter** (Dune, brontes): needs the flash-loan events of each
  swap's transaction, which come from contracts other than the pool. The
  ingestor reads only the pool's logs. Fetching receipts would add one
  request per transaction, which Phase 5 removed.
- **JIT liquidity and liquidations** (mev-inspect-py, brontes): they show in
  liquidity and lending events, not swaps. They are neither wash trading nor
  a source of this scanner's false positives.
- **CEX-DEX arbitrage** (brontes): needs centralised-exchange quotes at block
  resolution. The remaining false positives on the clean pools are round
  trips by searcher-like accounts that may be this, but without quotes it
  cannot be shown.
- **LP-share self-dealing** (Solidus Labs): needs each trader's LP position
  over time (mints, burns, LP token transfers), which is not ingested.
- **A supervised model trained on rule labels** (Bitquery): a model trained
  on what the rules flag re-learns the rules. Here, detectors are measured
  against injected campaigns and real clean pools instead.
- **Exchange and bridge labels** (Dune): there is no permissively licensed
  label set to ship. `FUNDING_EXCLUDED_FUNDERS` takes the operator's own.
