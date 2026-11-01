"""
Fixture loading and conversion to the ORM objects the detectors consume.
"""

from __future__ import annotations

import gzip
import json
from dataclasses import dataclass
from datetime import datetime
from importlib import resources
from typing import Any, cast

from models.schemas import SwapTrade

# Columns a fixture row may carry; anything else in a row is ignored.
_TRADE_FIELDS = {column.name for column in SwapTrade.__table__.columns} - {"id"}


@dataclass(frozen=True)
class Fixture:
    meta: dict[str, Any]
    rows: list[dict[str, Any]]  # trade dicts as the ingestor produced them

    @property
    def name(self) -> str:
        return str(self.meta["name"])

    @property
    def kind(self) -> str:
        return str(self.meta["kind"])


def fixture_names() -> list[str]:
    root = resources.files("evaluation") / "fixtures"
    return sorted(
        entry.name.removesuffix(".json.gz")
        for entry in root.iterdir()
        if entry.name.endswith(".json.gz")
    )


def load_fixture(name: str) -> Fixture:
    path = resources.files("evaluation") / "fixtures" / f"{name}.json.gz"
    with path.open("rb") as raw, gzip.open(raw, "rt") as fh:
        payload = json.load(fh)
    rows = []
    for row in payload["trades"]:
        trade = {key: value for key, value in row.items() if key in _TRADE_FIELDS}
        trade["block_timestamp"] = datetime.fromisoformat(row["block_timestamp"])
        rows.append(trade)
    return Fixture(meta=payload["meta"], rows=rows)


class EvalTrade:
    """A trade with exactly SwapTrade's column attributes, without the ORM.

    Detectors only read and set column attributes, and instantiating ~10^4
    mapped SwapTrade objects per scenario dominated the harness's run time.
    tests/test_evaluation.py checks that detectors give identical results on
    EvalTrade and SwapTrade inputs.
    """

    __slots__ = tuple(sorted(_TRADE_FIELDS | {"id"}))

    def __init__(self, **values: Any) -> None:
        for name in self.__slots__:
            setattr(self, name, values.get(name))


def to_swap_trades(rows: list[dict[str, Any]], orm: bool = False) -> list[SwapTrade]:
    """Session-less trades with ids 1..n, time-ordered.

    Detectors identify trades by id, so ids must be unique within a run.
    With `orm`, real SwapTrade objects are built instead of EvalTrade.
    """
    ordered = sorted(rows, key=lambda r: (r["block_number"], r["log_index"]))
    trades: list[SwapTrade] = []
    factory: Any = SwapTrade if orm else EvalTrade
    for trade_id, row in enumerate(ordered, start=1):
        trade = factory(id=trade_id, **row)
        trade.is_wash_trade = False
        trade.wash_trade_score = 0.0
        trade.detection_method = None
        trades.append(cast(SwapTrade, trade))
    return trades
