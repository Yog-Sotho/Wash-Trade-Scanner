"""
Shared pytest configuration.

Tests marked `integration` need a real PostgreSQL database (see
tests/integration/). They are skipped by default and run only with
`--integration`, in which case an unreachable database is an error, never a
silent skip.
"""

import pytest


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        "--integration",
        action="store_true",
        default=False,
        help="run integration tests against the PostgreSQL database in DATABASE_*",
    )


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    if config.getoption("--integration"):
        return
    skip = pytest.mark.skip(reason="integration test: run with --integration and a PostgreSQL DB")
    for item in items:
        if "integration" in item.keywords:
            item.add_marker(skip)
