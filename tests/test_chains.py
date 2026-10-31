"""
Tests for chain configuration resolution.
"""

import pytest

from config.chains import CHAINS, get_chain_config
from config.settings import Settings, settings


def test_rpc_url_comes_from_settings(monkeypatch):
    monkeypatch.setattr(settings, "ETH_RPC_URL", "https://eth.example.org/key")
    assert get_chain_config(1)["rpc_url"] == "https://eth.example.org/key"


def test_rpc_url_falls_back_to_static_default_when_setting_empty(monkeypatch):
    monkeypatch.setattr(settings, "ETH_RPC_URL", "")
    static = next(c for c in CHAINS if c["chain_id"] == 1)["rpc_url"]
    assert get_chain_config(1)["rpc_url"] == static


def test_get_chain_config_does_not_mutate_chains(monkeypatch):
    static = next(c for c in CHAINS if c["chain_id"] == 1)["rpc_url"]
    monkeypatch.setattr(settings, "ETH_RPC_URL", "https://eth.example.org/key")

    resolved = get_chain_config(1)
    resolved["rpc_url"] = "mutated"

    assert next(c for c in CHAINS if c["chain_id"] == 1)["rpc_url"] == static


def test_get_chain_config_unknown_chain():
    with pytest.raises(ValueError, match="not found"):
        get_chain_config(999_999)


def test_every_chain_has_an_rpc_setting():
    """Guards against a chain being added to CHAINS without a way to configure
    its endpoint, or a mapping entry naming a field that does not exist."""
    chain_ids = {c["chain_id"] for c in CHAINS}
    assert set(Settings.RPC_URL_ENV_VARS) == chain_ids
    for env_var in Settings.RPC_URL_ENV_VARS.values():
        assert env_var in Settings.model_fields, env_var
