"""
Every declared setting must be read by the application.

A setting nothing reads is a promise the code does not keep: users configure
it and nothing changes (Phase 1 found eight, including RPC_RETRIES and the
documented retention period).
"""

import ast
import re
from pathlib import Path

from config.settings import Settings

ROOT = Path(__file__).resolve().parent.parent
APP_PACKAGES = ("api", "config", "core", "evaluation", "models", "scripts")


def _app_source() -> str:
    return "\n".join(
        path.read_text()
        for package in APP_PACKAGES
        for path in (ROOT / package).rglob("*.py")
        if path.name != "settings.py"
    )


def _settings_members() -> dict[str, set[str]]:
    """Each property/method of Settings -> the fields it reads via self.X."""
    tree = ast.parse((ROOT / "config" / "settings.py").read_text())
    settings_class = next(
        node for node in tree.body if isinstance(node, ast.ClassDef) and node.name == "Settings"
    )
    members: dict[str, set[str]] = {}
    for node in settings_class.body:
        if isinstance(node, ast.FunctionDef):
            members[node.name] = {
                n.attr
                for n in ast.walk(node)
                if isinstance(n, ast.Attribute)
                and isinstance(n.value, ast.Name)
                and n.value.id == "self"
            }
    return members


def test_every_setting_is_read_by_the_application():
    source = _app_source()
    read = set(re.findall(r"\bsettings\.([A-Za-z_][A-Za-z0-9_]*)", source))
    consumed = set(read)
    # Fields read through a Settings property the application uses.
    for member, fields in _settings_members().items():
        if member in read:
            consumed |= fields
    # Per-chain RPC URLs are read by name through RPC_URL_ENV_VARS (rpc_urls).
    if "rpc_urls" in read:
        consumed |= set(Settings.RPC_URL_ENV_VARS.values())

    unused = sorted(set(Settings.model_fields) - consumed)
    assert unused == [], f"settings nothing reads: {unused}"


def test_the_hygiene_check_would_catch_an_unused_setting():
    """Guard against the check passing vacuously."""
    source = _app_source()
    assert "settings.NO_SUCH_SETTING" not in source
    assert "TRADE_RETENTION_DAYS" in Settings.model_fields
    assert re.search(r"\bsettings\.TRADE_RETENTION_DAYS\b", source)
