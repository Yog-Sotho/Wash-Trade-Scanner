"""
requirements.txt / requirements-dev.txt are hash-locked resolutions of
pyproject.toml (`make lock`). CI and the Docker image install from them, so a
dependency added to or changed in pyproject.toml without relocking would be
silently missing or out of range. This keeps the two in step offline.
"""

import re
import tomllib
from pathlib import Path

import pytest
from packaging.requirements import Requirement
from packaging.utils import canonicalize_name

ROOT = Path(__file__).resolve().parent.parent
_PIN = re.compile(r"^([A-Za-z0-9][A-Za-z0-9._-]*)==([^\s;\\]+)")


def _pins(name: str) -> dict[str, str]:
    pins: dict[str, str] = {}
    lines = (ROOT / name).read_text().splitlines()
    for line in lines:
        match = _PIN.match(line)
        if match:
            pins[canonicalize_name(match.group(1))] = match.group(2)
    return pins


def _project() -> dict:
    return tomllib.loads((ROOT / "pyproject.toml").read_text())["project"]


@pytest.mark.parametrize(
    ("lock", "groups"),
    [("requirements.txt", ["runtime"]), ("requirements-dev.txt", ["runtime", "dev"])],
)
def test_lock_satisfies_every_declared_requirement(lock, groups):
    project = _project()
    declared = []
    if "runtime" in groups:
        declared += project["dependencies"]
    if "dev" in groups:
        declared += project["optional-dependencies"]["dev"]
    pins = _pins(lock)
    for spec in declared:
        req = Requirement(spec)
        name = canonicalize_name(req.name)
        assert name in pins, f"{req.name} is in pyproject.toml but not in {lock}: run make lock"
        assert req.specifier.contains(pins[name], prereleases=True), (
            f"{lock} pins {req.name}=={pins[name]}, outside {req.specifier}: run make lock"
        )


@pytest.mark.parametrize("lock", ["requirements.txt", "requirements-dev.txt"])
def test_every_pin_is_hashed(lock):
    """pip --require-hashes rejects a lock with any unhashed entry."""
    text = (ROOT / lock).read_text()
    entries = re.split(r"\n(?=[A-Za-z0-9])", text.split("\n", 2)[2])
    for entry in entries:
        if _PIN.match(entry):
            assert "--hash=sha256:" in entry, entry.splitlines()[0]


def test_runtime_lock_has_no_dev_only_packages():
    runtime = _pins("requirements.txt")
    for tool in ("pytest", "ruff", "mypy", "pip-audit", "bandit"):
        assert canonicalize_name(tool) not in runtime
