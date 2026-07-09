"""
Unit tests for input validation utilities.
"""

import pytest
from pydantic import ValidationError

from core.validators import AuditParameters, TrainingParameters, validate_address


def test_validate_address_checksums():
    result = validate_address("0x" + "a" * 40)
    assert result.lower() == "0x" + "a" * 40


def test_validate_address_rejects_bad_format():
    with pytest.raises(ValueError):
        validate_address("not-an-address")


def test_audit_parameters_valid():
    params = AuditParameters(chain_id=1, pool_address="0x" + "b" * 40)
    assert params.chain_id == 1
    assert params.use_ml is True


def test_audit_parameters_rejects_bad_address():
    with pytest.raises(ValidationError):
        AuditParameters(chain_id=1, pool_address="0xnotanaddress")


def test_audit_parameters_end_block_must_exceed_start():
    with pytest.raises(ValidationError):
        AuditParameters(
            chain_id=1,
            pool_address="0x" + "c" * 40,
            start_block=100,
            end_block=50,
        )


def test_audit_parameters_rejects_huge_block_range():
    with pytest.raises(ValidationError):
        AuditParameters(
            chain_id=1,
            pool_address="0x" + "c" * 40,
            start_block=0,
            end_block=10_000_001,
        )


def test_training_parameters_valid():
    params = TrainingParameters(chain_id=1, pool_addresses=["0x" + "d" * 40])
    assert len(params.pool_addresses) == 1


def test_training_parameters_rejects_bad_address():
    with pytest.raises(ValidationError):
        TrainingParameters(chain_id=1, pool_addresses=["not-an-address"])


def test_training_parameters_rejects_empty_pool_list():
    with pytest.raises(ValidationError):
        TrainingParameters(chain_id=1, pool_addresses=[])
