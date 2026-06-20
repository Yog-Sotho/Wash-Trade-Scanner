
import pytest
from pydantic import ValidationError
from core.validators import AuditParameters, TrainingParameters

def test_audit_parameters_block_range_limit():
    # Valid range
    params = AuditParameters(chain_id=1, pool_address="0xB4e16d0168e52d35CaCD2c6185b44281Ec28C9Dc", start_block=100, end_block=200)
    assert params.end_block == 200

    # Range too large with start_block
    with pytest.raises(ValidationError, match="Block range exceeds maximum of 10,000,000"):
        AuditParameters(chain_id=1, pool_address="0xB4e16d0168e52d35CaCD2c6185b44281Ec28C9Dc", start_block=0, end_block=10_000_001)

    # end_block too large with start_block=None
    with pytest.raises(ValidationError, match="Block range exceeds maximum of 10,000,000"):
        AuditParameters(chain_id=1, pool_address="0xB4e16d0168e52d35CaCD2c6185b44281Ec28C9Dc", end_block=10_000_001)

def test_training_parameters_max_length():
    # Valid length
    pool_addresses = ["0xB4e16d0168e52d35CaCD2c6185b44281Ec28C9Dc"] * 1000
    params = TrainingParameters(chain_id=1, pool_addresses=pool_addresses)
    assert len(params.pool_addresses) == 1000

    # Length too large
    pool_addresses = ["0xB4e16d0168e52d35CaCD2c6185b44281Ec28C9Dc"] * 1001
    with pytest.raises(ValidationError):
        TrainingParameters(chain_id=1, pool_addresses=pool_addresses)
