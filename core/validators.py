"""
Input validation utilities using Pydantic.
"""

import re

from pydantic import BaseModel, Field, ValidationInfo, field_validator
from web3 import Web3

VALID_ADDRESS_RE = re.compile(r"^0x[a-fA-F0-9]{40}$")


class AuditParameters(BaseModel):
    """Validated parameters for audit operations."""

    chain_id: int = Field(..., ge=1, le=999999999, description="CAIP-2 chain ID")
    pool_address: str = Field(..., min_length=42, max_length=42)
    start_block: int | None = Field(None, ge=0)
    end_block: int | None = Field(None, ge=0)
    use_ml: bool = Field(True)
    use_heuristics: bool = Field(True)

    @field_validator("pool_address")
    @classmethod
    def validate_pool_address(cls, v: str) -> str:
        # Lowercase is the stored form of every address column; the ingestor
        # converts to EIP-55 itself where web3 requires it.
        return validate_address(v).lower()

    @field_validator("end_block")
    @classmethod
    def validate_block_range(cls, v: int | None, info: ValidationInfo) -> int | None:
        if v is not None:
            start = info.data.get("start_block")
            if start is not None and v <= start:
                raise ValueError("end_block must be greater than start_block")
            if start is not None and v - start > 10_000_000:
                raise ValueError("Block range exceeds maximum of 10,000,000")
        return v


class TrainingParameters(BaseModel):
    """Validated parameters for model training."""

    chain_id: int = Field(..., ge=1, le=999999999)
    pool_addresses: list[str] = Field(..., min_length=1)
    use_heuristic_labels: bool = Field(True)
    contamination: float | None = Field(None, ge=0.001, le=0.5)

    @field_validator("pool_addresses")
    @classmethod
    def validate_pool_addresses(cls, v: list[str]) -> list[str]:
        validated: list[str] = []
        for addr in v:
            if not VALID_ADDRESS_RE.match(addr):
                raise ValueError(f"Invalid address: {addr}")
            validated.append(Web3.to_checksum_address(addr))
        return validated


def validate_address(addr: str) -> str:
    """Validate an address and return its EIP-55 checksummed form.

    Mixed-case input must carry a valid EIP-55 checksum, so a mistyped address
    is rejected instead of silently naming a different contract. All-lowercase
    or all-uppercase input carries no checksum and is accepted.
    """
    if not VALID_ADDRESS_RE.match(addr):
        raise ValueError(f"Invalid Ethereum address: {addr}")
    body = addr[2:]
    if body not in (body.lower(), body.upper()) and not Web3.is_checksum_address(addr):
        raise ValueError(f"Invalid EIP-55 checksum: {addr}")
    return Web3.to_checksum_address(addr)
