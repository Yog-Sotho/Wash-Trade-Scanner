"""
Input validation utilities using Pydantic.
"""

import re
from typing import Optional

from pydantic import BaseModel, Field, field_validator, model_validator
from web3 import Web3

VALID_ADDRESS_RE = re.compile(r"^0x[a-fA-F0-9]{40}$")


class AuditParameters(BaseModel):
    """Validated parameters for audit operations."""

    chain_id: int = Field(..., ge=1, le=999999999, description="CAIP-2 chain ID")
    pool_address: str = Field(..., min_length=42, max_length=42)
    start_block: Optional[int] = Field(None, ge=0)
    end_block: Optional[int] = Field(None, ge=0)
    use_ml: bool = Field(True)
    use_heuristics: bool = Field(True)

    @field_validator("pool_address")
    @classmethod
    def validate_pool_address(cls, v: str) -> str:
        if not VALID_ADDRESS_RE.match(v):
            raise ValueError(f"Invalid Ethereum address format: {v}")
        try:
            return Web3.to_checksum_address(v)
        except ValueError as exc:
            raise ValueError(f"Invalid checksum address: {v}") from exc

    @model_validator(mode="after")
    def validate_block_range(self) -> "AuditParameters":
        if self.start_block is not None and self.end_block is not None:
            if self.end_block <= self.start_block:
                raise ValueError("end_block must be greater than start_block")
            if self.end_block - self.start_block > 10_000_000:
                raise ValueError("Block range exceeds maximum of 10,000,000")
        elif self.end_block is not None and self.end_block > 10_000_000:
            # If start_block is None, it defaults to chain start or 0,
            # so we should still check if end_block is too far from a reasonable start
            pass  # We'll handle the fully resolved range in the ingestor
        return self


class TrainingParameters(BaseModel):
    """Validated parameters for model training."""

    chain_id: int = Field(..., ge=1, le=999999999)
    pool_addresses: list[str] = Field(..., min_length=1)
    use_heuristic_labels: bool = Field(True)
    contamination: Optional[float] = Field(None, ge=0.001, le=0.5)

    @field_validator("pool_addresses")
    @classmethod
    def validate_pool_addresses(cls, v: list[str]) -> list[str]:
        validated = []
        for addr in v:
            if not VALID_ADDRESS_RE.match(addr):
                raise ValueError(f"Invalid address: {addr}")
            validated.append(Web3.to_checksum_address(addr))
        return validated


def validate_address(addr: str) -> str:
    """Validate and checksum an Ethereum address."""
    if not VALID_ADDRESS_RE.match(addr):
        raise ValueError(f"Invalid Ethereum address: {addr}")
    return Web3.to_checksum_address(addr)
