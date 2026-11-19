"""Evidence bundles: why each trade is labelled wash.

Revision ID: 0003
Revises: 0002
Create Date: 2026-11-19

A label carried only a score and the deciding detector's name; it now carries
each detector that fired, what it saw and what its hit is worth (roadmap
Phase 6, explainability).
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0003"
down_revision: str | None = "0002"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "swap_trades",
        sa.Column("wash_evidence", sa.JSON(none_as_null=True), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("swap_trades", "wash_evidence")
