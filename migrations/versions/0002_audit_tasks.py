"""Audit tasks: background audits started through the API, persisted.

Revision ID: 0002
Revises: 0001
Create Date: 2026-11-19

Until now they lived in the API process's memory: unbounded, never expired,
lost on restart and invisible to other instances (roadmap 4.4).
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0002"
down_revision: str | None = "0001"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "audit_tasks",
        sa.Column("id", sa.String(length=32), nullable=False),
        sa.Column("status", sa.String(length=16), nullable=False),
        sa.Column("chain_id", sa.Integer(), nullable=False),
        sa.Column("pool_address", sa.String(length=42), nullable=False),
        sa.Column("parameters", sa.JSON(), nullable=False),
        sa.Column("result", sa.JSON(), nullable=True),
        sa.Column("error", sa.Text(), nullable=True),
        sa.Column("request_id", sa.String(length=128), nullable=True),
        sa.Column("created_at", sa.DateTime(), server_default=sa.text("now()"), nullable=False),
        sa.Column("heartbeat_at", sa.DateTime(), server_default=sa.text("now()"), nullable=False),
        sa.Column("finished_at", sa.DateTime(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_audit_tasks_finished_at", "audit_tasks", ["finished_at"], unique=False)
    op.create_index(
        "ix_audit_tasks_status_pool",
        "audit_tasks",
        ["status", "chain_id", "pool_address"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_audit_tasks_status_pool", table_name="audit_tasks")
    op.drop_index("ix_audit_tasks_finished_at", table_name="audit_tasks")
    op.drop_table("audit_tasks")
