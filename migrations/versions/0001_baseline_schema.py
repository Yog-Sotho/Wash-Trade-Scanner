"""Baseline schema: the tables as of Phase 2 (incl. swap_trades.tx_from/tx_to).

Revision ID: 0001
Revises:
Create Date: 2026-11-17

Databases created before migrations existed (by create_all) are adopted at
this revision by core.storage: they differ from it only by the tx_from/tx_to
columns and their index, which are added before stamping.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0001"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "address_clusters",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("cluster_id", sa.String(length=64), nullable=False),
        sa.Column("addresses", sa.JSON(), nullable=False),
        sa.Column("creation_date", sa.DateTime(), server_default=sa.text("now()"), nullable=True),
        sa.Column("last_updated", sa.DateTime(), nullable=True),
        sa.Column("confidence_score", sa.Float(), nullable=False),
        sa.Column("evidence", sa.JSON(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        op.f("ix_address_clusters_cluster_id"), "address_clusters", ["cluster_id"], unique=False
    )
    op.create_table(
        "detection_audit_logs",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("chain_id", sa.Integer(), nullable=False),
        sa.Column("pool_address", sa.String(length=42), nullable=False),
        sa.Column("detection_type", sa.String(length=50), nullable=False),
        sa.Column("start_block", sa.BigInteger(), nullable=False),
        sa.Column("end_block", sa.BigInteger(), nullable=False),
        sa.Column("trades_processed", sa.Integer(), nullable=False),
        sa.Column("wash_trades_detected", sa.Integer(), nullable=False),
        sa.Column("detection_duration_seconds", sa.Float(), nullable=True),
        sa.Column("parameters_used", sa.JSON(), nullable=True),
        sa.Column("results_summary", sa.JSON(), nullable=True),
        sa.Column("created_at", sa.DateTime(), server_default=sa.text("now()"), nullable=True),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        op.f("ix_detection_audit_logs_chain_id"), "detection_audit_logs", ["chain_id"], unique=False
    )
    op.create_index(
        "ix_detection_audit_logs_chain_pool_time",
        "detection_audit_logs",
        ["chain_id", "pool_address", "created_at"],
        unique=False,
    )
    op.create_index(
        op.f("ix_detection_audit_logs_pool_address"),
        "detection_audit_logs",
        ["pool_address"],
        unique=False,
    )
    op.create_table(
        "swap_trades",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("chain_id", sa.Integer(), nullable=False),
        sa.Column("dex_name", sa.String(length=50), nullable=False),
        sa.Column("pool_address", sa.String(length=42), nullable=False),
        sa.Column("token_in", sa.String(length=42), nullable=False),
        sa.Column("token_out", sa.String(length=42), nullable=False),
        sa.Column("amount_in", sa.Float(), nullable=False),
        sa.Column("amount_out", sa.Float(), nullable=False),
        sa.Column("sender", sa.String(length=42), nullable=False),
        sa.Column("recipient", sa.String(length=42), nullable=False),
        sa.Column("tx_from", sa.String(length=42), nullable=True),
        sa.Column("tx_to", sa.String(length=42), nullable=True),
        sa.Column("transaction_hash", sa.String(length=66), nullable=False),
        sa.Column("block_number", sa.BigInteger(), nullable=False),
        sa.Column("block_timestamp", sa.DateTime(), nullable=False),
        sa.Column("gas_price", sa.Float(), nullable=True),
        sa.Column("gas_used", sa.Float(), nullable=True),
        sa.Column("log_index", sa.Integer(), nullable=False),
        sa.Column("amount_in_usd", sa.Float(), nullable=True),
        sa.Column("amount_out_usd", sa.Float(), nullable=True),
        sa.Column("volume_usd", sa.Float(), nullable=True),
        sa.Column("is_wash_trade", sa.Boolean(), nullable=False),
        sa.Column("wash_trade_score", sa.Float(), nullable=False),
        sa.Column("detection_method", sa.String(length=50), nullable=True),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        op.f("ix_swap_trades_block_timestamp"), "swap_trades", ["block_timestamp"], unique=False
    )
    op.create_index(
        "ix_swap_trades_chain_dex_pool",
        "swap_trades",
        ["chain_id", "dex_name", "pool_address"],
        unique=False,
    )
    op.create_index(op.f("ix_swap_trades_chain_id"), "swap_trades", ["chain_id"], unique=False)
    op.create_index(op.f("ix_swap_trades_dex_name"), "swap_trades", ["dex_name"], unique=False)
    op.create_index(
        op.f("ix_swap_trades_is_wash_trade"), "swap_trades", ["is_wash_trade"], unique=False
    )
    op.create_index(
        op.f("ix_swap_trades_pool_address"), "swap_trades", ["pool_address"], unique=False
    )
    op.create_index(
        "ix_swap_trades_pool_timestamp",
        "swap_trades",
        ["pool_address", "block_timestamp"],
        unique=False,
    )
    op.create_index(op.f("ix_swap_trades_recipient"), "swap_trades", ["recipient"], unique=False)
    op.create_index(op.f("ix_swap_trades_sender"), "swap_trades", ["sender"], unique=False)
    op.create_index(
        "ix_swap_trades_sender_timestamp",
        "swap_trades",
        ["sender", "block_timestamp"],
        unique=False,
    )
    op.create_index(op.f("ix_swap_trades_token_in"), "swap_trades", ["token_in"], unique=False)
    op.create_index(op.f("ix_swap_trades_token_out"), "swap_trades", ["token_out"], unique=False)
    op.create_index(
        op.f("ix_swap_trades_transaction_hash"), "swap_trades", ["transaction_hash"], unique=False
    )
    op.create_index(op.f("ix_swap_trades_tx_from"), "swap_trades", ["tx_from"], unique=False)
    op.create_index(
        "ix_swap_trades_tx_log", "swap_trades", ["transaction_hash", "log_index"], unique=True
    )
    op.create_table(
        "token_risk_profiles",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("chain_id", sa.Integer(), nullable=False),
        sa.Column("pool_address", sa.String(length=42), nullable=False),
        sa.Column("token_address", sa.String(length=42), nullable=False),
        sa.Column("overall_risk_score", sa.Float(), nullable=False),
        sa.Column("wash_trade_volume_ratio", sa.Float(), nullable=False),
        sa.Column("bot_trade_ratio", sa.Float(), nullable=False),
        sa.Column("circular_trade_ratio", sa.Float(), nullable=False),
        sa.Column("self_trade_ratio", sa.Float(), nullable=False),
        sa.Column("total_trades_analyzed", sa.Integer(), nullable=False),
        sa.Column("total_volume_usd", sa.Float(), nullable=False),
        sa.Column("wash_trade_volume_usd", sa.Float(), nullable=False),
        sa.Column("first_trade_timestamp", sa.DateTime(), nullable=True),
        sa.Column("last_updated", sa.DateTime(), nullable=True),
        sa.Column("created_at", sa.DateTime(), server_default=sa.text("now()"), nullable=True),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        op.f("ix_token_risk_profiles_chain_id"), "token_risk_profiles", ["chain_id"], unique=False
    )
    op.create_index(
        "ix_token_risk_profiles_chain_pool",
        "token_risk_profiles",
        ["chain_id", "pool_address"],
        unique=False,
    )
    op.create_index(
        op.f("ix_token_risk_profiles_pool_address"),
        "token_risk_profiles",
        ["pool_address"],
        unique=False,
    )
    op.create_index(
        "ix_token_risk_profiles_token", "token_risk_profiles", ["token_address"], unique=False
    )
    op.create_index(
        op.f("ix_token_risk_profiles_token_address"),
        "token_risk_profiles",
        ["token_address"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(op.f("ix_token_risk_profiles_token_address"), table_name="token_risk_profiles")
    op.drop_index("ix_token_risk_profiles_token", table_name="token_risk_profiles")
    op.drop_index(op.f("ix_token_risk_profiles_pool_address"), table_name="token_risk_profiles")
    op.drop_index("ix_token_risk_profiles_chain_pool", table_name="token_risk_profiles")
    op.drop_index(op.f("ix_token_risk_profiles_chain_id"), table_name="token_risk_profiles")
    op.drop_table("token_risk_profiles")
    op.drop_index("ix_swap_trades_tx_log", table_name="swap_trades")
    op.drop_index(op.f("ix_swap_trades_tx_from"), table_name="swap_trades")
    op.drop_index(op.f("ix_swap_trades_transaction_hash"), table_name="swap_trades")
    op.drop_index(op.f("ix_swap_trades_token_out"), table_name="swap_trades")
    op.drop_index(op.f("ix_swap_trades_token_in"), table_name="swap_trades")
    op.drop_index("ix_swap_trades_sender_timestamp", table_name="swap_trades")
    op.drop_index(op.f("ix_swap_trades_sender"), table_name="swap_trades")
    op.drop_index(op.f("ix_swap_trades_recipient"), table_name="swap_trades")
    op.drop_index("ix_swap_trades_pool_timestamp", table_name="swap_trades")
    op.drop_index(op.f("ix_swap_trades_pool_address"), table_name="swap_trades")
    op.drop_index(op.f("ix_swap_trades_is_wash_trade"), table_name="swap_trades")
    op.drop_index(op.f("ix_swap_trades_dex_name"), table_name="swap_trades")
    op.drop_index(op.f("ix_swap_trades_chain_id"), table_name="swap_trades")
    op.drop_index("ix_swap_trades_chain_dex_pool", table_name="swap_trades")
    op.drop_index(op.f("ix_swap_trades_block_timestamp"), table_name="swap_trades")
    op.drop_table("swap_trades")
    op.drop_index(op.f("ix_detection_audit_logs_pool_address"), table_name="detection_audit_logs")
    op.drop_index("ix_detection_audit_logs_chain_pool_time", table_name="detection_audit_logs")
    op.drop_index(op.f("ix_detection_audit_logs_chain_id"), table_name="detection_audit_logs")
    op.drop_table("detection_audit_logs")
    op.drop_index(op.f("ix_address_clusters_cluster_id"), table_name="address_clusters")
    op.drop_table("address_clusters")
