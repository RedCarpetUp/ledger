"""payment_requests_data

Revision ID: a7997149d740
Revises: 2590045263e5
Create Date: 2020-12-23 10:06:41.777669

"""
import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "a7997149d740"
down_revision = "57e039ce4b31"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "v3_payment_requests_data",
        sa.Column("id", sa.INTEGER(), nullable=False),
        sa.Column("type", sa.VARCHAR(length=20), nullable=False),
        sa.Column("payment_request_amount", sa.NUMERIC(), nullable=False),
        sa.Column("payment_request_status", sa.VARCHAR(length=20), nullable=False),
        sa.Column("source_account_id", sa.INTEGER(), nullable=False),
        sa.Column("destination_account_id", sa.INTEGER(), nullable=False),
        sa.Column("user_id", sa.INTEGER(), nullable=False),
        sa.Column("payment_request_id", sa.VARCHAR(length=50), nullable=False),
        sa.Column("row_status", sa.VARCHAR(length=20), nullable=False),
        sa.Column("payment_reference_id", sa.VARCHAR(length=120), nullable=True),
        sa.Column("created_at", sa.TIMESTAMP(), nullable=False),
        sa.Column("updated_at", sa.TIMESTAMP(), nullable=False),
        sa.Column("intermediary_payment_date", sa.TIMESTAMP(), nullable=True),
        sa.Column("payment_received_in_bank_date", sa.TIMESTAMP(), nullable=True),
        sa.Column("payment_request_mode", sa.VARCHAR(length=20), nullable=True),
        sa.Column("payment_execution_charges", sa.NUMERIC(), nullable=True),
        sa.Column("payment_gateway_id", sa.INTEGER(), nullable=True),
        sa.Column(
            "gateway_response",
            postgresql.JSONB(),
            server_default=sa.text("'{}'::jsonb"),
            nullable=True,
        ),
        sa.Column(
            "collection_by",
            sa.VARCHAR(length=20),
            server_default="customer",
            nullable=True,
        ),
        sa.Column("collection_request_id", sa.VARCHAR(length=50), nullable=True),
        sa.Column("payment_request_comments", sa.TEXT(), nullable=True),
        sa.Column("prepayment_amount", sa.NUMERIC(), nullable=True),
        sa.Column("net_payment_amount", sa.NUMERIC(), nullable=True),
        sa.Column("fee_amount", sa.NUMERIC(), nullable=True),
        sa.Column("expire_date", sa.TIMESTAMP(), nullable=True),
        sa.Column(
            "coupon_data",
            postgresql.JSONB(),
            server_default=sa.text("'{}'::jsonb"),
            nullable=True,
        ),
        sa.Column("gross_request_amount", sa.NUMERIC(), nullable=True),
        sa.Column("coupon_code", sa.VARCHAR(length=25), nullable=True),
        sa.Column(
            "extra_details",
            postgresql.JSONB(),
            server_default=sa.text("'{}'::jsonb"),
            nullable=True,
        ),
        sa.Column("performed_by", sa.Integer(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.execute(""" ALTER TABLE journal_entries ALTER COLUMN debit DROP NOT NULL; """)
    op.add_column("journal_entries", sa.Column("user_id", sa.Integer, nullable=False))
    op.execute(
        """create index index_on_extra_details_payment_request_id on ledger_trigger_event((extra_details->>'payment_request_id'))"""
    )
    op.add_column("v3_loans", sa.Column("tenure_in_months", sa.Integer, nullable=True))


def downgrade() -> None:
    pass
