"""empty message

Revision ID: 0f3bfac8a1f8
Revises: 3f2cf62a87e1
Create Date: 2020-08-31 04:04:09.429440

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "0f3bfac8a1f8"
down_revision = "3f2cf62a87e1"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "user_instrument",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("created_at", sa.TIMESTAMP(), nullable=False),
        sa.Column("updated_at", sa.TIMESTAMP(), nullable=False),
        sa.Column("performed_by", sa.Integer(), nullable=True),
        sa.Column("type", sa.String(), nullable=False),
        sa.Column("user_id", sa.Integer(), nullable=False),
        sa.Column("loan_id", sa.Integer(), nullable=False),
        sa.Column("details", sa.JSON(), server_default="{}", nullable=True),
        sa.Column("kyc_status", sa.String(length=20), server_default="PENDING", nullable=True),
        sa.Column("name", sa.String(), nullable=False),
        sa.Column("activation_date", sa.Date(), nullable=True),
        sa.Column("instrument_id", sa.String(), nullable=False),
        sa.Column("status", sa.String(), nullable=False, server_default="INACTIVE"),
        ### card-level columns
        sa.Column("credit_limit", sa.Numeric(), nullable=True),
        sa.Column("activation_type", sa.String(length=12), nullable=True),
        ### upi-level columns
        ### instrument settlings level columns
        sa.Column("no_of_txn_per_day", sa.Integer(), nullable=True),
        sa.Column("single_txn_spend_limit", sa.Integer(), nullable=True),
        sa.Column("daily_spend_limit", sa.Integer(), nullable=True),
        sa.Column("international_usage", sa.BOOLEAN(), server_default="false", nullable=False),
        sa.ForeignKeyConstraint(["loan_id"], ["loan.id"],),
        sa.ForeignKeyConstraint(["user_id"], ["v3_users.id"],),
        sa.PrimaryKeyConstraint("id"),
    )

    # op.create_index(op.f("ix_v3_user_cards_user_id"), "user_instrument", ["user_id"], unique=False)
    # op.add_column("v3_user_cards", sa.Column("statement_period_in_days", sa.Integer(), nullable=True))


def downgrade() -> None:
    pass
