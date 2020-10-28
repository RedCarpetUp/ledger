"""empty message

Revision ID: e2467923fab7
Revises: c0a3d3487d44
Create Date: 2020-10-06

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "e2467923fab7"
down_revision = "c0a3d3487d44"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "loan_schedule",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("loan_id", sa.Integer(), nullable=False),
        sa.Column("bill_id", sa.Integer(), nullable=True),
        sa.Column("emi_number", sa.Integer(), nullable=False),
        sa.Column("due_date", sa.Date(), nullable=False),
        sa.Column("principal_due", sa.DECIMAL(), nullable=False),
        sa.Column("interest_due", sa.DECIMAL(), nullable=False),
        sa.Column("dpd", sa.Integer(), nullable=True),
        sa.Column("last_payment_date", sa.Date(), nullable=True),
        sa.Column("total_closing_balance", sa.DECIMAL(), nullable=False),
        sa.Column("payment_received", sa.DECIMAL(), nullable=False),
        sa.Column("payment_status", sa.String(6), nullable=False),
        sa.Column("created_at", sa.TIMESTAMP(), nullable=False),
        sa.Column("updated_at", sa.TIMESTAMP(), nullable=False),
        sa.Column("performed_by", sa.Integer(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.ForeignKeyConstraint(["bill_id"], ["loan_data.id"], name="fk_card_emis_bill_id"),
        sa.ForeignKeyConstraint(["loan_id"], ["v3_loans.id"], name="fk_card_emis_loan_id"),
    )

    op.create_table(
        "emi_payment_mapping_new",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("payment_request_id", sa.String(), nullable=False, index=True),
        sa.Column("emi_id", sa.Integer(), nullable=False, index=True),
        sa.Column("amount_settled", sa.DECIMAL(), nullable=False),
        sa.Column("created_at", sa.TIMESTAMP(), nullable=False),
        sa.Column("updated_at", sa.TIMESTAMP(), nullable=False),
        sa.Column("performed_by", sa.Integer(), nullable=False),
        sa.Column("row_status", sa.String(8), nullable=False, default="active"),
        sa.PrimaryKeyConstraint("id"),
        sa.ForeignKeyConstraint(["emi_id"], ["loan_schedule.id"], name="fk_emi_id_mapping"),
    )

    op.create_index(
        "idx_uniq_on_row_status_emi_payment_mapping",
        "emi_payment_mapping_new",
        ["payment_request_id", "emi_id"],
        unique=True,
        postgresql_where=sa.text("row_status = 'active'"),
    )

    op.create_table(
        "payment_split",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("payment_request_id", sa.String(), nullable=False, index=True),
        sa.Column("component", sa.String(50), nullable=False),
        sa.Column("amount_settled", sa.DECIMAL(), nullable=False),
        sa.Column("created_at", sa.TIMESTAMP(), nullable=False),
        sa.Column("updated_at", sa.TIMESTAMP(), nullable=False),
        sa.Column("performed_by", sa.Integer(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )


def downgrade() -> None:
    pass
