"""loan_table

Revision ID: d5e975fd205c
Revises: 568935283001
Create Date: 2020-04-28 15:32:45.585137

"""
import sqlalchemy as sa
from alembic import op
from sqlalchemy.sql.elements import True_

# revision identifiers, used by Alembic.
revision = "d5e975fd205c"
down_revision = "568935283001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "rc_lenders",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("performed_by", sa.Integer(), nullable=False),
        sa.Column("lender_name", sa.String(50), nullable=False),
        sa.Column("row_status", sa.String(50), nullable=False),
        sa.Column("created_at", sa.TIMESTAMP(), nullable=False),
        sa.Column("updated_at", sa.TIMESTAMP(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )

    op.create_table(
        "product",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("product_name", sa.String(), nullable=False),
        sa.Column("updated_at", sa.TIMESTAMP(), nullable=False),
        sa.Column("created_at", sa.TIMESTAMP(), nullable=False),
        sa.Column("performed_by", sa.Integer(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )

    op.create_table(
        "v3_loans",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("performed_by", sa.Integer(), nullable=False),
        sa.Column("user_id", sa.Integer(), nullable=True),
        sa.Column("amortization_date", sa.Date(), nullable=True),  # TODO: change back to nullable=False
        sa.Column("created_at", sa.TIMESTAMP(), nullable=False),
        sa.Column("updated_at", sa.TIMESTAMP(), nullable=False),
        sa.Column("interest_type", sa.String(30), nullable=True),  # TODO: change back to nullable=False
        sa.Column("loan_status", sa.String(50), nullable=True),  # TODO: change back to nullable=False
        sa.Column("product_id", sa.Integer(), nullable=True),
        sa.Column("lender_id", sa.Integer(), nullable=True),
        sa.Column("product_type", sa.String(), nullable=True),
        sa.Column("dpd", sa.Integer, nullable=True),
        sa.Column("ever_dpd", sa.Integer, nullable=True),
        sa.Column("interest_free_period_in_days", sa.Integer, nullable=True),
        sa.Column("rc_rate_of_interest_monthly", sa.Numeric(), nullable=True),
        sa.Column("lender_rate_of_interest_annual", sa.Numeric(), nullable=True),
        sa.Column("min_tenure", sa.Integer(), nullable=True),
        sa.Column("min_multiplier", sa.Numeric(), nullable=True),
        sa.Column("can_close_early", sa.Boolean(), server_default="true", nullable=True),
        sa.ForeignKeyConstraint(["lender_id"], ["rc_lenders.id"], name="fk_v3_user_cards_lender_id"),
        sa.ForeignKeyConstraint(["product_id"], ["product.id"], name="fk_loan_product_id"),
        sa.PrimaryKeyConstraint("id"),
    )

    op.create_table(
        "loan_data",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("updated_at", sa.TIMESTAMP(), nullable=False),
        sa.Column("created_at", sa.TIMESTAMP(), nullable=False),
        sa.Column("performed_by", sa.Integer(), nullable=False),
        sa.Column("user_id", sa.Integer(), nullable=False),
        sa.Column("bill_start_date", sa.Date(), nullable=False),
        sa.Column("bill_close_date", sa.Date(), nullable=False),
        sa.Column("bill_due_date", sa.Date(), nullable=False),
        sa.Column("bill_tenure", sa.Integer(), nullable=False),
        sa.Column("loan_id", sa.Integer(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.ForeignKeyConstraint(["loan_id"], ["v3_loans.id"], name="fk_loan_data_loan_id"),
        sa.Column("is_generated", sa.Boolean(), nullable=False, server_default="false"),
        sa.Column("principal", sa.Numeric(), nullable=True),
        sa.Column("principal_instalment", sa.Numeric(), nullable=True),
        sa.Column("interest_to_charge", sa.Numeric(), nullable=True),
    )

    op.create_table(
        "card_transaction",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("loan_id", sa.Integer(), nullable=False),
        sa.Column("txn_time", sa.TIMESTAMP(), nullable=False),
        sa.Column("amount", sa.Numeric(), nullable=False),
        sa.Column("mcc", sa.String(10), nullable=True),
        sa.Column("source", sa.String(30), nullable=False),
        sa.Column("description", sa.String(100), nullable=True),
        sa.Column("created_at", sa.TIMESTAMP(), nullable=False),
        sa.Column("updated_at", sa.TIMESTAMP(), nullable=False),
        sa.Column("performed_by", sa.Integer(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.ForeignKeyConstraint(["loan_id"], ["loan_data.id"], name="fk_card_transaction_loan_id"),
    )

    op.create_table(
        "ledger_trigger_event",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("performed_by", sa.Integer(), nullable=False),
        sa.Column("post_date", sa.TIMESTAMP(), nullable=False),
        sa.Column("amount", sa.DECIMAL(), nullable=True),
        sa.Column("extra_details", sa.JSON(), nullable=False),
        sa.Column("name", sa.String(), nullable=False),
        sa.Column("loan_id", sa.Integer(), nullable=True),
        sa.Column("created_at", sa.TIMESTAMP(), nullable=False),
        sa.Column("updated_at", sa.TIMESTAMP(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.ForeignKeyConstraint(["loan_id"], ["v3_loans.id"], name="fk_ledger_trigger_event_loan_id"),
    )

    op.create_table(
        "ledger_entry",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("event_id", sa.Integer(), nullable=False),
        sa.Column("debit_account", sa.Integer(), nullable=False),
        sa.Column("debit_account_balance", sa.DECIMAL(), nullable=False),
        sa.Column("credit_account", sa.Integer(), nullable=False),
        sa.Column("credit_account_balance", sa.DECIMAL(), nullable=False),
        sa.Column("amount", sa.DECIMAL(), nullable=False),
        sa.Column("created_at", sa.TIMESTAMP(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.ForeignKeyConstraint(["event_id"], ["ledger_trigger_event.id"], name="fk_fee_event_id"),
    )

    op.create_table(
        "loan_moratorium",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("performed_by", sa.Integer(), nullable=False),
        sa.Column("loan_id", sa.Integer(), nullable=True),
        sa.Column("start_date", sa.Date(), nullable=False),
        sa.Column("end_date", sa.Date(), nullable=False),
        sa.Column("created_at", sa.TIMESTAMP(), nullable=False),
        sa.Column("updated_at", sa.TIMESTAMP(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.ForeignKeyConstraint(["loan_id"], ["v3_loans.id"], name="fk_loan_moratorium_loan_id"),
    )

    op.create_table(
        "fee",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("performed_by", sa.Integer(), nullable=False),
        sa.Column("loan_id", sa.Integer(), nullable=False),
        sa.Column("event_id", sa.Integer(), nullable=False),
        sa.Column("bill_id", sa.Integer(), nullable=True),
        sa.Column("name", sa.String(30), nullable=False),
        sa.Column("net_amount", sa.DECIMAL(), nullable=False),
        sa.Column("sgst_rate", sa.DECIMAL(), nullable=False),
        sa.Column("cgst_rate", sa.DECIMAL(), nullable=False),
        sa.Column("igst_rate", sa.DECIMAL(), nullable=False),
        sa.Column("gross_amount", sa.DECIMAL(), nullable=False),
        sa.Column("net_amount_paid", sa.DECIMAL(), nullable=True),
        sa.Column("sgst_paid", sa.DECIMAL(), nullable=True),
        sa.Column("cgst_paid", sa.DECIMAL(), nullable=True),
        sa.Column("igst_paid", sa.DECIMAL(), nullable=True),
        sa.Column("gross_amount_paid", sa.DECIMAL(), nullable=True),
        sa.Column("fee_status", sa.String(10), nullable=False, default="UNPAID"),
        sa.Column("created_at", sa.TIMESTAMP(), nullable=False),
        sa.Column("updated_at", sa.TIMESTAMP(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.ForeignKeyConstraint(["loan_id"], ["v3_loans.id"], name="fk_fee_loan_id"),
        sa.ForeignKeyConstraint(["bill_id"], ["loan_data.id"], name="fk_fee_bill_id"),
        sa.ForeignKeyConstraint(["event_id"], ["ledger_trigger_event.id"], name="fk_fee_event_id"),
    )

    op.create_table(
        "event_dpd",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("performed_by", sa.Integer(), nullable=False),
        sa.Column("loan_id", sa.Integer(), nullable=False),
        sa.Column("event_id", sa.Integer(), nullable=False),
        sa.Column("credit", sa.DECIMAL(), nullable=True),
        sa.Column("debit", sa.DECIMAL(), nullable=True),
        sa.Column("balance", sa.DECIMAL(), nullable=True),
        sa.Column("dpd", sa.Integer, nullable=False),
        sa.Column("bill_id", sa.Integer(), nullable=False),
        sa.Column("row_status", sa.String(length=20), nullable=False),
        sa.Column("created_at", sa.TIMESTAMP(), nullable=False),
        sa.Column("updated_at", sa.TIMESTAMP(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.ForeignKeyConstraint(["loan_id"], ["v3_loans.id"], name="fk_event_dpd_loan_id"),
        sa.ForeignKeyConstraint(["event_id"], ["ledger_trigger_event.id"], name="fk_event_dpd_event_id"),
        sa.ForeignKeyConstraint(["bill_id"], ["loan_data.id"], name="fk_event_dpd_bill_id"),
    )


def downgrade() -> None:
    pass
