"""loan_table

Revision ID: d5e975fd205c
Revises: 568935283001
Create Date: 2020-04-28 15:32:45.585137

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "d5e975fd205c"
down_revision = "568935283001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "user_card",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("user_id", sa.Integer(), nullable=False),
        sa.Column("card_type", sa.String(), nullable=False),
        sa.Column("card_activation_date", sa.Date(), nullable=True),
        sa.Column("statement_period_in_days", sa.Integer(), nullable=False),
        sa.Column("interest_free_period_in_days", sa.Integer, nullable=True),
        sa.Column("created_at", sa.TIMESTAMP(), nullable=False),
        sa.Column("updated_at", sa.TIMESTAMP(), nullable=False),
        sa.Column("performed_by", sa.Integer(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"], name="fk_user_card_user_id"),
    )
    op.create_table(
        "loan_data",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("updated_at", sa.TIMESTAMP(), nullable=False),
        sa.Column("created_at", sa.TIMESTAMP(), nullable=False),
        sa.Column("performed_by", sa.Integer(), nullable=False),
        sa.Column("user_id", sa.Integer(), nullable=False),
        sa.Column("agreement_date", sa.TIMESTAMP(), nullable=False),
        sa.Column("card_id", sa.Integer(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.ForeignKeyConstraint(["card_id"], ["user_card.id"], name="fk_loan_data_card_id"),
        sa.Column("rc_rate_of_interest_annual", sa.Numeric(), nullable=False),
        sa.Column("lender_rate_of_interest_annual", sa.Numeric(), nullable=False),
        sa.Column("lender_id", sa.Integer(), nullable=False),
        sa.Column("is_generated", sa.Boolean(), nullable=False, server_default="false"),
        sa.Column("principal", sa.Numeric(), nullable=True),
        sa.Column("principal_instalment", sa.Numeric(), nullable=True),
        sa.Column("interest_to_charge", sa.Numeric(), nullable=True),
    )
    op.create_table(
        "loan_emis",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("updated_at", sa.TIMESTAMP(), nullable=False),
        sa.Column("created_at", sa.TIMESTAMP(), nullable=False),
        sa.Column("performed_by", sa.Integer(), nullable=False),
        sa.Column("loan_id", sa.Integer(), nullable=False),
        sa.Column("due_date", sa.TIMESTAMP(), nullable=False),
        sa.Column("last_payment_date", sa.TIMESTAMP(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.ForeignKeyConstraint(["loan_id"], ["loan_data.id"], name="fk_loan_emis_loan_id"),
    )

    op.create_table(
        "card_transaction",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("loan_id", sa.Integer(), nullable=False),
        sa.Column("txn_time", sa.TIMESTAMP(), nullable=False),
        sa.Column("amount", sa.Numeric(), nullable=False),
        sa.Column("description", sa.String(100), nullable=True),
        sa.Column("created_at", sa.TIMESTAMP(), nullable=False),
        sa.Column("updated_at", sa.TIMESTAMP(), nullable=False),
        sa.Column("performed_by", sa.Integer(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.ForeignKeyConstraint(["loan_id"], ["loan_data.id"], name="fk_card_transaction_loan_id"),
    )

    op.create_table(
        "card_emis",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("due_date", sa.Date(), nullable=True),
        sa.Column("due_amount", sa.DECIMAL(), nullable=False),
        sa.Column("total_due_amount", sa.DECIMAL(), nullable=False),
        sa.Column("interest_current_month", sa.DECIMAL(), nullable=False),
        sa.Column("interest_next_month", sa.DECIMAL(), nullable=False),
        sa.Column("interest", sa.DECIMAL(), nullable=False),
        sa.Column("emi_number", sa.Integer(), nullable=False),
        sa.Column("late_fee", sa.DECIMAL(), nullable=False),
        sa.Column("row_status", sa.String(50), nullable=False),
        sa.Column("created_at", sa.TIMESTAMP(), nullable=False),
        sa.Column("updated_at", sa.TIMESTAMP(), nullable=False),
        sa.Column("performed_by", sa.Integer(), nullable=False),
        sa.Column("interest_received", sa.DECIMAL(), nullable=False),
        sa.Column("late_fee_received", sa.DECIMAL(), nullable=False),
        sa.Column("payment_received", sa.DECIMAL(), nullable=False),
        sa.Column("payment_status", sa.String(10), nullable=False),
        sa.Column("last_payment_date", sa.Date(), nullable=True),
        sa.Column("total_closing_balance_post_due_date", sa.DECIMAL(), nullable=False),
        sa.Column("total_closing_balance", sa.DECIMAL(), nullable=False),
        sa.Column("dpd", sa.Integer(), nullable=True),
        sa.Column("card_id", sa.Integer(), nullable=False),
        sa.Column("extra_details", sa.JSON(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.ForeignKeyConstraint(["card_id"], ["user_card.id"], name="fk_card_emis_card_id"),
    )

    op.create_table(
        "ledger_trigger_event",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("performed_by", sa.Integer(), nullable=False),
        sa.Column("post_date", sa.TIMESTAMP(), nullable=False),
        sa.Column("amount", sa.DECIMAL(), nullable=True),
        sa.Column("extra_details", sa.JSON(), nullable=False),
        sa.Column("name", sa.String(), nullable=False),
        sa.Column("card_id", sa.Integer(), nullable=True),
        sa.Column("created_at", sa.TIMESTAMP(), nullable=False),
        sa.Column("updated_at", sa.TIMESTAMP(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.ForeignKeyConstraint(["card_id"], ["user_card.id"], name="fk_ledger_trigger_event_card_id"),
    )

    op.create_table(
        "emi_payment_mapping",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("performed_by", sa.Integer(), nullable=False),
        sa.Column("card_id", sa.Integer(), nullable=True),
        sa.Column("emi_number", sa.Integer(), nullable=False),
        sa.Column("payment_date", sa.TIMESTAMP(), nullable=False),
        sa.Column("payment_request_id", sa.String(), nullable=False),
        sa.Column("interest_received", sa.DECIMAL(), nullable=True),
        sa.Column("late_fee_received", sa.DECIMAL(), nullable=True),
        sa.Column("principal_received", sa.DECIMAL(), nullable=True),
        sa.Column("created_at", sa.TIMESTAMP(), nullable=False),
        sa.Column("updated_at", sa.TIMESTAMP(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.ForeignKeyConstraint(["card_id"], ["user_card.id"], name="fk_emi_payment_mapping_card_id"),
    )

    op.create_table(
        "loan_moratorium",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("performed_by", sa.Integer(), nullable=False),
        sa.Column("card_id", sa.Integer(), nullable=True),
        sa.Column("start_date", sa.Date(), nullable=False),
        sa.Column("end_date", sa.Date(), nullable=False),
        sa.Column("created_at", sa.TIMESTAMP(), nullable=False),
        sa.Column("updated_at", sa.TIMESTAMP(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.ForeignKeyConstraint(["card_id"], ["user_card.id"], name="fk_loan_moratorium_card_id"),
    )


def downgrade() -> None:
    pass
