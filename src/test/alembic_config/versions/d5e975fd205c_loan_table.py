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
        "v3_card_types",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("created_at", sa.TIMESTAMP(), nullable=False),
        sa.Column("updated_at", sa.TIMESTAMP(), nullable=False),
        sa.Column("performed_by", sa.Integer(), nullable=True),
        sa.Column("name", sa.String(length=20), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("name"),
    )

    op.create_table(
        "v3_card_kit_numbers",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("created_at", sa.TIMESTAMP(), nullable=False),
        sa.Column("updated_at", sa.TIMESTAMP(), nullable=False),
        sa.Column("performed_by", sa.Integer(), nullable=True),
        sa.Column("kit_number", sa.String(length=12), nullable=False),
        sa.Column("card_type", sa.String(length=5), nullable=False),
        sa.Column("last_5_digits", sa.String(length=5), nullable=False),
        sa.Column("status", sa.String(length=15), nullable=False),
        sa.ForeignKeyConstraint(["card_type"], ["v3_card_types.name"],),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("kit_number"),
    )
    op.add_column(
        "v3_card_kit_numbers",
        sa.Column("card_type_id", sa.Integer(), nullable=False, server_default=sa.text("1")),
    )
    op.drop_constraint("v3_card_kit_numbers_card_type_fkey", "v3_card_kit_numbers", type_="foreignkey")
    op.create_foreign_key(None, "v3_card_kit_numbers", "v3_card_types", ["card_type_id"], ["id"])
    op.drop_column("v3_card_kit_numbers", "card_type")

    op.create_table(
        "v3_card_names",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("created_at", sa.TIMESTAMP(), nullable=False),
        sa.Column("updated_at", sa.TIMESTAMP(), nullable=False),
        sa.Column("performed_by", sa.Integer(), nullable=True),
        sa.Column("name", sa.String(length=20), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("name"),
    )
    op.add_column("v3_card_kit_numbers", sa.Column("card_name_id", sa.Integer(), nullable=False))
    op.create_foreign_key(None, "v3_card_kit_numbers", "v3_card_names", ["card_name_id"], ["id"])
    op.add_column("v3_card_kit_numbers", sa.Column("card_type", sa.String(length=12), nullable=True))
    op.drop_column("v3_card_kit_numbers", "card_type_id")
    op.add_column(
        "v3_card_kit_numbers", sa.Column("extra_details", sa.JSON(), server_default="{}", nullable=False)
    )

    op.create_table(
        "v3_user_cards",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("created_at", sa.TIMESTAMP(), nullable=False),
        sa.Column("updated_at", sa.TIMESTAMP(), nullable=False),
        sa.Column("performed_by", sa.Integer(), nullable=True),
        sa.Column("user_id", sa.Integer(), nullable=False),
        sa.Column("kit_number", sa.String(length=12), nullable=False),
        sa.Column("credit_limit", sa.Numeric(), nullable=False),
        sa.Column("cash_withdrawal_limit", sa.Numeric(), nullable=False),
        sa.Column("drawdown_id", sa.Integer(), nullable=True),
        sa.Column("lender_id", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(["lender_id"], ["rc_lenders.id"], name="fk_v3_user_cards_lender_id"),
        sa.Column("details", sa.JSON(), server_default="{}", nullable=True),
        sa.Column("row_status", sa.String(length=20), nullable=False),
        sa.ForeignKeyConstraint(["drawdown_id"], ["v3_loans.id"],),
        sa.ForeignKeyConstraint(["kit_number"], ["v3_card_kit_numbers.kit_number"],),
        sa.ForeignKeyConstraint(["user_id"], ["v3_users.id"],),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "idx_uniq_kit_number_row_status",
        "v3_user_cards",
        ["kit_number", "row_status"],
        unique=True,
        postgresql_where=sa.text("row_status = 'active'"),
    )
    op.create_index(op.f("ix_v3_user_cards_user_id"), "v3_user_cards", ["user_id"], unique=False)
    op.add_column(
        "v3_user_cards",
        sa.Column("activation_type", sa.String(length=12), server_default="P", nullable=False),
    )
    op.add_column(
        "v3_user_cards",
        sa.Column("kyc_status", sa.String(length=20), server_default="PENDING", nullable=True),
    )
    op.add_column("v3_user_cards", sa.Column("card_type", sa.String(), nullable=True))
    op.add_column("v3_user_cards", sa.Column("card_activation_date", sa.Date(), nullable=True))
    op.add_column("v3_user_cards", sa.Column("statement_period_in_days", sa.Integer(), nullable=True))
    op.add_column("v3_user_cards", sa.Column("interest_free_period_in_days", sa.Integer, nullable=True))
    op.add_column("v3_user_cards", sa.Column("rc_rate_of_interest_monthly", sa.Numeric(), nullable=True))
    op.add_column(
        "v3_user_cards", sa.Column("lender_rate_of_interest_annual", sa.Numeric(), nullable=True)
    )
    op.add_column("v3_user_cards", sa.Column("dpd", sa.Integer, nullable=True))
    op.add_column("v3_user_cards", sa.Column("ever_dpd", sa.Integer, nullable=True))
    with op.batch_alter_table("v3_user_cards") as batch_op:
        batch_op.add_column(sa.Column("no_of_txn_per_day", sa.Integer(), nullable=True))
        batch_op.add_column(sa.Column("single_txn_spend_limit", sa.Integer(), nullable=True))
        batch_op.add_column(sa.Column("daily_spend_limit", sa.Integer(), nullable=True))
        batch_op.add_column(
            sa.Column("international_usage", sa.BOOLEAN(), server_default="false", nullable=False)
        )

    # op.create_table(
    #     "user_card",
    #     sa.Column("id", sa.Integer(), nullable=False),
    #     sa.Column("user_id", sa.Integer(), nullable=False),
    #     sa.Column("card_type", sa.String(), nullable=False),
    #     sa.Column("card_activation_date", sa.Date(), nullable=True),
    #     sa.Column("statement_period_in_days", sa.Integer(), nullable=False),
    #     sa.Column("interest_free_period_in_days", sa.Integer, nullable=True),
    #     sa.Column("rc_rate_of_interest_monthly", sa.Numeric(), nullable=False),
    #     sa.Column("lender_rate_of_interest_annual", sa.Numeric(), nullable=False),
    #     sa.Column("dpd", sa.Integer, nullable=True),
    #     sa.Column("created_at", sa.TIMESTAMP(), nullable=False),
    #     sa.Column("updated_at", sa.TIMESTAMP(), nullable=False),
    #     sa.Column("performed_by", sa.Integer(), nullable=False),
    #     sa.PrimaryKeyConstraint("id"),
    #     sa.ForeignKeyConstraint(["user_id"], ["users.id"], name="fk_user_card_user_id"),
    #     sa.Column("lender_id", sa.Integer(), nullable=False),
    #     sa.ForeignKeyConstraint(["lender_id"], ["rc_lenders.id"], name="fk_user_card_lender_id"),
    # )

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
        sa.Column("card_id", sa.Integer(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.ForeignKeyConstraint(["card_id"], ["v3_user_cards.id"], name="fk_loan_data_card_id"),
        sa.Column("lender_id", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(["lender_id"], ["rc_lenders.id"], name="fk_loan_data_lender_id"),
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
        sa.Column("source", sa.String(30), nullable=False),
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
        sa.Column("atm_fee", sa.DECIMAL(), nullable=False),
        sa.Column("row_status", sa.String(50), nullable=False),
        sa.Column("created_at", sa.TIMESTAMP(), nullable=False),
        sa.Column("updated_at", sa.TIMESTAMP(), nullable=False),
        sa.Column("performed_by", sa.Integer(), nullable=False),
        sa.Column("interest_received", sa.DECIMAL(), nullable=False),
        sa.Column("late_fee_received", sa.DECIMAL(), nullable=False),
        sa.Column("atm_fee_received", sa.DECIMAL(), nullable=False),
        sa.Column("payment_received", sa.DECIMAL(), nullable=False),
        sa.Column("payment_status", sa.String(10), nullable=False),
        sa.Column("last_payment_date", sa.Date(), nullable=True),
        sa.Column("total_closing_balance_post_due_date", sa.DECIMAL(), nullable=False),
        sa.Column("total_closing_balance", sa.DECIMAL(), nullable=False),
        sa.Column("dpd", sa.Integer(), nullable=True),
        sa.Column("card_id", sa.Integer(), nullable=False),
        sa.Column("extra_details", sa.JSON(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.ForeignKeyConstraint(["card_id"], ["v3_user_cards.id"], name="fk_card_emis_card_id"),
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
        sa.ForeignKeyConstraint(
            ["card_id"], ["v3_user_cards.id"], name="fk_ledger_trigger_event_card_id"
        ),
    )

    op.create_table(
        "ledger_entry",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("event_id", sa.Integer(), nullable=False),
        sa.Column("debit_account", sa.Integer(), nullable=False),
        sa.Column("credit_account", sa.Integer(), nullable=False),
        sa.Column("amount", sa.DECIMAL(), nullable=False),
        sa.Column("updated_at", sa.TIMESTAMP(), nullable=False),
        sa.Column("created_at", sa.TIMESTAMP(), nullable=False),
        sa.Column("performed_by", sa.Integer(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.ForeignKeyConstraint(["event_id"], ["ledger_trigger_event.id"], name="fk_fee_event_id"),
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
        sa.Column("atm_fee_received", sa.DECIMAL(), nullable=True),
        sa.Column("principal_received", sa.DECIMAL(), nullable=True),
        sa.Column("row_status", sa.String(50), nullable=False),
        sa.Column("created_at", sa.TIMESTAMP(), nullable=False),
        sa.Column("updated_at", sa.TIMESTAMP(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.ForeignKeyConstraint(
            ["card_id"], ["v3_user_cards.id"], name="fk_emi_payment_mapping_card_id"
        ),
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
        sa.ForeignKeyConstraint(["card_id"], ["v3_user_cards.id"], name="fk_loan_moratorium_card_id"),
    )

    op.create_table(
        "fee",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("performed_by", sa.Integer(), nullable=False),
        sa.Column("card_id", sa.Integer(), nullable=False),
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
        sa.ForeignKeyConstraint(["card_id"], ["v3_user_cards.id"], name="fk_fee_card_id"),
        sa.ForeignKeyConstraint(["bill_id"], ["loan_data.id"], name="fk_fee_bill_id"),
        sa.ForeignKeyConstraint(["event_id"], ["ledger_trigger_event.id"], name="fk_fee_event_id"),
    )

    op.create_table(
        "event_dpd",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("performed_by", sa.Integer(), nullable=False),
        sa.Column("card_id", sa.Integer(), nullable=False),
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
        sa.ForeignKeyConstraint(["card_id"], ["v3_user_cards.id"], name="fk_event_dpd_card_id"),
        sa.ForeignKeyConstraint(["event_id"], ["ledger_trigger_event.id"], name="fk_event_dpd_event_id"),
        sa.ForeignKeyConstraint(["bill_id"], ["loan_data.id"], name="fk_event_dpd_bill_id"),
    )


def downgrade() -> None:
    pass
