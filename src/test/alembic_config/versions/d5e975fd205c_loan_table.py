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
    )
    op.create_table(
        "loan_emis",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("updated_at", sa.TIMESTAMP(), nullable=False),
        sa.Column("created_at", sa.TIMESTAMP(), nullable=False),
        sa.Column("performed_by", sa.Integer(), nullable=False),
        sa.Column("loan_id", sa.Integer(), nullable=False),
        sa.Column("due_date", sa.TIMESTAMP(), nullable=False),
        sa.Column("last_payment_date", sa.TIMESTAMP(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.ForeignKeyConstraint(["loan_id"], ["loan_data.id"], name="fk_loan_emis_loan_id"),
    )

    op.create_table(
        "card_transaction",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("loan_id", sa.Integer(), nullable=False),
        sa.Column("txn_time", sa.TIMESTAMP(), nullable=False),
        sa.Column("amount", sa.DECIMAL(), nullable=False),
        sa.Column("description", sa.String(100), nullable=True),
        sa.Column("created_at", sa.TIMESTAMP(), nullable=False),
        sa.Column("updated_at", sa.TIMESTAMP(), nullable=False),
        sa.Column("performed_by", sa.Integer(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.ForeignKeyConstraint(["loan_id"], ["loan_data.id"], name="fk_card_transaction_loan_id"),
    )


def downgrade() -> None:
    pass
