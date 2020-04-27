"""ledger

Revision ID: 568935283001
Revises: 6b8e1aa27f1f
Create Date: 2020-04-27 19:10:48.061667

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "568935283001"
down_revision = "6b8e1aa27f1f"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "book_account",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("identifier", sa.Integer(), nullable=False),
        sa.Column("book_type", sa.String(), nullable=False),
        sa.Column("account_type", sa.String(), nullable=False),
        sa.Column("created_at", sa.TIMESTAMP(), nullable=False),
        sa.Column("updated_at", sa.TIMESTAMP(), nullable=False),
        sa.Column("performed_by", sa.Integer(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )

    op.create_table(
        "ledger_entry",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("event_id", sa.Integer(), nullable=False),
        sa.Column("from_book_account", sa.Integer(), nullable=False),
        sa.Column("to_book_account", sa.Integer(), nullable=False),
        sa.Column("amount", sa.DECIMAL(), nullable=False),
        sa.Column("business_date", sa.TIMESTAMP(), nullable=False),
        sa.Column("updated_at", sa.TIMESTAMP(), nullable=False),
        sa.Column("created_at", sa.TIMESTAMP(), nullable=False),
        sa.Column("performed_by", sa.Integer(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )


def downgrade() -> None:
    pass
