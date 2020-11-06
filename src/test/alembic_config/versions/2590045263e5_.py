"""empty message

Revision ID: 2590045263e5
Revises: 2490045263e3
Create Date: 2020-11-06 20:00:00.861857

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "2590045263e5"
down_revision = "2490045263e3"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "journal_entries",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("voucher_type", sa.String(50), nullable=False),
        sa.Column("date_ledger", sa.TIMESTAMP(), nullable=False),
        sa.Column("ledger", sa.String(50), nullable=False),
        sa.Column("alias", sa.String(50), nullable=True),
        sa.Column("group_name", sa.String(50), nullable=False),
        sa.Column("debit", sa.Numeric(), nullable=False),
        sa.Column("credit", sa.Numeric(), nullable=False),
        sa.Column("narration", sa.String(50), nullable=True),
        sa.Column("instrument_date", sa.TIMESTAMP(), nullable=False),
        sa.Column("sort_order", sa.Integer(), nullable=False),
        sa.Column("ptype", sa.String(50), nullable=False),
        sa.Column("created_at", sa.TIMESTAMP(), nullable=False),
        sa.Column("row_status", sa.String(length=20), nullable=False),
        sa.Column("updated_at", sa.TIMESTAMP(), nullable=False),
        sa.Column("performed_by", sa.Integer(), nullable=False),
        sa.Column("event_id", sa.Integer(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.ForeignKeyConstraint(
            ["event_id"], ["ledger_trigger_event.id"], name="fk_journal_entries_event_id"
        ),
    )


def downgrade() -> None:
    pass
