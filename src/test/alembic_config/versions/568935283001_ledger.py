"""ledger

Revision ID: 568935283001
Revises: 1a5c43fc5b50
Create Date: 2020-04-27 19:10:48.061667

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "568935283001"
down_revision = "1a5c43fc5b50"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "book_account",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("identifier", sa.Integer(), nullable=False),
        sa.Column("identifier_type", sa.String(50), nullable=False),
        sa.Column("book_name", sa.String(50), nullable=False),
        sa.Column("account_type", sa.String(50), nullable=False),
        sa.Column("book_date", sa.Date(), nullable=True),
        sa.Column("created_at", sa.TIMESTAMP(), nullable=False),
        sa.Column("updated_at", sa.TIMESTAMP(), nullable=False),
        sa.Column("performed_by", sa.Integer(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )


def downgrade() -> None:
    pass
