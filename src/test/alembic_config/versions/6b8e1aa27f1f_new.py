"""new

Revision ID: 6b8e1aa27f1f
Revises: 1a5c43fc5b50
Create Date: 2020-04-27 16:37:01.275605

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "6b8e1aa27f1f"
down_revision = "1a5c43fc5b50"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "ledger_trigger_event",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("performed_by", sa.Integer(), nullable=False),
        sa.Column("extra_details", sa.JSON(), nullable=False),
        sa.Column("name", sa.String(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )


def downgrade() -> None:
    pass
