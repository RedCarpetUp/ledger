"""empty message

Revision ID: c0a3d3487d44
Revises: 0f3bfac8a1f8
Create Date: 2020-09-01 16:52:54.968463

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "c0a3d3487d44"
down_revision = "0f3bfac8a1f8"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("loan", sa.Column("downpayment_percent", sa.Numeric(), nullable=True))


def downgrade() -> None:
    pass
