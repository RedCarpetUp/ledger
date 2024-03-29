"""empty message

Revision ID: 2490045263e3
Revises: c0a3d3487d44
Create Date: 2020-10-04 06:08:40.861857

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "2490045263e3"
down_revision = "e2467923fab7"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("card_transaction", sa.Column("status", sa.String(15), nullable=False))
    op.add_column("card_transaction", sa.Column("trace_no", sa.String(20), nullable=False))
    op.add_column("card_transaction", sa.Column("txn_ref_no", sa.String(50), nullable=False))


def downgrade() -> None:
    pass
