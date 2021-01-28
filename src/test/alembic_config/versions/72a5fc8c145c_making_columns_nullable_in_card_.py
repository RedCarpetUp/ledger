"""making columns nullable in card_transaction

Revision ID: 72a5fc8c145c
Revises: a7997149d740
Create Date: 2021-01-28 11:53:23.151625

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "72a5fc8c145c"
down_revision = "a7997149d740"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.alter_column("card_transaction", "description", nullable=True)
    op.alter_column("card_transaction", "trace_no", nullable=True)
    op.alter_column("card_transaction", "txn_ref_no", nullable=True)
    op.alter_column("card_transaction", "status", nullable=True)


def downgrade() -> None:
    op.alter_column("card_transaction", "description", nullable=False)
    op.alter_column("card_transaction", "trace_no", nullable=False)
    op.alter_column("card_transaction", "txn_ref_no", nullable=False)
    op.alter_column("card_transaction", "status", nullable=False)
