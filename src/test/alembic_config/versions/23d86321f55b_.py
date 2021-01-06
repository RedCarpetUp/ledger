"""empty message

Revision ID: 23d86321f55b
Revises: a7997149d740
Create Date: 2021-01-05 20:35:12.104566

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "23d86321f55b"
down_revision = "a7997149d740"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """create index index_on_extra_details_payment_request_id on ledger_trigger_event((extra_details->>'payment_request_id'))"""
    )


def downgrade() -> None:
    pass
