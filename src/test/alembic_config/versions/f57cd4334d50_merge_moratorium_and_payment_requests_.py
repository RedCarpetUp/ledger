"""merge moratorium and payment_requests_data

Revision ID: f57cd4334d50
Revises: a7997149d740, aad4ea760350
Create Date: 2021-02-04 11:39:29.335828

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "f57cd4334d50"
down_revision = ("a7997149d740", "aad4ea760350")
branch_labels = None
depends_on = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
