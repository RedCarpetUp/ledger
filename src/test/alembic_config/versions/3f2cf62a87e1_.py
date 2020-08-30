"""empty message

Revision ID: 3f2cf62a87e1
Revises: 832b0e429413
Create Date: 2020-08-30 00:51:58.593594

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "3f2cf62a87e1"
down_revision = "832b0e429413"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_index(
        None,
        "card_emis",
        ["loan_id", "bill_id", "emi_number", "row_status"],
        unique=True,
        postgresql_where=sa.text("row_status = 'active'"),
    )


def downgrade() -> None:
    pass
