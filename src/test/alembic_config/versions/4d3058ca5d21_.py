"""empty message

Revision ID: 4d3058ca5d21
Revises: 72a5fc8c145c
Create Date: 2021-02-23 15:30:45.720199

"""
import sqlalchemy as sa
from alembic import op

from rush.models import Loan

# revision identifiers, used by Alembic.
revision = "4d3058ca5d21"
down_revision = "6a86ce49ea3f"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "v3_collection_order_mapping",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("collection_request_id", sa.String(32), nullable=False),
        sa.Column("batch_id", sa.Integer(), nullable=False),
        sa.Column("amount_to_pay", sa.Numeric(), nullable=False),
        sa.Column("amount_paid", sa.Numeric(), nullable=False, default=0),
        sa.Column("row_status", sa.String(20), nullable=False, default="active"),
        sa.Column("extra_details", sa.JSON(), server_default="{}", nullable=True),
        sa.Column("performed_by", sa.Integer(), nullable=False),
        sa.Column("created_at", sa.TIMESTAMP(), nullable=False),
        sa.Column("updated_at", sa.TIMESTAMP(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.ForeignKeyConstraint(["batch_id"], ["v3_loans.id"]),
    )


def downgrade() -> None:
    pass
