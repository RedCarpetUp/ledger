"""net_product_price_sub_product_type

Revision ID: 9e3c39133b32
Revises: 4d3058ca5d21
Create Date: 2021-05-07 00:52:20.376074

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "9e3c39133b32"
down_revision = "4d3058ca5d21"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("loan_data", sa.Column("gross_principal", sa.Numeric(), nullable=True))
    op.add_column("v3_loans", sa.Column("sub_product_type", sa.String(15), nullable=True))


def downgrade() -> None:
    pass
