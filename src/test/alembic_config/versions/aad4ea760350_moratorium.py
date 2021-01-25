"""moratorium

Revision ID: aad4ea760350
Revises: 2590045263e5
Create Date: 2020-12-14 11:03:42.799678

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "aad4ea760350"
down_revision = "2590045263e5"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "moratorium_interest",
        sa.Column("id", sa.Integer, primary_key=True, nullable=False),
        sa.Column("moratorium_id", sa.Integer, nullable=False),
        sa.Column("emi_number", sa.Integer, nullable=False),
        sa.Column("interest", sa.Numeric, nullable=False),
        sa.Column("bill_id", sa.Integer, nullable=False),
        sa.Column("due_date", sa.Date(), nullable=False),
        sa.Column("created_at", sa.TIMESTAMP(), nullable=False),
        sa.Column("updated_at", sa.TIMESTAMP(), nullable=False),
        sa.Column("performed_by", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(
            ["moratorium_id"],
            ["loan_moratorium.id"],
        ),
        sa.ForeignKeyConstraint(
            ["bill_id"],
            ["loan_data.id"],
        ),
    )


def downgrade() -> None:
    pass
