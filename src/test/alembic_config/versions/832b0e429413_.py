"""empty message

Revision ID: 832b0e429413
Revises: 8f3690240c02
Create Date: 2020-08-28 01:52:37.047280

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "832b0e429413"
down_revision = "8f3690240c02"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_unique_constraint("uq_product_name", "product", ["product_name"])

    op.create_table(
        "ephemeral_account",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("performed_by", sa.Integer(), nullable=False),
        sa.Column("user_id", sa.Integer(), nullable=False),
        sa.Column("product_type", sa.String(50), nullable=False),
        sa.Column("created_at", sa.TIMESTAMP(), nullable=False),
        sa.Column("updated_at", sa.TIMESTAMP(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.ForeignKeyConstraint(["user_id"], ["v3_users.id"]),
        sa.ForeignKeyConstraint(["product_type"], ["product.product_name"]),
    )

    op.alter_column("fee", "loan_id", nullable=True)
    op.add_column("fee", sa.Column("ephemeral_account_id", sa.Integer(), nullable=True))
    op.create_foreign_key(None, "fee", "ephemeral_account", ["ephemeral_account_id"], ["id"])
    op.create_check_constraint(
        None, "fee", "NOT(loan_id IS NULL AND bill_id IS NULL and ephemeral_account_id IS NULL)"
    )

    op.add_column("loan", sa.Column("ephemeral_account_id", sa.Integer(), nullable=True))
    op.create_foreign_key(None, "loan", "ephemeral_account", ["ephemeral_account_id"], ["id"])
    op.create_foreign_key(None, "loan", "product", ["product_type"], ["product_name"])


def downgrade() -> None:
    pass
