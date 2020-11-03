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
        "user_product",
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

    op.drop_column("fee", "bill_id")
    op.drop_column("fee", "loan_id")
    op.add_column("fee", sa.Column("user_id", sa.Integer(), nullable=False))
    op.create_foreign_key(None, "fee", "v3_users", ["user_id"], ["id"])

    op.add_column("fee", sa.Column("identifier", sa.String(), nullable=False))
    op.add_column("fee", sa.Column("identifier_id", sa.Integer(), nullable=False))

    op.drop_column("v3_loans", "product_id")
    op.add_column("v3_loans", sa.Column("user_product_id", sa.Integer(), nullable=True))
    op.create_foreign_key(None, "v3_loans", "user_product", ["user_product_id"], ["id"])
    op.create_foreign_key(None, "v3_loans", "product", ["product_type"], ["product_name"])


def downgrade() -> None:
    pass
