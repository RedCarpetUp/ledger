"""loan_table

Revision ID: d5e975fd2053
Revises: d5e975fd205c
Create Date: 2020-07-30 15:32:45.585137

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "d5e975fd205r"
down_revision = "d5e975fd205c"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "rewards_catalog",
        sa.Column("id", sa.Integer(), nullable=True),
        sa.Column("reward_type", sa.String(), nullable=False),
        sa.Column("from_time", sa.Date(), nullable=True),
        sa.Column("to_time", sa.Date(), nullable=True),
        sa.Column("min_val", sa.DECIMAL(), nullable=False),
        sa.Column("max_val", sa.DECIMAL(), nullable=False),
        sa.Column("calc_type", sa.String(), nullable=False),
        sa.Column("created_at", sa.TIMESTAMP(), nullable=False),
        sa.Column("updated_at", sa.TIMESTAMP(), nullable=False),
        sa.Column("performed_by", sa.Integer(), nullable=False),
        sa.Column("reward_rules", sa.JSON(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )

    op.create_table(
        "product",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("product_name", sa.String(), nullable=False),
        sa.Column("updated_at", sa.TIMESTAMP(), nullable=False),
        sa.Column("created_at", sa.TIMESTAMP(), nullable=False),
        sa.Column("performed_by", sa.Integer(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_table(
        "reward_master",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("identifier_type", sa.String(), nullable=False),
        sa.Column("reward_id", sa.Integer(), nullable=False),
        sa.Column("updated_at", sa.TIMESTAMP(), nullable=False),
        sa.Column("created_at", sa.TIMESTAMP(), nullable=False),
        sa.Column("performed_by", sa.Integer(), nullable=False),
        sa.Column("product_id", sa.Integer(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.ForeignKeyConstraint(
            ["reward_id"], ["rewards_catalog.id"], name="fk_reward_master_reward_id"
        ),
        sa.ForeignKeyConstraint(["product_id"], ["product.id"], name="fk_reward_master_product_id"),
    )


def downgrade() -> None:
    pass
