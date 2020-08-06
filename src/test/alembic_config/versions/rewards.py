"""loan_table

Revision ID: d5e975fd2053
Revises: 8f3690240c02
Create Date: 2020-07-30 15:32:45.585137

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "d5e975fd205r"
down_revision = "8f3690240c02"
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
        "rewards",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("identifier_type", sa.String(), nullable=False),
        sa.Column("reward_id", sa.Integer(), nullable=False),
        sa.Column("updated_at", sa.TIMESTAMP(), nullable=False),
        sa.Column("created_at", sa.TIMESTAMP(), nullable=False),
        sa.Column("performed_by", sa.Integer(), nullable=False),
        sa.Column("product_id", sa.Integer(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.ForeignKeyConstraint(["reward_id"], ["rewards_catalog.id"], name="fk_rewards_reward_id"),
        sa.ForeignKeyConstraint(["product_id"], ["product.id"], name="fk_rewards_product_id"),
    )

    op.create_table(
        "merchant_interest_rates",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("merchant_id", sa.String(), nullable=False),
        sa.Column("product_id", sa.Integer(), nullable=False),
        sa.Column("interest_rate", sa.DECIMAL(), nullable=False),
        sa.Column("updated_at", sa.TIMESTAMP(), nullable=False),
        sa.Column("created_at", sa.TIMESTAMP(), nullable=False),
        sa.Column("performed_by", sa.Integer(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.ForeignKeyConstraint(
            ["product_id"], ["product.id"], name="fk_merchant_interest_rates_product_id"
        ),
    )


def downgrade() -> None:
    pass
