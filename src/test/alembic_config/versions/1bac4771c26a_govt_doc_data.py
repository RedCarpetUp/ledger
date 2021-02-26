"""govt_doc_data

Revision ID: 1bac4771c26a
Revises: 6b050482851b
Create Date: 2021-02-26 13:55:28.925043

"""
import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "1bac4771c26a"
down_revision = "6b050482851b"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "v3_govt_doc_data",
        sa.Column("response", postgresql.JSONB(), server_default=sa.text("'{}'::jsonb"), nullable=False),
        sa.Column("id_number", sa.TEXT(), nullable=False),
        sa.Column("id_number_hash", sa.TEXT(), nullable=False),
        sa.Column("dob", sa.TIMESTAMP(), nullable=True),
        sa.Column("client_id", sa.String(length=75), nullable=True, server_default=""),
        sa.Column("name", sa.String(length=100), nullable=True),
        sa.Column("type", sa.String(length=25), nullable=False),
        sa.Column("status", sa.String(length=15), nullable=False),
        sa.Column("row_status", sa.String(length=15), nullable=False, server_default="active"),
        sa.Column("gender", sa.String(length=1), nullable=True),
        sa.Column("user_id", sa.Integer(), nullable=False),
        sa.Column("request", postgresql.JSONB(), server_default=sa.text("'{}'::jsonb"), nullable=False),
        sa.Column("api_type", sa.String(length=50), nullable=True),
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("created_at", sa.TIMESTAMP(), nullable=False),
        sa.Column("updated_at", sa.TIMESTAMP(), nullable=False),
        sa.Column("performed_by", sa.Integer(), nullable=True),
        sa.ForeignKeyConstraint(
            ["user_id"],
            ["v3_users.id"],
        ),
        sa.PrimaryKeyConstraint("id"),
    )


def downgrade() -> None:
    op.drop_table("v3_govt_doc_data")
