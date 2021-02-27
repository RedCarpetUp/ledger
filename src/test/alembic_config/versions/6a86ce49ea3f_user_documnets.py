"""user documnets

Revision ID: 6a86ce49ea3f
Revises: 6b050482851b
Create Date: 2021-02-27 09:33:16.114788

"""
import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "6a86ce49ea3f"
down_revision = "6b050482851b"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "v3_user_documents",
        sa.Column("id", sa.INTEGER(), nullable=False),
        sa.Column("user_id", sa.INTEGER(), nullable=False),
        sa.Column("document_type", sa.VARCHAR(length=50), nullable=False),
        sa.Column("document_identification", sa.TEXT(), nullable=True),
        sa.Column("sequence", sa.INTEGER(), server_default="1", nullable=False),
        sa.Column("image_url", sa.VARCHAR(length=255), nullable=False),
        sa.Column(
            "text_details_json",
            postgresql.JSONB(),
            server_default=sa.text("'{}'::jsonb"),
            nullable=False,
        ),
        sa.Column("validity_date", sa.TIMESTAMP(), nullable=True),
        sa.Column("verification_date", sa.TIMESTAMP(), nullable=True),
        sa.Column("verification_status", sa.VARCHAR(length=255), nullable=True),
        sa.Column("reject_reason", sa.TEXT(), nullable=True),
        sa.Column("comments", sa.TEXT(), nullable=True),
        sa.Column("created_at", sa.TIMESTAMP(), nullable=False),
        sa.Column("updated_at", sa.TIMESTAMP(), nullable=False),
        sa.Column("lender_id", sa.INTEGER(), nullable=True),
        sa.Column("issue_date", sa.TIMESTAMP(), nullable=True),
        sa.Column(
            "row_status",
            sa.VARCHAR(),
            server_default=sa.text("'active'::character varying"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(["lender_id"], ["v3_users.id"], name="fk_v3_user_documents_lender_id"),
        sa.ForeignKeyConstraint(["user_id"], ["v3_users.id"], name="fk_v3_user_documents_user_id"),
        sa.PrimaryKeyConstraint("id", name="v3_user_documents_pkey"),
    )


def downgrade() -> None:
    op.drop_table("v3_user_documents")
