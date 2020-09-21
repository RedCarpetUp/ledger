"""add user

Revision ID: 1a5c43fc5b50
Revises: 888cd05eb8af
Create Date: 2020-04-25 15:42:19.320844

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "1a5c43fc5b50"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    # op.create_table(
    #     "users",
    #     sa.Column("id", sa.Integer(), nullable=False),
    #     sa.Column("performed_by", sa.Integer(), nullable=False),
    #     sa.Column("name", sa.String(), nullable=False),
    #     sa.Column("email", sa.String(), nullable=False),
    #     sa.Column("fullname", sa.String(), nullable=False),
    #     sa.Column("nickname", sa.String(), nullable=False),
    #     sa.Column("created_at", sa.TIMESTAMP(), nullable=False),
    #     sa.Column("updated_at", sa.TIMESTAMP(), nullable=False),
    #     sa.PrimaryKeyConstraint("id"),
    # )
    # pass

    op.create_table(
        "v3_users",
        sa.Column("id", sa.INTEGER(), autoincrement=True, nullable=False),
        sa.Column("performed_by", sa.Integer(), nullable=False),
        sa.Column("created_at", sa.TIMESTAMP(), autoincrement=False, nullable=False),
        sa.Column("updated_at", sa.TIMESTAMP(), autoincrement=False, nullable=False),
        sa.Column("v3_user_history_id", sa.INTEGER(), autoincrement=False, nullable=True),
        # sa.Column("phone_number", sa.VARCHAR(length=20), autoincrement=False, nullable=False),
        sa.PrimaryKeyConstraint("id", name="v3_users_pkey"),
        postgresql_ignore_search_path=False,
    )

    op.create_table(
        "v3_user_data",
        sa.Column("id", sa.INTEGER(), autoincrement=True, nullable=False),
        sa.Column("performed_by", sa.Integer(), nullable=False),
        sa.Column("user_id", sa.INTEGER(), autoincrement=False, nullable=False),
        sa.Column("first_name", sa.VARCHAR(length=255), autoincrement=False, nullable=True),
        sa.Column("last_name", sa.VARCHAR(length=255), autoincrement=False, nullable=True),
        sa.Column("email", sa.VARCHAR(length=255), autoincrement=False, nullable=True),
        sa.Column("date_of_birth", sa.TIMESTAMP(), autoincrement=False, nullable=True),
        sa.Column("pocket_money", sa.VARCHAR(length=30), autoincrement=False, nullable=True),
        sa.Column("referred_by", sa.VARCHAR(length=50), autoincrement=False, nullable=True),
        sa.Column("status", sa.VARCHAR(length=50), autoincrement=False, nullable=False),
        sa.Column("access_token", sa.VARCHAR(length=50), autoincrement=False, nullable=False),
        sa.Column("signup_otp", sa.INTEGER(), autoincrement=False, nullable=False),
        sa.Column("signup_otp_created_at", sa.TIMESTAMP(), autoincrement=False, nullable=False),
        sa.Column(
            "credit_limit",
            sa.NUMERIC(),
            server_default=sa.text("0"),
            autoincrement=False,
            nullable=False,
        ),
        sa.Column(
            "available_credit_limit",
            sa.NUMERIC(),
            server_default=sa.text("0"),
            autoincrement=False,
            nullable=False,
        ),
        sa.Column(
            "total_credit_used",
            sa.NUMERIC(),
            server_default=sa.text("0"),
            autoincrement=False,
            nullable=False,
        ),
        sa.Column(
            "rc_cash_balance",
            sa.NUMERIC(),
            server_default=sa.text("0"),
            autoincrement=False,
            nullable=False,
        ),
        sa.Column(
            "total_credit_payment_pending",
            sa.NUMERIC(),
            server_default=sa.text("0"),
            autoincrement=False,
            nullable=False,
        ),
        sa.Column(
            "total_overdue_payment",
            sa.NUMERIC(),
            server_default=sa.text("0"),
            autoincrement=False,
            nullable=False,
        ),
        sa.Column(
            "amount_due_as_of_today",
            sa.NUMERIC(),
            server_default=sa.text("0"),
            autoincrement=False,
            nullable=False,
        ),
        sa.Column(
            "amount_paid_as_of_today",
            sa.NUMERIC(),
            server_default=sa.text("0"),
            autoincrement=False,
            nullable=False,
        ),
        sa.Column(
            "amount_paid_by_due_date",
            sa.NUMERIC(),
            server_default=sa.text("0"),
            autoincrement=False,
            nullable=False,
        ),
        sa.Column(
            "amount_paid_after_due_date",
            sa.NUMERIC(),
            server_default=sa.text("0"),
            autoincrement=False,
            nullable=False,
        ),
        sa.Column(
            "unpaid_pending_amount",
            sa.NUMERIC(),
            server_default=sa.text("0"),
            autoincrement=False,
            nullable=False,
        ),
        sa.Column("created_at", sa.TIMESTAMP(), autoincrement=False, nullable=False),
        sa.Column("updated_at", sa.TIMESTAMP(), autoincrement=False, nullable=False),
        sa.Column("gcm_id", sa.VARCHAR(length=200), autoincrement=False, nullable=True),
        sa.Column("pusher_channel", sa.VARCHAR(length=50), autoincrement=False, nullable=True),
        sa.Column("gender", sa.VARCHAR(length=20), autoincrement=False, nullable=True),
        sa.Column("row_status", sa.VARCHAR(length=20), autoincrement=False, nullable=False),
        sa.Column("is_ambassador", sa.BOOLEAN(), autoincrement=False, nullable=True),
        sa.Column("became_ambassador_at", sa.TIMESTAMP(), autoincrement=False, nullable=True),
        sa.Column(
            "view_tags", sa.JSON(), server_default=sa.text("'{}'"), autoincrement=False, nullable=True
        ),
        sa.Column("has_app", sa.BOOLEAN(), autoincrement=False, nullable=True),
        sa.Column("unique_id", sa.VARCHAR(length=50), autoincrement=False, nullable=True),
        sa.ForeignKeyConstraint(["user_id"], ["v3_users.id"], name="fk_rails_0cbf2ae769"),
        sa.PrimaryKeyConstraint("id", name="v3_user_data_pkey"),
        postgresql_ignore_search_path=False,
    )

    op.create_table(
        "v3_user_identities",
        sa.Column("id", sa.INTEGER(), nullable=False),
        sa.Column("performed_by", sa.Integer(), nullable=False),
        sa.Column("user_id", sa.INTEGER(), autoincrement=False, nullable=False),
        sa.Column("identity", sa.VARCHAR(length=255), autoincrement=False, nullable=False),
        sa.Column("identity_type", sa.VARCHAR(length=50), autoincrement=False, nullable=False),
        sa.Column("comments", sa.TEXT(), autoincrement=False, nullable=True),
        sa.Column("row_status", sa.VARCHAR(length=20), autoincrement=False, nullable=False),
        sa.Column("created_at", sa.TIMESTAMP(), autoincrement=False, nullable=False),
        sa.Column("updated_at", sa.TIMESTAMP(), autoincrement=False, nullable=False),
        sa.ForeignKeyConstraint(["user_id"], ["v3_users.id"], name="fk_rails_97a0647e3a"),
        sa.PrimaryKeyConstraint("id", name="v3_user_identities_pkey"),
    )

    op.create_index("index_on_identity_v3_user_identity", "v3_user_identities", ["identity"])
    op.create_index("index_on_user_id_v3_user_identity", "v3_user_identities", ["user_id"])

    op.create_table(
        "v3_roles",
        sa.Column("id", sa.INTEGER(), nullable=False),
        sa.Column("performed_by", sa.Integer(), nullable=False),
        sa.Column("name", sa.VARCHAR(length=20), autoincrement=False, nullable=False),
        sa.Column("comments", sa.TEXT(), autoincrement=False, nullable=True),
        sa.Column("created_at", sa.TIMESTAMP(), autoincrement=False, nullable=False),
        sa.Column("updated_at", sa.TIMESTAMP(), autoincrement=False, nullable=False),
        sa.Column("data", sa.JSON(), autoincrement=False, nullable=True),
        sa.PrimaryKeyConstraint("id", name="v3_roles_pkey"),
    )
    op.create_index("index_on_name_and_id_v3_roles", "v3_roles", ["name", "id"], unique=False)

    op.create_table(
        "v3_user_roles",
        sa.Column("id", sa.INTEGER(), nullable=False),
        sa.Column("performed_by", sa.Integer(), nullable=False),
        sa.Column("user_id", sa.INTEGER(), autoincrement=False, nullable=False),
        sa.Column("role_id", sa.INTEGER(), autoincrement=False, nullable=False),
        sa.Column("created_at", sa.TIMESTAMP(), autoincrement=False, nullable=False),
        sa.Column("updated_at", sa.TIMESTAMP(), autoincrement=False, nullable=False),
        sa.Column("data", sa.JSON(), autoincrement=False, nullable=True),
        sa.Column("row_status", sa.VARCHAR(length=20), autoincrement=False, nullable=False),
        sa.ForeignKeyConstraint(["role_id"], ["v3_roles.id"], name="fk_rails_fdf6a6d9f6"),
        sa.ForeignKeyConstraint(["user_id"], ["v3_users.id"], name="fk_rails_f70ade1178"),
        sa.PrimaryKeyConstraint("id", name="v3_user_roles_pkey"),
    )

    op.create_index("index_on_user_id_v3_user_roles", "v3_user_roles", ["user_id"])

    op.add_column(
        "v3_user_data", sa.Column("email_verified", sa.Boolean(), server_default="false", nullable=False)
    )
    op.add_column("v3_user_data", sa.Column("corporate_email", sa.String(length=255), nullable=True))
    op.add_column(
        "v3_user_data",
        sa.Column("corporate_email_verified", sa.Boolean(), nullable=True, server_default="false"),
    )
    op.add_column("v3_user_data", sa.Column("ecdsa_signing_key", sa.String(length=100), nullable=True))
    op.add_column("v3_user_data", sa.Column("referral_code", sa.String(length=50), nullable=True))
    op.add_column("v3_user_data", sa.Column("utm_campaign", sa.String(length=50), nullable=True))
    op.add_column("v3_user_data", sa.Column("utm_medium", sa.String(length=50), nullable=True))
    op.add_column("v3_user_data", sa.Column("utm_source", sa.String(length=50), nullable=True))
    op.add_column("v3_user_data", sa.Column("assigned_to", sa.Integer(), nullable=True))
    op.create_foreign_key(None, "v3_user_data", "v3_users", ["assigned_to"], ["id"])
    op.add_column("v3_user_data", sa.Column("lender_id", sa.Integer(), nullable=True))

    op.create_table(
        "v3_loans",
        sa.Column("id", sa.INTEGER(), autoincrement=True, nullable=False),
        sa.Column("user_id", sa.INTEGER(), autoincrement=False, nullable=False),
        sa.Column("created_at", sa.TIMESTAMP(), autoincrement=False, nullable=False),
        sa.Column("updated_at", sa.TIMESTAMP(), autoincrement=False, nullable=False),
        sa.Column("is_deleted", sa.BOOLEAN(), autoincrement=False, nullable=True),
        sa.ForeignKeyConstraint(["user_id"], ["v3_users.id"], name="fk_rails_c5de6289f4"),
        sa.PrimaryKeyConstraint("id", name="v3_loans_pkey"),
        postgresql_ignore_search_path=False,
    )
    op.create_index("index_on_v3_loans_user_id_and_id", "v3_loans", ["user_id", "id"], unique=False)


def downgrade() -> None:
    pass
