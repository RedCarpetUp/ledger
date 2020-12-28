"""get_lender_balance

Revision ID: 57e039ce4b31
Revises: 2590045263e5
Create Date: 2020-12-24 12:33:12.628613

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "57e039ce4b31"
down_revision = "2590045263e5"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.alter_column("ledger_entry", "debit_account_balance", nullable=True)
    op.alter_column("ledger_entry", "credit_account_balance", nullable=True)

    op.execute(
        """
       create or replace function get_lender_account_balance(
            book_identifier integer,
            book_name varchar(50),
            till_date timestamp DEFAULT now() at time zone 'Asia/Kolkata'
        )
        returns numeric
        language plpgsql
        as
        $$
        DECLARE
        book_id integer;
        account_balance numeric;
        BEGIN
            SELECT id INTO book_id
            FROM book_account AS ba
            WHERE ba.identifier = $1 AND ba.book_name = $2;

            with balances as (
              select 
                book_id as id, 
                sum(
                  case when debit_account = book_id then l.amount else 0 end
                ) as debit_balance, 
                sum(
                  case when credit_account = book_id then l.amount else 0 end
                ) as credit_balance 
              from 
                ledger_entry l,
                ledger_trigger_event lte
              where 
                (debit_account = book_id 
                or credit_account = book_id) and lte.id = l.event_id
                and lte.post_date <= $3 
              group by 
                1
            )

            select 
              case when book.account_type in ('a', 'e') then debit_balance - credit_balance else credit_balance - debit_balance end INTO account_balance 
            from 
              balances 
              join book_account book on book.id = balances.id;

             RETURN account_balance;
        END;
        $$;
    """
    )


def downgrade() -> None:
    pass
