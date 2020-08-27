"""get_account_balance

Revision ID: 8f3690240c02
Revises: d5e975fd205c
Create Date: 2020-07-17 08:27:52.206387

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "8f3690240c02"
down_revision = "d5e975fd205c"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
    create function get_account_balance_by_book_id(
      book_account integer, till_date timestamp DEFAULT now() at time zone 'Asia/Kolkata'
    ) RETURNS numeric as $$ with balances as (
      select 
        $1 as id, 
        sum(
          case when debit_account = $1 then l.amount else 0 end
        ) as debit_balance, 
        sum(
          case when credit_account = $1 then l.amount else 0 end
        ) as credit_balance 
      from 
        ledger_entry l,
        ledger_trigger_event lte
      where 
        (debit_account = $1 
        or credit_account = $1) and lte.id = l.event_id
        and lte.post_date <= $2 
      group by 
        1
    ) 
    select 
      case when book.account_type in ('a', 'e') then debit_balance - credit_balance else credit_balance - debit_balance end as account_balance 
    from 
      balances 
      join book_account book on book.id = balances.id;
    $$ language SQL;
    """
    )

    op.execute(
        """
    create function get_account_balance_between_periods_by_book_id(
      book_account integer, from_date timestamp, till_date timestamp DEFAULT now() at time zone 'Asia/Kolkata' 
    ) RETURNS numeric as $$ with balances as (
      select 
        $1 as id, 
        sum(
          case when debit_account = $1 then l.amount else 0 end
        ) as debit_balance, 
        sum(
          case when credit_account = $1 then l.amount else 0 end
        ) as credit_balance 
      from 
        ledger_entry l,
        ledger_trigger_event lte
      where 
        (debit_account = $1 
        or credit_account = $1) and lte.id = l.event_id
        and lte.post_date >= $2 and lte.post_date <= $3 
      group by 
        1
    ) 
    select 
      case when book.account_type in ('a', 'e') then debit_balance - credit_balance else credit_balance - debit_balance end as account_balance 
    from 
      balances 
      join book_account book on book.id = balances.id;
    $$ language SQL;
    """
    )

    op.execute(
        """
    create function get_account_balance(
      identifier integer, identifier_type char, 
      book_name char, account_type char, till_date timestamp DEFAULT now() at time zone 'Asia/Kolkata'
    ) RETURNS numeric as $$ with book as (
      select 
        * 
      from 
        book_account 
      where 
        identifier = $1 
        and identifier_type = $2 
        and book_name = $3 
        and account_type = $4
    ) 
    select 
      get_account_balance_by_book_id(id, $5) as account_balance 
    from book
    $$ language SQL;
    """
    )

    op.execute(
        """
    create function get_account_balance_between_periods(
      identifier integer, identifier_type char, 
      book_name char, account_type char, from_date timestamp, till_date timestamp DEFAULT now() at time zone 'Asia/Kolkata'
    ) RETURNS numeric as $$ with book as (
      select 
        * 
      from 
        book_account 
      where 
        identifier = $1 
        and identifier_type = $2 
        and book_name = $3 
        and account_type = $4
    ) 
    select 
      get_account_balance_between_periods_by_book_id(id, $5, $6) as account_balance 
    from book
    $$ language SQL;
    """
    )


def downgrade() -> None:
    op.execute("DROP FUNCTION get_account_balance_by_book_id")
    op.execute("DROP FUNCTION get_account_balance_between_periods_by_book_id")
    op.execute("DROP FUNCTION get_account_balance")
    op.execute("DROP FUNCTION get_account_balance_between_periods")
