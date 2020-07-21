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
    create function get_account_balance(
      identifier integer, identifier_type char, 
      book_name char, account_type char
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
    ), 
    balances as (
      select 
        b.id, 
        sum(
          case when l.debit_account = b.id then l.amount else 0 end
        ) as debit_balance, 
        sum(
          case when l.credit_account = b.id then l.amount else 0 end
        ) as credit_balance 
      from 
        ledger_entry l, 
        book b 
      where 
        l.debit_account = b.id 
        or l.credit_account = b.id 
      group by 
        1
    ) 
    select 
      case when book.account_type in ('a', 'e') then debit_balance - credit_balance else credit_balance - debit_balance end as account_balance 
    from 
      balances 
      join book on book.id = balances.id;
    $$ language SQL;
    """
    )

    op.execute(
        """
    create function get_account_balance(
      identifier integer, identifier_type char, 
      book_name char, account_type char, to_date timestamp
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
    ), 
    balances as (
      select 
        b.id, 
        sum(
          case when l.debit_account = b.id then l.amount else 0 end
        ) as debit_balance, 
        sum(
          case when l.credit_account = b.id then l.amount else 0 end
        ) as credit_balance 
      from 
        ledger_entry l, 
        book b,
        ledger_trigger_event lte
      where 
        (l.debit_account = b.id 
        or l.credit_account = b.id) and lte.id = l.event_id
        and lte.post_date < $5 
      group by 
        1
    ) 
    select 
      case when book.account_type in ('a', 'e') then debit_balance - credit_balance else credit_balance - debit_balance end as account_balance 
    from 
      balances 
      join book on book.id = balances.id;
    $$ language SQL;
    """
    )


def downgrade() -> None:
    op.execute("DROP FUNCTION get_account_balance(integer, char, char, char, timestamp)")
    op.execute("DROP FUNCTION get_account_balance(integer, char, char, char)")
