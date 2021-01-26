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
    CREATE OR REPLACE FUNCTION get_account_balance_by_book_id(
      book_account integer, till_date timestamp, event_id integer 
    ) RETURNS numeric as $$
    select 
      case when le.debit_account = $1 then debit_account_balance else credit_account_balance end as account_balance 
    from 
      ledger_entry le, 
      ledger_trigger_event lte 
    where 
      (
        le.debit_account = $1 
        or le.credit_account = $1
      ) 
      and lte.id = le.event_id 
      and (($3 is not null and event_id <= $3) or ($3 is null and post_date <= $2)) 
    order by 
      le.id desc limit 1;
    $$ language SQL;
    """
    )

    op.execute(
        """
    CREATE OR REPLACE FUNCTION get_account_balance_between_periods_by_book_id(
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
    CREATE OR REPLACE FUNCTION get_account_balance(
      identifier integer, identifier_type varchar, 
      book_name varchar, account_type varchar, till_date timestamp, event_id integer 
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
      get_account_balance_by_book_id(id, $5, $6) as account_balance 
    from book
    $$ language SQL;
    """
    )

    op.execute(
        """
    create function get_account_balance_between_periods(
      identifier integer, identifier_type varchar, 
      book_name varchar, account_type varchar, from_date timestamp, till_date timestamp
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

    # To optimize, add a filter to get only unpaid bills.
    op.execute(
        """
        CREATE OR REPLACE VIEW loan_min_view as
        select
          loan_id,
          sum(bill_min_balance) as min_balance
        from
          (
            select
              loan_id,
              ba.balance as bill_min_balance
            from
              loan_data ld, book_account ba
            where
              is_generated = true and
              ba.identifier = ld.id and ba.identifier_type = 'bill' and
              ba.book_name = 'min' and ba.account_type = 'a'
          ) bills
        group by
          loan_id;
        """
    )

    # To optimize, add a filter to get only unpaid bills.
    op.execute(
        """
        CREATE OR REPLACE VIEW loan_max_view as
        select
          loan_id,
          sum(bill_max_balance) as max_balance
        from
          (
            select
              loan_id,
              ba.balance as bill_max_balance
            from
              loan_data ld, book_account ba
            where
              is_generated = true and
              ba.identifier = ld.id and ba.identifier_type = 'bill' and
              ba.book_name = 'max' and ba.account_type = 'a'
          ) bills
        group by
          loan_id;
        """
    )

    op.execute(
        """
    CREATE FUNCTION calculate_book_account_balance()
        RETURNS trigger
        LANGUAGE plpgsql
    AS
    $$
    DECLARE
    debit_account book_account%ROWTYPE;
    credit_account book_account%ROWTYPE;
    BEGIN
        select * INTO STRICT debit_account from book_account where id = NEW.debit_account;
        select * into strict credit_account from book_account where id = NEW.credit_account;

        NEW.debit_account_balance = (select case when debit_account.account_type in ('a', 'e') then debit_account.balance + NEW.amount else debit_account.balance - NEW.amount end);
        NEW.credit_account_balance = (select case when credit_account.account_type in ('a', 'e') then credit_account.balance - NEW.amount else credit_account.balance + NEW.amount end);
        UPDATE book_account set balance = NEW.debit_account_balance where id = NEW.debit_account;
        UPDATE book_account set balance = NEW.credit_account_balance where id = NEW.credit_account;
        RETURN NEW;
    END;
    $$;
        """
    )
    op.execute(
        """
    CREATE TRIGGER balance_insert_trigger
    BEFORE INSERT
    ON ledger_entry
    FOR EACH ROW
    EXECUTE PROCEDURE calculate_book_account_balance();
    """
    )


def downgrade() -> None:
    op.execute("DROP TRIGGER balance_insert_trigger on ledger_entry")
    op.execute("DROP FUNCTION calculate_book_account_balance")
    op.execute("DROP VIEW loan_max_view")
    op.execute("DROP VIEW loan_min_view")
    op.execute("DROP FUNCTION get_account_balance_by_book_id")
    op.execute("DROP FUNCTION get_account_balance_between_periods_by_book_id")
    op.execute("DROP FUNCTION get_account_balance")
    op.execute("DROP FUNCTION get_account_balance_between_periods")
