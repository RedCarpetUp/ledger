"""kv_ledger_accrued_interest

Revision ID: 6b050482851b
Revises: 72a5fc8c145c
Create Date: 2021-02-09 15:35:53.245516

"""
from alembic import op

# revision identifiers, used by Alembic.
revision = "6b050482851b"
down_revision = "72a5fc8c145c"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        create view kv_accrued_view_card_ledger as 
        select 
          lte.id as event_id, 
          lte.loan_id, 
          date_trunc('month' :: text, ls.due_date) + '1 mon -1 days' :: interval AS accrual_date, 
          total_closing_balance as outstanding_amt, 
          30 as number_days, 
          lte.amount, 
          'Card TL' as int_type 
        from 
          loan_schedule ls, 
          ledger_trigger_event lte 
        where 
          lte.name = 'accrue_interest' 
          and (lte.extra_details ->> 'emi_id'):: int = ls.id 
        order by 
          lte.post_date;
        """
    )


def downgrade() -> None:
    op.execute("DROP VIEW kv_accrued_view_card_ledger")
