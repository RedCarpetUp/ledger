from decimal import Decimal

from pendulum import Date


def get_revenue_earned_in_a_period(session, from_date: Date, to_date: Date) -> Decimal:
    q = """
    with revenue_books as (
      select 
        id 
      from 
        book_account 
      where 
        account_type = 'r' 
        and book_name in (
          'interest_accrued', 'late_fee', 
          'card_processing_fee', 'reload_fee'
        )
    ), 
    accrued_revenue as (
      select 
        *, 
        get_account_balance_between_periods_by_book_id(
          id, :from_date, :to_date
        ) as accrued_amount 
      from 
        revenue_books
    ), 
    total_accrued_interest as (
      select 
        sum(accrued_amount) as total_accrued_interest 
      from 
        accrued_revenue
    ), 
    receivable_books as (
      select 
        id 
      from 
        book_account 
      where 
        account_type = 'a' 
        and book_name in (
          'interest_receivable', 'late_fine_receivable', 
          'card_processing_fee_receivable', 
          'reload_fee_receivable'
        )
    ), 
    remaining_revenue as (
      select 
        *, 
        get_account_balance_between_periods_by_book_id(
          id, :from_date, :to_date
        ) as remaining_amount 
      from 
        receivable_books
    ), 
    total_revenue_remaining as (
      select 
        sum(remaining_amount) as total_remaining 
      from 
        remaining_revenue
    ) 
    select 
      total_accrued_interest - total_remaining as revenue_earned 
    from 
      total_accrued_interest, 
      total_revenue_remaining;
    """
    revenue_earned = session.execute(q, params={"from_date": from_date, "to_date": to_date}).scalar()
    return revenue_earned or 0
