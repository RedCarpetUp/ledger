interest_on_dmi_portfolio = """
with recursive all_cards AS (
  select 
    id as card_id, 
    1 + lender_rate_of_interest_annual / 100 / 365 as per_day_interest 
  from 
    user_card
), 
lender_payable_balance_change_dates as (
  select 
    distinct card_id, 
    post_date :: date as portfolio_balance_change_date 
  from 
    ledger_trigger_event 
    join all_cards using(card_id) 
  where 
    name in (
      'card_transaction', 'payment_received', 
      'merchant_refund'
    ) 
    and post_date::date >= :from_date
    and post_date::date <= :to_date
), 
relevant_cards AS (
    select distinct card_id from lender_payable_balance_change_dates
),
-- Reducing a day because need to consider interest on the first day as well. i.e the from date.
start_date_and_card_id AS (
    select card_id, (:from_date - interval '1 day')::date as portfolio_balance_change_date from relevant_cards
),
lender_payable_balance_change_dates_with_start_date AS (
    select * from start_date_and_card_id
    union
    select * from lender_payable_balance_change_dates
),
days_to_charge_interest_for AS (
  select 
    *, 
    coalesce(
      LEAD(portfolio_balance_change_date) over (
        partition by card_id 
        order by 
          portfolio_balance_change_date
      ), 
      :to_date
    ) - portfolio_balance_change_date as days_to_charge_interest 
  from 
    lender_payable_balance_change_dates_with_start_date
), 
days_wise_balance AS (
  select 
    card_id, 
    portfolio_balance_change_date, 
    days_to_charge_interest, 
    pow(
      per_day_interest, days_to_charge_interest
    ) as interest_multiplier, 
-- Getting the balance at the end of that day. So adding 23:59:59.
    coalesce(
      get_account_balance(
        card_id, 'card', 'lender_payable', 
        'l', portfolio_balance_change_date + interval '23 hours' + interval '59 minutes' + interval '59 seconds'
      ), 
      0
    ) as balance, 
    row_number() over (
      order by 
        card_id, 
        portfolio_balance_change_date
    ) as rn 
  from 
    days_to_charge_interest_for 
    join all_cards using(card_id) 
  order by 
    card_id, 
    portfolio_balance_change_date
), 
interst_calc AS (
  select 
    card_id, 
    rn, 
    balance as balance_with_interest, 
    round((interest_multiplier * balance) - balance, 2) as interest 
  from 
    days_wise_balance 
  where 
    rn = 1 
  union 
  select 
    d.card_id, 
    d.rn, 
    d.balance + i.interest as balance_with_interest, 
    round((d.balance + i.interest) * d.interest_multiplier - (d.balance + i.interest), 2) as interest 
  from 
    days_wise_balance d, 
    interst_calc i 
  where 
    d.rn = i.rn + 1
) 
select 
  card_id, 
  round(
    sum(interest), 
    2
  ) as lender_share 
from 
  interst_calc 
group by 
  1;
"""
