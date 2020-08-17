from decimal import Decimal

from dateutil.relativedelta import relativedelta
from pendulum import DateTime
from sqlalchemy.orm import Session

from rush.accrue_financial_charges import create_fee_entry
from rush.card import BaseCard
from rush.card.base_card import BaseBill
from rush.ledger_events import bill_generate_event
from rush.ledger_utils import get_account_balance_from_str
from rush.min_payment import add_min_to_all_bills
from rush.models import (
    LedgerTriggerEvent,
    LoanMoratorium,
)
from rush.utils import (
    div,
    get_current_ist_time,
    mul,
)


def get_or_create_bill_for_card_swipe(user_card: BaseCard, txn_time: DateTime) -> BaseBill:
    session = user_card.session
    # Get the most recent bill
    last_bill = user_card.get_latest_bill()
    txn_date = txn_time.date()
    lender_id = user_card.table.lender_id
    if last_bill:
        does_swipe_belong_to_current_bill = txn_date < last_bill.bill_close_date
        if does_swipe_belong_to_current_bill:
            return {"result": "success", "bill": last_bill}
        new_bill_date = last_bill.bill_close_date
    else:
        new_bill_date = user_card.table.card_activation_date
    new_closing_date = new_bill_date + relativedelta(months=1)
    # Check if some months of bill generation were skipped and if they were then generate their bills
    months_diff = (txn_date.year - new_closing_date.year) * 12 + txn_date.month - new_closing_date.month
    if months_diff > 0:
        for i in range(months_diff + 1):
            new_bill = user_card.create_bill(
                bill_start_date=new_bill_date + relativedelta(months=i, day=1),
                bill_close_date=new_bill_date + relativedelta(months=i + 1, day=1),
                bill_due_date=new_bill_date + relativedelta(months=i + 1, day=15),
                lender_id=lender_id,
                is_generated=False,
            )
            bill_generate(user_card)
        last_bill = user_card.get_latest_bill()
        new_bill_date = last_bill.bill_close_date
    new_bill = user_card.create_bill(
        bill_start_date=new_bill_date,
        bill_close_date=new_bill_date + relativedelta(months=1, day=1),
        bill_due_date=new_bill_date + relativedelta(months=1, day=15),
        lender_id=lender_id,
        is_generated=False,
    )
    return {"result": "success", "bill": new_bill}


def bill_generate(user_card: BaseCard) -> BaseBill:
    session = user_card.session
    bill = user_card.get_latest_bill_to_generate()  # Get the first bill which is not generated.
    if not bill:
        bill = get_or_create_bill_for_card_swipe(
            user_card, get_current_ist_time()
        )  # TODO not sure about this
        if bill["result"] == "error":
            return bill
        bill = bill["bill"]
    lt = LedgerTriggerEvent(name="bill_generate", card_id=user_card.id, post_date=bill.bill_close_date)
    session.add(lt)
    session.flush()

    bill_generate_event(session, bill, user_card.id, lt)

    bill.table.is_generated = True

    _, billed_amount = get_account_balance_from_str(
        session, book_string=f"{bill.id}/bill/principal_receivable/a"
    )
    principal_instalment = div(billed_amount, bill.table.bill_tenure)

    # Update the bill row here.
    bill.table.principal = billed_amount
    bill.table.principal_instalment = principal_instalment
    bill.table.interest_to_charge = bill.get_interest_to_charge(
        user_card.table.rc_rate_of_interest_monthly
    )

    bill_closing_date = bill.bill_start_date + relativedelta(months=+1)
    # Don't add in min if user is in moratorium.
    if not LoanMoratorium.is_in_moratorium(session, user_card.id, bill_closing_date):
        # After the bill has generated. Call the min generation event on all unpaid bills.
        add_min_to_all_bills(session, bill_closing_date, user_card)

    atm_transactions_sum = bill.sum_of_atm_transactions()
    if atm_transactions_sum > 0:
        add_atm_fee(session, bill, bill.table.bill_close_date, atm_transactions_sum)

    from rush.create_emi import refresh_schedule

    refresh_schedule(user_card)

    return bill


def extend_tenure(session: Session, user_card: BaseCard, new_tenure: int) -> None:
    unpaid_bills = user_card.get_unpaid_bills()
    for bill in unpaid_bills:
        bill.table.bill_tenure = new_tenure
        principal_instalment = div(bill.table.principal, bill.table.bill_tenure)
        # Update the bill rows here
        bill.table.principal_instalment = principal_instalment
        bill.table.interest_to_charge = bill.get_interest_to_charge(
            user_card.table.rc_rate_of_interest_monthly
        )
    session.flush()
    # Refresh the schedule
    from rush.create_emi import refresh_schedule

    refresh_schedule(user_card)


def add_atm_fee(
    session: Session, bill: BaseBill, post_date: DateTime, atm_transactions_amount: Decimal,
) -> None:
    atm_fee_perc = Decimal(2)
    atm_fee_without_gst = mul(atm_transactions_amount / 100, atm_fee_perc)

    event = LedgerTriggerEvent(name="atm_fee_added", card_id=bill.table.card_id, post_date=post_date)
    session.add(event)
    session.flush()

    fee = create_fee_entry(session, bill, event, "atm_fee", atm_fee_without_gst)
    event.amount = fee.gross_amount
