from pendulum import DateTime
from sqlalchemy.orm import Session

from rush.card.base_card import BaseLoan
from rush.ledger_events import add_min_amount_event
from rush.ledger_utils import get_remaining_bill_balance
from rush.models import LedgerTriggerEvent


def add_min_to_all_bills(session: Session, post_date: DateTime, user_card: BaseLoan) -> None:
    unpaid_bills = user_card.get_unpaid_bills()
    min_event = LedgerTriggerEvent(
        name="min_amount_added", loan_id=user_card.loan_id, post_date=post_date, amount=0
    )
    session.add(min_event)
    session.flush()
    for bill in unpaid_bills:
        max_remaining_amount = get_remaining_bill_balance(session, bill)["total_due"]
        amount_already_present_in_min = bill.get_remaining_min()
        if amount_already_present_in_min == max_remaining_amount:
            continue

        amount_that_can_be_added_in_min = max_remaining_amount - amount_already_present_in_min
        scheduled_min_amount = bill.get_min_for_schedule()
        min_amount_to_add = min(scheduled_min_amount, amount_that_can_be_added_in_min)

        add_min_amount_event(session, bill, min_event, min_amount_to_add)
        min_event.amount += min_amount_to_add
