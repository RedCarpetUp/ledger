from pendulum import DateTime
from sqlalchemy.orm import Session

from rush.card import BaseCard
from rush.ledger_events import add_min_amount_event
from rush.ledger_utils import get_remaining_bill_balance
from rush.models import LedgerTriggerEvent


def add_min_to_all_bills(session: Session, post_date: DateTime, user_card: BaseCard) -> None:
    unpaid_bills = user_card.get_unpaid_bills()
    min_event = LedgerTriggerEvent(
        name="min_amount_added", loan_id=user_card.loan_id, post_date=post_date, amount=0
    )
    session.add(min_event)
    session.flush()
    for bill in unpaid_bills:
        min_amount = bill.get_min_for_schedule()
        max_remaining_amount = get_remaining_bill_balance(session, bill)["total_due"]
        max_min_amount_to_add = min(min_amount, max_remaining_amount)
        add_min_amount_event(session, bill, min_event, max_min_amount_to_add)
        min_event.amount += max_min_amount_to_add
