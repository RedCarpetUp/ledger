from pendulum import DateTime
from sqlalchemy.orm import Session

from rush.card.base_card import BaseLoan
from rush.ledger_events import add_min_amount_event
from rush.models import LedgerTriggerEvent


def add_min_to_all_bills(session: Session, post_date: DateTime, user_loan: BaseLoan) -> None:
    unpaid_bills = user_loan.get_unpaid_generated_bills()
    min_event = LedgerTriggerEvent(
        name="min_amount_added", loan_id=user_loan.loan_id, post_date=post_date, amount=0
    )
    session.add(min_event)
    session.flush()
    for bill in unpaid_bills:
        min_amount = bill.get_min_amount_to_add()
        if min_amount == 0:
            continue
        add_min_amount_event(session, bill, min_event, min_amount)
        min_event.amount += min_amount
