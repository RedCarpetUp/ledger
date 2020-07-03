from pendulum import DateTime
from sqlalchemy.orm import Session

from rush.card import BaseCard
from rush.ledger_events import add_min_amount_event
from rush.models import LedgerTriggerEvent


def add_min_to_all_bills(session: Session, post_date: DateTime, user_card: BaseCard) -> None:
    unpaid_bills = user_card.get_unpaid_bills()
    min_event = LedgerTriggerEvent(
        name="min_amount_added", card_id=user_card.id, post_date=post_date, amount=0
    )
    session.add(min_event)
    session.flush()
    for bill in unpaid_bills:
        min_amount = bill.get_min_per_month()
        add_min_amount_event(session, bill, min_event, min_amount)
        min_event.amount += min_amount
