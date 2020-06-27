from pendulum import DateTime
from sqlalchemy.orm import Session

from rush.ledger_events import add_min_amount_event
from rush.ledger_utils import get_all_unpaid_bills
from rush.models import (
    LedgerTriggerEvent,
    UserCard,
)
from rush.utils import mul, div


def add_min_to_all_bills(session: Session, post_date: DateTime, user_card: UserCard) -> None:
    unpaid_bills = get_all_unpaid_bills(session, user_card.user_id)
    min_event = LedgerTriggerEvent(name="min_amount_added", post_date=post_date)
    session.add(min_event)
    session.flush()
    for bill in unpaid_bills:
        # TODO get tenure from loan table.
        interest_on_principal = mul(bill.principal, div(div(bill.rc_rate_of_interest_annual, 12), 100))
        min_amount = bill.principal_instalment + interest_on_principal

        add_min_amount_event(session, bill, min_event, min_amount)
        min_event.amount += min_amount
