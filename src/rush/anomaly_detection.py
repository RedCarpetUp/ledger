from typing import List

from pendulum import DateTime
from sqlalchemy import func
from sqlalchemy.orm import Session

from rush.accrue_financial_charges import (
    can_remove_interest,
    reverse_interest_charges,
)
from rush.models import (
    LedgerTriggerEvent,
    UserCard,
)


def get_affected_events(session: Session, user_card: UserCard) -> List[LedgerTriggerEvent]:
    rank_func = (
        func.rank()
        .over(order_by=LedgerTriggerEvent.post_date.desc(), partition_by=LedgerTriggerEvent.name)
        .label("rnk")
    )
    events = (
        session.query(LedgerTriggerEvent, rank_func)
        .filter(
            LedgerTriggerEvent.card_id == user_card.id,
            # These are the only events which can be affected by a payment.
            LedgerTriggerEvent.name.in_(["accrue_interest", "accrue_late_fine"]),
        )
        .from_self(LedgerTriggerEvent)
        .filter(rank_func == 1)
        .all()
    )
    return events


def run_anomaly(session: Session, user_card: UserCard, event_date: DateTime) -> None:
    events = get_affected_events(session, user_card)
    for event in events:
        if event.name == "accrue_interest":
            if can_remove_interest(session, user_card, event, event_date):
                reverse_interest_charges(session, event, user_card, event_date)
        elif event.name == "accrue_late_fine":
            pass
            # do_prerequisites_meet = accrue_late_charges_prerequisites(
            #     session, bill, event_date=event.post_date
            # )
            # if not do_prerequisites_meet:
            #     reverse_late_charges(session, bill, event)
