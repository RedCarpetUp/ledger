from decimal import Decimal

from pendulum import DateTime
from sqlalchemy.orm import Session

from rush.anomaly_detection import run_anomaly
from rush.ledger_events import payment_received_event
from rush.models import (
    LedgerTriggerEvent,
    UserCard,
)


def payment_received(
    session: Session, user_card: UserCard, payment_amount: Decimal, payment_date: DateTime
) -> None:
    lt = LedgerTriggerEvent(
        name="payment_received", card_id=user_card.id, amount=payment_amount, post_date=payment_date
    )
    session.add(lt)
    session.flush()
    payment_received_event(session, user_card, lt)
    run_anomaly(session, user_card, payment_date)
