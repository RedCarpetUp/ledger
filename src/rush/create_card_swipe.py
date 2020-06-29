from decimal import Decimal

from pendulum import DateTime
from sqlalchemy.orm.session import Session

from rush.create_bill import get_or_create_bill_for_card_swipe
from rush.ledger_events import card_transaction_event
from rush.models import (
    CardTransaction,
    LedgerTriggerEvent,
    UserCard,
)


def create_card_swipe(
    session: Session, user_card: UserCard, txn_time: DateTime, amount: Decimal, description: str
) -> CardTransaction:
    card_bill = get_or_create_bill_for_card_swipe(session, user_card, txn_time)
    swipe = CardTransaction(
        loan_id=card_bill.id, txn_time=txn_time, amount=amount, description=description
    )
    session.add(swipe)
    session.flush()

    lt = LedgerTriggerEvent(
        performed_by=user_card.user_id,
        name="card_transaction",
        card_id=user_card.id,
        post_date=txn_time,
        amount=amount,
        extra_details={"swipe_id": swipe.id},
    )
    session.add(lt)
    session.flush()  # need id. TODO Gotta use table relationships
    card_transaction_event(session, user_card, lt)
    return swipe
