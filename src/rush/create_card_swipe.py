from decimal import Decimal

from pendulum import DateTime
from sqlalchemy.orm.session import Session

from rush.card import BaseCard
from rush.create_bill import get_or_create_bill_for_card_swipe
from rush.ledger_events import card_transaction_event
from rush.models import (
    CardTransaction,
    LedgerTriggerEvent,
)


def create_card_swipe(
    session: Session,
    user_card: BaseCard,
    txn_time: DateTime,
    amount: Decimal,
    description: str,
    source: str = "ECOM",
) -> CardTransaction:
    if not hasattr(user_card, "card_activation_date"):
        return {"result": "error", "message": "Card has not been activated"}
    if txn_time.date() < user_card.card_activation_date:
        return {"result": "error", "message": "Transaction cannot happen before activation"}
    card_bill = get_or_create_bill_for_card_swipe(user_card, txn_time)
    if card_bill["result"] == "error":
        return card_bill
    card_bill = card_bill["bill"]
    swipe = CardTransaction(  # This can be moved to user card too.
        loan_id=card_bill.id, txn_time=txn_time, amount=amount, description=description, source=source
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
    return {"result": "success", "data": swipe}
