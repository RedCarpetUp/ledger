from decimal import Decimal
from typing import Dict

from pendulum import DateTime
from sqlalchemy.orm.session import Session

from rush.card import BaseCard
from rush.create_bill import get_or_create_bill_for_card_swipe
from rush.ledger_events import card_transaction_event
from rush.models import (
    CardTransaction,
    LedgerTriggerEvent,
    MerchantInterest,
)
from rush.utils import (
    div,
    mul,
)


def create_card_swipe(
    session: Session,
    user_card: BaseCard,
    txn_time: DateTime,
    amount: Decimal,
    description: str,
    merchant_id: str = "rc1",
) -> Dict:
    if not hasattr(user_card, "card_activation_date"):
        return {"result": "error", "message": "Card has not been activated"}
    if txn_time.date() < user_card.card_activation_date:
        return {"result": "error", "message": "Transaction cannot happen before activation"}
    card_bill = get_or_create_bill_for_card_swipe(user_card, txn_time)
    if card_bill["result"] == "error":
        return card_bill
    card_bill = card_bill["bill"]
    roi = (
        session.query(MerchantInterest.interest_rate)
        .filter(
            MerchantInterest.merchant_id == merchant_id,
            MerchantInterest.product_id == user_card.product_id,
        )
        .one_or_none()
    )
    if not roi:
        roi = user_card.rc_rate_of_interest_monthly
    interest_to_be_charged = mul(amount, div(roi, 100))
    swipe = CardTransaction(  # This can be moved to user card too.
        loan_id=card_bill.id,
        txn_time=txn_time,
        amount=amount,
        description=description,
        merchant_id=merchant_id,
        interest=interest_to_be_charged,
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
