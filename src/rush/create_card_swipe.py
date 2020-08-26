from decimal import Decimal
from typing import (
    Any,
    Dict,
    Optional,
)

from pendulum import DateTime
from sqlalchemy.orm.session import Session

from rush.card.base_card import BaseLoan
from rush.card.ruby_card import RubyCard
from rush.create_bill import get_or_create_bill_for_card_swipe
from rush.ledger_events import (
    card_transaction_event,
    disburse_money_to_card,
)
from rush.models import (
    CardTransaction,
    LedgerTriggerEvent,
)


def create_card_swipe(
    session: Session,
    user_card: BaseLoan,
    txn_time: DateTime,
    amount: Decimal,
    description: str,
    source: Optional[str] = "ECOM",
    mcc: Optional[str] = None,
) -> Dict[str, Any]:
    if not hasattr(user_card, "amortization_date") or not user_card.amortization_date:
        return {"result": "error", "message": "Card has not been activated"}

    if txn_time.date() < user_card.amortization_date:
        return {"result": "error", "message": "Transaction cannot happen before activation"}
    card_bill = get_or_create_bill_for_card_swipe(user_card, txn_time)
    if card_bill["result"] == "error":
        return card_bill
    card_bill = card_bill["bill"]
    swipe = CardTransaction(  # This can be moved to user card too.
        loan_id=card_bill.id,
        txn_time=txn_time,
        amount=amount,
        description=description,
        source=source,
        mcc=mcc,
    )
    session.add(swipe)
    session.flush()

    lt = LedgerTriggerEvent(
        performed_by=user_card.user_id,
        name="card_transaction",
        loan_id=user_card.loan_id,
        post_date=txn_time,
        amount=amount,
        extra_details={"swipe_id": swipe.id},
    )
    session.add(lt)
    session.flush()  # need id. TODO Gotta use table relationships

    if isinstance(user_card, RubyCard):  # Need to load card balance at every swipe.
        disburse_money_to_card(session, user_card, lt)

    card_transaction_event(session=session, user_card=user_card, event=lt, mcc=mcc)
    return {"result": "success", "data": swipe}
