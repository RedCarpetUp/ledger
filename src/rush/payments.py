from decimal import Decimal

from pendulum import DateTime
from sqlalchemy.orm import Session

from rush.anomaly_detection import run_anomaly
from rush.card import (
    BaseCard,
    get_user_card,
)
from rush.ledger_events import (
    payment_received_event,
    refund_event,
)
from rush.models import (
    CardTransaction,
    LedgerTriggerEvent,
    LoanData,
)
from rush.utils import get_current_ist_time


def payment_received(
    session: Session,
    user_card: BaseCard,
    payment_amount: Decimal,
    payment_date: DateTime,
    payment_request_id: str,
    payment_type: str = "loan payment",
) -> None:
    lt = LedgerTriggerEvent(
        name="payment_received",
        card_id=user_card.id,
        amount=payment_amount,
        post_date=payment_date,
        extra_details={
            "payment_request_id": payment_request_id,
            "gateway_charges": 0.5,
            "payment_type": payment_type,
        },
    )
    session.add(lt)
    session.flush()
    lender_id = (
        session.query(LoanData.lender_id).filter(LoanData.card_id == user_card.id).limit(1).scalar() or 0
    )
    payment_received_event(session, user_card, f"{lender_id}/lender/pg_account/a", lt, payment_type)
    run_anomaly(session, user_card, payment_date)


def refund_payment(session, user_id: int, bill_id: int, refund_request_id: str) -> bool:

    bill = (
        session.query(CardTransaction.amount)
        .join(LoanData, CardTransaction.loan_id == LoanData.id)
        .filter(LoanData.id == bill_id)
        .first()
    )
    user_card = get_user_card(session, user_id)
    amount = Decimal(bill.amount)
    lt = LedgerTriggerEvent(
        name="merchant_refund",
        amount=amount,
        post_date=get_current_ist_time(),
        extra_details={"payment_request_id": refund_request_id},
    )
    session.add(lt)
    session.flush()
    current_bill = session.query(LoanData).filter(LoanData.id == bill_id).one()
    refund_event(session, current_bill, user_card, lt)
    return True
