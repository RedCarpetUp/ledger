from decimal import Decimal

from pendulum import DateTime
from sqlalchemy.orm import Session

from rush.anomaly_detection import run_anomaly
from rush.ledger_events import (
    payment_received_event,
    refund_event,
)
from rush.models import (
    CardTransaction,
    LedgerTriggerEvent,
    LoanData,
    UserCard,
)
from rush.utils import get_current_ist_time


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


def refund_payment(session, user_id: int, bill_id: int) -> bool:

    bill = (
        session.query(CardTransaction.amount)
        .join(LoanData, CardTransaction.loan_id == LoanData.id)
        .filter(LoanData.id == bill_id)
        .first()
    )
    user_card = session.query(UserCard).filter(UserCard.user_id == user_id).one()
    amount = Decimal(bill.amount)
    lt = LedgerTriggerEvent(name="refund_bill", amount=amount, post_date=get_current_ist_time())
    session.add(lt)
    session.flush()
    current_bill = session.query(LoanData).filter(LoanData.id == bill_id).one()
    refund_event(session, current_bill, user_card, lt)
    return True
