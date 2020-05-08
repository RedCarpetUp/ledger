from decimal import Decimal

from pendulum import DateTime
from sqlalchemy.orm import Session

from rush.ledger_events import payment_received_event
from rush.models import LedgerTriggerEvent, LoanData


def payment_received(
    session: Session, user_id: int, payment_amount: Decimal, payment_date: DateTime
) -> LoanData:
    # TODO write the proper logic to figure the bill.
    # Currently getting the latest bill.
    bill = (
        session.query(LoanData)
        .filter(LoanData.user_id == user_id)
        .order_by(LoanData.agreement_date.desc())
        .first()
    )
    lt = LedgerTriggerEvent(name="bill_close", amount=payment_amount, post_date=payment_date)
    session.add(lt)
    session.flush()

    payment_received_event(session, bill, lt)
    return bill  # This doesn't make sense but :shrug:
