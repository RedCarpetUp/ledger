from decimal import Decimal

from pendulum import DateTime
from sqlalchemy.orm import Session

from rush.ledger_events import (
    payment_received_event,
    refund_or_prepayment_event,
)
from rush.ledger_utils import get_all_unpaid_bills
from rush.models import (
    CardTransaction,
    LedgerTriggerEvent,
    LoanData,
    User,
)
from rush.utils import get_current_ist_time


def payment_received(
    session: Session, user_id: int, payment_amount: Decimal, payment_date: DateTime
) -> LoanData:
    # TODO write the proper logic to figure the bill.

    unpaid_bills = get_all_unpaid_bills(session, user_id)
    # assert len(unpaid_bills) == 2
    lt = LedgerTriggerEvent(name="bill_close", amount=payment_amount, post_date=payment_date)
    session.add(lt)
    session.flush()
    unpaid_bills.reverse()
    payment_received_event(session, unpaid_bills, lt)

    return unpaid_bills.pop(0)  # This doesn't make sense but :shrug:


def refund_payment(session, user_id: int, bill_id: int) -> bool:

    bill = (
        session.query(CardTransaction.amount)
        .join(LoanData, CardTransaction.loan_id == LoanData.id)
        .filter(LoanData.id == bill_id)
        .first()
    )
    amount = Decimal(bill.amount)
    lt = LedgerTriggerEvent(name="refund_bill", amount=amount, post_date=get_current_ist_time())
    session.add(lt)
    session.flush()
    refund_or_prepayment_event(session, f"after", bill_id, lt)
    return True
