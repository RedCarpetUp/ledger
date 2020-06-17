from decimal import Decimal

from pendulum import DateTime
from sqlalchemy.orm import Session

from rush.ledger_events import payment_received_event
from rush.ledger_utils import get_all_unpaid_bills
from rush.models import (
    LedgerTriggerEvent,
    LoanData,
    User,
)


def payment_received(
    session: Session, user_id: int, payment_amount: Decimal, payment_date: DateTime
) -> LoanData:
    # TODO write the proper logic to figure the bill.

    unpaid_bills = get_all_unpaid_bills(session, user_id)
    # assert len(unpaid_bills) == 2
    lt = LedgerTriggerEvent(
        name="bill_close", amount=payment_amount, post_date=payment_date)
    session.add(lt)
    session.flush()
    unpaid_bills.reverse()
    payment_received_event(session, unpaid_bills, lt)

    return unpaid_bills.pop(0)  # This doesn't make sense but :shrug:
