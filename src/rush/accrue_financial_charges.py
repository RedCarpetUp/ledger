from sqlalchemy.orm import Session

from rush.ledger_events import (
    accrue_interest_event,
    accrue_late_fine_event,
)
from rush.ledger_utils import (
    is_bill_closed,
    is_min_paid,
)
from rush.models import (
    LedgerTriggerEvent,
    LoanData,
)
from rush.utils import get_current_ist_time


def accrue_interest(session: Session, user_id: int) -> LoanData:
    bill = (
        session.query(LoanData)
        .filter(LoanData.user_id == user_id)
        .order_by(LoanData.agreement_date.desc())
        .first()
    )  # Get the latest bill of that user.
    is_closed = is_bill_closed(session, bill)
    if not is_closed:  # if bill isn't paid fully accrue interest.
        # TODO get correct date here.
        lt = LedgerTriggerEvent(name="accrue_interest", post_date=get_current_ist_time())
        session.add(lt)
        session.flush()

        accrue_interest_event(session, bill, lt)
    return bill


def accrue_late_charges(session: Session, user_id: int) -> LoanData:
    bill = (
        session.query(LoanData)
        .filter(LoanData.user_id == user_id)
        .order_by(LoanData.agreement_date.desc())
        .first()
    )  # Get the latest bill of that user.
    is_paid = is_min_paid(session, bill)
    if not is_paid:  # if min isn't paid charge late fine.
        # TODO get correct date here.
        lt = LedgerTriggerEvent(name="accrue_late_fine", post_date=get_current_ist_time())
        session.add(lt)
        session.flush()

        accrue_late_fine_event(session, bill, lt)
    return bill
