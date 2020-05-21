from typing import Optional

from pendulum import DateTime
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


def accrue_interest_prerequisites(
    session: Session, bill: LoanData, to_date: Optional[DateTime] = None
) -> bool:
    if not is_bill_closed(session, bill, to_date):  # If not closed, we can accrue interest.
        return True
    return False  # prerequisites failed.


def accrue_interest(session: Session, user_id: int) -> LoanData:
    bill = (
        session.query(LoanData)
        .filter(LoanData.user_id == user_id)
        .order_by(LoanData.agreement_date.desc())
        .first()
    )  # Get the latest bill of that user.
    can_charge_interest = accrue_interest_prerequisites(session, bill)
    if can_charge_interest:  # if bill isn't paid fully accrue interest.
        # TODO get correct date here.
        lt = LedgerTriggerEvent(name="accrue_interest", post_date=get_current_ist_time())
        session.add(lt)
        session.flush()

        accrue_interest_event(session, bill, lt)
    return bill


def accrue_late_charges_prerequisites(
    session: Session, bill: LoanData, to_date: Optional[DateTime] = None
) -> bool:
    if not is_min_paid(session, bill, to_date):  # if not paid, we can charge late fee.
        return True
    return False


def accrue_late_charges(session: Session, user_id: int) -> LoanData:
    bill = (
        session.query(LoanData)
        .filter(LoanData.user_id == user_id)
        .order_by(LoanData.agreement_date.desc())
        .first()
    )  # Get the latest bill of that user.
    can_charge_fee = accrue_late_charges_prerequisites(session, bill)
    if can_charge_fee:  # if min isn't paid charge late fine.
        # TODO get correct date here.
        lt = LedgerTriggerEvent(name="accrue_late_fine", post_date=get_current_ist_time())
        session.add(lt)
        session.flush()

        accrue_late_fine_event(session, bill, lt)
    return bill
