from sqlalchemy.orm import Session

from rush.ledger_events import accrue_interest_event
from rush.ledger_utils import is_bill_closed
from rush.models import LoanData, LedgerTriggerEvent
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
        lt = LedgerTriggerEvent(name="bill_generate", post_date=get_current_ist_time())
        session.add(lt)
        session.flush()

        accrue_interest_event(session, bill, lt)
    return bill
