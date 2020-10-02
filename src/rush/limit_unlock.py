from decimal import Decimal
from typing import Optional

from pendulum import Date
from sqlalchemy.orm import Session

from rush.ledger_events import limit_unlock_event
from rush.ledger_utils import get_account_balance_from_str
from rush.models import (
    LedgerTriggerEvent,
    Loan,
)
from rush.utils import get_current_ist_time


def limit_unlock(
    session: Session,
    loan: Loan,
    amount: Decimal,
    event_date: Optional[Date] = None,
    locked_limit_str: str = "locked_limit",
    unlock_limit_str: str = "available_limit",
) -> None:
    # incase extra limit is unlocked, raise error.
    _, locked_limit = get_account_balance_from_str(
        session=session, book_string=f"{loan.id}/card/{locked_limit_str}/l"
    )
    assert locked_limit >= amount

    post_date = get_current_ist_time().date() if not event_date else event_date
    event = LedgerTriggerEvent(
        performed_by=loan.user_id,
        name="limit_unlock_event",
        loan_id=loan.id,
        post_date=post_date,
        amount=amount,
    )
    session.add(event)
    session.flush()

    limit_unlock_event(
        session=session,
        loan=loan,
        event=event,
        amount=amount,
        locked_limit_str=locked_limit_str,
        unlock_limit_str=unlock_limit_str,
    )
