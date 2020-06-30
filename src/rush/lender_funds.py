from decimal import Decimal

from pendulum import DateTime
from sqlalchemy.orm.session import Session

from rush.ledger_events import (
    get_account_balance_from_str,
    lender_disbursal_event,
    lender_interest_incur_event,
    m2p_transfer_event,
)
from rush.models import LedgerTriggerEvent
from rush.utils import get_current_ist_time


def lender_disbursal(session: Session, amount: Decimal) -> Decimal:
    lt = LedgerTriggerEvent(name="lender_disbursal", amount=amount, post_date=get_current_ist_time())
    session.add(lt)
    session.flush()
    lender_disbursal_event(session, lt)
    _, lender_capital = get_account_balance_from_str(session, "62311/lender/lender_capital/l")
    return lender_capital


def m2p_transfer(session: Session, amount: Decimal) -> Decimal:
    lt = LedgerTriggerEvent(name="m2p_transfer", amount=amount, post_date=get_current_ist_time())
    session.add(lt)
    session.flush()
    m2p_transfer_event(session, lt)
    _, lender_pool = get_account_balance_from_str(session, "62311/lender/pool_balance/a")
    return lender_pool


def lender_interest_incur(session: Session) -> bool:
    lt = LedgerTriggerEvent(name="lender_interest_incur", post_date=DateTime(2020, 6, 27))
    session.add(lt)
    session.flush()
    lender_interest_incur_event(session, lt)
    return True
