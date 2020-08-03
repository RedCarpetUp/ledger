from decimal import Decimal

from pendulum import (
    Date,
    DateTime,
)
from sqlalchemy.orm.session import Session

from rush.ledger_events import (
    get_account_balance_from_str,
    lender_disbursal_event,
    lender_interest_incur_event,
    m2p_transfer_event,
)
from rush.models import LedgerTriggerEvent, Lenders
from rush.utils import get_current_ist_time


def lender_disbursal(session: Session, amount: Decimal, lender_id: int) -> Decimal:
    if verify_lender(session, lender_id):
        lt = LedgerTriggerEvent(
            name="lender_disbursal",
            amount=amount,
            post_date=get_current_ist_time(),
            extra_details={"lender_id": lender_id},
        )
        session.add(lt)
        session.flush()
        lender_disbursal_event(session, lt, lender_id)
        _, lender_capital = get_account_balance_from_str(session, f"{lender_id}/lender/lender_capital/l")
        return lender_capital
    return {"result": "error", "message": "Invalid lender"}


def m2p_transfer(session: Session, amount: Decimal, lender_id: int) -> Decimal:
    if verify_lender(session, lender_id):
        lt = LedgerTriggerEvent(
            name="m2p_transfer",
            amount=amount,
            post_date=get_current_ist_time(),
            extra_details={"lender_id": lender_id},
        )
        session.add(lt)
        session.flush()
        m2p_transfer_event(session, lt, lender_id)
        _, lender_pool = get_account_balance_from_str(session, f"{lender_id}/lender/pool_balance/a")
        return lender_pool
    return {"result": "error", "message": "Invalid lender"}


def lender_interest_incur(session: Session, from_date: Date, to_date: Date) -> bool:
    lt = LedgerTriggerEvent(name="incur_lender_interest", post_date=to_date, amount=0)
    session.add(lt)
    session.flush()
    lender_interest_incur_event(session, from_date, to_date, lt)
    return True


def verify_lender(session: Session, lender_id: int) -> bool:
    lender = (
        session.query(Lenders).filter(Lenders.id == lender_id, Lenders.row_status == "active").first()
    )
    return True if lender else False
