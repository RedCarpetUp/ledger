from decimal import Decimal

from pendulum import DateTime
from sqlalchemy.orm.session import Session

from rush.ledger_events import card_transaction_event
from rush.models import LedgerTriggerEvent


def lender_disbursal(session: Session, amount: Decimal) -> Decimal:
    generate_date = parse_date("2019-05-01").date()
    lt = LedgerTriggerEvent(post_date=generate_date, amount=amount)
    lender_disbursal_event(session, lt)
    _, lender_capital = get_account_balance_from_str(session, "62311/lender/lender_capital/l")
    return lender_capital


def m2p_transaction(session: Session, amount: Decimal) -> Decimal:
    generate_date = parse_date("2019-05-01").date()
    lt = LedgerTriggerEvent(post_date=generate_date, amount=amount)
    m2p_transaction_event(session, lt)
    _, lender_pool = get_account_balance_from_str(session, "62311/lender/pool_balance/a")
    return lender_pool
