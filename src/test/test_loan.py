from decimal import Decimal

from dateutil.relativedelta import relativedelta
from pendulum import parse as parse_date  # type: ignore
from sqlalchemy.orm import (
    Session,
    session,
)

from rush.accrue_financial_charges import accrue_interest_on_all_bills
from rush.card import (
    BaseCard,
    create_user_card,
)
from rush.create_bill import bill_generate
from rush.create_card_swipe import create_card_swipe
from rush.ledger_utils import get_account_balance_from_str
from rush.lender_funds import lender_interest_incur
from rush.models import (
    CardKitNumbers,
    CardNames,
    Lenders,
    User,
)
from rush.payments import payment_received
from rush.recon.revenue_earned import get_revenue_earned_in_a_period
from rush.termloan import create_user_term_loan
from rush.utils import get_current_ist_time


def test_lenders(session: Session) -> None:
    l1 = Lenders(id=62311, performed_by=123, lender_name="DMI")
    session.add(l1)
    l2 = Lenders(id=1756833, performed_by=123, lender_name="Redux")
    session.add(l2)
    session.flush()
    a = session.query(Lenders).first()


def tets_term_loan(session: Session) -> None:
    u = User(id=2, performed_by=123,)
    session.add(u)
    session.commit()
    a = session.query(User).first()
    loan=create_user_term_loan(
        session=session,
        user_id=a.id,
        bill_start_date=parse_date("2020-01-01 14:23:11"),
        bill_close_date=parse_date("2021-01-01 14:23:11"),
        lender_id=62311,
        amount=Decimal("10000.00"),
        tenure=12,
        interest_free_period_in_days=0,
    )
