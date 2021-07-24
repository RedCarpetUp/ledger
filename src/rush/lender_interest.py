from decimal import Decimal

from sqlalchemy.orm import Session

from rush.models import LedgerLoanData
from rush.utils import (
    div,
    mul,
)


def lender_interest(session: Session, amount: Decimal, loan_id: int) -> Decimal:
    lender_interest_rate = (
        session.query(LedgerLoanData.lender_rate_of_interest_annual)
        .filter(LedgerLoanData.loan_id == loan_id)
        .limit(1)
        .scalar()
        or 0
    )
    amount = div(mul(lender_interest_rate, amount), 36500)
    return amount
