from decimal import Decimal

from sqlalchemy.orm import Session

from rush.models import LoanData
from rush.utils import (
    div,
    mul,
)


def lender_interest(session: Session, amount: Decimal, card_id: int) -> Decimal:
    lender_interest_rate = (
        session.query(LoanData.lender_rate_of_interest_annual)
        .filter(LoanData.card_id == card_id)
        .limit(1)
        .scalar()
        or 0
    )
    amount = div(mul(lender_interest_rate, amount), 36500)
    return amount
