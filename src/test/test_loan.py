from decimal import Decimal

from pendulum import parse as parse_date  # type: ignore
from sqlalchemy.orm import Session

from rush.models import (
    Lenders,
    LoanData,
    Product,
    User,
)
from rush.term_loan import create_term_loan, get_term_loan
from rush.term_loan.base_loan import BaseLoan


def create_products(session: Session) -> None:
    tl_product = Product(product_name="tenure_loan")
    session.add(tl_product)
    session.flush()

def create_lenders(session: Session) -> None:
    dmi = Lenders(id=62311, performed_by=123, lender_name="DMI")
    session.add(dmi)

    redux = Lenders(id=1756833, performed_by=123, lender_name="Redux")
    session.add(redux)
    session.flush()


def test_term_loan(session: Session) -> None:
    user = User(id=4, performed_by=123,)
    session.add(user)
    session.flush()

    create_products(session=session)
    create_lenders(session=session)

    loan = create_term_loan(
        session=session,
        loan_class=BaseLoan,
        product_type="tenure_loan",
        user_id=user.id,
        bill_start_date=parse_date("2020-01-01 14:23:11"),
        bill_close_date=parse_date("2021-01-01 14:23:11"),
        lender_id=62311,
        amount=Decimal("10000.00"),
        tenure=12,
        interest_free_period_in_days=0,
    )

    assert isinstance(loan, LoanData) == True


def test_get_term_loan(session: Session) -> None:
    user = User(id=4, performed_by=123,)
    session.add(user)
    session.flush()

    create_products(session=session)
    create_lenders(session=session)

    loan = create_term_loan(
        session=session,
        loan_class=BaseLoan,
        product_type="tenure_loan",
        user_id=user.id,
        bill_start_date=parse_date("2020-01-01 14:23:11"),
        bill_close_date=parse_date("2021-01-01 14:23:11"),
        lender_id=62311,
        amount=Decimal("10000.00"),
        tenure=12,
        interest_free_period_in_days=0,
    )

    assert isinstance(loan, LoanData) == True

    loan = get_term_loan(session=session, user_id=user.id, product_type="tenure_loan")
    assert isinstance(loan, LoanData) == True
    assert loan.loan_id is not None
