from decimal import Decimal

from pendulum import parse as parse_date  # type: ignore
from sqlalchemy.orm import Session

from rush.card import (
    create_user_product,
    get_user_product,
)
from rush.card.term_loan import TermLoan
from rush.models import (
    Lenders,
    Product,
    User,
)


def create_lenders(session: Session) -> None:
    dmi = Lenders(id=62311, performed_by=123, lender_name="DMI")
    session.add(dmi)

    redux = Lenders(id=1756833, performed_by=123, lender_name="Redux")
    session.add(redux)
    session.flush()


def create_products(session: Session) -> None:
    hc_product = Product(product_name="term_loan")
    session.add(hc_product)
    session.flush()


def create_user(session: Session) -> None:
    u = User(id=4, performed_by=123,)
    session.add(u)
    session.flush()


def create_test_term_loan(session: Session) -> TermLoan:
    loan = create_user_product(
        session=session,
        user_id=4,
        loan_creation_date=parse_date("2020-08-01").date(),
        card_type="term_loan",
        lender_id=62311,
        bill_start_date=parse_date("2020-08-01").date(),
        bill_close_date=parse_date("2020-08-01").date(),
        interest_free_period_in_days=15,
        tenure=12,
        amount=Decimal(10000),
    )

    return loan


def test_create_term_loan(session: Session) -> None:
    create_lenders(session=session)
    create_products(session=session)
    create_user(session=session)
    loan = create_test_term_loan(session=session)

    assert loan.product_type == "term_loan"

    user_card = get_user_product(session=session, user_id=loan.user_id, card_type="term_loan")
    assert isinstance(user_card, TermLoan) == True
