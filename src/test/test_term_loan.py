from decimal import Decimal

from pendulum import parse as parse_date  # type: ignore
from sqlalchemy.orm import Session

from rush.card import (
    create_user_product,
    get_user_product,
)
from rush.card.term_loan import TermLoan
from rush.models import (
    CardEmis,
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
        bill_close_date=parse_date("2021-08-01").date(),
        interest_free_period_in_days=15,
        tenure=12,
        amount=Decimal(10000),
        product_order_date=parse_date("2020-08-01").date(),
    )

    return loan


def test_create_term_loan(session: Session) -> None:
    create_lenders(session=session)
    create_products(session=session)
    create_user(session=session)
    loan = create_test_term_loan(session=session)

    assert loan.product_type == "term_loan"
    assert loan.amortization_date == parse_date("2020-08-01").date()

    user_card = get_user_product(session=session, user_id=loan.user_id, card_type="term_loan")
    assert isinstance(user_card, TermLoan) == True

    all_emis_query = (
        session.query(CardEmis)
        .filter(CardEmis.loan_id == user_card.loan_id, CardEmis.row_status == "active")
        .order_by(CardEmis.emi_number.asc())
    )
    emis_dict = [u.as_dict() for u in all_emis_query.all()]

    for emi in emis_dict:
        print(emi["emi_number"], emi["due_date"], emi["total_due_amount"], emi["interest"])

    assert len(emis_dict) == 13
