from decimal import Decimal

from pendulum import parse as parse_date  # type: ignore
from sqlalchemy.orm import Session

from rush.card import (
    create_user_product,
    get_user_product,
)
from rush.card.reset_card import ResetCard
from rush.card.utils import create_user_product_mapping
from rush.ledger_utils import get_account_balance_from_str
from rush.models import (
    CardEmis,
    Lenders,
    LoanData,
    Product,
    User,
)


def create_lenders(session: Session) -> None:
    redux = Lenders(id=1756833, performed_by=123, lender_name="Redux")
    session.add(redux)
    session.flush()


def create_products(session: Session) -> None:
    hc_product = Product(product_name="term_loan_reset")
    session.add(hc_product)
    session.flush()


def create_user(session: Session) -> None:
    u = User(
        id=6,
        performed_by=123,
    )
    session.add(u)
    session.flush()


def create_test_term_loan(session: Session, **kwargs) -> ResetCard:  # type: ignore
    user_product_id = kwargs.get("user_product_id")
    date_str = kwargs["date_str"]
    loan = create_user_product(
        session=session,
        user_id=6,
        card_type="term_loan_reset",
        lender_id=1756833,
        interest_free_period_in_days=15,
        tenure=12,
        amount=Decimal(10000),
        product_order_date=parse_date(date_str).date(),
        user_product_id=user_product_id,
        downpayment_percent=Decimal("0"),
    )

    return loan


def test_product_amortization_1() -> None:
    amortization_date = ResetCard.calculate_amortization_date(
        product_order_date=parse_date("2020-08-01").date()
    )
    assert amortization_date == parse_date("2020-09-01").date()


def test_create_term_loan(session: Session) -> None:
    create_lenders(session=session)
    create_products(session=session)
    create_user(session=session)

    user_product = create_user_product_mapping(
        session=session, user_id=6, product_type="term_loan_reset"
    )

    loan_creation_data = {"date_str": "2020-08-01", "user_product_id": user_product.id}

    # create loan
    loan = create_test_term_loan(session=session, **loan_creation_data)

    _, rc_cash_balance = get_account_balance_from_str(
        session=session, book_string=f"12345/redcarpet/rc_cash/a"
    )
    assert rc_cash_balance == Decimal("-10000")

    assert loan.product_type == "term_loan_reset"
    assert loan.amortization_date == parse_date("2020-09-01").date()

    user_loan = get_user_product(
        session=session, user_id=loan.user_id, card_type="term_loan_reset", loan_id=loan.id
    )
    assert isinstance(user_loan, ResetCard) == True

    loan_data = session.query(LoanData).filter(LoanData.loan_id == user_loan.loan_id).one()

    assert loan_data.bill_start_date == parse_date("2020-09-01").date()
    assert loan_data.bill_close_date == parse_date("2021-08-01").date()
    assert loan_data.principal_instalment == Decimal("833.33")

    _, principal_receivable = get_account_balance_from_str(
        session=session, book_string=f"{loan_data.id}/bill/principal_receivable/a"
    )
    assert principal_receivable == Decimal("10000")

    _, loan_lender_payable = get_account_balance_from_str(
        session=session, book_string=f"{loan.loan_id}/loan/lender_payable/l"
    )
    assert loan_lender_payable == Decimal("10000")

    all_emis_query = (
        session.query(CardEmis)
        .filter(
            CardEmis.loan_id == user_loan.loan_id,
            CardEmis.row_status == "active",
            CardEmis.bill_id == None,
        )
        .order_by(CardEmis.emi_number.asc())
    )
    emis_dict = [u.as_dict() for u in all_emis_query.all()]

    assert len(emis_dict) == 12
    assert emis_dict[0]["due_date"] == parse_date("2020-09-01").date()
    assert emis_dict[0]["emi_number"] == 1
    assert emis_dict[0]["interest"] == Decimal("306.67")
    assert emis_dict[0]["total_due_amount"] % 10 == 0
    assert emis_dict[0]["total_due_amount"] == Decimal("1140")

    assert emis_dict[-1]["due_date"] == parse_date("2021-08-01").date()
    assert emis_dict[-1]["emi_number"] == 12
    assert emis_dict[-1]["interest"] == Decimal("306.67")
    assert emis_dict[-1]["total_due_amount"] % 10 == 0
