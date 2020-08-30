from decimal import Decimal

from pendulum import parse as parse_date  # type: ignore
from sqlalchemy.orm import Session

from rush.card import (
    create_user_product,
    get_user_product,
)
from rush.card.term_loan_pro import TermLoanPro
from rush.models import (
    CardEmis,
    Lenders,
    LoanData,
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
    hc_product = Product(product_name="term_loan_pro")
    session.add(hc_product)
    session.flush()


def create_user(session: Session) -> None:
    u = User(
        id=4,
        performed_by=123,
    )
    session.add(u)
    session.flush()


def create_test_term_loan(session: Session, date_str: str) -> TermLoanPro:
    loan = create_user_product(
        session=session,
        user_id=4,
        card_type="term_loan_pro",
        lender_id=62311,
        interest_free_period_in_days=15,
        tenure=12,
        amount=Decimal(10000),
        product_order_date=parse_date(date_str).date(),
    )

    return loan


def test_product_amortization_1() -> None:
    amortization_date = TermLoanPro.calculate_amortization_date(
        product_order_date=parse_date("2020-08-01").date()
    )
    assert amortization_date == parse_date("2020-09-01").date()


def test_product_amortization_2() -> None:
    amortization_date = TermLoanPro.calculate_amortization_date(
        product_order_date=parse_date("2020-08-05").date()
    )
    assert amortization_date == parse_date("2020-09-05").date()


def test_product_amortization_3() -> None:
    amortization_date = TermLoanPro.calculate_amortization_date(
        product_order_date=parse_date("2020-08-12").date()
    )
    assert amortization_date == parse_date("2020-09-12").date()


def test_product_amortization_4() -> None:
    amortization_date = TermLoanPro.calculate_amortization_date(
        product_order_date=parse_date("2020-08-15").date()
    )
    assert amortization_date == parse_date("2020-09-15").date()


def test_product_amortization_5() -> None:
    amortization_date = TermLoanPro.calculate_amortization_date(
        product_order_date=parse_date("2020-08-24").date()
    )
    assert amortization_date == parse_date("2020-09-24").date()


def test_product_amortization_6() -> None:
    amortization_date = TermLoanPro.calculate_amortization_date(
        product_order_date=parse_date("2020-08-26").date()
    )
    assert amortization_date == parse_date("2020-09-26").date()


def test_calculate_downpayment_amount() -> None:
    downpayment_amount = TermLoanPro.calculate_downpayment_amount(
        product_price=Decimal(10000), tenure=12
    )
    assert downpayment_amount == Decimal("2000")


def test_create_term_loan(session: Session) -> None:
    create_lenders(session=session)
    create_products(session=session)
    create_user(session=session)
    loan = create_test_term_loan(session=session, date_str="2020-08-01")

    assert loan.product_type == "term_loan_pro"
    assert loan.amortization_date == parse_date("2020-09-01").date()

    user_loan = get_user_product(session=session, user_id=loan.user_id, card_type="term_loan_pro")
    assert isinstance(user_loan, TermLoanPro) == True

    loan_data = session.query(LoanData).filter(LoanData.loan_id == user_loan.loan_id).one()

    assert loan_data.bill_start_date == parse_date("2020-09-01").date()
    assert loan_data.bill_close_date == parse_date("2021-08-16").date()

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

    assert emis_dict[-1]["due_date"] == parse_date("2021-08-01").date()
    assert emis_dict[-1]["emi_number"] == 12
    assert emis_dict[-1]["interest"] == Decimal("306.67")
    assert emis_dict[-1]["total_due_amount"] % 10 == 0


def test_create_term_loan_2(session: Session) -> None:
    create_lenders(session=session)
    create_products(session=session)
    create_user(session=session)
    loan = create_test_term_loan(session=session, date_str="2018-10-22")

    assert loan.product_type == "term_loan_pro"
    assert loan.amortization_date == parse_date("2018-11-22").date()

    user_loan = get_user_product(session=session, user_id=loan.user_id, card_type="term_loan_pro")
    assert isinstance(user_loan, TermLoanPro) == True

    loan_data = session.query(LoanData).filter(LoanData.loan_id == user_loan.loan_id).one()

    assert loan_data.bill_start_date == parse_date("2018-11-22").date()
    assert loan_data.bill_close_date == parse_date("2019-11-06").date()

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
    assert emis_dict[0]["due_date"] == parse_date("2018-11-22").date()
    assert emis_dict[0]["emi_number"] == 1
    assert emis_dict[0]["interest"] == Decimal("306.67")
    assert emis_dict[0]["total_due_amount"] % 10 == 0

    assert emis_dict[1]["due_date"] == parse_date("2018-12-22").date()
    assert emis_dict[1]["emi_number"] == 2
    assert emis_dict[1]["interest"] == Decimal("306.67")
    assert emis_dict[1]["total_due_amount"] % 10 == 0

    assert emis_dict[-1]["due_date"] == parse_date("2019-10-22").date()
    assert emis_dict[-1]["emi_number"] == 12
    assert emis_dict[-1]["interest"] == Decimal("306.67")
    assert emis_dict[-1]["total_due_amount"] % 10 == 0
