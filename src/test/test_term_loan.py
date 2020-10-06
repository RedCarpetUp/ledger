from decimal import Decimal

from pendulum import parse as parse_date  # type: ignore
from sqlalchemy.orm import Session

from rush.card import (
    create_user_product,
    get_user_product,
)
from rush.card.term_loan import TermLoan
from rush.card.utils import create_user_product_mapping
from rush.ledger_utils import get_account_balance_from_str
from rush.models import (
    CardEmis,
    LedgerTriggerEvent,
    Lenders,
    LoanData,
    Product,
    User,
)
from rush.payments import payment_received


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
    u = User(
        id=4,
        performed_by=123,
    )
    session.add(u)
    session.flush()


def create_test_term_loan(session: Session, **kwargs) -> TermLoan:  # type: ignore
    user_product_id = kwargs.get("user_product_id")
    date_str = kwargs["date_str"]
    loan = create_user_product(
        session=session,
        user_id=4,
        card_type="term_loan",
        lender_id=62311,
        interest_free_period_in_days=15,
        tenure=12,
        amount=Decimal(10000),
        product_order_date=parse_date(date_str).date(),
        user_product_id=user_product_id,
        downpayment_percent=Decimal("20"),
    )

    return loan


def test_product_amortization_1() -> None:
    amortization_date = TermLoan.calculate_amortization_date(
        product_order_date=parse_date("2020-08-01").date()
    )
    assert amortization_date == parse_date("2020-08-01").date()


def test_product_amortization_2() -> None:
    amortization_date = TermLoan.calculate_amortization_date(
        product_order_date=parse_date("2020-08-05").date()
    )
    assert amortization_date == parse_date("2020-08-05").date()


def test_product_amortization_3() -> None:
    amortization_date = TermLoan.calculate_amortization_date(
        product_order_date=parse_date("2020-08-12").date()
    )
    assert amortization_date == parse_date("2020-08-12").date()


def test_product_amortization_4() -> None:
    amortization_date = TermLoan.calculate_amortization_date(
        product_order_date=parse_date("2020-08-15").date()
    )
    assert amortization_date == parse_date("2020-08-15").date()


def test_product_amortization_5() -> None:
    amortization_date = TermLoan.calculate_amortization_date(
        product_order_date=parse_date("2020-08-24").date()
    )
    assert amortization_date == parse_date("2020-08-24").date()


def test_product_amortization_6() -> None:
    amortization_date = TermLoan.calculate_amortization_date(
        product_order_date=parse_date("2020-08-26").date()
    )
    assert amortization_date == parse_date("2020-08-26").date()


def test_calculate_downpayment_amount() -> None:
    downpayment_amount = TermLoan.bill_class.calculate_downpayment_amount_payable(
        product_price=Decimal(10000), tenure=12, downpayment_perc=Decimal("20")
    )
    assert downpayment_amount == Decimal("2910")


def test_create_term_loan(session: Session) -> None:
    create_lenders(session=session)
    create_products(session=session)
    create_user(session=session)

    user_product = create_user_product_mapping(session=session, user_id=4, product_type="term_loan")

    loan_creation_data = {"date_str": "2020-08-01", "user_product_id": user_product.id}

    _downpayment_amount = TermLoan.bill_class.calculate_downpayment_amount_payable(
        product_price=Decimal("10000"), tenure=12, downpayment_perc=Decimal("20")
    )

    # downpayment
    payment_received(
        session=session,
        user_loan=None,
        payment_amount=_downpayment_amount,
        payment_date=parse_date("2020-08-01").date(),
        payment_request_id="dummy_downpayment",
        payment_type="downpayment",
        user_product_id=user_product.id,
        lender_id=62311,
    )

    downpayment_event = (
        session.query(LedgerTriggerEvent)
        .filter(
            LedgerTriggerEvent.name == "payment_received",
            LedgerTriggerEvent.loan_id.is_(None),
            LedgerTriggerEvent.user_product_id == user_product.id,
        )
        .one()
    )

    assert downpayment_event.post_date.date() == parse_date("2020-08-01").date()
    assert downpayment_event.amount == Decimal("2910")

    _, downpayment_balance = get_account_balance_from_str(
        session=session, book_string=f"{user_product.id}/product/downpayment/l"
    )
    assert downpayment_balance == Decimal("2910")

    _, product_lender_payable = get_account_balance_from_str(
        session=session, book_string=f"{user_product.id}/product/lender_payable/l"
    )
    assert product_lender_payable == Decimal("-2910")

    # create loan
    loan = create_test_term_loan(session=session, **loan_creation_data)

    _, rc_cash_balance = get_account_balance_from_str(
        session=session, book_string=f"12345/redcarpet/rc_cash/a"
    )
    assert rc_cash_balance == Decimal("-10000")

    assert loan.product_type == "term_loan"
    assert loan.amortization_date == parse_date("2020-08-01").date()

    user_loan = get_user_product(
        session=session, user_id=loan.user_id, card_type="term_loan", loan_id=loan.id
    )
    assert isinstance(user_loan, TermLoan) == True

    loan_data = session.query(LoanData).filter(LoanData.loan_id == user_loan.loan_id).one()

    assert loan_data.bill_start_date == parse_date("2020-08-01").date()
    assert loan_data.bill_close_date == parse_date("2021-07-01").date()
    assert loan_data.principal_instalment == Decimal("666.67")

    _, principal_receivable = get_account_balance_from_str(
        session=session, book_string=f"{loan_data.id}/bill/principal_receivable/a"
    )
    assert principal_receivable == Decimal("7090")

    _, loan_lender_payable = get_account_balance_from_str(
        session=session, book_string=f"{loan.loan_id}/loan/lender_payable/l"
    )
    assert loan_lender_payable == Decimal("7090")

    _, product_lender_payable = get_account_balance_from_str(
        session=session, book_string=f"{user_product.id}/product/lender_payable/l"
    )
    assert product_lender_payable == Decimal("0")

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
    assert emis_dict[0]["due_date"] == parse_date("2020-08-01").date()
    assert emis_dict[0]["emi_number"] == 1
    assert emis_dict[0]["interest"] == Decimal("243.33")
    assert emis_dict[0]["total_due_amount"] % 10 == 0
    assert emis_dict[0]["total_due_amount"] == Decimal("2910")

    assert emis_dict[-1]["due_date"] == parse_date("2021-07-01").date()
    assert emis_dict[-1]["emi_number"] == 12
    assert emis_dict[-1]["interest"] == Decimal("243.33")
    assert emis_dict[-1]["total_due_amount"] % 10 == 0


def test_create_term_loan_2(session: Session) -> None:
    create_lenders(session=session)
    create_products(session=session)
    create_user(session=session)

    user_product = create_user_product_mapping(session=session, user_id=4, product_type="term_loan")

    loan_creation_data = {"date_str": "2015-10-09", "user_product_id": user_product.id}

    _downpayment_amount = TermLoan.bill_class.calculate_downpayment_amount_payable(
        product_price=Decimal("10000"), tenure=12, downpayment_perc=Decimal("20")
    )

    assert _downpayment_amount == Decimal("2910")

    # downpayment
    payment_received(
        session=session,
        user_loan=None,
        payment_amount=_downpayment_amount,
        payment_date=parse_date(loan_creation_data["date_str"]).date(),
        payment_request_id="dummy_downpayment",
        payment_type="downpayment",
        user_product_id=user_product.id,
        lender_id=62311,
    )

    downpayment_event = (
        session.query(LedgerTriggerEvent)
        .filter(
            LedgerTriggerEvent.name == "payment_received",
            LedgerTriggerEvent.loan_id.is_(None),
            LedgerTriggerEvent.user_product_id == user_product.id,
        )
        .one()
    )

    assert downpayment_event.post_date.date() == parse_date("2015-10-09").date()
    assert downpayment_event.amount == Decimal("2910")

    _, downpayment_balance = get_account_balance_from_str(
        session=session, book_string=f"{user_product.id}/product/downpayment/l"
    )
    assert downpayment_balance == Decimal("2910")

    _, product_lender_payable = get_account_balance_from_str(
        session=session, book_string=f"{user_product.id}/product/lender_payable/l"
    )
    assert product_lender_payable == Decimal("-2910")

    # create loan
    loan = create_test_term_loan(session=session, **loan_creation_data)

    _, rc_cash_balance = get_account_balance_from_str(
        session=session, book_string=f"12345/redcarpet/rc_cash/a"
    )
    assert rc_cash_balance == Decimal("-10000")

    assert loan.product_type == "term_loan"
    assert loan.amortization_date == parse_date("2015-10-09").date()

    user_loan = get_user_product(
        session=session, user_id=loan.user_id, card_type="term_loan", loan_id=loan.id
    )
    assert isinstance(user_loan, TermLoan) == True

    loan_data = session.query(LoanData).filter(LoanData.loan_id == user_loan.loan_id).one()

    assert loan_data.bill_start_date == parse_date("2015-10-09").date()
    assert loan_data.bill_close_date == parse_date("2016-09-09").date()

    _, principal_receivable = get_account_balance_from_str(
        session=session, book_string=f"{loan_data.id}/bill/principal_receivable/a"
    )
    assert principal_receivable == Decimal("7090")

    _, loan_lender_payable = get_account_balance_from_str(
        session=session, book_string=f"{loan.loan_id}/loan/lender_payable/l"
    )
    assert loan_lender_payable == Decimal("7090")

    _, product_lender_payable = get_account_balance_from_str(
        session=session, book_string=f"{user_product.id}/product/lender_payable/l"
    )
    assert product_lender_payable == Decimal("0")

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
    assert emis_dict[0]["due_date"] == parse_date("2015-10-09").date()
    assert emis_dict[0]["emi_number"] == 1
    assert emis_dict[0]["interest"] == Decimal("243.33")
    assert emis_dict[0]["total_due_amount"] % 10 == 0

    assert emis_dict[1]["due_date"] == parse_date("2015-11-09").date()
    assert emis_dict[1]["emi_number"] == 2
    assert emis_dict[1]["interest"] == Decimal("243.33")
    assert emis_dict[1]["total_due_amount"] % 10 == 0

    assert emis_dict[-1]["due_date"] == parse_date("2016-09-09").date()
    assert emis_dict[-1]["emi_number"] == 12
    assert emis_dict[-1]["interest"] == Decimal("243.33")
    assert emis_dict[-1]["total_due_amount"] % 10 == 0
