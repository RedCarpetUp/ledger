from decimal import Decimal
from test.utils import (
    pay_payment_request,
    payment_request_data,
)

import pytest
from pendulum import parse as parse_date  # type: ignore
from sqlalchemy.orm import Session

from rush.accrue_financial_charges import (
    accrue_interest_on_all_bills,
    get_interest_left_to_accrue,
)
from rush.card import (
    create_user_product,
    get_user_product,
)
from rush.card.term_loan import (
    TermLoan,
    is_down_payment_paid,
)
from rush.card.utils import (
    create_loan,
    create_user_product_mapping,
)
from rush.ledger_utils import (
    get_account_balance_from_str,
    is_bill_closed,
)
from rush.loan_schedule.calculations import get_down_payment
from rush.models import (
    Fee,
    LedgerLoanData,
    LedgerTriggerEvent,
    Lenders,
    PaymentRequestsData,
    Product,
    User,
)
from rush.payments import (
    payment_received,
    settle_payment_in_bank,
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
    txn_loan_product = Product(product_name="transaction_loan")
    session.add(txn_loan_product)
    rebel_product = Product(product_name="rebel")
    session.add(rebel_product)
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
    amortization_date = TermLoan.calculate_first_emi_date(
        product_order_date=parse_date("2020-08-01").date()
    )
    assert amortization_date == parse_date("2020-08-01").date()


def test_product_amortization_2() -> None:
    amortization_date = TermLoan.calculate_first_emi_date(
        product_order_date=parse_date("2020-08-05").date()
    )
    assert amortization_date == parse_date("2020-08-05").date()


def test_product_amortization_3() -> None:
    amortization_date = TermLoan.calculate_first_emi_date(
        product_order_date=parse_date("2020-08-12").date()
    )
    assert amortization_date == parse_date("2020-08-12").date()


def test_product_amortization_4() -> None:
    amortization_date = TermLoan.calculate_first_emi_date(
        product_order_date=parse_date("2020-08-15").date()
    )
    assert amortization_date == parse_date("2020-08-15").date()


def test_product_amortization_5() -> None:
    amortization_date = TermLoan.calculate_first_emi_date(
        product_order_date=parse_date("2020-08-24").date()
    )
    assert amortization_date == parse_date("2020-08-24").date()


def test_product_amortization_6() -> None:
    amortization_date = TermLoan.calculate_first_emi_date(
        product_order_date=parse_date("2020-08-26").date()
    )
    assert amortization_date == parse_date("2020-08-26").date()


def test_calculate_downpayment_amount() -> None:
    downpayment_amount = get_down_payment(
        principal=Decimal("10000"),
        down_payment_percentage=Decimal("20"),
        interest_rate_monthly=Decimal(3),
        interest_type="flat",
        number_of_instalments=12,
        include_first_emi_amount=True,
    )
    assert downpayment_amount == Decimal("2910")


def test_create_term_loan(session: Session) -> None:
    create_lenders(session=session)
    create_products(session=session)
    create_user(session=session)

    user_product = create_user_product_mapping(session=session, user_id=4, product_type="term_loan")
    create_loan(session=session, user_product=user_product, lender_id=62311)
    user_loan = get_user_product(session=session, user_id=user_product.user_id, card_type="term_loan")
    assert isinstance(user_loan, TermLoan) == True

    loan_creation_data = {"date_str": "2020-08-01", "user_product_id": user_product.id}

    # create loan
    loan = create_test_term_loan(session=session, **loan_creation_data)

    _downpayment_amount = get_down_payment(
        principal=Decimal("10000"),
        down_payment_percentage=Decimal("20"),
        interest_rate_monthly=Decimal(3),
        interest_type="flat",
        number_of_instalments=12,
        include_first_emi_amount=True,
    )

    assert _downpayment_amount == Decimal(2910)

    # downpayment
    assert is_down_payment_paid(loan) == False
    payment_date = parse_date("2020-08-01")
    payment_request_id = "dummy_downpayment"
    payment_request_data(
        session=session,
        type="collection",
        payment_request_amount=_downpayment_amount,
        user_id=user_product.user_id,
        payment_request_id=payment_request_id,
    )
    payment_requests_data = pay_payment_request(
        session=session, payment_request_id=payment_request_id, payment_date=payment_date
    )
    payment_received(
        session=session,
        user_loan=user_loan,
        payment_request_data=payment_requests_data,
    )
    settle_payment_in_bank(
        session=session,
        payment_request_id=payment_request_id,
        gateway_expenses=payment_requests_data.payment_execution_charges,
        gross_payment_amount=payment_requests_data.payment_request_amount,
        settlement_date=payment_requests_data.payment_received_in_bank_date,
        user_loan=user_loan,
    )
    payment_ledger_event = (
        session.query(LedgerTriggerEvent)
        .filter(
            LedgerTriggerEvent.name == "payment_received",
            LedgerTriggerEvent.extra_details["payment_request_id"].astext == payment_request_id,
        )
        .first()
    )
    assert payment_ledger_event.amount == _downpayment_amount

    _, rc_cash_balance = get_account_balance_from_str(
        session=session, book_string=f"12345/redcarpet/rc_cash/a"
    )
    assert rc_cash_balance == Decimal("-10000")

    assert is_down_payment_paid(loan) == True
    loan.loan_status = "Started"

    assert loan.product_type == "term_loan"
    assert loan.amortization_date == parse_date("2020-08-01").date()

    loan_data = session.query(LedgerLoanData).filter(LedgerLoanData.loan_id == user_loan.loan_id).one()

    assert loan_data.bill_start_date == parse_date("2020-08-01").date()
    assert loan_data.bill_close_date == parse_date("2021-07-01").date()

    _, principal_receivable = get_account_balance_from_str(
        session=session, book_string=f"{loan_data.id}/bill/principal_receivable/a"
    )
    assert principal_receivable == Decimal("7090")

    _, loan_lender_payable = get_account_balance_from_str(
        session=session, book_string=f"{loan.loan_id}/loan/lender_payable/l"
    )
    assert loan_lender_payable == Decimal("7090.5")

    assert loan.get_remaining_min() == Decimal("0")

    assert loan.get_remaining_max() == Decimal("7090")
    assert loan.sub_product_type == "tenure_loan"
    all_emis = user_loan.get_loan_schedule()

    assert len(all_emis) == 12
    assert all_emis[0].due_date == parse_date("2020-08-01").date()
    assert all_emis[0].emi_number == 1
    assert all_emis[0].interest_due == Decimal("243.33")
    assert all_emis[0].total_due_amount == Decimal("2910")
    assert all_emis[0].total_due_amount % 10 == 0
    assert all_emis[0].payment_status == "Paid"

    assert all_emis[-1].due_date == parse_date("2021-07-01").date()
    assert all_emis[-1].emi_number == 12
    assert all_emis[-1].interest_due == Decimal("243.33")
    assert all_emis[-1].total_due_amount == Decimal("910")
    assert all_emis[-1].total_due_amount % 10 == 0

    interest_left_to_accrue = get_interest_left_to_accrue(session, user_loan)
    assert interest_left_to_accrue == Decimal("2919.96")

    accrue_interest_on_all_bills(session=session, post_date=all_emis[0].due_date, user_loan=user_loan)

    interest_left_to_accrue = get_interest_left_to_accrue(session, user_loan)
    assert interest_left_to_accrue == Decimal("2676.63")

    # Doing extra payment of 2910
    payment_date = parse_date("2020-08-02")
    payment_request_id = "asdf234"
    payment_request_data(
        session=session,
        type="collection",
        payment_request_amount=Decimal("12919.96"),
        user_id=user_product.user_id,
        payment_request_id=payment_request_id,
    )
    payment_requests_data = pay_payment_request(
        session=session, payment_request_id=payment_request_id, payment_date=payment_date
    )
    payment_received(
        session=session,
        user_loan=user_loan,
        payment_request_data=payment_requests_data,
    )

    early_closing_fee = (
        session.query(Fee)
        .filter(
            Fee.identifier == "loan",
            Fee.identifier_id == user_loan.loan_id,
            Fee.name == "early_close_fee",
        )
        .one()
    )
    assert early_closing_fee.gross_amount == Decimal("2676.63")
    assert early_closing_fee.fee_status == "PAID"

    assert user_loan.loan_status == "COMPLETED"
    bill = user_loan.get_all_bills()[0]
    assert bill.is_bill_closed() is True

    _, pre_payment_balance = get_account_balance_from_str(
        session=session, book_string=f"{user_loan.loan_id}/loan/pre_payment/l"
    )
    assert pre_payment_balance == Decimal("2910")


def test_create_term_loan_2(session: Session) -> None:
    create_lenders(session=session)
    create_products(session=session)
    create_user(session=session)

    user_product = create_user_product_mapping(session=session, user_id=4, product_type="term_loan")
    create_loan(session=session, user_product=user_product, lender_id=62311)
    user_loan = get_user_product(session=session, user_id=user_product.user_id, card_type="term_loan")
    assert isinstance(user_loan, TermLoan) == True

    loan_creation_data = {"date_str": "2015-10-09", "user_product_id": user_product.id}

    # create loan
    loan = create_test_term_loan(session=session, **loan_creation_data)

    _downpayment_amount = get_down_payment(
        principal=Decimal("10000"),
        down_payment_percentage=Decimal("20"),
        interest_rate_monthly=Decimal(3),
        interest_type="flat",
        number_of_instalments=12,
        include_first_emi_amount=True,
    )

    assert is_down_payment_paid(loan) == False
    assert _downpayment_amount == Decimal("2910")

    # downpayment
    payment_date = parse_date(loan_creation_data["date_str"])
    payment_request_id = "dummy_downpayment"
    payment_request_data(
        session=session,
        type="collection",
        payment_request_amount=_downpayment_amount,
        user_id=user_product.user_id,
        payment_request_id=payment_request_id,
    )
    payment_requests_data = pay_payment_request(
        session=session, payment_request_id=payment_request_id, payment_date=payment_date
    )
    payment_received(
        session=session,
        user_loan=user_loan,
        payment_request_data=payment_requests_data,
    )
    settle_payment_in_bank(
        session=session,
        payment_request_id=payment_request_id,
        gateway_expenses=payment_requests_data.payment_execution_charges,
        gross_payment_amount=payment_requests_data.payment_request_amount,
        settlement_date=payment_requests_data.payment_received_in_bank_date,
        user_loan=user_loan,
    )
    payment_ledger_event = (
        session.query(LedgerTriggerEvent)
        .filter(
            LedgerTriggerEvent.name == "payment_received",
            LedgerTriggerEvent.extra_details["payment_request_id"].astext == payment_request_id,
        )
        .first()
    )
    assert payment_ledger_event.amount == _downpayment_amount
    assert is_down_payment_paid(loan) == True

    loan.loan_status = "Started"

    _, rc_cash_balance = get_account_balance_from_str(
        session=session, book_string=f"12345/redcarpet/rc_cash/a"
    )
    assert rc_cash_balance == Decimal("-10000")

    assert loan.product_type == "term_loan"
    assert loan.amortization_date == parse_date("2015-10-09").date()

    loan_data = session.query(LedgerLoanData).filter(LedgerLoanData.loan_id == user_loan.loan_id).one()

    assert loan_data.bill_start_date == parse_date("2015-10-09").date()
    assert loan_data.bill_close_date == parse_date("2016-09-09").date()

    _, principal_receivable = get_account_balance_from_str(
        session=session, book_string=f"{loan_data.id}/bill/principal_receivable/a"
    )
    assert principal_receivable == Decimal("7090")

    _, loan_lender_payable = get_account_balance_from_str(
        session=session, book_string=f"{loan.loan_id}/loan/lender_payable/l"
    )
    assert loan_lender_payable == Decimal("7090.5")

    all_emis = user_loan.get_loan_schedule()

    assert len(all_emis) == 12
    assert all_emis[0].due_date == parse_date("2015-10-09").date()
    assert all_emis[0].emi_number == 1
    assert all_emis[0].interest_due == Decimal("243.33")
    assert all_emis[0].total_due_amount % 10 == 0

    assert all_emis[1].due_date == parse_date("2015-11-09").date()
    assert all_emis[1].emi_number == 2
    assert all_emis[1].interest_due == Decimal("243.33")
    assert all_emis[1].total_due_amount % 10 == 0

    assert all_emis[-1].due_date == parse_date("2016-09-09").date()
    assert all_emis[-1].emi_number == 12
    assert all_emis[-1].interest_due == Decimal("243.33")
    assert all_emis[-1].total_due_amount % 10 == 0
