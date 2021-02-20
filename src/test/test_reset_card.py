from decimal import Decimal
from test.utils import (
    pay_payment_request,
    payment_request_data,
)

import pytest
from dateutil.relativedelta import relativedelta
from pendulum import parse as parse_date  # type: ignore
from sqlalchemy.orm import Session

from rush.accrue_financial_charges import accrue_interest_on_all_bills
from rush.card import (
    create_user_product,
    get_user_product,
)
from rush.card.reset_card import ResetCard
from rush.card.utils import (
    create_activation_fee,
    create_loan,
    create_user_product_mapping,
)
from rush.ledger_utils import get_account_balance_from_str
from rush.limit_unlock import limit_unlock
from rush.loan_schedule.loan_schedule import is_payment_closing_schedule
from rush.min_payment import add_min_to_all_bills
from rush.models import (
    Lenders,
    LoanData,
    Product,
    User,
)
from rush.payments import (
    payment_received,
    settle_payment_in_bank,
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
        interest_rate=Decimal(3),
    )

    return loan


def test_product_amortization_1() -> None:
    amortization_date = ResetCard.calculate_first_emi_date(
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
    create_loan(session=session, user_product=user_product, lender_id=1756833)
    user_loan = get_user_product(
        session=session, user_id=user_product.user_id, card_type="term_loan_reset"
    )
    assert isinstance(user_loan, ResetCard) == True

    fee = create_activation_fee(
        session=session,
        user_loan=user_loan,
        post_date=parse_date("2020-08-01 00:00:00"),
        gross_amount=Decimal("100"),
        include_gst_from_gross_amount=False,
        fee_name="reset_joining_fees",
    )

    payment_date = parse_date("2020-08-01")
    amount = fee.gross_amount
    payment_request_id = "dummy_reset_fee_1"
    payment_request_data(
        session=session,
        type="reset_joining_fees",
        payment_request_amount=amount,
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

    session.flush()

    loan_creation_data = {"date_str": "2020-08-01", "user_product_id": user_product.id}

    # create loan
    loan = create_test_term_loan(session=session, **loan_creation_data)

    _, rc_cash_balance = get_account_balance_from_str(
        session=session, book_string=f"12345/redcarpet/rc_cash/a"
    )
    assert rc_cash_balance == Decimal("-10000")

    assert loan.product_type == "term_loan_reset"
    assert loan.amortization_date == parse_date("2020-08-01").date()

    loan_data = session.query(LoanData).filter(LoanData.loan_id == user_loan.loan_id).one()

    assert loan_data.bill_start_date == parse_date("2020-09-01").date()
    assert loan_data.bill_close_date == parse_date("2021-08-01").date()

    _, principal_receivable = get_account_balance_from_str(
        session=session, book_string=f"{loan_data.id}/bill/principal_receivable/a"
    )
    assert principal_receivable == Decimal("10000")

    _, loan_lender_payable = get_account_balance_from_str(
        session=session, book_string=f"{loan.loan_id}/loan/lender_payable/l"
    )
    assert loan_lender_payable == Decimal("9882.50")

    all_emis = user_loan.get_loan_schedule()

    assert len(all_emis) == 12
    assert all_emis[0].due_date == parse_date("2020-09-01").date()
    assert all_emis[0].emi_number == 1
    assert all_emis[0].interest_due == Decimal("300.67")
    assert all_emis[0].total_due_amount == Decimal("1134")

    assert all_emis[-1].due_date == parse_date("2021-08-01").date()
    assert all_emis[-1].emi_number == 12
    assert all_emis[-1].interest_due == Decimal("300.67")
    assert all_emis[-1].total_due_amount == Decimal("1134")

    payment_date = parse_date("2020-08-25")
    amount = Decimal(11000)
    payment_request_id = "dummy_reset_fee_2"
    payment_request_data(
        session=session,
        type="collection",
        payment_request_amount=amount,
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

    _, principal_receivable = get_account_balance_from_str(
        session=session, book_string=f"{loan_data.id}/bill/principal_receivable/a"
    )
    assert principal_receivable == Decimal(0)

    _, pre_payment_balance = get_account_balance_from_str(
        session=session, book_string=f"{loan.loan_id}/loan/pre_payment/l"
    )
    assert pre_payment_balance == Decimal("1000")

    # add min amount for months in between.
    add_min_to_all_bills(session=session, post_date=parse_date("2020-09-01"), user_loan=loan)
    accrue_interest_on_all_bills(
        session=session, post_date=all_emis[0].due_date + relativedelta(days=1), user_loan=user_loan
    )

    _, pre_payment_balance = get_account_balance_from_str(
        session=session, book_string=f"{loan.loan_id}/loan/pre_payment/l"
    )
    assert pre_payment_balance == Decimal("699.33")  # interest got accrued and adjusted from prepayment.

    assert is_payment_closing_schedule(session, user_loan, Decimal(2608)) is True

    max_amount = user_loan.get_remaining_max()
    assert max_amount == Decimal("0")

    limit_unlock(session=session, loan=loan, amount=Decimal("1000"))

    _, locked_limit = get_account_balance_from_str(
        session=session, book_string=f"{loan.id}/card/locked_limit/l"
    )
    assert locked_limit == Decimal("9000")

    _, available_limit = get_account_balance_from_str(
        session=session, book_string=f"{loan.id}/card/available_limit/l"
    )
    assert available_limit == Decimal("1000")


def test_reset_loan_limit_unlock_success(session: Session) -> None:
    create_lenders(session=session)
    create_products(session=session)
    create_user(session=session)

    user_product = create_user_product_mapping(
        session=session, user_id=6, product_type="term_loan_reset"
    )
    create_loan(session=session, user_product=user_product, lender_id=1756833)
    user_loan = get_user_product(
        session=session, user_id=user_product.user_id, card_type="term_loan_reset"
    )
    assert isinstance(user_loan, ResetCard) == True

    fee = create_activation_fee(
        session=session,
        user_loan=user_loan,
        post_date=parse_date("2020-08-01 00:00:00"),
        gross_amount=Decimal("100"),
        include_gst_from_gross_amount=False,
        fee_name="reset_joining_fees",
    )

    payment_date = parse_date("2020-08-01")
    amount = fee.gross_amount
    payment_request_id = "dummy_reset_fee_2"
    payment_request_data(
        session=session,
        type="reset_joining_fees",
        payment_request_amount=amount,
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

    session.flush()

    loan_creation_data = {"date_str": "2020-08-01", "user_product_id": user_product.id}

    # create loan
    loan = create_test_term_loan(session=session, **loan_creation_data)

    _, locked_limit = get_account_balance_from_str(
        session=session, book_string=f"{loan.id}/card/locked_limit/l"
    )

    assert locked_limit == Decimal("10000")

    _, available_limit = get_account_balance_from_str(
        session=session, book_string=f"{loan.id}/card/available_limit/l"
    )

    assert available_limit == Decimal("0")

    limit_unlock(session=session, loan=loan, amount=Decimal("10000"))

    _, locked_limit = get_account_balance_from_str(
        session=session, book_string=f"{loan.id}/card/locked_limit/l"
    )

    assert locked_limit == Decimal("0")

    _, available_limit = get_account_balance_from_str(
        session=session, book_string=f"{loan.id}/card/available_limit/l"
    )

    assert available_limit == Decimal("10000")


def test_reset_loan_limit_unlock_error(session: Session) -> None:
    create_lenders(session=session)
    create_products(session=session)
    create_user(session=session)

    user_product = create_user_product_mapping(
        session=session, user_id=6, product_type="term_loan_reset"
    )
    create_loan(session=session, user_product=user_product, lender_id=1756833)
    user_loan = get_user_product(
        session=session, user_id=user_product.user_id, card_type="term_loan_reset"
    )
    assert isinstance(user_loan, ResetCard) == True

    fee = create_activation_fee(
        session=session,
        user_loan=user_loan,
        post_date=parse_date("2020-08-01 00:00:00"),
        gross_amount=Decimal("100"),
        include_gst_from_gross_amount=False,
        fee_name="reset_joining_fees",
    )

    payment_date = parse_date("2020-08-01")
    amount = fee.gross_amount
    payment_request_id = "dummy_reset_fee"
    payment_request_data(
        session=session,
        type="reset_joining_fees",
        payment_request_amount=amount,
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

    session.flush()

    loan_creation_data = {"date_str": "2020-08-01", "user_product_id": user_product.id}

    # create loan
    loan = create_test_term_loan(session=session, **loan_creation_data)

    _, locked_limit = get_account_balance_from_str(
        session=session, book_string=f"{loan.id}/card/locked_limit/l"
    )

    assert locked_limit == Decimal("10000")

    # now trying to unlock more than 10000
    with pytest.raises(AssertionError):
        limit_unlock(session=session, loan=loan, amount=Decimal("10001"))


def test_reset_loan_early_payment(session: Session) -> None:
    pass
