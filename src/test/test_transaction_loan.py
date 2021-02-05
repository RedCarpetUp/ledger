from decimal import Decimal
from test.utils import (
    pay_payment_request,
    payment_request_data,
)

from dateutil.relativedelta import relativedelta
from pendulum import parse as parse_date  # type: ignore
from pendulum.parser import parse
from sqlalchemy.orm import Session
from sqlalchemy.sql.expression import intersect

from rush.accrue_financial_charges import accrue_interest_on_all_bills
from rush.card import (
    create_user_product,
    get_user_product,
)
from rush.card.base_card import BaseBill
from rush.card.rebel_card import RebelCard
from rush.card.transaction_loan import (
    TransactionLoan,
    transaction_to_loan,
)
from rush.create_bill import bill_generate
from rush.create_card_swipe import create_card_swipe
from rush.ledger_utils import get_account_balance_from_str
from rush.min_payment import add_min_to_all_bills
from rush.models import (
    CardTransaction,
    Lenders,
    LoanData,
    Product,
    User,
)
from rush.payments import payment_received


def create_lenders(session: Session) -> None:
    redux = Lenders(id=1756833, performed_by=123, lender_name="Redux")
    session.add(redux)
    session.flush()


def create_products(session: Session) -> None:
    hc_product = Product(product_name="rebel")
    session.add(hc_product)
    session.flush()
    transaction_loan_product = Product(product_name="transaction_loan")
    session.add(transaction_loan_product)
    session.flush()


def create_user(session: Session) -> None:
    u = User(
        id=469,
        performed_by=123,
    )
    session.add(u)
    session.flush()


def test_transaction_loan(session: Session) -> None:
    create_lenders(session=session)
    create_products(session=session)
    create_user(session=session)

    user_card = create_user_product(
        session=session,
        card_type="rebel",
        rc_rate_of_interest_monthly=Decimal(3),
        lender_id=1756833,
        interest_free_period_in_days=45,
        user_id=469,
        card_activation_date=parse_date("2020-11-01").date(),
        interest_type="reducing",
        tenure=12,
    )

    user_loan: RebelCard = get_user_product(session, 469, card_type="rebel")

    swipe = create_card_swipe(
        session=session,
        user_loan=user_card,
        txn_time=parse_date("2020-11-03 19:23:11"),
        amount=Decimal(1200),
        description="thor.com",
        txn_ref_no="dummy_txn_ref_no_1",
        trace_no="123457",
    )
    swipe2emi = create_card_swipe(
        session=session,
        user_loan=user_card,
        txn_time=parse_date("2020-11-04 19:23:11"),
        amount=Decimal(1200),
        description="thor.com",
        txn_ref_no="dummy_txn_ref_no_2",
        trace_no="123458",
    )
    session.flush()

    assert swipe["data"].loan_id == swipe2emi["data"].loan_id

    bill_id = swipe["data"].loan_id

    _, unbilled_amount = get_account_balance_from_str(session, book_string=f"{bill_id}/bill/unbilled/a")
    assert unbilled_amount == 2400

    transaction_loan: TransactionLoan = transaction_to_loan(
        session=session,
        transaction_id=swipe2emi["data"].id,
        user_id=469,
        post_date=parse_date("2020-11-05"),
        tenure=12,
        interest_rate=Decimal(3),
    )["data"]

    assert isinstance(transaction_loan, TransactionLoan)
    assert transaction_loan.can_close_early == False
    assert transaction_loan.get_remaining_min() == 0
    assert transaction_loan.get_remaining_max() == 1200

    assert user_loan.get_child_loans()[0].id == transaction_loan.id

    _, unbilled_amount = get_account_balance_from_str(session, book_string=f"{bill_id}/bill/unbilled/a")
    assert unbilled_amount == 1200

    transaction_loan_bill = (
        session.query(LoanData).filter(LoanData.loan_id == transaction_loan.id).scalar()
    )

    _, principal_receivable = get_account_balance_from_str(
        session, book_string=f"{transaction_loan_bill.id}/bill/principal_receivable/a"
    )
    assert principal_receivable == 1200

    bill_date = parse_date("2020-11-30 00:00:00")
    bill = bill_generate(user_loan=user_loan, creation_time=bill_date)
    latest_bill = user_loan.get_latest_bill()
    assert latest_bill is not None
    assert isinstance(latest_bill, BaseBill) == True

    assert bill.bill_start_date == parse_date("2020-11-01").date()
    assert bill.table.is_generated is True

    _, unbilled_amount = get_account_balance_from_str(
        session, book_string=f"{bill_id}/bill/unbilled/a", to_date=bill_date
    )
    assert unbilled_amount == 0

    _, billed_amount = get_account_balance_from_str(
        session, book_string=f"{bill_id}/bill/principal_receivable/a", to_date=bill_date
    )
    assert billed_amount == 1200

    statement_entries = session.query(CardTransaction).filter(CardTransaction.source == "LEDGER").all()
    assert len(statement_entries) == 1

    # paying min amount for rebel loan
    payment_date = parse_date("2020-12-02")
    amount = Decimal(121)
    payment_request_id = "bill_payment"
    payment_request_data(
        session=session,
        type="collection",
        payment_request_amount=amount,
        user_id=469,
        payment_request_id=payment_request_id,
    )
    payment_requests_data = pay_payment_request(
        session=session, payment_request_id=payment_request_id, payment_date=payment_date
    )
    payment_received(
        session=session,
        user_loan=user_loan,
        payment_request_data=payment_requests_data,
        skip_closing=False,
    )

    assert user_loan.get_remaining_min(date_to_check_against=parse_date("2020-12-03 00:00:00")) == 140
    assert (
        transaction_loan.get_remaining_min(date_to_check_against=parse_date("2020-12-03 00:00:00"))
        == 140
    )

    accrue_interest_on_all_bills(
        session=session, post_date=parse_date("2020-12-17 00:00:00"), user_loan=transaction_loan
    )

    # generating next month's bill
    bill_date = parse_date("2020-12-31 00:00:00")
    bill = bill_generate(user_loan=user_loan, creation_time=bill_date)
    latest_bill = user_loan.get_latest_bill()
    assert latest_bill is not None
    assert isinstance(latest_bill, BaseBill) == True

    assert bill.bill_start_date == parse_date("2020-12-01").date()
    assert bill.table.is_generated is True

    _, unbilled_amount = get_account_balance_from_str(
        session, book_string=f"{bill_id}/bill/unbilled/a", to_date=bill_date
    )
    assert unbilled_amount == 0

    _, billed_amount = get_account_balance_from_str(
        session, book_string=f"{bill_id}/bill/principal_receivable/a", to_date=bill_date
    )
    assert billed_amount == 1079

    statement_entries = session.query(CardTransaction).filter(CardTransaction.source == "LEDGER").all()
    assert len(statement_entries) == 2

    assert user_loan.get_remaining_min(date_to_check_against=parse_date("2021-01-02 00:00:00")) == 401
    assert (
        transaction_loan.get_remaining_min(date_to_check_against=parse_date("2021-01-02 00:00:00"))
        == 280
    )
    assert (
        transaction_loan.get_remaining_max(date_to_check_against=parse_date("2021-01-02 00:00:00"))
        == 1240
    )


def test_transaction_loan2(session: Session) -> None:
    create_lenders(session=session)
    create_products(session=session)
    create_user(session=session)

    user_card = create_user_product(
        session=session,
        card_type="rebel",
        rc_rate_of_interest_monthly=Decimal(3),
        lender_id=1756833,
        interest_free_period_in_days=45,
        user_id=469,
        card_activation_date=parse_date("2020-11-01").date(),
        interest_type="reducing",
        tenure=12,
    )

    user_loan: RebelCard = get_user_product(session, 469, card_type="rebel")

    swipe = create_card_swipe(
        session=session,
        user_loan=user_card,
        txn_time=parse_date("2020-11-03 19:23:11"),
        amount=Decimal(1200),
        description="thor.com",
        txn_ref_no="dummy_txn_ref_no_1",
        trace_no="123457",
    )
    swipe2emi = create_card_swipe(
        session=session,
        user_loan=user_card,
        txn_time=parse_date("2020-11-04 19:23:11"),
        amount=Decimal(1200),
        description="thor.com",
        txn_ref_no="dummy_txn_ref_no_2",
        trace_no="123458",
    )
    session.flush()

    assert swipe["data"].loan_id == swipe2emi["data"].loan_id

    transaction_loan: TransactionLoan = transaction_to_loan(
        session=session,
        transaction_id=swipe2emi["data"].id,
        user_id=469,
        post_date=parse_date("2020-11-05"),
        tenure=12,
        interest_rate=Decimal(3),
    )["data"]

    bill_date = parse_date("2020-11-30 00:00:00")
    bill_generate(user_loan=user_loan, creation_time=bill_date)

    statement_entries = session.query(CardTransaction).filter(CardTransaction.source == "LEDGER").all()
    assert len(statement_entries) == 1

    assert user_loan.get_remaining_min(date_to_check_against=parse_date("2020-12-02 19:23:11")) == 261
    assert (
        transaction_loan.get_remaining_min(date_to_check_against=parse_date("2020-12-02 19:23:11"))
        == 140
    )

    assert (
        user_loan.get_remaining_max(date_to_check_against=parse_date("2020-12-02 19:23:11"))
        == swipe["data"].amount + statement_entries[0].amount
    )

    payment_date = parse_date("2020-12-02")
    amount = Decimal("1480")
    payment_request_id = "bill_payment"
    payment_request_data(
        session=session,
        type="collection",
        payment_request_amount=amount,
        user_id=469,
        payment_request_id=payment_request_id,
    )
    payment_requests_data = pay_payment_request(
        session=session, payment_request_id=payment_request_id, payment_date=payment_date
    )
    payment_received(
        session=session,
        user_loan=user_loan,
        payment_request_data=payment_requests_data,
        skip_closing=False,
    )

    assert user_loan.get_remaining_min(date_to_check_against=parse_date("2020-12-03 00:00:00")) == 0
    assert (
        transaction_loan.get_remaining_min(date_to_check_against=parse_date("2020-12-03 00:00:00")) == 0
    )

    assert (
        user_loan.get_remaining_max(
            date_to_check_against=parse_date("2020-12-03 00:00:00"), include_child_loans=False
        )
        == 0
    )
    assert (
        transaction_loan.get_remaining_max(date_to_check_against=parse_date("2020-12-03 00:00:00"))
        == 920
    )

    user_loan_schedule = user_loan.get_loan_schedule()
    transaction_loan_schedule = transaction_loan.get_loan_schedule()

    for emi_number in range(0, len(user_loan_schedule)):
        assert user_loan_schedule[emi_number].due_date == transaction_loan_schedule[emi_number].due_date

    assert user_loan_schedule[0].payment_status == "Paid"
    user_loan_schedule = user_loan_schedule[1:]
    assert all(
        emi.payment_status == "UnPaid" and emi.payment_received == 0 for emi in user_loan_schedule
    )

    assert transaction_loan_schedule[0].payment_status == "Paid"
    assert transaction_loan_schedule[1].payment_status == "Paid"
    transaction_loan_schedule = transaction_loan_schedule[2:]
    assert all(emi.payment_status == "UnPaid" for emi in transaction_loan_schedule)

    bill_date = parse_date("2020-12-31 00:00:00")
    bill_generate(user_loan=user_loan, creation_time=bill_date)

    statement_entries = session.query(CardTransaction).filter(CardTransaction.source == "LEDGER").all()
    assert len(statement_entries) == 2
