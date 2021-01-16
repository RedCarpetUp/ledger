import pdb
from decimal import Decimal
from test.utils import (
    pay_payment_request,
    payment_request_data,
)
from typing import Sized

from pendulum import parse as parse_date  # type: ignore
from pendulum.parser import parse
from sqlalchemy.orm import Session
from sqlalchemy.sql.sqltypes import DECIMAL
from sqlalchemy.util.langhelpers import only_once

from rush.card import (
    create_user_product,
    get_user_product,
    transaction_loan,
)
from rush.card.base_card import (
    BaseBill,
    BaseLoan,
)
from rush.card.rebel_card import RebelCard
from rush.card.transaction_loan import TransactionLoan
from rush.card.utils import create_user_product_mapping
from rush.create_bill import bill_generate
from rush.create_card_swipe import create_card_swipe
from rush.ledger_utils import get_account_balance_from_str
from rush.models import (
    LedgerTriggerEvent,
    Lenders,
    LoanData,
    Product,
    User,
)
from rush.payments import (
    find_split_to_slide_in_loan,
    payment_received,
    payment_received_event,
)
from rush.transaction_loan import (
    transaction_to_loan,
    transaction_to_loan_new,
)
from rush.utils import get_current_ist_time


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

    user_product = create_user_product_mapping(
        session=session, user_id=469, product_type="rebel", lender_id=1756833
    )
    user_card = create_user_product(
        session=session,
        card_type="rebel",
        rc_rate_of_interest_monthly=Decimal(3),
        lender_id=1756833,
        interest_free_period_in_days=45,
        user_id=469,
        user_product_id=user_product.id,
        card_activation_date=parse_date("2020-11-01").date(),
        interest_type="reducing",
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
        post_date=parse_date("2020-11-01"),
    )["data"]

    assert isinstance(transaction_loan, TransactionLoan)
    assert transaction_loan.get_remaining_min() == Decimal("140")
    assert transaction_loan.get_remaining_max() == Decimal("1200")

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

    bill_date = parse_date("2020-12-01 00:00:00")
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

    bill = bill_generate(user_loan=transaction_loan, creation_time=bill_date)
    latest_bill = transaction_loan.get_latest_bill()
    assert latest_bill is not None
    assert isinstance(latest_bill, BaseBill) == True

    assert bill.bill_start_date == parse_date("2020-11-01").date()
    assert bill.table.is_generated is True

    _, unbilled_amount = get_account_balance_from_str(
        session, book_string=f"{transaction_loan_bill.id}/bill/unbilled/a", to_date=bill_date
    )
    assert unbilled_amount == 0

    _, billed_amount = get_account_balance_from_str(
        session, book_string=f"{transaction_loan_bill.id}/bill/principal_receivable/a", to_date=bill_date
    )
    assert billed_amount == 1200

    # paying min amount for rebel loan
    lt = LedgerTriggerEvent(
        name="payment_received",
        loan_id=user_loan.loan_id if user_loan else None,
        amount=121,
        post_date=parse_date("2020-12-02 19:23:11"),
        extra_details={
            "payment_request_id": "dummy_payment",
            "payment_type": "principal",
            "user_product_id": user_product.id if user_product.id else user_loan.user_product_id,
            "lender_id": user_loan.lender_id,
        },
    )
    session.add(lt)
    session.flush()

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

    assert user_loan.get_remaining_min(date_to_check_against=parse_date("2020-12-03 00:00:00")) == 0
    assert (
        transaction_loan.get_remaining_min(date_to_check_against=parse_date("2020-12-03 00:00:00"))
        == 140
    )

    # generating next month's bill
    bill_date = parse_date("2021-01-01 00:00:00")
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

    bill = bill_generate(user_loan=transaction_loan, creation_time=bill_date)
    latest_bill = transaction_loan.get_latest_bill()
    assert latest_bill is not None
    assert isinstance(latest_bill, BaseBill) == True

    assert bill.bill_start_date == parse_date("2020-11-01").date()
    assert bill.table.is_generated is True

    _, unbilled_amount = get_account_balance_from_str(
        session, book_string=f"{transaction_loan_bill.id}/bill/unbilled/a", to_date=bill_date
    )
    assert unbilled_amount == 0

    _, billed_amount = get_account_balance_from_str(
        session, book_string=f"{transaction_loan_bill.id}/bill/principal_receivable/a", to_date=bill_date
    )
    assert billed_amount == 1200

    assert user_loan.get_remaining_min(date_to_check_against=parse_date("2021-01-03 00:00:00")) == 121
    assert (
        transaction_loan.get_remaining_min(date_to_check_against=parse_date("2021-01-03 00:00:00"))
        == 140
    )


def test_transaction_loan2(session: Session) -> None:
    create_lenders(session=session)
    create_products(session=session)
    create_user(session=session)

    user_product = create_user_product_mapping(
        session=session, user_id=469, product_type="rebel", lender_id=1756833
    )
    user_card = create_user_product(
        session=session,
        card_type="rebel",
        rc_rate_of_interest_monthly=Decimal(3),
        lender_id=1756833,
        interest_free_period_in_days=45,
        user_id=469,
        user_product_id=user_product.id,
        card_activation_date=parse_date("2020-11-01").date(),
        interest_type="reducing",
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
        post_date=parse_date("2020-11-01"),
    )["data"]

    bill_date = parse_date("2020-12-01 00:00:00")
    bill_generate(user_loan=user_loan, creation_time=bill_date)
    bill_generate(user_loan=transaction_loan, creation_time=bill_date)

    assert user_loan.get_remaining_min(date_to_check_against=parse_date("2020-12-01 19:23:11")) == 121
    assert (
        transaction_loan.get_remaining_min(date_to_check_against=parse_date("2020-12-01 19:23:11"))
        == 140
    )

    assert user_loan.get_remaining_max(date_to_check_against=parse_date("2020-12-01 19:23:11")) == 1200
    assert (
        transaction_loan.get_remaining_max(date_to_check_against=parse_date("2020-12-01 19:23:11"))
        == 1200
    )

    lt = LedgerTriggerEvent(
        name="payment_received",
        loan_id=user_loan.loan_id if user_loan else None,
        amount=1480,
        post_date=parse_date("2020-12-02 19:23:11"),
        extra_details={
            "payment_request_id": "dummy_payment",
            "payment_type": "principal",
            "user_product_id": user_product.id if user_product.id else user_loan.user_product_id,
            "lender_id": user_loan.lender_id,
        },
    )
    session.add(lt)
    session.flush()

    payment_date = parse_date("2020-12-02")
    amount = Decimal(1480)
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

    assert user_loan.get_remaining_max(date_to_check_against=parse_date("2020-12-03 00:00:00")) == 0
    assert (
        transaction_loan.get_remaining_max(date_to_check_against=parse_date("2020-12-03 00:00:00")) != 0
    )

    user_loan_schedule = user_loan.get_loan_schedule()
    assert user_loan_schedule[0].payment_status == "Paid"
    user_loan_schedule = user_loan_schedule[1:]
    assert all(
        emi.payment_status == "UnPaid" and emi.payment_received == 0 for emi in user_loan_schedule
    )

    transaction_loan_schedule = transaction_loan.get_loan_schedule()
    assert transaction_loan_schedule[0].payment_status == "Paid"
    transaction_loan_schedule = transaction_loan_schedule[1:]
    assert all(emi.payment_status == "UnPaid" for emi in transaction_loan_schedule)


def test_transaction_loan_new(session: Session) -> None:
    create_lenders(session=session)
    create_products(session=session)
    create_user(session=session)

    user_product = create_user_product_mapping(
        session=session, user_id=469, product_type="rebel", lender_id=1756833
    )
    user_card = create_user_product(
        session=session,
        card_type="rebel",
        rc_rate_of_interest_monthly=Decimal(3),
        lender_id=1756833,
        interest_free_period_in_days=45,
        user_id=469,
        user_product_id=user_product.id,
        card_activation_date=parse_date("2020-11-01").date(),
        interest_type="reducing",
    )

    swipe = create_card_swipe(
        session=session,
        user_loan=user_card,
        txn_time=parse_date("2020-11-03 19:23:11"),
        amount=Decimal(1200),
        description="thor.com",
        txn_ref_no="dummy_txn_ref_no",
        trace_no="123456",
    )
    session.flush()
    bill_id = swipe["data"].loan_id
    _, unbilled_amount = get_account_balance_from_str(session, book_string=f"{bill_id}/bill/unbilled/a")
    assert unbilled_amount == 1200
    user_loan: RebelCard = get_user_product(session, 469, card_type="rebel")
    bill_date = parse_date("2020-12-01 00:00:00")
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

    lt = LedgerTriggerEvent(
        name="payment_received",
        loan_id=user_loan.loan_id if user_loan else None,
        amount=1200,
        post_date=parse_date("2020-12-02 19:23:11"),
        extra_details={
            "payment_request_id": "dummy_payment_0",
            "payment_type": "principal",
            "user_product_id": user_product.id if user_product.id else user_loan.user_product_id,
            "lender_id": user_loan.lender_id,
        },
    )
    session.add(lt)
    session.flush()

    lender_id = user_loan.lender_id

    payment_date = parse_date("2020-12-02")
    amount = Decimal(1200)
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
    payment_received_event(
        session=session,
        user_loan=user_loan,
        debit_book_str=f"{lender_id}/lender/pg_account/a",
        event=lt,
        skip_closing=False,
        amount_to_adjust=amount,
        payment_request_data=payment_requests_data,
    )

    swipe1 = create_card_swipe(
        session=session,
        user_loan=user_card,
        txn_time=parse_date("2020-12-03 19:23:11"),
        amount=Decimal(1200),
        description="thor.com",
        txn_ref_no="dummy_txn_ref_no_1",
        trace_no="123457",
    )
    swipe2 = create_card_swipe(
        session=session,
        user_loan=user_card,
        txn_time=parse_date("2020-12-04 19:23:11"),
        amount=Decimal(1200),
        description="thor.com",
        txn_ref_no="dummy_txn_ref_no_2",
        trace_no="123458",
    )
    swipe2emi = create_card_swipe(
        session=session,
        user_loan=user_card,
        txn_time=parse_date("2020-12-05 19:23:11"),
        amount=Decimal(1200),
        description="thor.com",
        txn_ref_no="dummy_txn_ref_no_3",
        trace_no="123459",
    )
    session.flush()

    bill_id = swipe1["data"].loan_id

    _, unbilled_amount = get_account_balance_from_str(session, book_string=f"{bill_id}/bill/unbilled/a")
    assert unbilled_amount == 3600

    transaction_bill = transaction_to_loan_new(
        session=session,
        transaction_id=swipe2emi["data"].id,
        user_id=469,
        post_date=parse_date("2020-12-06 19:23:11"),
        tenure=12,
    )["data"]

    assert isinstance(transaction_bill, LoanData)

    bill_date = parse_date("2021-01-01 00:00:00")
    bills = bill_generate(user_loan=user_loan, creation_time=bill_date)

    assert len(bills) == 2
    assert any(bill.id == transaction_bill.id for bill in bills)

    latest_bill = user_loan.get_latest_bill()
    assert latest_bill is not None
    assert isinstance(latest_bill, BaseBill) == True

    for bill in bills:
        assert bill.bill_start_date == parse_date("2020-12-01").date()
        assert bill.table.is_generated is True
        if (
            get_account_balance_from_str(session, book_string=f"{bill.id}/bill/principal_receivable/a")[
                1
            ]
            == 2400
        ):
            regular_bill = bill

    assert any(
        get_account_balance_from_str(session, book_string=f"{bill.id}/bill/principal_receivable/a")[1]
        == 2400
        for bill in bills
    )

    assert any(
        get_account_balance_from_str(session, book_string=f"{bill.id}/bill/principal_receivable/a")[1]
        == 1200
        for bill in bills
    )

    payment_split_info = find_split_to_slide_in_loan(
        session=session, user_loan=user_loan, total_amount_to_slide=Decimal(2540)
    )

    assert len(payment_split_info) == 2
    assert payment_split_info[0]["type"] == "principal"
    assert payment_split_info[0]["amount_to_adjust"] == Decimal("1693.33")
    assert payment_split_info[1]["type"] == "principal"
    assert payment_split_info[1]["amount_to_adjust"] == Decimal("846.67")

    lt = LedgerTriggerEvent(
        name="payment_received",
        loan_id=user_loan.loan_id if user_loan else None,
        amount=2540,
        post_date=parse_date("2021-01-02 19:23:11"),
        extra_details={
            "payment_request_id": "dummy_payment",
            "payment_type": "principal",
            "user_product_id": user_product.id if user_product.id else user_loan.user_product_id,
            "lender_id": user_loan.lender_id,
        },
    )
    session.add(lt)
    session.flush()

    lender_id = user_loan.lender_id

    payment_date = parse_date("2020-08-03")
    amount = Decimal(2540)
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
    payment_received_event(
        session=session,
        user_loan=user_loan,
        debit_book_str=f"{lender_id}/lender/pg_account/a",
        event=lt,
        skip_closing=False,
        amount_to_adjust=amount,
        payment_request_data=payment_requests_data,
    )

    assert user_loan.get_remaining_min(date_to_check_against=parse_date("2021-01-03 00:00:00")) == 0

    _, unbilled_amount = get_account_balance_from_str(
        session, book_string=f"{regular_bill.id}/bill/unbilled/a", to_date=bill_date
    )
    assert unbilled_amount == 0

    _, unbilled_amount = get_account_balance_from_str(
        session, book_string=f"{transaction_bill.id}/bill/unbilled/a", to_date=bill_date
    )
    assert unbilled_amount == 0
