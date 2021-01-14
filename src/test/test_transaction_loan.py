import pdb
from decimal import Decimal

from pendulum import parse as parse_date  # type: ignore
from sqlalchemy.orm import Session
from sqlalchemy.util.langhelpers import only_once

from rush.card import (
    create_user_product,
    get_user_product,
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
    payment_received_event,
)
from rush.txn_loan import (
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
    txn_loan_product = Product(product_name="transaction_loan")
    session.add(txn_loan_product)
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

    swipe = create_card_swipe(
        session=session,
        user_loan=user_card,
        txn_time=parse_date("2020-12-03 19:23:11"),
        amount=Decimal(1200),
        description="thor.com",
        txn_ref_no="dummy_txn_ref_no_1",
        trace_no="123457",
    )
    swipe2emi = create_card_swipe(
        session=session,
        user_loan=user_card,
        txn_time=parse_date("2020-12-04 19:23:11"),
        amount=Decimal(1200),
        description="thor.com",
        txn_ref_no="dummy_txn_ref_no_2",
        trace_no="123458",
    )
    session.flush()

    bill_id = swipe["data"].loan_id

    _, unbilled_amount = get_account_balance_from_str(session, book_string=f"{bill_id}/bill/unbilled/a")
    assert unbilled_amount == 2400

    txn_loan = transaction_to_loan(
        session=session,
        txn_id=swipe2emi["data"].id,
        user_id=469,
        post_date=parse_date("2020-12-05 19:23:11"),
    )["data"]

    assert isinstance(txn_loan, TransactionLoan)

    assert txn_loan.get_remaining_min() == Decimal("140")

    assert txn_loan.get_remaining_max() == Decimal("1200")

    assert user_loan.get_transaction_loans()[0].id == txn_loan.id

    _, unbilled_amount = get_account_balance_from_str(session, book_string=f"{bill_id}/bill/unbilled/a")
    assert unbilled_amount == 1200

    txn_loan_bill = session.query(LoanData).filter(LoanData.loan_id == txn_loan.id).scalar()

    _, principal_receivable = get_account_balance_from_str(
        session, book_string=f"{txn_loan_bill.id}/bill/principal_receivable/a"
    )
    assert principal_receivable == 1200

    payment_split_info = find_split_to_slide_in_loan(
        session=session, user_loan=user_loan, total_amount_to_slide=1340
    )

    bills = user_loan.get_unpaid_bills()

    assert any(bill.id == txn_loan_bill.id for bill in bills)

    assert len(payment_split_info) == 2
    assert payment_split_info[0]["type"] == "principal"
    assert payment_split_info[0]["amount_to_adjust"] == Decimal(670)
    assert payment_split_info[1]["type"] == "principal"
    assert payment_split_info[1]["amount_to_adjust"] == Decimal(670)

    lt = LedgerTriggerEvent(
        name="payment_received",
        loan_id=user_loan.loan_id if user_loan else None,
        amount=1340,
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
    payment_received_event(
        session=session,
        user_loan=user_loan,
        debit_book_str=f"{lender_id}/lender/pg_account/a",
        event=lt,
        skip_closing=False,
        user_product_id=user_product.id if user_product.id else user_loan.user_product_id,
    )

    assert user_loan.get_remaining_min(date_to_check_against=parse_date("2021-01-03 00:00:00")) == 0

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
    assert billed_amount == 1200


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
    payment_received_event(
        session=session,
        user_loan=user_loan,
        debit_book_str=f"{lender_id}/lender/pg_account/a",
        event=lt,
        skip_closing=False,
        user_product_id=user_product.id if user_product.id else user_loan.user_product_id,
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
        session=session, user_loan=user_loan, total_amount_to_slide=2540
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
    payment_received_event(
        session=session,
        user_loan=user_loan,
        debit_book_str=f"{lender_id}/lender/pg_account/a",
        event=lt,
        skip_closing=False,
        user_product_id=user_product.id if user_product.id else user_loan.user_product_id,
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
