from decimal import Decimal
from test.utils import (
    collection_request_data,
    pay_payment_request,
    payment_request_data,
)

import pytest
from dateutil.relativedelta import relativedelta
from pendulum import parse as parse_date  # type: ignore
from sqlalchemy.orm import Session

from rush.accrue_financial_charges import (
    accrue_interest_on_all_bills,
    accrue_late_charges,
    get_interest_left_to_accrue,
)
from rush.card import (
    create_user_product,
    get_user_product,
)
from rush.card.reset_card import ResetCard
from rush.card.utils import (
    create_loan,
    create_loan_fee,
    create_user_product_mapping,
)
from rush.create_card_swipe import create_card_swipe
from rush.ledger_utils import get_account_balance_from_str
from rush.limit_unlock import limit_unlock
from rush.min_payment import add_min_to_all_bills
from rush.models import (
    CollectionOrders,
    Fee,
    JournalEntry,
    LedgerTriggerEvent,
    Lenders,
    LoanData,
    Product,
    User,
)
from rush.payments import (
    payment_received,
    refund_payment,
    settle_payment_in_bank,
)
from rush.writeoff_and_recovery import write_off_loan


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
        amount=kwargs.get("amount", Decimal(10000)),
        product_order_date=parse_date(date_str).date(),
        user_product_id=user_product_id,
        downpayment_percent=Decimal("0"),
        interest_rate=kwargs.get("interest_rate", Decimal(3)),
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

    fee = create_loan_fee(
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
    payment_ledger_event = (
        session.query(LedgerTriggerEvent)
        .filter(
            LedgerTriggerEvent.name == "payment_received",
            LedgerTriggerEvent.extra_details["payment_request_id"].astext == payment_request_id,
        )
        .first()
    )
    assert payment_ledger_event.amount == amount

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

    interest_left_to_accrue = get_interest_left_to_accrue(session, user_loan)
    assert interest_left_to_accrue == Decimal("3608.04")

    swipe2 = create_card_swipe(
        session=session,
        user_loan=user_loan,
        txn_time=parse_date("2020-08-01 11:22:11"),
        amount=Decimal(1000),
        description="Flipkart.com",
        txn_ref_no="dummy_txn_ref_no_2",
        trace_no="123456",
    )
    session.flush()
    _, loan_lender_payable = get_account_balance_from_str(
        session=session, book_string=f"{user_loan.loan_id}/loan/lender_payable/l"
    )
    assert loan_lender_payable == Decimal("882.50")

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

    payment_ledger_event = (
        session.query(LedgerTriggerEvent)
        .filter(
            LedgerTriggerEvent.name == "payment_received",
            LedgerTriggerEvent.extra_details["payment_request_id"].astext == payment_request_id,
        )
        .first()
    )
    assert payment_ledger_event.amount == amount

    _, principal_receivable = get_account_balance_from_str(
        session=session, book_string=f"{loan_data.id}/bill/principal_receivable/a"
    )
    assert principal_receivable == Decimal(1000)

    _, pre_payment_balance = get_account_balance_from_str(
        session=session, book_string=f"{loan.loan_id}/loan/pre_payment/l"
    )
    assert pre_payment_balance == Decimal("0")

    _, early_close_balance = get_account_balance_from_str(
        session=session, book_string=f"{loan.loan_id}/loan/early_close_fee/r"
    )
    assert early_close_balance == Decimal("847.46")

    interest_left_to_accrue = get_interest_left_to_accrue(session, user_loan)
    assert interest_left_to_accrue == Decimal("2608.04")

    assert user_loan.get_remaining_max() == Decimal(0)

    payment_date = parse_date("2020-08-30")
    amount = Decimal(2000)
    payment_request_id = "dummy_reset_fee_3"
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
    payment_ledger_event = (
        session.query(LedgerTriggerEvent)
        .filter(
            LedgerTriggerEvent.name == "payment_received",
            LedgerTriggerEvent.extra_details["payment_request_id"].astext == payment_request_id,
        )
        .first()
    )
    assert payment_ledger_event.amount == amount

    early_closing_fees = (
        session.query(Fee)
        .filter(
            Fee.identifier == "loan",
            Fee.identifier_id == user_loan.loan_id,
            Fee.name == "early_close_fee",
        )
        .order_by(Fee.id)
        .all()
    )
    assert early_closing_fees[0].gross_amount == Decimal("1000")
    assert early_closing_fees[0].fee_status == "PAID"
    assert early_closing_fees[1].gross_amount == Decimal("2000")
    assert early_closing_fees[1].fee_status == "PAID"

    interest_left_to_accrue = get_interest_left_to_accrue(session, user_loan)
    assert interest_left_to_accrue == Decimal("608.04")

    # add min amount for months in between.
    add_min_to_all_bills(session=session, post_date=parse_date("2020-09-01"), user_loan=loan)
    accrue_interest_on_all_bills(session=session, post_date=all_emis[0].due_date, user_loan=user_loan)

    max_amount = user_loan.get_remaining_max()
    assert max_amount == Decimal("300.67")

    accrue_interest_on_all_bills(session=session, post_date=all_emis[1].due_date, user_loan=user_loan)

    max_amount = user_loan.get_remaining_max()
    assert max_amount == Decimal("601.34")

    accrue_interest_on_all_bills(session=session, post_date=all_emis[2].due_date, user_loan=user_loan)
    # Only 6.7 rupee should get accrued because rest went to early charges.
    max_amount = user_loan.get_remaining_max()
    assert max_amount == Decimal("608.04")

    limit_unlock(session=session, loan=loan, amount=Decimal("1000"))

    _, locked_limit = get_account_balance_from_str(
        session=session, book_string=f"{loan.id}/card/locked_limit/l"
    )
    assert locked_limit == Decimal("9000")
    _, locked_limit = get_account_balance_from_str(
        session=session, book_string=f"{loan.id}/card/locked_limit/a"
    )
    assert locked_limit == Decimal("9000")

    _, available_limit = get_account_balance_from_str(
        session=session, book_string=f"{loan.id}/card/available_limit/l"
    )
    assert available_limit == Decimal("0")


def test_reset_journal_entries(session: Session) -> None:
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

    fee = create_loan_fee(
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
    payment_ledger_event = (
        session.query(LedgerTriggerEvent)
        .filter(
            LedgerTriggerEvent.name == "payment_received",
            LedgerTriggerEvent.extra_details["payment_request_id"].astext == payment_request_id,
        )
        .first()
    )
    assert payment_ledger_event.amount == amount
    session.flush()

    loan_creation_data = {"date_str": "2020-08-01", "user_product_id": user_product.id}

    # create loan
    loan = create_test_term_loan(session=session, **loan_creation_data)

    # add min amount for months in between.
    add_min_to_all_bills(session=session, post_date=parse_date("2020-09-01"), user_loan=loan)
    add_min_to_all_bills(session=session, post_date=parse_date("2020-10-01"), user_loan=loan)
    add_min_to_all_bills(session=session, post_date=parse_date("2020-11-01"), user_loan=loan)
    payment_request_data(
        session=session,
        type="collection",
        payment_request_amount=Decimal(1134),
        user_id=6,
        payment_request_id="reset_1",
    )
    payment_date = parse_date("2020-09-07")
    payment_request_id = "reset_1"
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
    assert payment_ledger_event.amount == Decimal(1134)

    session.flush()
    entrys = (
        session.query(JournalEntry)
        .filter(
            JournalEntry.loan_id == user_loan.loan_id,
            JournalEntry.instrument_date == payment_requests_data.payment_received_in_bank_date,
        )
        .all()
    )
    assert len(entrys) == 3
    assert entrys[0].ptype == "TL-Customer"
    assert entrys[1].ptype == "TL-Customer"
    assert entrys[2].ptype == "TL-Customer"
    limit_unlock(session=session, loan=loan, amount=Decimal("1000"))

    min_amount = user_loan.get_remaining_min(date_to_check_against=parse_date("2020-12-01").date())
    assert min_amount == Decimal(3402)

    accrue_late_charges(session, loan, parse_date("2020-10-16"), Decimal(118))
    payment_request_data(
        session=session,
        type="collection",
        payment_request_amount=Decimal(1252),
        user_id=6,
        payment_request_id="reset_2",
    )
    payment_date = parse_date("2020-10-24")
    payment_request_id = "reset_2"
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
    assert payment_ledger_event.amount == Decimal(1252)
    entrys = (
        session.query(JournalEntry)
        .filter(
            JournalEntry.loan_id == user_loan.loan_id,
            JournalEntry.instrument_date == payment_requests_data.payment_received_in_bank_date,
        )
        .all()
    )
    assert len(entrys) == 7
    assert entrys[0].ptype == "TL-Customer"
    assert entrys[1].ptype == "TL-Customer"
    assert entrys[2].ptype == "TL-Customer"
    assert entrys[3].ledger == "CGST" and entrys[3].ptype == "Late Fee-TL-Customer"
    assert entrys[4].ledger == "Late Fee" and entrys[4].ptype == "Late Fee-TL-Customer"
    assert entrys[5].ledger == "SGST" and entrys[5].ptype == "Late Fee-TL-Customer"
    assert entrys[6].narration == "Late Fee" and entrys[6].ptype == "Late Fee-TL-Customer"
    limit_unlock(session=session, loan=loan, amount=Decimal("1000"))

    _, locked_limit = get_account_balance_from_str(
        session=session, book_string=f"{loan.id}/card/locked_limit/l"
    )
    assert locked_limit == Decimal("8000")

    _, available_limit = get_account_balance_from_str(
        session=session, book_string=f"{loan.id}/card/available_limit/l"
    )
    assert available_limit == Decimal("2000")

    refund_date = parse_date("2020-10-24 15:24:34")
    payment_request_id = "reset_2_refund"
    amount = Decimal(100)
    payment_request_data(
        session=session,
        type="collection",
        payment_request_amount=amount,
        user_id=user_loan.user_id,
        payment_request_id=payment_request_id,
    )
    payment_requests_data = pay_payment_request(
        session=session, payment_request_id=payment_request_id, payment_date=refund_date
    )
    refund_payment(session, user_loan, payment_requests_data)

    payment_ledger_event = (
        session.query(LedgerTriggerEvent)
        .filter(
            LedgerTriggerEvent.name == "transaction_refund",
            LedgerTriggerEvent.extra_details["payment_request_id"].astext == payment_request_id,
        )
        .first()
    )
    assert payment_ledger_event.amount == amount

    _, merchant_refund_off_balance = get_account_balance_from_str(
        session, book_string=f"{loan.loan_id}/loan/refund_off_balance/l"
    )
    assert merchant_refund_off_balance == Decimal("100")

    session.flush()
    entrys = (
        session.query(JournalEntry)
        .filter(
            JournalEntry.loan_id == user_loan.loan_id,
            JournalEntry.instrument_date == payment_requests_data.payment_received_in_bank_date,
        )
        .all()
    )
    assert len(entrys) == 3
    assert entrys[0].ptype == "TL-Merchant"
    assert entrys[1].ptype == "TL-Merchant"
    assert entrys[2].ptype == "TL-Merchant"

    amount = user_loan.get_total_outstanding()
    assert amount == 7632

    payment_request_data(
        session=session,
        type="collection",
        payment_request_amount=Decimal(amount),
        user_id=6,
        payment_request_id="reset_3_writeoff",
        collection_by="rc_lender_payment",
        collection_request_id="reset_3_red",
    )
    collection_request_data(
        session=session,
        collection_request_id="reset_3_red",
        amount_paid=Decimal(amount),
        amount_to_pay=Decimal(amount),
        batch_id=loan.id,
    )
    payment_date = parse_date("2021-01-02")
    payment_request_id = "reset_3_writeoff"
    payment_requests_data = pay_payment_request(
        session=session, payment_request_id=payment_request_id, payment_date=payment_date
    )
    payment_received(
        session=session,
        user_loan=user_loan,
        payment_request_data=payment_requests_data,
    )
    session.flush()
    entrys = (
        session.query(JournalEntry)
        .filter(
            JournalEntry.loan_id == user_loan.loan_id,
            JournalEntry.instrument_date == payment_requests_data.payment_received_in_bank_date,
        )
        .all()
    )
    assert len(entrys) == 3
    assert entrys[0].ptype == "TL-Redcarpet"
    assert entrys[1].ptype == "TL-Redcarpet"
    assert entrys[2].ptype == "TL-Redcarpet"

    assert user_loan.loan_status == "WRITTEN_OFF"


def test_reset_journal_entries_kv(session: Session) -> None:
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

    fee = create_loan_fee(
        session=session,
        user_loan=user_loan,
        post_date=parse_date("2018-11-14 00:00:00"),
        gross_amount=Decimal("200"),
        include_gst_from_gross_amount=True,
        fee_name="reset_joining_fees",
    )

    payment_date = parse_date("2018-11-14")
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
    payment_ledger_event = (
        session.query(LedgerTriggerEvent)
        .filter(
            LedgerTriggerEvent.name == "payment_received",
            LedgerTriggerEvent.extra_details["payment_request_id"].astext == payment_request_id,
        )
        .first()
    )
    assert payment_ledger_event.amount == amount
    entrys = (
        session.query(JournalEntry)
        .filter(
            JournalEntry.user_id == user_loan.user_id,
            JournalEntry.instrument_date == payment_requests_data.payment_received_in_bank_date,
        )
        .all()
    )
    assert len(entrys) == 7
    assert entrys[0].ptype == "CF-Customer"
    assert entrys[1].ptype == "CF-Customer"
    assert entrys[2].ptype == "CF-Customer"
    assert entrys[3].ledger == "CGST" and entrys[3].ptype == "CF Processing Fee-Customer"
    assert entrys[4].ledger == "Processing Fee" and entrys[4].ptype == "CF Processing Fee-Customer"
    assert entrys[5].ledger == "SGST" and entrys[5].ptype == "CF Processing Fee-Customer"
    assert entrys[6].narration == "Processing Fee" and entrys[6].ptype == "CF Processing Fee-Customer"

    loan_creation_data = {
        "date_str": "2018-11-14",
        "user_product_id": user_product.id,
        "amount": 12000,
        "interest_rate": 12,
    }

    # create loan
    loan = create_test_term_loan(session=session, **loan_creation_data)
    fee = create_loan_fee(
        session=session,
        user_loan=user_loan,
        post_date=parse_date("2018-11-14 00:00:00"),
        gross_amount=Decimal("600"),
        include_gst_from_gross_amount=True,
        fee_name="card_activation_fees",
    )

    payment_request_data(
        session=session,
        type="collection",
        payment_request_amount=Decimal(2380),
        user_id=6,
        payment_request_id="reset_1",
    )

    payment_date = parse_date("2018-11-14")
    payment_request_id = "reset_1"
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
    assert payment_ledger_event.amount == Decimal(2380)

    session.flush()
    entrys = (
        session.query(JournalEntry)
        .filter(
            JournalEntry.loan_id == user_loan.loan_id,
            JournalEntry.instrument_date == payment_requests_data.payment_received_in_bank_date,
        )
        .all()
    )
    assert len(entrys) == 7
    assert entrys[0].ptype == "TL-Customer"
    assert entrys[1].ptype == "TL-Customer"
    assert entrys[2].ptype == "TL-Customer"
    accrue_late_charges(session, loan, parse_date("2018-12-17"), Decimal(100))
    payment_request_data(
        session=session,
        type="collection",
        payment_request_amount=Decimal(1280),
        user_id=6,
        payment_request_id="reset_2",
    )
    payment_date = parse_date("2018-12-17")
    payment_request_id = "reset_2"
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
    assert payment_ledger_event.amount == Decimal(1280)
    session.flush()
    entrys = (
        session.query(JournalEntry)
        .filter(
            JournalEntry.loan_id == user_loan.loan_id,
            JournalEntry.instrument_date == payment_requests_data.payment_received_in_bank_date,
        )
        .all()
    )

    assert len(entrys) == 7
    assert entrys[0].ptype == "TL-Customer"
    assert entrys[1].ptype == "TL-Customer"
    assert entrys[2].ptype == "TL-Customer"
    assert entrys[3].ledger == "CGST" and entrys[3].ptype == "Late Fee-TL-Customer"
    assert entrys[4].ledger == "Late Fee" and entrys[4].ptype == "Late Fee-TL-Customer"
    assert entrys[5].ledger == "SGST" and entrys[5].ptype == "Late Fee-TL-Customer"
    assert entrys[6].narration == "Late Fee" and entrys[6].ptype == "Late Fee-TL-Customer"

    accrue_late_charges(session, loan, parse_date("2019-03-25"), Decimal(100))
    payment_request_data(
        session=session,
        type="collection",
        payment_request_amount=Decimal(617),
        user_id=6,
        payment_request_id="reset_3",
    )
    payment_date = parse_date("2019-03-25")
    payment_request_id = "reset_3"
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
    assert payment_ledger_event.amount == Decimal(617)
    session.flush()
    entrys = (
        session.query(JournalEntry)
        .filter(
            JournalEntry.loan_id == user_loan.loan_id,
            JournalEntry.instrument_date == payment_requests_data.payment_received_in_bank_date,
        )
        .all()
    )

    assert len(entrys) == 7
    assert entrys[0].ptype == "TL-Customer"
    assert entrys[1].ptype == "TL-Customer"
    assert entrys[2].ptype == "TL-Customer"
    assert entrys[3].ledger == "CGST" and entrys[3].ptype == "Late Fee-TL-Customer"
    assert entrys[4].ledger == "Late Fee" and entrys[4].ptype == "Late Fee-TL-Customer"
    assert entrys[5].ledger == "SGST" and entrys[5].ptype == "Late Fee-TL-Customer"
    assert entrys[6].narration == "Late Fee" and entrys[6].ptype == "Late Fee-TL-Customer"

    accrue_late_charges(session, loan, parse_date("2019-04-12"), Decimal(120))
    accrue_late_charges(session, loan, parse_date("2019-04-12"), Decimal(120))
    CollectionOrders()
    payment_request_data(
        session=session,
        type="collection",
        payment_request_amount=Decimal(user_loan.get_total_outstanding()),
        user_id=6,
        payment_request_id="reset_3_writeoff",
        collection_by="rc_lender_payment",
        collection_request_id="reset_3_redcarpet",
    )
    collection_request_data(
        session=session,
        collection_request_id="reset_3_redcarpet",
        amount_paid=Decimal(amount),
        amount_to_pay=Decimal(amount),
        batch_id=loan.id,
    )
    payment_date = parse_date("2019-04-14")
    payment_request_id = "reset_3_writeoff"
    payment_requests_data = pay_payment_request(
        session=session, payment_request_id=payment_request_id, payment_date=payment_date
    )
    payment_received(
        session=session,
        user_loan=user_loan,
        payment_request_data=payment_requests_data,
    )
    session.flush()
    entrys = (
        session.query(JournalEntry)
        .filter(
            JournalEntry.loan_id == user_loan.loan_id,
            JournalEntry.instrument_date == payment_requests_data.payment_received_in_bank_date,
        )
        .all()
    )

    assert user_loan.loan_status == "WRITTEN_OFF"
    assert len(entrys) == 3
    assert entrys[0].ptype == "TL-Redcarpet"
    assert entrys[1].ptype == "TL-Redcarpet"
    assert entrys[2].ptype == "TL-Redcarpet"

    amount = user_loan.get_total_outstanding()
    payment_request_data(
        session=session,
        type="collection",
        payment_request_amount=amount,
        user_id=6,
        payment_request_id="reset_5",
    )
    payment_date = parse_date("2020-10-20")
    payment_request_id = "reset_5"
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
    assert payment_ledger_event.amount == amount
    session.flush()
    entrys = (
        session.query(JournalEntry)
        .filter(
            JournalEntry.loan_id == user_loan.loan_id,
            JournalEntry.instrument_date == payment_requests_data.payment_received_in_bank_date,
        )
        .all()
    )
    assert len(entrys) == 3
    assert entrys[0].ptype == "TL-Customer"
    assert entrys[1].ptype == "TL-Customer"
    assert entrys[2].ptype == "TL-Customer"

    assert user_loan.loan_status == "RECOVERED"


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

    fee = create_loan_fee(
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
    payment_ledger_event = (
        session.query(LedgerTriggerEvent)
        .filter(
            LedgerTriggerEvent.name == "payment_received",
            LedgerTriggerEvent.extra_details["payment_request_id"].astext == payment_request_id,
        )
        .first()
    )
    assert payment_ledger_event.amount == amount

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

    fee = create_loan_fee(
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
    payment_ledger_event = (
        session.query(LedgerTriggerEvent)
        .filter(
            LedgerTriggerEvent.name == "payment_received",
            LedgerTriggerEvent.extra_details["payment_request_id"].astext == payment_request_id,
        )
        .first()
    )
    assert payment_ledger_event.amount == amount

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


@pytest.mark.run_these_please
def test_blah(session: Session):
    create_lenders(session=session)
    create_products(session=session)
    create_user(session=session)

    user_product = create_user_product_mapping(
        session=session, user_id=6, product_type="term_loan_reset"
    )

    create_loan(session=session, user_product=user_product, lender_id=1756833)

    product_info = {
        "upi": False,
        "card": True,
        "name": "Reset Card",
        "ratio_on": "principal",
        "card_type": "V",
        "lender_id": 1756833,
        "identifier": "96ff91a5caf24e9cac728ea917bf8bd0",
        "description": "",
        "joining_fee": 500,
        "monthly_emi": 1510.0,
        "principal_amount": 11700,
        "task_reopen_date": "2021-04-06T19:06:30.978754",
        "tenure_in_months": 9,
        "total_plan_amount": 13590.0,
        "limit_unlock_ratio": 0.8461000000000001,
        "payment_request_type": "reset_joining_fees",
        "current_disposition_code": "Closed",
        "interest_rate_total_percent": 15.39,
        "interest_rate_monthly_percent": 1.71
    }

    from rush.utils import get_current_ist_time

    loan = create_user_product(
        session=session,
        user_id=69,
        card_type="term_loan_reset",
        lender_id=1756833,
        interest_free_period_in_days=15,
        tenure=product_info["tenure_in_months"],
        amount=Decimal(product_info["principal_amount"]),
        interest_rate=product_info["interest_rate_monthly_percent"],
        product_order_date=get_current_ist_time(),
        user_product_id=user_product.id,
        downpayment_percent=Decimal("0"),
    )