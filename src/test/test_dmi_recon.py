from decimal import Decimal
from test.test_current import (
    pay_payment_request,
    payment_request_data,
)

from dateutil.relativedelta import relativedelta
from pendulum import parse as parse_date  # type: ignore
from sqlalchemy.orm import Session

from rush.accrue_financial_charges import accrue_interest_on_all_bills
from rush.card import create_user_product
from rush.card.base_card import BaseLoan
from rush.create_bill import bill_generate
from rush.create_card_swipe import create_card_swipe
from rush.ledger_utils import get_account_balance_from_str
from rush.lender_funds import lender_interest_incur
from rush.models import (
    CardKitNumbers,
    CardNames,
    Lenders,
    Product,
    User,
)
from rush.payments import payment_received
from rush.recon.revenue_earned import get_revenue_earned_in_a_period


def create_products(session: Session) -> None:
    ruby_product = Product(product_name="ruby")
    session.add(ruby_product)
    session.flush()


def card_db_updates(session: Session) -> None:
    create_products(session=session)

    cn = CardNames(name="ruby")
    session.add(cn)
    session.flush()
    ckn = CardKitNumbers(kit_number="00000", card_name_id=cn.id, last_5_digits="0000", status="active")
    session.add(ckn)
    session.flush()

    ckn = CardKitNumbers(kit_number="11111", card_name_id=cn.id, last_5_digits="0000", status="active")
    session.add(ckn)
    session.flush()


def test_lenders(session: Session) -> None:
    l1 = Lenders(id=62311, performed_by=123, lender_name="DMI")
    session.add(l1)
    l2 = Lenders(id=1756833, performed_by=123, lender_name="Redux")
    session.add(l2)
    session.flush()
    a = session.query(Lenders).first()


def _create_user_raghav_and_do_swipes(session: Session) -> BaseLoan:
    # Create user
    user_raghav = User(
        performed_by=123,
        id=40,
        # name="Raghav",
        # fullname="Raghav S",
        # nickname="Rag",
        # email="asas@gmailc.om",
    )
    session.add(user_raghav)
    session.flush()

    # Create user's card
    user_loan_raghav = create_user_product(
        session=session,
        user_id=user_raghav.id,
        card_activation_date=parse_date("2020-01-01").date(),
        card_type="ruby",
        rc_rate_of_interest_monthly=Decimal(3),
        lender_id=62311,
        kit_number="11111",
    )

    # Create card swipes.
    swipe_1_raghav = create_card_swipe(
        session=session,
        user_loan=user_loan_raghav,
        txn_time=parse_date("2020-01-01 14:23:11"),
        amount=Decimal(700),
        description="Amazon.com",
        txn_ref_no="dummy_txn_ref_no",
        trace_no="123456",
    )
    swipe_2_raghav = create_card_swipe(
        session=session,
        user_loan=user_loan_raghav,
        txn_time=parse_date("2020-01-02 11:22:11"),
        amount=Decimal(200),
        description="Flipkart.com",
        txn_ref_no="dummy_txn_ref_no",
        trace_no="123456",
    )
    swipe_3_raghav = create_card_swipe(
        session=session,
        user_loan=user_loan_raghav,
        txn_time=parse_date("2020-01-15 11:22:11"),
        amount=Decimal(200),
        description="Flipkart.com",
        txn_ref_no="dummy_txn_ref_no",
        trace_no="123456",
    )
    assert (
        swipe_1_raghav["data"].loan_id
        == swipe_2_raghav["data"].loan_id
        == swipe_3_raghav["data"].loan_id
    )  # belong to same bill.
    _, unbilled_bal_raghav_bill_1 = get_account_balance_from_str(
        session, f"{swipe_1_raghav['data'].loan_id}/bill/unbilled/a"
    )
    assert unbilled_bal_raghav_bill_1 == 1100

    _, lender_payable_raghav = get_account_balance_from_str(
        session, f"{user_loan_raghav.loan_id}/loan/lender_payable/l"
    )
    assert lender_payable_raghav == 1100
    return user_loan_raghav


def _create_user_ananth_and_do_swipes(session: Session) -> BaseLoan:
    # Create user
    user_ananth = User(
        performed_by=123,
        id=30,
        # name="Ananth",
        # fullname="Ananth V",
        # nickname="Ant",
        # email="ananth@gmailc.om",
    )
    session.add(user_ananth)
    session.flush()

    # Create user's card
    user_loan_ananth = create_user_product(
        session=session,
        user_id=user_ananth.id,
        card_activation_date=parse_date("2020-01-01").date(),
        card_type="ruby",
        rc_rate_of_interest_monthly=Decimal(3),
        lender_id=62311,
        kit_number="00000",
    )

    # Create card swipes.
    swipe_1_ananth = create_card_swipe(
        session=session,
        user_loan=user_loan_ananth,
        txn_time=parse_date("2020-01-15 14:23:11"),
        amount=Decimal(1000),
        description="Amazon.com",
        txn_ref_no="dummy_txn_ref_no",
        trace_no="123456",
    )
    swipe_2_ananth = create_card_swipe(
        session=session,
        user_loan=user_loan_ananth,
        txn_time=parse_date("2020-01-12 11:22:11"),
        amount=Decimal(5000),
        description="Flipkart.com",
        txn_ref_no="dummy_txn_ref_no",
        trace_no="123456",
    )
    swipe_3_ananth = create_card_swipe(
        session=session,
        user_loan=user_loan_ananth,
        txn_time=parse_date("2020-01-25 11:22:11"),
        amount=Decimal("500.75"),
        description="Flipkart.com",
        txn_ref_no="dummy_txn_ref_no",
        trace_no="123456",
    )
    assert (
        swipe_1_ananth["data"].loan_id
        == swipe_2_ananth["data"].loan_id
        == swipe_3_ananth["data"].loan_id
    )  # belong to same bill.
    _, unbilled_bal_ananth_bill_1 = get_account_balance_from_str(
        session, f"{swipe_1_ananth['data'].loan_id}/bill/unbilled/a"
    )
    assert unbilled_bal_ananth_bill_1 == Decimal("6500.75")

    _, lender_payable_ananth = get_account_balance_from_str(
        session, f"{user_loan_ananth.loan_id}/loan/lender_payable/l"
    )
    assert lender_payable_ananth == Decimal("6500.75")
    return user_loan_ananth


def test_dmi_recon_process_1(session: Session) -> None:
    test_lenders(session)
    card_db_updates(session)
    user_loan_raghav = _create_user_raghav_and_do_swipes(session)
    user_loan_ananth = _create_user_ananth_and_do_swipes(session)

    # Incur interest for last month's period.
    lender_interest_incur(
        session, from_date=parse_date("2020-01-01").date(), to_date=parse_date("2020-01-31").date()
    )

    _, lender_payable_raghav = get_account_balance_from_str(
        session, f"{user_loan_raghav.loan_id}/loan/lender_payable/l"
    )
    assert lender_payable_raghav == Decimal("1114.90")

    _, lender_payable_ananth = get_account_balance_from_str(
        session, f"{user_loan_ananth.loan_id}/loan/lender_payable/l"
    )
    assert lender_payable_ananth == Decimal("6558.73")  # This is incorrect. Earlier it was 6557.24.

    # We generate the two bills.
    bill_raghav = bill_generate(user_loan=user_loan_raghav)
    bill_ananth = bill_generate(user_loan=user_loan_ananth)

    _, billed_amount_raghav = get_account_balance_from_str(
        session, book_string=f"{bill_raghav.id}/bill/principal_receivable/a"
    )
    _, billed_amount_ananth = get_account_balance_from_str(
        session, book_string=f"{bill_ananth.id}/bill/principal_receivable/a"
    )
    assert billed_amount_raghav == 1100
    assert billed_amount_ananth == Decimal("6500.75")

    # Revenue earned is 0 because there's no payment yet.
    from_date = parse_date("2020-02-01").date()
    to_date = parse_date("2020-02-28").date()
    revenue_earned = get_revenue_earned_in_a_period(session, from_date=from_date, to_date=to_date)
    assert revenue_earned == 0

    # Some payment comes
    payment_date = parse_date("2020-02-10 15:23:20")
    payment_request_id = "r23gs23"
    amount = Decimal(200)
    payment_request_data(
        session=session,
        type="collection",
        payment_request_amount=amount,
        user_id=user_loan_raghav.user_id,
        payment_request_id=payment_request_id,
    )
    pay_payment_request(
        session=session,
        amount=amount,
        payment_request_id=payment_request_id,
    )
    payment_received(
        session=session,
        user_loan=user_loan_raghav,
        payment_amount=amount,
        payment_date=payment_date,
        payment_request_id=payment_request_id,
    )

    payment_date = parse_date("2020-02-13 15:23:20")
    payment_request_id = "r23gs24"
    amount = Decimal(500)
    payment_request_data(
        session=session,
        type="collection",
        payment_request_amount=amount,
        user_id=user_loan_ananth.user_id,
        payment_request_id=payment_request_id,
    )
    pay_payment_request(
        session=session,
        amount=amount,
        payment_request_id=payment_request_id,
    )
    payment_received(
        session=session,
        user_loan=user_loan_ananth,
        payment_amount=amount,
        payment_date=payment_date,
        payment_request_id=payment_request_id,
    )

    # Got adjusted in principal because interest is not accrued yet.
    _, billed_amount_raghav = get_account_balance_from_str(
        session, book_string=f"{bill_raghav.id}/bill/principal_receivable/a"
    )
    _, billed_amount_ananth = get_account_balance_from_str(
        session, book_string=f"{bill_ananth.id}/bill/principal_receivable/a"
    )
    assert billed_amount_raghav == 900
    assert billed_amount_ananth == Decimal("6000.75")

    accrue_interest_on_all_bills(
        session, bill_raghav.table.bill_due_date + relativedelta(days=1), user_loan_raghav
    )
    accrue_interest_on_all_bills(
        session, bill_ananth.table.bill_due_date + relativedelta(days=1), user_loan_ananth
    )

    # Check for interest accrued
    _, interest_due_raghav = get_account_balance_from_str(
        session, book_string=f"{bill_raghav.id}/bill/interest_receivable/a"
    )
    _, interest_due_ananth = get_account_balance_from_str(
        session, book_string=f"{bill_ananth.id}/bill/interest_receivable/a"
    )
    assert interest_due_raghav == Decimal("33.33")
    assert interest_due_ananth == Decimal("195.27")

    # Payment came after interest has been accrued.
    payment_date = parse_date("2020-02-26 15:23:20")
    payment_request_id = "r23gs25"
    amount = Decimal(100)
    payment_request_data(
        session=session,
        type="collection",
        payment_request_amount=amount,
        user_id=user_loan_raghav.user_id,
        payment_request_id=payment_request_id,
    )
    pay_payment_request(
        session=session,
        amount=amount,
        payment_request_id=payment_request_id,
    )
    payment_received(
        session=session,
        user_loan=user_loan_raghav,
        payment_amount=amount,
        payment_date=payment_date,
        payment_request_id=payment_request_id,
    )

    payment_date = parse_date("2020-02-25 15:23:20")
    payment_request_id = "r23gs26"
    amount = Decimal(50)
    payment_request_data(
        session=session,
        type="collection",
        payment_request_amount=amount,
        user_id=user_loan_ananth.user_id,
        payment_request_id=payment_request_id,
    )
    pay_payment_request(
        session=session,
        amount=amount,
        payment_request_id=payment_request_id,
    )
    payment_received(
        session=session,
        user_loan=user_loan_ananth,
        payment_amount=amount,
        payment_date=payment_date,
        payment_request_id=payment_request_id,
    )

    _, interest_due_raghav = get_account_balance_from_str(
        session, book_string=f"{bill_raghav.id}/bill/interest_receivable/a"
    )
    _, interest_due_ananth = get_account_balance_from_str(
        session, book_string=f"{bill_ananth.id}/bill/interest_receivable/a"
    )
    assert interest_due_raghav == 0
    assert interest_due_ananth == Decimal("145.27")

    # Now that we've received the payment, we check how much revenue we've made.
    revenue_earned = get_revenue_earned_in_a_period(session, from_date=from_date, to_date=to_date)
    assert revenue_earned == Decimal("83.33")

    # Incur interest for this month's period.
    lender_interest_incur(session, from_date=from_date, to_date=to_date)

    _, lender_payable_raghav = get_account_balance_from_str(
        session, f"{user_loan_raghav.loan_id}/loan/lender_payable/l"
    )
    assert lender_payable_raghav == Decimal("829.82")
