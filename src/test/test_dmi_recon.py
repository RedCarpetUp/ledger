from decimal import Decimal

from dateutil.relativedelta import relativedelta
from pendulum import parse as parse_date  # type: ignore
from sqlalchemy.orm import Session

from rush.accrue_financial_charges import accrue_interest_on_all_bills
from rush.card import (
    BaseCard,
    create_user_card,
)
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
    get_or_create,
)
from rush.payments import payment_received
from rush.recon.revenue_earned import get_revenue_earned_in_a_period


def card_db_updates(session: Session) -> None:
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


def _create_user_raghav_and_do_swipes(session: Session) -> BaseCard:
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
    product = get_or_create(session=session, model=Product, product_name="ruby")
    # Create user's card
    user_card_raghav = create_user_card(
        session=session,
        user_id=user_raghav.id,
        card_activation_date=parse_date("2020-01-01").date(),
        card_type="ruby",
        lender_id=62311,
        kit_number="11111",
        product_id=product.id,
    )

    # Create card swipes.
    swipe_1_raghav = create_card_swipe(
        session=session,
        user_card=user_card_raghav,
        txn_time=parse_date("2020-01-01 14:23:11"),
        amount=Decimal(700),
        description="Amazon.com",
    )
    swipe_2_raghav = create_card_swipe(
        session=session,
        user_card=user_card_raghav,
        txn_time=parse_date("2020-01-02 11:22:11"),
        amount=Decimal(200),
        description="Flipkart.com",
    )
    swipe_3_raghav = create_card_swipe(
        session=session,
        user_card=user_card_raghav,
        txn_time=parse_date("2020-01-15 11:22:11"),
        amount=Decimal(200),
        description="Flipkart.com",
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
        session, f"{user_card_raghav.id}/card/lender_payable/l"
    )
    assert lender_payable_raghav == 1100
    return user_card_raghav


def _create_user_ananth_and_do_swipes(session: Session) -> BaseCard:
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
    product = get_or_create(session=session, model=Product, product_name="ruby")
    # Create user's card
    user_card_ananth = create_user_card(
        session=session,
        user_id=user_ananth.id,
        card_activation_date=parse_date("2020-01-01").date(),
        card_type="ruby",
        lender_id=62311,
        kit_number="00000",
        product_id=product.id,
    )

    # Create card swipes.
    swipe_1_ananth = create_card_swipe(
        session=session,
        user_card=user_card_ananth,
        txn_time=parse_date("2020-01-15 14:23:11"),
        amount=Decimal(1000),
        description="Amazon.com",
    )
    swipe_2_ananth = create_card_swipe(
        session=session,
        user_card=user_card_ananth,
        txn_time=parse_date("2020-01-12 11:22:11"),
        amount=Decimal(5000),
        description="Flipkart.com",
    )
    swipe_3_ananth = create_card_swipe(
        session=session,
        user_card=user_card_ananth,
        txn_time=parse_date("2020-01-25 11:22:11"),
        amount=Decimal("500.75"),
        description="Flipkart.com",
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
        session, f"{user_card_ananth.id}/card/lender_payable/l"
    )
    assert lender_payable_ananth == Decimal("6500.75")
    return user_card_ananth


def test_dmi_recon_process_1(session: Session) -> None:
    test_lenders(session)
    card_db_updates(session)
    user_card_raghav = _create_user_raghav_and_do_swipes(session)
    user_card_ananth = _create_user_ananth_and_do_swipes(session)

    # Incur interest for last month's period.
    lender_interest_incur(
        session, from_date=parse_date("2020-01-01").date(), to_date=parse_date("2020-01-31").date()
    )

    _, lender_payable_raghav = get_account_balance_from_str(
        session, f"{user_card_raghav.id}/card/lender_payable/l"
    )
    assert lender_payable_raghav == Decimal("1114.90")

    _, lender_payable_ananth = get_account_balance_from_str(
        session, f"{user_card_ananth.id}/card/lender_payable/l"
    )
    assert lender_payable_ananth == Decimal("6557.24")

    # We generate the two bills.
    bill_raghav = bill_generate(user_card=user_card_raghav)
    bill_ananth = bill_generate(user_card=user_card_ananth)

    _, billed_amount_raghav = get_account_balance_from_str(
        session, book_string=f"{bill_raghav['bill'].id}/bill/principal_receivable/a"
    )
    _, billed_amount_ananth = get_account_balance_from_str(
        session, book_string=f"{bill_ananth['bill'].id}/bill/principal_receivable/a"
    )
    assert billed_amount_raghav == 1100
    assert billed_amount_ananth == Decimal("6500.75")

    # Revenue earned is 0 because there's no payment yet.
    from_date = parse_date("2020-02-01").date()
    to_date = parse_date("2020-02-28").date()
    revenue_earned = get_revenue_earned_in_a_period(session, from_date=from_date, to_date=to_date)
    assert revenue_earned == 0

    # Some payment comes
    payment_received(
        session=session,
        user_card=user_card_raghav,
        payment_amount=200,
        payment_date=parse_date("2020-02-10 15:23:20"),
        payment_request_id="r23gs23",
    )
    payment_received(
        session=session,
        user_card=user_card_ananth,
        payment_amount=500,
        payment_date=parse_date("2020-02-13 15:33:20"),
        payment_request_id="r23gs23",
    )

    # Got adjusted in principal because interest is not accrued yet.
    _, billed_amount_raghav = get_account_balance_from_str(
        session, book_string=f"{bill_raghav['bill'].id}/bill/principal_receivable/a"
    )
    _, billed_amount_ananth = get_account_balance_from_str(
        session, book_string=f"{bill_ananth['bill'].id}/bill/principal_receivable/a"
    )
    assert billed_amount_raghav == 900
    assert billed_amount_ananth == Decimal("6000.75")

    accrue_interest_on_all_bills(
        session, bill_raghav["bill"].table.bill_due_date + relativedelta(days=1), user_card_raghav
    )
    accrue_interest_on_all_bills(
        session, bill_ananth["bill"].table.bill_due_date + relativedelta(days=1), user_card_ananth
    )

    # Check for interest accrued
    _, interest_due_raghav = get_account_balance_from_str(
        session, book_string=f"{bill_raghav['bill'].id}/bill/interest_receivable/a"
    )
    _, interest_due_ananth = get_account_balance_from_str(
        session, book_string=f"{bill_ananth['bill'].id}/bill/interest_receivable/a"
    )
    assert interest_due_raghav == Decimal("33.33")
    assert interest_due_ananth == Decimal("195.27")

    # Payment came after interest has been accrued.
    payment_received(
        session=session,
        user_card=user_card_raghav,
        payment_amount=100,
        payment_date=parse_date("2020-02-26 15:23:20"),
        payment_request_id="r23gs23",
    )
    payment_received(
        session=session,
        user_card=user_card_ananth,
        payment_amount=50,
        payment_date=parse_date("2020-02-25 15:33:20"),
        payment_request_id="r23gs23",
    )

    _, interest_due_raghav = get_account_balance_from_str(
        session, book_string=f"{bill_raghav['bill'].id}/bill/interest_receivable/a"
    )
    _, interest_due_ananth = get_account_balance_from_str(
        session, book_string=f"{bill_ananth['bill'].id}/bill/interest_receivable/a"
    )
    assert interest_due_raghav == 0
    assert interest_due_ananth == Decimal("145.27")

    # Now that we've received the payment, we check how much revenue we've made.
    revenue_earned = get_revenue_earned_in_a_period(session, from_date=from_date, to_date=to_date)
    assert revenue_earned == Decimal("83.33")

    # Incur interest for this month's period.
    lender_interest_incur(session, from_date=from_date, to_date=to_date)

    _, lender_payable_raghav = get_account_balance_from_str(
        session, f"{user_card_raghav.id}/card/lender_payable/l"
    )
    assert lender_payable_raghav == Decimal("829.51")