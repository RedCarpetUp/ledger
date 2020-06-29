import contextlib
import json
from decimal import Decimal
from io import StringIO

import alembic
from alembic.command import current as alembic_current
from pendulum import parse as parse_date  # type: ignore
from sqlalchemy.orm import Session

from rush.accrue_financial_charges import accrue_late_charges
from rush.anomaly_detection import run_anomaly
from rush.create_bill import bill_generate
from rush.create_card_swipe import create_card_swipe
from rush.create_emi import (
    create_emis_for_card,
    refresh_schedule,
)
from rush.ledger_utils import (
    get_account_balance_from_str,
    get_all_unpaid_bills,
    is_bill_closed,
)
from rush.lender_funds import (
    lender_disbursal,
    m2p_transfer,
)
from rush.models import (
    CardEmis,
    LedgerTriggerEvent,
    LoanData,
    User,
    UserCard,
    UserPy,
)
from rush.payments import payment_received
from rush.views import (
    bill_view,
    transaction_view,
)


def test_current(get_alembic: alembic.config.Config) -> None:
    """Test that the alembic current command does not erorr"""
    # Runs with no error
    # output = run_alembic_command(pg["engine"], "current", {})

    stdout = StringIO()
    with contextlib.redirect_stdout(stdout):
        # command_func(alembic_cfg, **command_kwargs)
        alembic_current(get_alembic, {})
    assert stdout.getvalue() == ""
    # assert output == ""


def test_user2(session: Session) -> None:
    u = User(performed_by=123, id=1, name="dfd", fullname="dfdf", nickname="dfdd", email="asas",)
    session.add(u)
    session.commit()
    a = session.query(User).first()
    print(a.id)
    u = UserPy(id=a.id, performed_by=123, email="sss", name="dfd", fullname="dfdf", nickname="dfdd",)


def test_user(session: Session) -> None:
    u = User(id=2, performed_by=123, name="dfd", fullname="dfdf", nickname="dfdd", email="asas",)
    session.add(u)
    session.commit()
    a = session.query(User).first()
    u = UserPy(id=a.id, performed_by=123, email="sss", name="dfd", fullname="dfdf", nickname="dfdd",)


def test_lender_disbursal(session: Session) -> None:
    amount = 100000
    val = lender_disbursal(session, amount)
    assert val == Decimal(100000)


def test_m2p_transfer(session: Session) -> None:
    amount = 50000
    val = m2p_transfer(session, amount)
    assert val == Decimal(50000)


def test_card_swipe(session: Session) -> None:
    uc = UserCard(user_id=2, card_activation_date=parse_date("2020-05-01"))
    session.add(uc)
    session.flush()
    user_card_id = uc.id

    swipe1 = create_card_swipe(
        session=session,
        user_card=uc,
        txn_time=parse_date("2020-05-01 14:23:11"),
        amount=Decimal(700),
        description="Amazon.com",
    )
    swipe2 = create_card_swipe(
        session=session,
        user_card=uc,
        txn_time=parse_date("2020-05-02 11:22:11"),
        amount=Decimal(200),
        description="Flipkart.com",
    )
    assert swipe1.loan_id == swipe2.loan_id
    bill_id = swipe1.loan_id

    _, unbilled_balance = get_account_balance_from_str(session, f"{bill_id}/bill/unbilled/a")
    assert unbilled_balance == 900
    # remaining card balance should be -900 because we've not loaded it yet and it's going in negative.
    _, card_balance = get_account_balance_from_str(session, f"{user_card_id}/card/available_limit/l")
    assert card_balance == -900

    _, lender_payable = get_account_balance_from_str(session, f"{user_card_id}/card/lender_payable/l")
    assert lender_payable == 900


def test_generate_bill_1(session: Session) -> None:
    a = User(id=99, performed_by=123, name="dfd", fullname="dfdf", nickname="dfdd", email="asas",)
    session.add(a)
    session.flush()

    # assign card
    uc = UserCard(user_id=a.id, card_activation_date=parse_date("2020-04-02"))
    session.add(uc)
    session.flush()

    user_card_id = uc.id

    swipe = create_card_swipe(
        session=session,
        user_card=uc,
        txn_time=parse_date("2020-04-08 19:23:11"),
        amount=Decimal(1000),
        description="BigBasket.com",
    )
    bill_id = swipe.loan_id

    _, unbilled_amount = get_account_balance_from_str(session, book_string=f"{bill_id}/bill/unbilled/a")
    assert unbilled_amount == 1000

    bill = bill_generate(session=session, user_card=uc)

    assert bill.is_generated is True

    _, unbilled_amount = get_account_balance_from_str(session, book_string=f"{bill_id}/bill/unbilled/a")
    assert unbilled_amount == 0  # Should be 0 because it has moved to billed account.

    _, billed_amount = get_account_balance_from_str(
        session, book_string=f"{bill_id}/bill/principal_receivable/a"
    )
    assert billed_amount == 1000

    _, min_amount = get_account_balance_from_str(session, book_string=f"{bill_id}/bill/min/a")
    assert min_amount == Decimal("113.33")

    _, interest_due = get_account_balance_from_str(
        session, book_string=f"{bill_id}/bill/interest_receivable/a"
    )
    assert interest_due == 30


def _partial_payment_bill_1(session: Session) -> None:
    user_card = session.query(UserCard).filter(UserCard.user_id == 99).one()
    payment_date = parse_date("2020-05-03")
    amount = Decimal(100)
    unpaid_bills = get_all_unpaid_bills(session, user_card.user_id)
    payment_received(
        session=session, user_card=user_card, payment_amount=amount, payment_date=payment_date,
    )

    bill = unpaid_bills[0]
    _, interest_due = get_account_balance_from_str(
        session, book_string=f"{bill.id}/bill/interest_receivable/a"
    )
    assert interest_due == 0

    _, principal_due = get_account_balance_from_str(
        session, book_string=f"{bill.id}/bill/principal_receivable/a"
    )
    assert principal_due == 930

    min_due = bill.get_minimum_amount_to_pay(session)
    assert min_due == Decimal("13.33")


def _min_payment_delayed_bill_1(session: Session) -> None:
    user = session.query(User).filter(User.id == 99).one()
    payment_date = parse_date("2020-05-03")
    amount = Decimal(130)
    bill = payment_received(
        session=session, user_id=user.id, payment_amount=amount, payment_date=payment_date,
    )

    _, principal_due = get_account_balance_from_str(
        session, book_string=f"{bill.id}/bill/principal_receivable/a"
    )
    # payment got late and 100 rupees got settled in late fine.
    assert principal_due == 970


def test_partial_payment_bill_1(session: Session) -> None:
    test_generate_bill_1(session)
    _partial_payment_bill_1(session)


def _accrue_late_fine_bill_1(session: Session) -> None:
    user_card = session.query(UserCard).filter(UserCard.user_id == 99).one()
    event_date = parse_date("2020-05-16 00:00:00")
    bill = accrue_late_charges(session, user_card, event_date)

    _, late_fine_due = get_account_balance_from_str(session, f"{bill.id}/bill/late_fine_receivable/a")
    assert late_fine_due == Decimal(100)

    min_due = bill.get_minimum_amount_to_pay(session)
    assert min_due == Decimal("113.33")


def test_accrue_late_fine_bill_1(session: Session) -> None:
    test_generate_bill_1(session)
    # did only partial payment so accrue late fee.
    _partial_payment_bill_1(session)
    _accrue_late_fine_bill_1(session)


def _pay_minimum_amount_bill_1(session: Session) -> None:
    user_card = session.query(UserCard).filter(UserCard.user_id == 99).one()

    unpaid_bills = get_all_unpaid_bills(session, user_card.user_id)

    # Pay 13.33 more. and 100 for late fee.
    payment_received(
        session=session,
        user_card=user_card,
        payment_amount=Decimal("113.33"),
        payment_date=parse_date("2020-05-20"),
    )
    bill = unpaid_bills[0]
    # assert is_min_paid(session, bill) is True
    min_due = bill.get_minimum_amount_to_pay(session)
    assert min_due == Decimal(0)

    _, late_fine_due = get_account_balance_from_str(session, f"{bill.id}/bill/late_fine_receivable/a")
    assert late_fine_due == Decimal(0)

    _, late_fine_due = get_account_balance_from_str(session, f"{bill.id}/bill/late_fine/r")
    assert late_fine_due == Decimal(100)

    _, principal_due = get_account_balance_from_str(
        session, book_string=f"{bill.id}/bill/principal_receivable/a"
    )
    # payment got late and 100 rupees got settled in late fine.
    assert principal_due == Decimal("916.67")


def test_accrue_interest_bill_1(session: Session) -> None:
    test_generate_bill_1(session)
    _partial_payment_bill_1(session)
    _accrue_late_fine_bill_1(session)
    _pay_minimum_amount_bill_1(session)


def test_late_fee_reversal_bill_1(session: Session) -> None:
    test_generate_bill_1(session)
    _partial_payment_bill_1(session)
    _accrue_late_fine_bill_1(session)

    user_card = session.query(UserCard).filter(UserCard.user_id == 99).one()

    unpaid_bills = get_all_unpaid_bills(session, user_card.user_id)

    # Pay 13.33 more. and 100 for late fee.
    payment_received(
        session=session,
        user_card=user_card,
        payment_amount=Decimal("113.33"),
        payment_date=parse_date("2020-05-14"),  # Payment came before the due date.
    )
    bill = unpaid_bills[0]
    # assert is_min_paid(session, bill) is True
    min_due = bill.get_minimum_amount_to_pay(session)
    assert min_due == Decimal(0)

    _, late_fine_due = get_account_balance_from_str(session, f"{bill.id}/bill/late_fine_receivable/a")
    assert late_fine_due == Decimal(0)

    _, late_fine_due = get_account_balance_from_str(session, f"{bill.id}/bill/late_fine/r")
    assert late_fine_due == Decimal(0)

    _, principal_due = get_account_balance_from_str(
        session, book_string=f"{bill.id}/bill/principal_receivable/a"
    )
    # payment got late and 100 rupees got settled in late fine.
    assert principal_due == Decimal("816.67")


def test_is_bill_paid_bill_1(session: Session) -> None:
    test_generate_bill_1(session)
    _partial_payment_bill_1(session)
    _accrue_late_fine_bill_1(session)
    _pay_minimum_amount_bill_1(session)

    user_card = session.query(UserCard).filter(UserCard.user_id == 99).one()

    bill = (
        session.query(LoanData)
        .filter(LoanData.user_id == user_card.user_id)
        .order_by(LoanData.agreement_date.desc())
        .first()
    )
    # Should be false because min is 130 and payment made is 120
    is_it_paid = is_bill_closed(session, bill)
    assert is_it_paid is False

    # Need to pay 916.67 more to close the bill.
    remaining_principal = Decimal("916.67")
    payment_received(
        session=session,
        user_card=user_card,
        payment_amount=remaining_principal,
        payment_date=parse_date("2020-05-05"),
    )
    is_it_paid_now = is_bill_closed(session, bill)
    assert is_it_paid_now is True


def _generate_bill_2(session: Session) -> None:
    user = session.query(User).filter(User.id == 99).one()
    uc = session.query(UserCard).filter_by(user_id=user.id).one()

    previous_bill = (  # get last generated bill.
        session.query(LoanData)
        .filter(LoanData.user_id == user.id, LoanData.is_generated.is_(True))
        .order_by(LoanData.agreement_date.desc())
        .first()
    )
    # Bill shouldn't be closed.
    assert is_bill_closed(session, previous_bill) is False

    # Do transaction to create new bill.
    create_card_swipe(
        session=session,
        user_card=uc,
        txn_time=parse_date("2020-05-08 19:23:11"),
        amount=Decimal(2000),
        description="BigBasket.com",
    )

    _, user_card_balance = get_account_balance_from_str(
        session=session, book_string=f"{uc.id}/card/available_limit/a"
    )
    assert user_card_balance == Decimal(-3000)

    bill_2 = bill_generate(session=session, user_card=uc)

    unpaid_bills = get_all_unpaid_bills(session, user.id)
    assert len(unpaid_bills) == 2

    _, principal_due = get_account_balance_from_str(
        session=session, book_string=f"{bill_2.id}/bill/principal_receivable/a"
    )
    assert principal_due == Decimal(2000)

    min_due = bill_2.get_minimum_amount_to_pay(session)
    assert min_due == Decimal("226.67")

    _, interest_due = get_account_balance_from_str(
        session, book_string=f"{bill_2.id}/bill/interest_receivable/a"
    )
    assert interest_due == 60

    first_bill = unpaid_bills[0]
    first_bill_min_due = first_bill.get_minimum_amount_to_pay(session)
    assert first_bill_min_due == Decimal("113.33")

    _, interest_due = get_account_balance_from_str(
        session, book_string=f"{first_bill.id}/bill/interest_receivable/a"
    )
    assert interest_due == 30


def _run_anomaly_bill_1(session: Session) -> None:
    user = session.query(User).filter(User.id == 99).one()

    bill = (
        session.query(LoanData)
        .filter(LoanData.user_id == user.id)
        .order_by(LoanData.agreement_date.desc())
        .first()
    )
    run_anomaly(session, bill)

    _, late_fine_due = get_account_balance_from_str(session, f"{bill.id}/bill/late_fine_receivable/a")
    assert late_fine_due == Decimal(0)

    _, late_fee_received = get_account_balance_from_str(
        session, book_string=f"{bill.id}/bill/late_fee_received/a"
    )
    assert late_fee_received == Decimal(0)

    _, principal_due = get_account_balance_from_str(
        session, book_string=f"{bill.id}/bill/principal_receivable/a"
    )
    # payment got moved from late received to principal received.
    assert principal_due == 970 - 100


# def test_anomaly_late_payment_received(session: Session) -> None:
#     test_generate_bill_1(session)
#     _accrue_late_fine_bill_1(session)  # Accrue late fine first.
#     _min_payment_delayed_bill_1(session)  # Payment comes in our system late.
#     _run_anomaly_bill_1(session)
#     _accrue_interest_bill_1(session)


def test_generate_bill_2(session: Session) -> None:
    test_generate_bill_1(session)
    _partial_payment_bill_1(session)
    _accrue_late_fine_bill_1(session)
    _pay_minimum_amount_bill_1(session)
    _generate_bill_2(session)


def test_emi_creation(session: Session) -> None:
    a = User(id=108, performed_by=123, name="dfd", fullname="dfdf", nickname="dfdd", email="asas",)
    session.add(a)

    # assign card
    uc = UserCard(user_id=a.id, card_activation_date=parse_date("2020-04-02"))
    session.flush()
    session.add(uc)

    create_card_swipe(
        session=session,
        user_card=uc,
        txn_time=parse_date("2020-04-08 19:23:11"),
        amount=Decimal(6000),
        description="BigBasket.com",
    )

    # Generate bill
    bill_april = bill_generate(session=session, user_card=uc)

    all_emis = (
        session.query(CardEmis)
        .filter(CardEmis.card_id == uc.id, CardEmis.row_status == "active")
        .order_by(CardEmis.due_date.asc())
        .all()
    )  # Get the latest emi of that user.

    last_emi = all_emis[11]
    assert last_emi.emi_number == 12


def test_subsequent_emi_creation(session: Session) -> None:
    a = User(id=160, performed_by=123, name="dfd", fullname="dfdf", nickname="dfdd", email="asas",)
    session.add(a)

    # assign card
    uc = UserCard(user_id=a.id, card_activation_date=parse_date("2020-04-02"))
    session.flush()
    session.add(uc)

    create_card_swipe(
        session=session,
        user_card=uc,
        txn_time=parse_date("2020-04-08 19:23:11"),
        amount=Decimal(6000),
        description="BigBasket.com",
    )

    generate_date = parse_date("2020-05-01").date()
    bill_april = bill_generate(session=session, user_card=uc)

    create_card_swipe(
        session=session,
        user_card=uc,
        txn_time=parse_date("2020-05-08 19:23:11"),
        amount=Decimal(6000),
        description="BigBasket.com",
    )

    generate_date = parse_date("2020-06-01").date()
    bill_may = bill_generate(session=session, user_card=uc)

    all_emis = (
        session.query(CardEmis)
        .filter(CardEmis.card_id == uc.id, CardEmis.row_status == "active")
        .order_by(CardEmis.due_date.asc())
        .all()
    )  # Get the latest emi of that user.

    last_emi = all_emis[12]
    first_emi = all_emis[0]
    second_emi = all_emis[1]
    assert first_emi.due_amount == 500
    assert last_emi.due_amount == 500
    assert second_emi.due_amount == 1000
    assert last_emi.emi_number == 13
    assert last_emi.due_date.strftime("%Y-%m-%d") == "2021-05-25"


def test_view(session: Session) -> None:
    test_generate_bill_1(session)
    _partial_payment_bill_1(session)
    _accrue_late_fine_bill_1(session)
    _pay_minimum_amount_bill_1(session)
    _generate_bill_2(session)
    user = session.query(User).filter(User.id == 99).one()

    json_value = bill_view(session, user.id)
    assert json.loads(json_value)
    bill = session.query(LoanData).filter(LoanData.user_id == user.id).first()
    json_value = transaction_view(session, bill_id=bill.id)
    # assert json.loads(json_value)


def test_refresh_schedule(session: Session) -> None:
    a = User(id=2005, performed_by=123, name="dfd", fullname="dfdf", nickname="dfdd", email="asas",)
    session.add(a)

    # assign card
    uc = UserCard(user_id=a.id, card_activation_date=parse_date("2020-04-02"))
    session.flush()
    session.add(uc)

    create_card_swipe(
        session=session,
        user_card=uc,
        txn_time=parse_date("2020-04-08 19:23:11"),
        amount=Decimal(6000),
        description="BigBasket.com",
    )

    generate_date = parse_date("2020-05-01").date()
    bill_april = bill_generate(session=session, user_card=uc)

    # Update later
    assert a.id == 2005


def test_schedule_for_interest_and_payment(session: Session) -> None:
    a = User(id=1991, performed_by=123, name="dfd", fullname="dfdf", nickname="dfdd", email="asas",)
    session.add(a)

    # assign card
    uc = UserCard(user_id=a.id, card_activation_date=parse_date("2020-05-01"))
    session.flush()
    session.add(uc)

    create_card_swipe(
        session=session,
        user_card=uc,
        txn_time=parse_date("2020-05-08 19:23:11"),
        amount=Decimal(6000),
        description="BigBasket.com",
    )

    generate_date = parse_date("2020-06-01").date()
    bill_may = bill_generate(session=session, user_card=uc)

    # Check calculated interest
    _, interest_due = get_account_balance_from_str(
        session, book_string=f"{bill_may.id}/bill/interest_receivable/a"
    )
    assert interest_due == 180

    # Check if emi is adjusted correctly in schedule
    all_emis_query = (
        session.query(CardEmis)
        .filter(CardEmis.card_id == uc.id, CardEmis.row_status == "active")
        .order_by(CardEmis.due_date.asc())
    )
    emis_dict = [u.__dict__ for u in all_emis_query.all()]
    first_emi = emis_dict[0]
    assert first_emi["interest_current_month"] == 84
    assert first_emi["interest_next_month"] == 96

    # Do Full Payment
    payment_date = parse_date("2020-06-30")
    amount = Decimal(6180)
    bill = payment_received(
        session=session, user_card=uc, payment_amount=amount, payment_date=payment_date,
    )

    # Refresh Schedule
    refresh_schedule(session, a.id)

    # Check if amount is adjusted correctly in schedule
    all_emis_query = (
        session.query(CardEmis)
        .filter(CardEmis.card_id == uc.id, CardEmis.row_status == "active")
        .order_by(CardEmis.due_date.asc())
    )
    emis_dict = [u.__dict__ for u in all_emis_query.all()]
    second_emi = emis_dict[1]
    assert second_emi["total_due_amount"] == 0


def test_with_live_user_loan_id_4134872(session: Session) -> None:
    a = User(
        id=1764433,
        performed_by=123,
        name="UPENDRA",
        fullname="UPENDRA SINGH",
        nickname="UPENDRA",
        email="upsigh921067@gmail.com",
    )
    session.add(a)

    # assign card
    # 25 days to enforce 15th june as first due date
    uc = UserCard(
        user_id=a.id,
        card_activation_date=parse_date("2020-05-20 00:00:00"),
        interest_free_period_in_days=25,
    )
    session.flush()
    session.add(uc)

    # Card transactions
    create_card_swipe(
        session=session,
        user_card=uc,
        txn_time=parse_date("2020-05-20 17:23:01"),
        amount=Decimal(129),
        description="PAYTM                  Noida         IND",
    )
    create_card_swipe(
        session=session,
        user_card=uc,
        txn_time=parse_date("2020-05-22 09:33:18"),
        amount=Decimal(115),
        description="TPL*UDIO               MUMBAI        IND",
    )
    create_card_swipe(
        session=session,
        user_card=uc,
        txn_time=parse_date("2020-05-22 09:50:46"),
        amount=Decimal(500),
        description="AIRTELMONEY            MUMBAI        IND",
    )
    create_card_swipe(
        session=session,
        user_card=uc,
        txn_time=parse_date("2020-05-22 12:50:05"),
        amount=Decimal(2),
        description="PHONEPE RECHARGE.      GURGAON       IND",
    )
    create_card_swipe(
        session=session,
        user_card=uc,
        txn_time=parse_date("2020-05-23 01:18:54"),
        amount=Decimal(100),
        description="WWW YESBANK IN         GURGAON       IND",
    )
    create_card_swipe(
        session=session,
        user_card=uc,
        txn_time=parse_date("2020-05-23 01:42:51"),
        amount=Decimal(54),
        description="WWW YESBANK IN         GURGAON       IND",
    )
    create_card_swipe(
        session=session,
        user_card=uc,
        txn_time=parse_date("2020-05-23 01:49:44"),
        amount=Decimal(1100),
        description="Payu Payments Pvt ltd  Gurgaon       IND",
    )
    create_card_swipe(
        session=session,
        user_card=uc,
        txn_time=parse_date("2020-05-23 13:12:33"),
        amount=Decimal(99),
        description="ULLU DIGITAL PRIVATE L MUMBAI        IND",
    )
    create_card_swipe(
        session=session,
        user_card=uc,
        txn_time=parse_date("2020-05-24 16:29:25"),
        amount=Decimal(2500),
        description="WWW YESBANK IN         GURGAON       IND",
    )
    create_card_swipe(
        session=session,
        user_card=uc,
        txn_time=parse_date("2020-05-24 22:09:42"),
        amount=Decimal(99),
        description="PayTM*KookuDigitalPriP Mumbai        IND",
    )
    create_card_swipe(
        session=session,
        user_card=uc,
        txn_time=parse_date("2020-05-25 08:33:40"),
        amount=Decimal(1400),
        description="WWW YESBANK IN         GURGAON       IND",
    )
    create_card_swipe(
        session=session,
        user_card=uc,
        txn_time=parse_date("2020-05-25 10:26:12"),
        amount=Decimal(380),
        description="WWW YESBANK IN         GURGAON       IND",
    )
    create_card_swipe(
        session=session,
        user_card=uc,
        txn_time=parse_date("2020-05-25 11:40:05"),
        amount=Decimal(199),
        description="PAYTM                  Noida         IND",
    )
    create_card_swipe(
        session=session,
        user_card=uc,
        txn_time=parse_date("2020-05-25 11:57:15"),
        amount=Decimal(298),
        description="PAYTM                  Noida         IND",
    )
    create_card_swipe(
        session=session,
        user_card=uc,
        txn_time=parse_date("2020-05-25 12:25:57"),
        amount=Decimal(298),
        description="PAYTM                  Noida         IND",
    )
    create_card_swipe(
        session=session,
        user_card=uc,
        txn_time=parse_date("2020-05-26 08:04:47"),
        amount=Decimal(1450),
        description="WWW YESBANK IN         GURGAON       IND",
    )
    create_card_swipe(
        session=session,
        user_card=uc,
        txn_time=parse_date("2020-05-26 14:47:41"),
        amount=Decimal(110),
        description="TPL*UDIO               MUMBAI        IND",
    )
    create_card_swipe(
        session=session,
        user_card=uc,
        txn_time=parse_date("2020-05-26 16:37:27"),
        amount=Decimal(700),
        description="WWW YESBANK IN         GURGAON       IND",
    )
    create_card_swipe(
        session=session,
        user_card=uc,
        txn_time=parse_date("2020-05-26 22:10:58"),
        amount=Decimal(160),
        description="Linkyun Technology Pri Gurgaon       IND",
    )
    create_card_swipe(
        session=session,
        user_card=uc,
        txn_time=parse_date("2020-05-27 12:25:25"),
        amount=Decimal(299),
        description="PAYTM                  Noida         IND",
    )
    create_card_swipe(
        session=session,
        user_card=uc,
        txn_time=parse_date("2020-05-28 20:38:02"),
        amount=Decimal(199),
        description="Linkyun Technology Pri Gurgaon       IND",
    )
    create_card_swipe(
        session=session,
        user_card=uc,
        txn_time=parse_date("2020-05-28 21:45:55"),
        amount=Decimal(800),
        description="WWW YESBANK IN         GURGAON       IND",
    )
    create_card_swipe(
        session=session,
        user_card=uc,
        txn_time=parse_date("2020-05-29 10:05:58"),
        amount=Decimal(525),
        description="Payu Payments Pvt ltd  Gurgaon       IND",
    )
    create_card_swipe(
        session=session,
        user_card=uc,
        txn_time=parse_date("2020-05-30 16:04:21"),
        amount=Decimal(1400),
        description="WWW YESBANK IN         GURGAON       IND",
    )

    # Generate bill
    generate_date = parse_date("2020-06-01").date()
    bill_may = bill_generate(session=session, user_card=uc)

    # Check if amount is adjusted correctly in schedule
    all_emis_query = (
        session.query(CardEmis)
        .filter(CardEmis.card_id == uc.id, CardEmis.row_status == "active")
        .order_by(CardEmis.due_date.asc())
    )
    emis_dict = [u.__dict__ for u in all_emis_query.all()]

    # Do Partial Payment
    payment_date = parse_date("2020-06-18 06:55:00")
    amount = Decimal(324)
    bill = payment_received(
        session=session, user_card=uc, payment_amount=amount, payment_date=payment_date,
    )

    # Refresh Schedule
    refresh_schedule(session, a.id)

    # Check if amount is adjusted correctly in schedule
    all_emis_query = (
        session.query(CardEmis)
        .filter(CardEmis.card_id == uc.id, CardEmis.row_status == "active")
        .order_by(CardEmis.due_date.asc())
    )
    emis_dict = [u.__dict__ for u in all_emis_query.all()]
    first_emi = emis_dict[0]
    second_emi = emis_dict[1]

    assert first_emi["interest"] == Decimal("387.48")
    assert first_emi["interest_received"] == Decimal("324")


def test_interest_reversal_interest_already_settled(session: Session) -> None:
    test_generate_bill_1(session)
    _partial_payment_bill_1(session)
    _accrue_late_fine_bill_1(session)
    _pay_minimum_amount_bill_1(session)

    #  Pay 500 rupees
    user_card = session.query(UserCard).filter(UserCard.user_id == 99).one()
    payment_date = parse_date("2020-05-14 19:23:11")
    amount = Decimal("886.67")
    unpaid_bills = get_all_unpaid_bills(session, user_card.user_id)
    payment_received(
        session=session, user_card=user_card, payment_amount=amount, payment_date=payment_date,
    )

    bill = unpaid_bills[0]

    _, interest_due = get_account_balance_from_str(
        session, book_string=f"{bill.id}/bill/interest_receivable/a"
    )
    assert interest_due == 0

    _, interest_earned = get_account_balance_from_str(
        session, book_string=f"{bill.id}/bill/interest_earned/r"
    )
    assert interest_earned == 0

    _, principal_due = get_account_balance_from_str(
        session, book_string=f"{bill.id}/bill/principal_receivable/a"
    )
    assert principal_due == Decimal(0)

    # TODO more testing scenarios.
    # 1. interest is not settled. 2. There are multiple bills.


def test_interest_reversal_multiple_bills(session: Session) -> None:
    test_generate_bill_1(session)
    _partial_payment_bill_1(session)
    _accrue_late_fine_bill_1(session)
    _pay_minimum_amount_bill_1(session)
    _generate_bill_2(session)

    #  Pay 500 rupees
    user_card = session.query(UserCard).filter(UserCard.user_id == 99).one()
    payment_date = parse_date("2020-06-14 19:23:11")
    amount = Decimal("2916.67")
    unpaid_bills = get_all_unpaid_bills(session, user_card.user_id)
    payment_received(
        session=session, user_card=user_card, payment_amount=amount, payment_date=payment_date,
    )

    first_bill = unpaid_bills[0]
    second_bill = unpaid_bills[1]

    _, interest_earned = get_account_balance_from_str(
        session, book_string=f"{first_bill.id}/bill/interest_earned/r"
    )
    assert interest_earned == 30  # 30 Interest got removed from first bill.

    _, interest_earned = get_account_balance_from_str(
        session, book_string=f"{second_bill.id}/bill/interest_earned/r"
    )
    assert interest_earned == 0

    assert is_bill_closed(session, first_bill) is True
    assert is_bill_closed(session, second_bill) is True  # 90 got settled in new bill.


def test_failed_interest_reversal_multiple_bills(session: Session) -> None:
    test_generate_bill_1(session)
    _partial_payment_bill_1(session)
    _accrue_late_fine_bill_1(session)
    _pay_minimum_amount_bill_1(session)
    _generate_bill_2(session)

    user_card = session.query(UserCard).filter(UserCard.user_id == 99).one()
    payment_date = parse_date(
        "2020-06-18 19:23:11"
    )  # Payment came after due date. Interest won't get reversed.
    amount = Decimal("2916.67")
    unpaid_bills = get_all_unpaid_bills(session, user_card.user_id)
    payment_received(
        session=session, user_card=user_card, payment_amount=amount, payment_date=payment_date,
    )

    first_bill = unpaid_bills[0]
    second_bill = unpaid_bills[1]

    _, interest_earned = get_account_balance_from_str(
        session, book_string=f"{first_bill.id}/bill/interest_earned/r"
    )
    assert interest_earned == 60  # 30 Interest did not get removed.

    _, interest_earned = get_account_balance_from_str(
        session, book_string=f"{second_bill.id}/bill/interest_earned/r"
    )
    assert interest_earned == 60
    assert is_bill_closed(session, first_bill) is True
    assert is_bill_closed(session, second_bill) is False
