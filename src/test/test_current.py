import contextlib
import json
from decimal import Decimal
from io import StringIO

import alembic
from alembic.command import current as alembic_current
from pendulum import parse as parse_date  # type: ignore
from sqlalchemy.orm import Session

from rush.accrue_financial_charges import (
    accrue_interest,
    accrue_late_charges,
)
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
    is_min_paid,
)
from rush.lender_funds import (
    lender_disbursal,
    m2p_transfer,
)
from rush.models import (
    CardEmis,
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

    _, unbilled_balance = get_account_balance_from_str(
        session, f"{swipe1.loan_id}/bill/unbilled_transactions/a"
    )
    assert unbilled_balance == 900
    # remaining card balance should be -900 because we've not loaded it yet and it's going in negative.
    _, card_balance = get_account_balance_from_str(session, f"{uc.user_id}/user/card_balance/l")
    assert card_balance == -900

    _, lender_payable = get_account_balance_from_str(session, "62311/lender/lender_payable/l")
    assert lender_payable == 900


def test_generate_bill_1(session: Session) -> None:
    a = User(id=99, performed_by=123, name="dfd", fullname="dfdf", nickname="dfdd", email="asas",)
    session.add(a)

    # assign card
    uc = UserCard(user_id=a.id, card_activation_date=parse_date("2020-04-02"))
    session.flush()
    session.add(uc)

    create_card_swipe(
        session=session,
        user_card=uc,
        txn_time=parse_date("2020-04-08 19:23:11"),
        amount=Decimal(1000),
        description="BigBasket.com",
    )

    generate_date = parse_date("2020-05-01").date()
    bill = bill_generate(session=session, generate_date=generate_date, user_id=a.id)

    _, unbilled_balance = get_account_balance_from_str(
        session, book_string=f"{bill.id}/bill/unbilled_transactions/a"
    )
    assert unbilled_balance == 0

    _, principal_due = get_account_balance_from_str(
        session, book_string=f"{bill.id}/bill/principal_due/a"
    )
    assert principal_due == 1000

    _, min_due = get_account_balance_from_str(session, book_string=f"{bill.id}/bill/min_due/a")
    assert min_due == 130


def _partial_payment_bill_1(session: Session) -> None:
    user = session.query(User).filter(User.id == 99).one()
    payment_date = parse_date("2020-05-03")
    amount = Decimal(120)
    bill = payment_received(
        session=session, user_id=user.id, payment_amount=amount, payment_date=payment_date,
    )

    _, principal_due = get_account_balance_from_str(
        session, book_string=f"{bill.id}/bill/principal_due/a"
    )
    assert principal_due == 1000 - amount


def _min_payment_delayed_bill_1(session: Session) -> None:
    user = session.query(User).filter(User.id == 99).one()
    payment_date = parse_date("2020-05-03")
    amount = Decimal(130)
    bill = payment_received(
        session=session, user_id=user.id, payment_amount=amount, payment_date=payment_date,
    )

    _, principal_due = get_account_balance_from_str(
        session, book_string=f"{bill.id}/bill/principal_due/a"
    )
    # payment got late and 100 rupees got settled in late fine.
    assert principal_due == 970


def test_partial_payment_bill_1(session: Session) -> None:
    test_generate_bill_1(session)
    _partial_payment_bill_1(session)


def _accrue_late_fine_bill_1(session: Session) -> None:
    user = session.query(User).filter(User.id == 99).one()
    bill = accrue_late_charges(session, user.id)

    _, late_fine_due = get_account_balance_from_str(session, f"{bill.id}/bill/late_fine_due/a")
    assert late_fine_due == Decimal(100)


def test_accrue_late_fine_bill_1(session: Session) -> None:
    test_generate_bill_1(session)
    # did only partial payment so accrue late fee.
    _partial_payment_bill_1(session)
    _accrue_late_fine_bill_1(session)


def _pay_minimum_amount_bill_1(session: Session) -> None:
    user = session.query(User).filter(User.id == 99).one()

    bill = (
        session.query(LoanData)
        .filter(LoanData.user_id == user.id)
        .order_by(LoanData.agreement_date.desc())
        .first()
    )
    # Should be false because min is 130 and payment made is 120
    assert is_min_paid(session, bill) is False

    # Pay 10 more. and 100 for late fee.
    bill = payment_received(
        session=session,
        user_id=user.id,
        payment_amount=Decimal(110),
        payment_date=parse_date("2020-05-20"),
    )
    assert is_min_paid(session, bill) is True


def test_is_min_paid_bill_1(session: Session) -> None:
    test_generate_bill_1(session)
    _partial_payment_bill_1(session)
    # did only partial payment so accrue late fee.
    _accrue_late_fine_bill_1(session)
    _pay_minimum_amount_bill_1(session)


def _accrue_interest_bill_1(session: Session) -> None:
    user = session.query(User).filter(User.id == 99).one()

    bill = accrue_interest(session, user.id)
    _, interest_due = get_account_balance_from_str(session, book_string=f"{bill.id}/bill/interest_due/a")
    assert interest_due == 30
    # The value should be positive 30 but it is coming to be negative of 30


def test_accrue_interest_bill_1(session: Session) -> None:
    test_generate_bill_1(session)
    _partial_payment_bill_1(session)
    _pay_minimum_amount_bill_1(session)
    _accrue_interest_bill_1(session)


def test_is_bill_paid_bill_1(session: Session) -> None:
    test_generate_bill_1(session)
    _partial_payment_bill_1(session)
    _accrue_late_fine_bill_1(session)
    _pay_minimum_amount_bill_1(session)
    _accrue_interest_bill_1(session)

    user = session.query(User).filter(User.id == 99).one()

    bill = (
        session.query(LoanData)
        .filter(LoanData.user_id == user.id)
        .order_by(LoanData.agreement_date.desc())
        .first()
    )
    # Should be false because min is 130 and payment made is 120
    is_it_paid = is_bill_closed(session, bill)
    assert is_it_paid is False

    # Need to pay 870 more to close the bill. 30 more interest.
    remaining_principal = Decimal(900)
    bill = payment_received(
        session=session,
        user_id=user.id,
        payment_amount=remaining_principal,
        payment_date=parse_date("2020-05-05"),
    )
    is_it_paid_now = is_bill_closed(session, bill)
    assert is_it_paid_now is True


def _generate_bill_2(session: Session) -> None:
    user = session.query(User).filter(User.id == 99).one()
    uc = session.query(UserCard).filter_by(user_id=user.id).one()

    previous_bill = (  # new bill isn't generated yet so get latest.
        session.query(LoanData)
        .filter(LoanData.user_id == user.id)
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
        session=session, book_string=f"{user.id}/user/card_balance/l"
    )
    assert user_card_balance == Decimal(-3000)

    generate_date = parse_date("2020-06-01").date()
    bill = bill_generate(session=session, generate_date=generate_date, user_id=user.id)

    _, principal_due = get_account_balance_from_str(
        session=session, book_string=f"{bill.id}/bill/principal_due/a"
    )
    assert principal_due == Decimal(2000)

    _, min_due = get_account_balance_from_str(session=session, book_string=f"{bill.id}/bill/min_due/a")
    assert min_due == Decimal(260)


def _run_anomaly_bill_1(session: Session) -> None:
    user = session.query(User).filter(User.id == 99).one()

    bill = (
        session.query(LoanData)
        .filter(LoanData.user_id == user.id)
        .order_by(LoanData.agreement_date.desc())
        .first()
    )
    run_anomaly(session, bill)

    _, late_fine_due = get_account_balance_from_str(session, f"{bill.id}/bill/late_fine_due/a")
    assert late_fine_due == Decimal(0)

    _, late_fee_received = get_account_balance_from_str(
        session, book_string=f"{bill.id}/bill/late_fee_received/a"
    )
    assert late_fee_received == Decimal(0)

    _, principal_due = get_account_balance_from_str(
        session, book_string=f"{bill.id}/bill/principal_due/a"
    )
    # payment got moved from late received to principal received.
    assert principal_due == 970 - 100


def test_anomaly_late_payment_received(session: Session) -> None:
    test_generate_bill_1(session)
    _accrue_late_fine_bill_1(session)  # Accrue late fine first.
    _min_payment_delayed_bill_1(session)  # Payment comes in our system late.
    _run_anomaly_bill_1(session)
    _accrue_interest_bill_1(session)


def test_generate_bill_2(session: Session) -> None:
    test_generate_bill_1(session)
    _partial_payment_bill_1(session)
    _accrue_late_fine_bill_1(session)
    _pay_minimum_amount_bill_1(session)
    _accrue_interest_bill_1(session)
    _generate_bill_2(session)
    user = session.query(User).filter(User.id == 99).one()
    unpaid_bills = get_all_unpaid_bills(session, user.id)
    assert len(unpaid_bills) == 2

    unpaid_bills = all_bills = session.query(LoanData).filter(LoanData.user_id == 99).all()
    # interest = get_interest_for_each_bill(session, unpaid_bills)
    # assert interest == Decimal(1404.00)


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

    bill = (
        session.query(LoanData)
        .filter(LoanData.user_id == a.id)
        .order_by(LoanData.agreement_date.desc())
        .first()
    )  # Get the latest bill of that user.

    last_emi = create_emis_for_card(session=session, user_card=uc, last_bill=bill)
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
    bill_april = bill_generate(session=session, generate_date=generate_date, user_id=a.id)

    create_card_swipe(
        session=session,
        user_card=uc,
        txn_time=parse_date("2020-05-08 19:23:11"),
        amount=Decimal(6000),
        description="BigBasket.com",
    )

    generate_date = parse_date("2020-06-01").date()
    bill_may = bill_generate(session=session, generate_date=generate_date, user_id=a.id)

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
    _accrue_interest_bill_1(session)
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
    bill_april = bill_generate(session=session, generate_date=generate_date, user_id=a.id)

    # Update later
    assert a.id == 2005
