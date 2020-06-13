import contextlib
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
from rush.create_bill import bill_generate
from rush.create_card_swipe import create_card_swipe
from rush.ledger_utils import (  # get_interest_for_each_bill,
    get_account_balance_from_str,
    get_all_unpaid_bills,
    is_bill_closed,
    is_min_paid,
)
from rush.models import (
    LoanData,
    User,
    UserCard,
    UserPy,
)
from rush.payments import payment_received


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
    # remaining card balance should be -900 because we've loaded it yet and it's going in negative.
    _, card_balance = get_account_balance_from_str(session, f"{uc.user_id}/user/user_card_balance/l")
    assert card_balance == -900


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

    _, user_card_balance = get_account_balance_from_str(
        session=session, book_string=f"{a.id}/user/user_card_balance/l"
    )
    assert user_card_balance == Decimal(-1000)

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
        session=session, book_string=f"{user.id}/user/user_card_balance/l"
    )
    assert user_card_balance == Decimal(-3000)

    generate_date = parse_date("2020-06-01").date()
    bill = bill_generate(session=session, generate_date=generate_date, user_id=user.id)
    _, opening_balance = get_account_balance_from_str(
        session=session, book_string=f"{bill.id}/bill/opening_balance/a"
    )
    assert opening_balance == Decimal(900)

    _, principal_due = get_account_balance_from_str(
        session=session, book_string=f"{bill.id}/bill/principal_due/a"
    )
    assert principal_due == Decimal(2900)

    _, min_due = get_account_balance_from_str(session=session, book_string=f"{bill.id}/bill/min_due/a")
    assert min_due == Decimal(377)


def test_generate_bill_2(session: Session) -> None:
    test_generate_bill_1(session)
    _partial_payment_bill_1(session)
    _accrue_late_fine_bill_1(session)
    _pay_minimum_amount_bill_1(session)
    _accrue_interest_bill_1(session)
    _generate_bill_2(session)
    user = session.query(User).filter(User.id == 99).one()
    unpaid_bills = get_all_unpaid_bills(session, user)
    assert len(unpaid_bills) == 2

    unpaid_bills = all_bills = session.query(LoanData).filter(LoanData.user_id == 99).all()
    # interest = get_interest_for_each_bill(session, unpaid_bills)
    # assert interest == Decimal(1404.00)
