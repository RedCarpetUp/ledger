import contextlib
from decimal import Decimal
from io import StringIO

import alembic
import sqlalchemy
from alembic.command import current as alembic_current
from pendulum import parse as parse_date  # type: ignore

from rush.create_bill import close_bill
from rush.create_card_swipe import create_card_swipe
from rush.exceptions import *
from rush.ledger_utils import (
    get_account_balance,
    get_book_account_by_string,
    get_account_balance_from_str,
)
from rush.models import (
    BookAccount,
    LedgerTriggerEvent,
    LoanData,
    LoanEmis,
    User,
    UserCard,
    UserPy,
    get_current_ist_time,
    get_or_create,
)
from rush.utils import (
    create_late_fine,
    generate_bill,
    get_bill_amount,
    settle_payment,
)


def test_current(getAlembic: alembic.config.Config) -> None:
    """Test that the alembic current command does not erorr"""
    # Runs with no error
    # output = run_alembic_command(pg["engine"], "current", {})

    stdout = StringIO()
    with contextlib.redirect_stdout(stdout):
        # command_func(alembic_cfg, **command_kwargs)
        alembic_current(getAlembic, {})
    assert stdout.getvalue() == ""
    # assert output == ""


def test_user2(session: sqlalchemy.orm.session.Session) -> None:
    u = User(performed_by=123, id=1, name="dfd", fullname="dfdf", nickname="dfdd", email="asas",)
    session.add(u)
    session.commit()
    a = session.query(User).first()
    print(a.id)
    u = UserPy(
        id=a.id, performed_by=123, email="sss", name="dfd", fullname="dfdf", nickname="dfdd",
    )


def test_user(session: sqlalchemy.orm.session.Session) -> None:
    u = User(id=2, performed_by=123, name="dfd", fullname="dfdf", nickname="dfdd", email="asas",)
    session.add(u)
    session.commit()
    a = session.query(User).first()
    u = UserPy(
        id=a.id, performed_by=123, email="sss", name="dfd", fullname="dfdf", nickname="dfdd",
    )


def test_card_swipe(session: sqlalchemy.orm.session.Session) -> None:
    uc = UserCard(user_id=2, card_activation_date=parse_date("2020-05-01"))
    session.add(uc)
    session.flush()

    create_card_swipe(
        session=session,
        user_card=uc,
        txn_time=parse_date("2020-05-01 14:23:11"),
        amount=Decimal(700),
        description="Amazon.com",
    )
    create_card_swipe(
        session=session,
        user_card=uc,
        txn_time=parse_date("2020-05-02 11:22:11"),
        amount=Decimal(200),
        description="Flipkart.com",
    )
    session.commit()
    unbilled_balance = get_account_balance_from_str(
        session, f"{uc.user_id}/user/unbilled_transactions/a"
    )
    assert unbilled_balance == 900
    # remaining card balance should be -900 because we've loaded it yet and it's going in negative.
    card_balance = get_account_balance_from_str(session, f"{uc.user_id}/user/user_card_balance/l")
    assert card_balance == -900


# def test_slide_full_payment(session: sqlalchemy.orm.session.Session) -> None:
# Jan Month
# Do transaction Rs 100
# Do transaction Rs 500

# Generate Bill (Feb 1)

# Full bill payment (Feb 2)
# pass


# def test_slide_partial_payment(session: sqlalchemy.orm.session.Session) -> None:
# Jan Month
# Do transaction Rs 100
# Do transaction Rs 500

# Generate Bill (Feb 1)

# Partial bill payment (Feb 2)

# Accrue Interest (Feb 15)
# pass


def test_slide_partial_payment_after_due_date(session: sqlalchemy.orm.session.Session) -> None:
    u = User(id=5, performed_by=123, name="dfd", fullname="dfdf", nickname="dfdd", email="asas",)
    session.add(u)
    session.commit()

    # assign card
    uc = UserCard(user_id=u.id, card_activation_date=parse_date("2020-04-05"))
    session.add(uc)
    session.flush()

    # First Month
    # Do transaction Rs 100
    # Do transaction Rs 500
    create_card_swipe(
        session=session,
        user_card=uc,
        txn_time=parse_date("2020-04-06 14:23:11"),
        amount=Decimal(100),
        description="Myntra.com",
    )

    create_card_swipe(
        session=session,
        user_card=uc,
        txn_time=parse_date("2020-04-15 14:23:11"),
        amount=Decimal(500),
        description="Google Play",
    )

    # Generate Bill (Feb 1)

    # Accrue Interest (Feb 15)

    # Add Late fee (Feb 15)

    # Partial bill payment (Feb 16)
    print("test")


def test_generate_bill(session: sqlalchemy.orm.session.Session) -> None:
    a = User(id=99, performed_by=123, name="dfd", fullname="dfdf", nickname="dfdd", email="asas",)
    session.add(a)
    session.commit()

    # assign card
    uc = UserCard(user_id=a.id, card_activation_date=parse_date("2020-04-02"))
    session.add(uc)
    session.flush()

    swipe1 = create_card_swipe(
        session=session,
        user_card=uc,
        txn_time=parse_date("2020-04-08 19:23:11"),
        amount=Decimal(100),
        description="BigBasket.com",
    )

    user_card_balance_book = get_book_account_by_string(
        session=session, book_string=f"{a.id}/user/user_card_balance/l"
    )

    user_card_balance = get_account_balance(session=session, book_account=user_card_balance_book)
    assert user_card_balance == Decimal(-100)

    bill = close_bill(session=session, user_id=a.id)

    unbilled_book = get_book_account_by_string(
        session, book_string=f"{bill.id}/bill/unbilled_transactions/a"
    )
    unbilled_balance = get_account_balance(session=session, book_account=unbilled_book)
    assert unbilled_balance == 0

    bill_schedules = session.query(LoanEmis).filter_by(loan_id=bill.id).all()
    for schedule in bill_schedules:
        principal_due_book = get_book_account_by_string(
            session=session, book_string=f"{schedule.id}/emi/principal_due/a"
        )
        principal_due = get_account_balance(session=session, book_account=principal_due_book)
        assert principal_due == Decimal("8.33")

        interest_due_book = get_book_account_by_string(
            session, book_string=f"{schedule.id}/emi/interest_due/a"
        )
        interest_due = get_account_balance(session=session, book_account=interest_due_book)
        assert interest_due == Decimal(3)
    # val = get_bill_amount(session, bill_date, prev_date, a)
    # assert val == 110


def test_payment(session: sqlalchemy.orm.session.Session) -> None:
    user = session.query(User).filter(User.id == 99).one()
    first_bill_date = parse_date("2020-05-01")
    payment_date = parse_date("2020-05-03")
    amount = Decimal(120)
    settle_payment(
        session=session,
        user=user,
        payment_amount=amount,
        payment_date=payment_date,
        first_bill_date=first_bill_date,
    )
    principal_paid = get_or_create(
        session=session,
        model=BookAccount,
        identifier=user.id,
        book_name="user_monthly_principal_paid",
        book_date=first_bill_date.date(),
        account_type="asset",
    )
    current_balance = get_account_balance(
        session=session, book_account=principal_paid, business_date=payment_date
    )
    assert current_balance == Decimal("8.33")

    interest_paid = get_or_create(
        session=session,
        model=BookAccount,
        identifier=user.id,
        book_name="user_monthly_interest_paid",
        book_date=first_bill_date.date(),
        account_type="asset",
    )
    payment_date = parse_date("2020-05-04")
    current_balance = get_account_balance(
        session=session, book_account=interest_paid, business_date=payment_date
    )
    assert current_balance == Decimal(3)


def test_late_fine(session: sqlalchemy.orm.session.Session) -> None:
    user = session.query(User).filter(User.id == 99).one()
    first_bill_date = parse_date("2020-05-01")
    create_late_fine(session=session, user=user, bill_date=first_bill_date, amount=Decimal(100))
    payment_date = parse_date("2020-05-05")
    settle_payment(
        session=session,
        user=user,
        payment_amount=Decimal(100),
        payment_date=payment_date,
        first_bill_date=first_bill_date,
    )

    late_fine_paid = get_or_create(
        session=session,
        model=BookAccount,
        identifier=user.id,
        book_name="user_late_fine_paid",
        book_date=first_bill_date.date(),
        account_type="asset",
    )

    current_balance = get_account_balance(
        session=session, book_account=late_fine_paid, business_date=payment_date
    )
    assert current_balance == Decimal(100)
