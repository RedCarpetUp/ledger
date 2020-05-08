import contextlib
from decimal import Decimal
from io import StringIO

import alembic
import sqlalchemy
from alembic.command import current as alembic_current
from pendulum import parse as parse_date  # type: ignore

from rush.create_bill import close_bill
from rush.create_card_swipe import create_card_swipe
from rush.ledger_utils import get_account_balance_from_str
from rush.models import (
    User,
    UserCard,
    UserPy,
)
from rush.payments import payment_received


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
    _, unbilled_balance = get_account_balance_from_str(
        session, f"{uc.user_id}/user/unbilled_transactions/a"
    )
    assert unbilled_balance == 900
    # remaining card balance should be -900 because we've loaded it yet and it's going in negative.
    _, card_balance = get_account_balance_from_str(
        session, f"{uc.user_id}/user/user_card_balance/l"
    )
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


# def test_slide_partial_payment_after_due_date(session: sqlalchemy.orm.session.Session) -> None:
#     u = User(id=5, performed_by=123, name="dfd", fullname="dfdf", nickname="dfdd", email="asas",)
#     session.add(u)
#     session.commit()
#
#     # assign card
#     uc = UserCard(user_id=u.id, card_activation_date=parse_date("2020-04-05"))
#     session.add(uc)
#     session.flush()
#
#     # First Month
#     # Do transaction Rs 100
#     # Do transaction Rs 500
#     create_card_swipe(
#         session=session,
#         user_card=uc,
#         txn_time=parse_date("2020-04-06 14:23:11"),
#         amount=Decimal(100),
#         description="Myntra.com",
#     )
#
#     create_card_swipe(
#         session=session,
#         user_card=uc,
#         txn_time=parse_date("2020-04-15 14:23:11"),
#         amount=Decimal(500),
#         description="Google Play",
#     )
#
#     # Generate Bill (Feb 1)
#
#     # Accrue Interest (Feb 15)
#
#     # Add Late fee (Feb 15)
#
#     # Partial bill payment (Feb 16)
#     print("test")


def test_generate_bill(session: sqlalchemy.orm.session.Session) -> None:
    a = User(id=99, performed_by=123, name="dfd", fullname="dfdf", nickname="dfdd", email="asas",)
    session.add(a)
    session.commit()

    # assign card
    uc = UserCard(user_id=a.id, card_activation_date=parse_date("2020-04-02"))
    session.add(uc)
    session.flush()

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

    closing_date = parse_date("2020-05-01").date()
    close_bill(session=session, closing_date=closing_date, user_id=a.id)
    session.commit()

    _, unbilled_balance = get_account_balance_from_str(
        session, book_string=f"{a.id}/user/unbilled_transactions/a"
    )
    assert unbilled_balance == 0

    _, principal_due = get_account_balance_from_str(
        session, book_string=f"{a.id}/user/principal_due/a"
    )
    assert principal_due == 1000


def test_payment(session: sqlalchemy.orm.session.Session) -> None:
    user = session.query(User).filter(User.id == 99).one()
    payment_date = parse_date("2020-05-03")
    amount = Decimal(120)
    payment_received(
        session=session, user_id=user.id, payment_amount=amount, payment_date=payment_date,
    )

    _, principal_due = get_account_balance_from_str(
        session, book_string=f"{user.id}/user/principal_due/a"
    )
    assert principal_due == 1000 - amount
