import contextlib
from decimal import Decimal
from io import StringIO

import alembic
import sqlalchemy
from alembic.command import current as alembic_current

from rush.exceptions import *
from rush.models import (
    BookAccount,
    User,
    UserPy,
    get_or_create,
)
from rush.utils import (
    get_account_balance,
    insert_card_swipe,
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
    print(a.id)
    u = UserPy(
        id=a.id, performed_by=123, email="sss", name="dfd", fullname="dfdf", nickname="dfdd",
    )


def test_insert_card_swipe(session: sqlalchemy.orm.session.Session) -> None:
    u = User(id=3, performed_by=123, name="dfd", fullname="dfdf", nickname="dfdd", email="asas",)
    session.add(u)
    session.commit()
    a = session.query(User).first()

    insert_card_swipe(
        session=session,
        user=a,
        event_name="card_transaction",
        extra_details={"payment_request_id": "test", "amount": 100},
        amount=100,
    )


def test_get_account_balance(session: sqlalchemy.orm.session.Session) -> None:
    u = User(id=4, performed_by=123, name="dfd", fullname="dfdf", nickname="dfdd", email="asas",)
    session.add(u)
    session.commit()
    a = session.query(User).first()

    insert_card_swipe(
        session=session,
        user=a,
        event_name="card_transaction",
        extra_details={"payment_request_id": "test", "amount": 100},
        amount=100,
    )

    book_account = get_or_create(
        session=session,
        model=BookAccount,
        identifier=a.id,
        book_type="user_card_balance",
        account_type="liability",
    )

    current_balance = get_account_balance(session=session, book_account=book_account)

    print(current_balance)

    assert current_balance == Decimal(-200)


def test_slide_full_payment(session: sqlalchemy.orm.session.Session) -> None:

    # Jan Month
    # Do transaction Rs 100
    # Do transaction Rs 500

    # Generate Bill (Feb 1)

    # Full bill payment (Feb 2)
    pass


def test_slide_partial_payment(session: sqlalchemy.orm.session.Session) -> None:

    # Jan Month
    # Do transaction Rs 100
    # Do transaction Rs 500

    # Generate Bill (Feb 1)

    # Partial bill payment (Feb 2)

    # Accrue Interest (Feb 15)
    pass


def test_slide_partial_payment_after_due_date(session: sqlalchemy.orm.session.Session) -> None:

    u = User(id=5, performed_by=123, name="dfd", fullname="dfdf", nickname="dfdd", email="asas",)
    session.add(u)
    session.commit()
    a = session.query(User).first()

    # Jan Month
    # Do transaction Rs 100
    # Do transaction Rs 500
    insert_card_swipe(
        session=session,
        user=a,
        event_name="card_transaction",
        extra_details={"payment_request_id": "test", "amount": 100},
        amount=100,
    )

    insert_card_swipe(
        session=session,
        user=a,
        event_name="card_transaction",
        extra_details={"payment_request_id": "test", "amount": 100},
        amount=500,
    )

    # Generate Bill (Feb 1)

    # Accrue Interest (Feb 15)

    # Add Late fee (Feb 15)

    # Partial bill payment (Feb 16)
    print("test")
