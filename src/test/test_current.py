import contextlib
from io import StringIO

import alembic
import sqlalchemy
from alembic.command import current as alembic_current

from rush.exceptions import *
from rush.models import User, UserPy
from rush.utils import insert_card_swipe


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
    u = User(
        # id=101,
        performed_by=123,
        user_id=101,
        name="dfd",
        fullname="dfdf",
        nickname="dfdd",
        email="asas",
    )
    session.add(u)
    session.commit()
    a = session.query(User).first()
    print(a.id)
    u = UserPy(
        id=a.id,
        performed_by=123,
        email="sss",
        user_id=101,
        name="dfd",
        fullname="dfdf",
        nickname="dfdd",
    )


def test_user(session: sqlalchemy.orm.session.Session) -> None:
    u = User(
        # id=100,
        performed_by=123,
        user_id=101,
        name="dfd",
        fullname="dfdf",
        nickname="dfdd",
        email="asas",
    )
    session.add(u)
    session.commit()
    a = session.query(User).first()
    print(a.id)
    u = UserPy(
        id=a.id,
        performed_by=123,
        email="sss",
        user_id=101,
        name="dfd",
        fullname="dfdf",
        nickname="dfdd",
    )


def test_insert_card_swipe(session: sqlalchemy.orm.session.Session) -> None:
    u = User(
        # id=100,
        performed_by=123,
        user_id=101,
        name="dfd",
        fullname="dfdf",
        nickname="dfdd",
        email="asas",
    )
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
