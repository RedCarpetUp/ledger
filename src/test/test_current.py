import contextlib
from decimal import Decimal
from io import StringIO

import alembic
import sqlalchemy
from alembic.command import current as alembic_current
from pendulum import parse as parse_date  # type: ignore

from rush.exceptions import *
from rush.models import (
    BookAccount,
    LedgerTriggerEvent,
    LoanData,
    LoanEmis,
    User,
    UserPy,
    get_current_ist_time,
    get_or_create,
)
from rush.utils import (
    create_late_fine,
    generate_bill,
    get_account_balance,
    get_bill_amount,
    insert_card_swipe,
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
    print(a.id)
    u = UserPy(
        id=a.id, performed_by=123, email="sss", name="dfd", fullname="dfdf", nickname="dfdd",
    )


def test_loan_create(session: sqlalchemy.orm.session.Session) -> None:

    u = User(id=1001, performed_by=123, name="dfd", fullname="dfdf", nickname="dfdd", email="asas",)
    session.add(u)
    session.flush()

    loan_data = LoanData(
        user_id=u.id,
        agreement_date=get_current_ist_time(),
        bill_generation_date=get_current_ist_time(),
    )
    session.add(loan_data)
    session.flush()

    print(loan_data.id)

    loan_emis = LoanEmis(
        loan_id=loan_data.id,
        due_date=get_current_ist_time(),
        last_payment_date=get_current_ist_time(),
    )

    session.add(loan_emis)
    session.flush()

    print(loan_emis.id)

    session.commit()


def test_insert_card_swipe(session: sqlalchemy.orm.session.Session) -> None:
    u = User(id=3, performed_by=123, name="dfd", fullname="dfdf", nickname="dfdd", email="asas",)
    session.add(u)
    session.commit()

    insert_card_swipe(
        session=session,
        user=u,
        event_name="card_transaction",
        extra_details={"payment_request_id": "test", "amount": 100},
        amount=100,
    )


def test_get_account_balance(session: sqlalchemy.orm.session.Session) -> None:
    u = User(id=4, performed_by=123, name="dfd", fullname="dfdf", nickname="dfdd", email="asas",)
    session.add(u)
    session.commit()

    insert_card_swipe(
        session=session,
        user=u,
        event_name="card_transaction",
        extra_details={"payment_request_id": "test", "amount": 100},
        amount=100,
    )

    book_account = get_or_create(
        session=session,
        model=BookAccount,
        identifier=u.id,
        book_type="user_card_balance",
        account_type="liability",
    )

    current_balance = get_account_balance(session=session, book_account=book_account)
    assert current_balance == Decimal(-100)


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

    # Jan Month
    # Do transaction Rs 100
    # Do transaction Rs 500
    insert_card_swipe(
        session=session,
        user=u,
        event_name="card_transaction",
        extra_details={"payment_request_id": "test", "amount": 100},
        amount=100,
    )

    insert_card_swipe(
        session=session,
        user=u,
        event_name="card_transaction",
        extra_details={"payment_request_id": "test", "amount": 100},
        amount=500,
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
    assert current_balance == Decimal(-100)

    current_date = parse_date("2020-05-01")
    bill_date = current_date

    generate_bill(
        session=session,
        bill_date=bill_date,
        interest_monthly=3,
        bill_tenure=12,
        user=a,
        business_date=get_current_ist_time(),
    )

    book_account = get_or_create(
        session=session,
        model=BookAccount,
        identifier=a.id,
        book_type="user_monthly_principal_" + str(bill_date.date()),
        account_type="asset",
    )
    current_balance = get_account_balance(session=session, book_account=book_account)
    assert current_balance == Decimal("8.33")

    book_account = get_or_create(
        session=session,
        model=BookAccount,
        identifier=a.id,
        book_type="user_monthly_interest_" + str(bill_date.date()),
        account_type="asset",
    )
    current_balance = get_account_balance(session=session, book_account=book_account)
    assert current_balance == Decimal(3)
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
        book_type="user_monthly_principal_paid_" + str(first_bill_date.date()),
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
        book_type="user_monthly_interest_paid_" + str(first_bill_date.date()),
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
        book_type="user_late_fine_paid_" + str(first_bill_date.date()),
        account_type="asset",
    )

    current_balance = get_account_balance(
        session=session, book_account=late_fine_paid, business_date=payment_date
    )
    assert current_balance == Decimal(100)
