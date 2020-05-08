from decimal import Decimal
from typing import Optional

import sqlalchemy
from pendulum import DateTime
from sqlalchemy.orm.session import Session

from rush.ledger_utils import get_account_balance
from rush.models import (
    BookAccount,
    LedgerEntry,
    LedgerTriggerEvent,
    User,
    get_current_ist_time,
    get_or_create,
)


def settle_payment(
    session: sqlalchemy.orm.session.Session,
    user: User,
    payment_amount: Decimal,
    payment_date: DateTime,
    first_bill_date: DateTime,
) -> None:

    amount_to_slide = []
    for loop in range(12):
        account_date = first_bill_date.add(months=loop)

        user_late_fine_due = get_or_create(
            session=session,
            model=BookAccount,
            identifier=user.id,
            book_name="user_late_fine",
            book_date=account_date.date(),
            account_type="asset",
        )
        user_late_fine_amount_due = get_account_balance(
            session=session, book_account=user_late_fine_due
        )

        user_late_fine_paid = get_or_create(
            session=session,
            model=BookAccount,
            identifier=user.id,
            book_name="user_late_fine_paid",
            book_date=account_date.date(),
            account_type="asset",
        )
        user_late_fine_amount_paid = get_account_balance(
            session=session, book_account=user_late_fine_paid
        )
        late_fee_remaining = user_late_fine_amount_due - user_late_fine_amount_paid

        user_monthly_principal = get_or_create(
            session=session,
            model=BookAccount,
            identifier=user.id,
            book_name="user_monthly_principal",
            book_date=account_date.date(),
            account_type="asset",
        )
        principal_balance = get_account_balance(
            session=session, book_account=user_monthly_principal
        )

        user_monthly_principal_paid = get_or_create(
            session=session,
            model=BookAccount,
            identifier=user.id,
            book_name="user_monthly_principal_paid",
            book_date=account_date.date(),
            account_type="asset",
        )
        principal_paid_balance = get_account_balance(
            session=session, book_account=user_monthly_principal_paid
        )

        user_monthly_interest = get_or_create(
            session=session,
            model=BookAccount,
            identifier=user.id,
            book_name="user_monthly_interest",
            book_date=account_date.date(),
            account_type="asset",
        )
        interest_balance = get_account_balance(session=session, book_account=user_monthly_interest)

        user_monthly_interest_paid = get_or_create(
            session=session,
            model=BookAccount,
            identifier=user.id,
            book_name="user_monthly_interest_paid",
            book_date=account_date.date(),
            account_type="asset",
        )
        interest_paid_balance = get_account_balance(
            session=session, book_account=user_monthly_interest_paid
        )
        principal_left = principal_balance - principal_paid_balance
        interest_left = interest_balance - interest_paid_balance

        if late_fee_remaining > 0:
            amount_to_slide.append((user_late_fine_paid, late_fee_remaining))
        if interest_left > 0:
            amount_to_slide.append((user_monthly_interest_paid, interest_left))
        if principal_left > 0:
            amount_to_slide.append((user_monthly_principal_paid, principal_left))

    payment_gateway = get_or_create(
        session=session,
        model=BookAccount,
        identifier=user.id,
        book_name="payment_gateway",
        account_type="asset",
    )

    # Create event
    lt = LedgerTriggerEvent(
        performed_by=user.id,
        name="payment_received",
        extra_details={"payment_request_id": "lsdad", "payment_date": payment_date.isoformat()},
    )
    session.add(lt)
    session.flush()

    # Create entries
    amount_left = payment_amount
    for book, amount in amount_to_slide:
        if amount_left <= 0:
            break

        le = LedgerEntry(
            event_id=lt.id,
            from_book_account=payment_gateway.id,
            to_book_account=book.id,
            amount=min(amount_left, amount),
            business_date=payment_date,
        )
        session.add(le)
        amount_left -= amount

    session.commit()


def create_late_fine(
    session: sqlalchemy.orm.session.Session, user: User, bill_date: DateTime, amount: Decimal
) -> None:
    lt = LedgerTriggerEvent(
        performed_by=user.id, name="late_fine", extra_details={"amount": str(amount)}
    )
    session.add(lt)
    session.flush()
    user_late_fine_from = get_or_create(
        session=session,
        model=BookAccount,
        identifier=user.id,
        book_name="user_late_fine",
        book_date=bill_date.date(),
        account_type="liability",
    )
    user_late_fine_to = get_or_create(
        session=session,
        model=BookAccount,
        identifier=user.id,
        book_name="user_late_fine",
        book_date=bill_date.date(),
        account_type="asset",
    )
    le = LedgerEntry(
        event_id=lt.id,
        from_book_account=user_late_fine_from.id,
        to_book_account=user_late_fine_to.id,
        amount=amount,
        business_date=bill_date,
    )
    session.add(le)
    session.commit()
