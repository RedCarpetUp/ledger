from decimal import Decimal
from typing import Dict, Optional

import sqlalchemy
from pendulum import DateTime
from sqlalchemy import func

from rush.models import (
    BookAccount,
    LedgerEntry,
    LedgerTriggerEvent,
    User,
    get_current_ist_time,
    get_or_create,
)


def insert_card_swipe(
    session: sqlalchemy.orm.session.Session,
    user: User,
    event_name: str,
    extra_details: Dict,
    amount: int,
    business_date: Optional[DateTime] = get_current_ist_time(),
) -> None:
    lt = LedgerTriggerEvent(performed_by=user.id, name=event_name, extra_details=extra_details)
    session.add(lt)
    session.flush()

    from_account = get_or_create(
        session=session,
        model=BookAccount,
        identifier=100,
        book_type="dmi_pool_account",
        account_type="liability",
    )
    to_account = get_or_create(
        session=session,
        model=BookAccount,
        identifier=100,
        book_type="dmi_limit_used",
        account_type="asset",
    )
    le1 = LedgerEntry(
        event_id=lt.id,
        from_book_account=from_account.id,
        to_book_account=to_account.id,
        amount=amount,
        business_date=business_date,
    )
    session.add(le1)

    from_account = get_or_create(
        session=session,
        model=BookAccount,
        identifier=user.id,
        book_type="user_card_balance",
        account_type="liability",
    )
    to_account = get_or_create(
        session=session,
        model=BookAccount,
        identifier=user.id,
        book_type="unbilled_transactions",
        account_type="asset",
    )
    le2 = LedgerEntry(
        event_id=lt.id,
        from_book_account=from_account.id,
        to_book_account=to_account.id,
        amount=amount,
        business_date=business_date,
    )
    session.add(le2)

    from_account = get_or_create(
        session=session,
        model=BookAccount,
        identifier=user.id,
        book_type="user_marvin_limit",
        account_type="liability",
    )
    to_account = get_or_create(
        session=session,
        model=BookAccount,
        identifier=user.id,
        book_type="user_marvin_limit_used",
        account_type="asset",
    )
    le3 = LedgerEntry(
        event_id=lt.id,
        from_book_account=from_account.id,
        to_book_account=to_account.id,
        amount=amount,
        business_date=business_date,
    )
    session.add(le3)
    session.commit()


def get_account_balance(
    session: sqlalchemy.orm.session.Session,
    book_account: BookAccount,
    business_date: Optional[DateTime] = None,
) -> Decimal:

    if not business_date:
        business_date = get_current_ist_time()

    debit_balance = (
        session.query(func.sum(LedgerEntry.amount))
        .filter(
            LedgerEntry.from_book_account == book_account.id,
            LedgerEntry.business_date <= business_date,
        )
        .scalar()
        or 0
    )

    credit_balance = (
        session.query(func.sum(LedgerEntry.amount))
        .filter(
            LedgerEntry.to_book_account == book_account.id,
            LedgerEntry.business_date <= business_date,
        )
        .scalar()
        or 0
    )
    final_balance = credit_balance - debit_balance

    return Decimal(final_balance)


def generate_bill(
    session: sqlalchemy.orm.session.Session,
    bill_date: DateTime,
    interest_yearly: int,
    bill_tenure: int,
    user: User,
    business_date: Optional[DateTime] = None,
) -> None:

    lt = LedgerTriggerEvent(
        performed_by=user.id, name="bill_generation", extra_details={"bill_date": str(bill_date)}
    )
    session.add(lt)
    session.flush()
    unbilled_transactions = get_or_create(
        session=session,
        model=BookAccount,
        identifier=user.id,
        book_type="unbilled_transactions",
        account_type="asset",
    )

    prev_date = bill_date.subtract(months=1)

    user_monthly = get_or_create(
        session=session,
        model=BookAccount,
        identifier=user.id,
        book_type="user_monthly_" + str(prev_date) + "to" + str(bill_date),
        account_type="asset",
    )

    unbilled_balance = get_account_balance(session=session, book_account=unbilled_transactions)

    total_bill_principal = unbilled_balance
    total_interest = unbilled_balance * interest_yearly / 100
    total_bill_amount = total_bill_principal + total_interest
    le3 = LedgerEntry(
        event_id=lt.id,
        from_book_account=unbilled_transactions.id,
        to_book_account=user_monthly.id,
        amount=total_bill_amount,
        business_date=business_date,
    )
    session.add(le3)

    account_monthy_interest = get_or_create(
        session=session,
        model=BookAccount,
        identifier=user.id,
        book_type="monthly_interest" + str(prev_date) + "to" + str(bill_date),
        account_type="asset",
    )

    le4 = LedgerEntry(
        event_id=lt.id,
        from_book_account=user_monthly.id,
        to_book_account=account_monthy_interest.id,
        amount=total_interest,
        business_date=business_date,
    )

    session.add(le4)
    session.commit()
