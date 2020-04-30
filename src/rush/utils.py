from decimal import Decimal
from typing import (
    Dict,
    Optional,
)

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
    final_balance = round(credit_balance - debit_balance, 2)

    return round(Decimal(final_balance), 2)


def generate_bill(
    session: sqlalchemy.orm.session.Session,
    bill_date: DateTime,
    interest_monthly: int,
    bill_tenure: int,
    user: User,
    business_date: Optional[DateTime] = get_current_ist_time(),
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

    unbilled_balance = get_account_balance(session=session, book_account=unbilled_transactions)

    total_bill_principal = round(unbilled_balance, 2)
    principal_per_month = round(unbilled_balance / bill_tenure, 2)
    interest_amount_per_month = round(unbilled_balance * interest_monthly / 100, 2)
    total_interest = round(interest_amount_per_month * bill_tenure, 2)
    total_bill_amount = round(total_bill_principal + total_interest, 2)

    for loop in range(bill_tenure):
        account_date = bill_date.add(months=loop)

        user_monthly_principal = get_or_create(
            session=session,
            model=BookAccount,
            identifier=user.id,
            book_type="user_monthly_principal_" + str(account_date.date()),
            account_type="asset",
        )

        le = LedgerEntry(
            event_id=lt.id,
            from_book_account=unbilled_transactions.id,
            to_book_account=user_monthly_principal.id,
            amount=principal_per_month,
            business_date=business_date,
        )
        session.add(le)

        account_monthy_interest_liability = get_or_create(
            session=session,
            model=BookAccount,
            identifier=user.id,
            book_type="user_monthly_interest" + str(account_date.date()),
            account_type="liability",
        )

        account_monthy_interest_asset = get_or_create(
            session=session,
            model=BookAccount,
            identifier=user.id,
            book_type="user_monthly_interest" + str(account_date.date()),
            account_type="asset",
        )

        le = LedgerEntry(
            event_id=lt.id,
            from_book_account=account_monthy_interest_liability.id,
            to_book_account=account_monthy_interest_asset.id,
            amount=interest_amount_per_month,
            business_date=business_date,
        )

        session.add(le)

    session.commit()


def get_bill_amount(
    session: sqlalchemy.orm.session.Session, bill_date: DateTime, prev_date: DateTime, user: User,
) -> Decimal:
    book_account_monthly = get_or_create(
        session=session,
        model=BookAccount,
        identifier=user.id,
        book_type="user_monthly_" + str(prev_date) + "to" + str(bill_date),
        account_type="asset",
    )
    monthly_amount = get_account_balance(session=session, book_account=book_account_monthly)

    book_account_interest = get_or_create(
        session=session,
        model=BookAccount,
        identifier=user.id,
        book_type="monthly_interest" + str(prev_date) + "to" + str(bill_date),
        account_type="asset",
    )
    interest_amount = get_account_balance(session=session, book_account=book_account_interest)
    return interest_amount + monthly_amount


def settle_payment(
    session: sqlalchemy.orm.session.Session,
    prev_date: DateTime,
    bill_date: DateTime,
    user: User,
    payment_amount: Decimal,
) -> None:
    payment_for_loan_book = get_or_create(
        session=session,
        model=BookAccount,
        identifier=user.id,
        book_type="payment_for_loan",
        account_type="asset",
    )
    payment_gateway = get_or_create(
        session=session,
        model=BookAccount,
        identifier=user.id,
        book_type="payment_gateway",
        account_type="asset",
    )
    # This will be at user level
    extra_payment_book = get_or_create(
        session=session,
        model=BookAccount,
        identifier=user.id,
        book_type="extra_payment",
        account_type="asset",
    )
    total_bill = get_bill_amount(
        session=session, bill_date=bill_date, prev_date=prev_date, user=user
    )
    amount_to_reslide = min(total_bill, payment_amount)
    extra_payment = max(0, payment_amount - amount_to_reslide)

    lt = LedgerTriggerEvent(
        performed_by=user.id,
        name="payment_received",
        extra_details={"payment_request_id": "lsdad", "payment_date": bill_date.isoformat()},
    )
    session.add(lt)
    session.flush()
    le4 = LedgerEntry(
        event_id=lt.id,
        from_book_account=payment_gateway.id,
        to_book_account=payment_for_loan_book.id,
        amount=payment_amount,
        business_date=bill_date,
    )
    session.add(le4)
    session.flush()
    le5 = LedgerEntry(
        event_id=lt.id,
        from_book_account=payment_for_loan_book.id,
        to_book_account=extra_payment_book.id,
        amount=extra_payment,
        business_date=bill_date,
    )
    session.add(le5)
    session.commit()
