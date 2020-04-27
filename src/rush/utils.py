import sqlalchemy

from rush.models import (
    LedgerTriggerEvent,
    LedgerEntry,
    get_current_ist_time,
    BookAccount,
    get_or_create,
    User,
)


def insert_card_swipe(
    session: sqlalchemy.orm.session.Session,
    user: User,
    event_name: str,
    extra_details: dict,
    amount: int,
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
        business_date=get_current_ist_time(),
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
        business_date=get_current_ist_time(),
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
        business_date=get_current_ist_time(),
    )
    session.add(le3)
    session.commit()
