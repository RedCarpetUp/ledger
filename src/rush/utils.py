import sqlalchemy

from rush.models import LedgerTriggerEvent, LedgerEntry, get_current_ist_time, BookAccount,get_or_create


def insert_payments(
    session: sqlalchemy.orm.session.Session, event_name: str, extra_details: dict
) -> None:
    u = LedgerTriggerEvent(performed_by=123, name=event_name, extra_details=extra_details)
    session.add(u)
    session.flush()

    from_account=get_or_create(session=session,model=BookAccount,identifier=100,book_type='dmi_pool_account',account_type='liability')
    to_account=get_or_create(session=session,model=BookAccount,identifier=100,book_type='dmi_limit_used',account_type='asset')
    le = LedgerEntry( event_id=u.id, from_book_account=from_account.id,to_book_account=to_account.id,amount=100,business_date=get_current_ist_time())
    session.add(le)

    from_account=get_or_create(session=session,model=BookAccount,identifier=200,book_type='user_card_balance',account_type='liability')
    to_account=get_or_create(session=session,model=BookAccount,identifier=200,book_type='unbilled_transactions',account_type='asset')
    le = LedgerEntry( event_id=u.id, from_book_account=from_account.id,to_book_account=to_account.id,amount=100,business_date=get_current_ist_time())
    session.add(le)

    from_account=get_or_create(session=session,model=BookAccount,identifier=200,book_type='user_marvin_limit',account_type='liability')
    to_account=get_or_create(session=session,model=BookAccount,identifier=200,book_type='user_marvin_limit_used',account_type='asset')
    le = LedgerEntry( event_id=u.id, from_book_account=from_account.id,to_book_account=to_account.id,amount=100,business_date=get_current_ist_time())
    session.add(le)
    session.commit()
