import sqlalchemy

from rush.models import LedgerTriggerEvent, LedgerEntry, get_current_ist_time, BookAccount,get_or_create


def insert_payments(
    session: sqlalchemy.orm.session.Session, event_name: str, extra_details: dict
) -> None:
    u = LedgerTriggerEvent(performed_by=123, name=event_name, extra_details=extra_details)
    session.add(u)
    session.commit()

    get_or_create(session=session,model=BookAccount,identifier='DMI',book_type='pool_account',account_type='liability')
    # le = LedgerEntry( event_id=123, from_book_account=,to_book_account=,amount=,business_date=get_current_ist_time())
    # session.add(le)
    session.commit()
