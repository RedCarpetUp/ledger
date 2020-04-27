import sqlalchemy

from rush.models import LedgerTriggerEvent


def insert_payments(
    session: sqlalchemy.orm.session.Session, event_name: str, extra_details: dict
) -> None:
    u = LedgerTriggerEvent(id=100, performed_by=123, name=event_name, extra_details=extra_details)
    session.add(u)
    session.commit()
