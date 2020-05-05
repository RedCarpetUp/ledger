from decimal import Decimal

from pendulum import DateTime
from sqlalchemy.orm import Session

from rush.models import (
    BookAccount,
    LedgerEntry,
    LedgerTriggerEvent,
)


def create_ledger_entry(
    session: Session, event_id: int, from_book_id: int, to_book_id: int, amount: Decimal,
) -> LedgerEntry:
    entry = LedgerEntry(
        event_id=event_id,
        from_book_account=from_book_id,
        to_book_account=to_book_id,
        amount=amount,
    )
    session.add(entry)
    session.flush()
    return entry
