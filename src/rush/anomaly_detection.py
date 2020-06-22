from typing import List

from sqlalchemy.orm import Session

from rush.accrue_financial_charges import (
    accrue_interest_prerequisites,
    accrue_late_charges_prerequisites,
    reverse_late_charges,
)
from rush.models import (
    BookAccount,
    LedgerEntry,
    LedgerTriggerEvent,
    LoanData,
)


def get_affected_events(session: Session, book_identifier: int) -> List[LedgerTriggerEvent]:
    # The book identifier here will be of a particular bill.
    # TODO Maybe have bill_id column in events table?
    all_book_accounts = (
        session.query(BookAccount.id).filter(BookAccount.identifier == book_identifier).subquery()
    )
    event_ids = (
        session.query(LedgerEntry.event_id)
        .filter(
            LedgerEntry.debit_account.in_(all_book_accounts)
            | LedgerEntry.credit_account.in_(all_book_accounts),
        )
        .subquery()
    )

    # These are the only events which can be affected by a delay in payment.
    ledger_events = (
        session.query(LedgerTriggerEvent)
        .filter(
            LedgerTriggerEvent.id.in_(event_ids),
            LedgerTriggerEvent.name.in_(["accrue_interest", "accrue_late_fine", "payment_received"]),
        )
        .order_by(LedgerTriggerEvent.post_date)
        .all()
    )
    return ledger_events


def run_anomaly(session: Session, bill: LoanData) -> None:
    events = get_affected_events(session, bill.id)
    for event in events:
        if event.name == "accrue_interest":
            do_prerequisites_meet = accrue_interest_prerequisites(session, bill, to_date=event.post_date)
            if not do_prerequisites_meet:
                print("reverse event")
        elif event.name == "accrue_late_fine":
            do_prerequisites_meet = accrue_late_charges_prerequisites(
                session, bill, to_date=event.post_date
            )
            if not do_prerequisites_meet:
                reverse_late_charges(session, bill, event)
