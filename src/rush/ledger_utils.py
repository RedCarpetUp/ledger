from decimal import Decimal
from typing import Tuple, Optional

import sqlalchemy
from pendulum import DateTime
from sqlalchemy import func
from sqlalchemy.orm import Session

from rush.models import (
    BookAccount,
    LedgerEntry,
    LoanData,
    get_or_create,
    LedgerTriggerEvent,
)


def create_ledger_entry(
    session: Session, event_id: int, from_book_id: int, to_book_id: int, amount: Decimal,
) -> LedgerEntry:
    entry = LedgerEntry(
        event_id=event_id, from_book_account=from_book_id, to_book_account=to_book_id, amount=amount,
    )
    session.add(entry)
    session.flush()
    return entry


def get_account_balance(
    session: sqlalchemy.orm.session.Session, book_account: BookAccount, to_date: Optional[DateTime]
) -> Decimal:
    debit_balance = session.query(func.sum(LedgerEntry.amount)).filter(
        LedgerEntry.from_book_account == book_account.id,
    )
    if to_date:
        debit_balance = debit_balance.filter(
            LedgerEntry.event_id == LedgerTriggerEvent.id, LedgerTriggerEvent.post_date < to_date,
        )
    debit_balance = debit_balance.scalar() or 0

    credit_balance = session.query(func.sum(LedgerEntry.amount)).filter(
        LedgerEntry.to_book_account == book_account.id,
    )
    if to_date:
        credit_balance = credit_balance.filter(
            LedgerEntry.event_id == LedgerTriggerEvent.id, LedgerTriggerEvent.post_date < to_date,
        )
    credit_balance = credit_balance.scalar() or 0
    final_balance = credit_balance - debit_balance

    return final_balance


def get_account_balance_from_str(
    session: Session, book_string: str, to_date: Optional[DateTime] = None
) -> Tuple[BookAccount, Decimal]:
    book_account = get_book_account_by_string(session, book_string)
    account_balance = get_account_balance(session, book_account, to_date=to_date)
    return book_account, account_balance


def get_book_account_by_string(session: Session, book_string) -> BookAccount:
    identifier, identifier_type, name, account_type = book_string.split("/")
    assert account_type in ("a", "l")
    assert identifier_type in ("user", "lender", "bill")

    book_account = get_or_create(
        session=session,
        model=BookAccount,
        identifier=identifier,
        identifier_type=identifier_type,
        book_name=name,
        account_type=account_type,
    )
    return book_account


def is_min_paid(session: Session, bill: LoanData, to_date: Optional[DateTime] = None) -> bool:
    _, min_due = get_account_balance_from_str(
        session, book_string=f"{bill.id}/bill/min_due/a", to_date=to_date
    )
    _, interest_received = get_account_balance_from_str(
        session, book_string=f"{bill.id}/bill/interest_received/a", to_date=to_date
    )
    _, principal_received = get_account_balance_from_str(
        session, book_string=f"{bill.id}/bill/principal_received/a", to_date=to_date
    )

    return interest_received + principal_received >= min_due


def is_bill_closed(session: Session, bill: LoanData, to_date: Optional[DateTime] = None) -> bool:
    # Check if principal is paid. If not, return false.
    _, principal_due = get_account_balance_from_str(
        session, book_string=f"{bill.id}/bill/principal_due/a", to_date=to_date
    )
    if principal_due != 0:
        return False

    # Check if interest is paid. If not, return false.
    _, interest_due = get_account_balance_from_str(
        session, book_string=f"{bill.id}/bill/interest_due/a", to_date=to_date
    )
    if interest_due != 0:
        return False

    # Check if late fine is paid. If not, return false.
    _, late_fine_due = get_account_balance_from_str(
        session, book_string=f"{bill.id}/bill/late_fine_due/a", to_date=to_date
    )
    if late_fine_due != 0:
        return False
    return True


def get_remaining_bill_balance(session: Session, bill: LoanData) -> dict:
    _, opening_balance = get_account_balance_from_str(
        session, book_string=f"{bill.id}/bill/principal_due/a"
    )
    _, principal_due = get_account_balance_from_str(
        session, book_string=f"{bill.id}/bill/principal_due/a"
    )
    _, interest_due = get_account_balance_from_str(session, book_string=f"{bill.id}/bill/interest_due/a")
    _, late_fine_due = get_account_balance_from_str(
        session, book_string=f"{bill.id}/bill/late_fine_due/a"
    )
    return {
        "total_due": principal_due + interest_due + late_fine_due,
        "principal_due": principal_due,
        "interest_due": interest_due,
        "late_fine": late_fine_due,
    }
