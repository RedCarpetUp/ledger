from decimal import Decimal
from typing import (
    Dict,
    Optional,
    Tuple,
)

import sqlalchemy
from pendulum import DateTime
from sqlalchemy import func
from sqlalchemy.orm import Session

from rush.models import (
    BookAccount,
    LedgerEntry,
    LedgerTriggerEvent,
    LoanData,
    get_or_create,
)


def create_ledger_entry(
    session: Session, event_id: int, debit_book_id: int, credit_book_id: int, amount: Decimal,
) -> LedgerEntry:
    entry = LedgerEntry(
        event_id=event_id, debit_account=debit_book_id, credit_account=credit_book_id, amount=amount,
    )
    session.add(entry)
    session.flush()
    return entry


def create_ledger_entry_from_str(
    session: Session, event_id: int, debit_book_str: str, credit_book_str: str, amount: Decimal,
) -> LedgerEntry:
    debit_account = get_book_account_by_string(session, book_string=debit_book_str)
    credit_account = get_book_account_by_string(session, book_string=credit_book_str)
    return create_ledger_entry(session, event_id, debit_account.id, credit_account.id, amount)


def get_account_balance(
    session: sqlalchemy.orm.session.Session, book_account: BookAccount, to_date: Optional[DateTime]
) -> Decimal:
    debit_balance = session.query(func.sum(LedgerEntry.amount)).filter(
        LedgerEntry.debit_account == book_account.id,
    )
    if to_date:
        debit_balance = debit_balance.filter(
            LedgerEntry.event_id == LedgerTriggerEvent.id, LedgerTriggerEvent.post_date < to_date,
        )
    debit_balance = debit_balance.scalar() or 0

    credit_balance = session.query(func.sum(LedgerEntry.amount)).filter(
        LedgerEntry.credit_account == book_account.id,
    )
    if to_date:
        credit_balance = credit_balance.filter(
            LedgerEntry.event_id == LedgerTriggerEvent.id, LedgerTriggerEvent.post_date < to_date,
        )
    credit_balance = credit_balance.scalar() or 0

    if book_account.account_type in ("a", "e"):
        final_balance = debit_balance - credit_balance
    elif book_account.account_type in ("l", "r"):
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
    assert account_type in ("a", "l", "r", "e")
    assert identifier_type in ("user", "lender", "bill", "redcarpet", "card")

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
        session, book_string=f"{bill.id}/bill/min/a", to_date=to_date
    )
    _, interest_received = get_account_balance_from_str(
        session, book_string=f"{bill.id}/bill/interest_received/a", to_date=to_date
    )
    _, principal_received = get_account_balance_from_str(
        session, book_string=f"{bill.id}/bill/principal_received/a", to_date=to_date
    )
    amount_received = interest_received + principal_received

    # Consider all receivables if event_date is not null. Assuming it's being checked for anomlay.
    # In that case the payment can be settled in any of the receivables.
    if to_date:
        _, late_fee_received = get_account_balance_from_str(
            session, book_string=f"{bill.id}/bill/late_fee_received/a", to_date=to_date
        )
        amount_received += late_fee_received
    return amount_received >= min_due


def is_bill_closed(session: Session, bill: LoanData, to_date: Optional[DateTime] = None) -> bool:
    # Check if principal is paid. If not, return false.
    _, principal_due = get_account_balance_from_str(
        session, book_string=f"{bill.id}/bill/principal_receivable/a", to_date=to_date
    )
    if principal_due != 0:
        return False

    # Check if interest is paid. If not, return false.
    _, interest_due = get_account_balance_from_str(
        session, book_string=f"{bill.id}/bill/interest_receivable/a", to_date=to_date
    )
    if interest_due != 0:
        return False

    # Check if late fine is paid. If not, return false.
    _, late_fine_due = get_account_balance_from_str(
        session, book_string=f"{bill.id}/bill/late_fine_receivable/a", to_date=to_date
    )
    if late_fine_due != 0:
        return False
    return True


def get_remaining_bill_balance(session: Session, bill: LoanData) -> Dict[str, Decimal]:
    _, principal_due = get_account_balance_from_str(
        session, book_string=f"{bill.id}/bill/principal_receivable/a"
    )
    _, interest_due = get_account_balance_from_str(
        session, book_string=f"{bill.id}/bill/interest_receivable/a"
    )
    _, late_fine_due = get_account_balance_from_str(
        session, book_string=f"{bill.id}/bill/late_fine_receivable/a"
    )
    return {
        "total_due": principal_due + interest_due + late_fine_due,
        "principal_due": principal_due,
        "interest_due": interest_due,
        "late_fine": late_fine_due,
    }
