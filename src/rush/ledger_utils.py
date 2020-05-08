from decimal import Decimal
from typing import Tuple

import sqlalchemy
from sqlalchemy import func
from sqlalchemy.orm import Session

from rush.models import (
    BookAccount,
    LedgerEntry,
    get_or_create,
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


def get_account_balance(session: sqlalchemy.orm.session.Session, book_account: BookAccount,) -> Decimal:

    debit_balance = (
        session.query(func.sum(LedgerEntry.amount))
        .filter(LedgerEntry.from_book_account == book_account.id,)
        .scalar()
        or 0
    )

    credit_balance = (
        session.query(func.sum(LedgerEntry.amount))
        .filter(LedgerEntry.to_book_account == book_account.id,)
        .scalar()
        or 0
    )
    final_balance = round(credit_balance - debit_balance, 2)

    return round(Decimal(final_balance), 2)


def get_account_balance_from_str(session: Session, book_string: str) -> Tuple[BookAccount, Decimal]:
    book_account = get_book_account_by_string(session, book_string)
    account_balance = get_account_balance(session, book_account)
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
