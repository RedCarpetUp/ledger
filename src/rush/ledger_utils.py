from decimal import Decimal
from typing import (
    Dict,
    Optional,
    Tuple,
)

from pendulum import DateTime
from sqlalchemy import func
from sqlalchemy.orm import Session

from rush.models import (
    BillFee,
    BookAccount,
    LedgerEntry,
    LoanData,
    get_or_create,
)


def create_ledger_entry(
    session: Session,
    event_id: int,
    debit_book_id: int,
    credit_book_id: int,
    amount: Decimal,
) -> LedgerEntry:
    entry = LedgerEntry(
        event_id=event_id,
        debit_account=debit_book_id,
        credit_account=credit_book_id,
        amount=amount,
    )
    session.add(entry)
    session.flush()
    return entry


def create_ledger_entry_from_str(
    session: Session,
    event_id: int,
    debit_book_str: str,
    credit_book_str: str,
    amount: Decimal,
) -> LedgerEntry:
    debit_account = get_book_account_by_string(session, book_string=debit_book_str)
    credit_account = get_book_account_by_string(session, book_string=credit_book_str)
    return create_ledger_entry(session, event_id, debit_account.id, credit_account.id, amount)


def get_account_balance_from_str(
    session: Session,
    book_string: str,
    to_date: Optional[DateTime] = None,
    from_date: Optional[DateTime] = None,
) -> Tuple[int, Decimal]:
    book_variables = breakdown_account_variables_from_str(book_string)
    if from_date and to_date:
        f = func.get_account_balance_between_periods(
            book_variables["identifier"],
            book_variables["identifier_type"],
            book_variables["name"],
            book_variables["account_type"],
            from_date,
            to_date,
        )
    elif to_date:
        f = func.get_account_balance(
            book_variables["identifier"],
            book_variables["identifier_type"],
            book_variables["name"],
            book_variables["account_type"],
            to_date,
        )
    else:
        f = func.get_account_balance(
            book_variables["identifier"],
            book_variables["identifier_type"],
            book_variables["name"],
            book_variables["account_type"],
        )
    account_balance = session.query(f).scalar() or 0
    return 0, Decimal(account_balance)


def breakdown_account_variables_from_str(book_string: str) -> dict:
    identifier, identifier_type, name, account_type = book_string.split("/")
    assert account_type in ("a", "l", "r", "e", "ca")
    assert identifier_type in (
        "user",
        "lender",
        "bill",
        "redcarpet",
        "card",
        "loan",
        "product",
    )
    return {
        "identifier": identifier,
        "identifier_type": identifier_type,
        "name": name,
        "account_type": account_type,
    }


def get_book_account_by_string(session: Session, book_string: str) -> BookAccount:
    book_variables = breakdown_account_variables_from_str(book_string)
    book_account = get_or_create(
        session=session,
        model=BookAccount,
        identifier=book_variables["identifier"],
        identifier_type=book_variables["identifier_type"],
        book_name=book_variables["name"],
        account_type=book_variables["account_type"],
    )
    return book_account


def is_bill_closed(session: Session, bill: LoanData, to_date: Optional[DateTime] = None) -> bool:
    # Check if max balance is zero. If not, return false.
    _, max_balance = get_account_balance_from_str(
        session, book_string=f"{bill.id}/bill/max/a", to_date=to_date
    )
    if max_balance != 0:
        return False
    return True
