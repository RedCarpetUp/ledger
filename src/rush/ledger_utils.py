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
    Fee,
    LedgerEntry,
    LedgerTriggerEvent,
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
    assert identifier_type in ("user", "lender", "bill", "redcarpet", "card", "loan")
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
    # Check if unbilled is zero. If not, return false.
    _, unbilled_balance = get_account_balance_from_str(
        session, book_string=f"{bill.id}/bill/unbilled/a", to_date=to_date
    )
    if unbilled_balance != 0:
        return False
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

    unpaid_fees = session.query(Fee).filter(Fee.bill_id == bill.id, Fee.fee_status == "UNPAID").all()
    if unpaid_fees:
        return False
    return True


def get_remaining_bill_balance(
    session: Session, bill: LoanData, to_date: Optional[DateTime] = None
) -> Dict[str, Decimal]:
    if bill.is_generated and to_date and to_date.date() < bill.bill_close_date:
        _, principal_due = get_account_balance_from_str(
            session, book_string=f"{bill.id}/bill/unbilled/a", to_date=to_date
        )
    else:
        _, principal_due = get_account_balance_from_str(
            session, book_string=f"{bill.id}/bill/principal_receivable/a", to_date=to_date
        )
    _, interest_due = get_account_balance_from_str(
        session, book_string=f"{bill.id}/bill/interest_receivable/a", to_date=to_date
    )
    d = {"principal_due": principal_due, "interest_due": interest_due}
    fees = session.query(Fee).filter(Fee.bill_id == bill.id, Fee.fee_status == "UNPAID").all()
    for fee in fees:
        fee_due_amount = fee.gross_amount - fee.gross_amount_paid
        d[fee.name] = fee_due_amount
    d["total_due"] = sum(v for _, v in d.items())  # sum of all values becomes total due.

    return d
