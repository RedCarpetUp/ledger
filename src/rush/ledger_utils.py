from decimal import Decimal
from typing import (
    List,
    Tuple,
)

import sqlalchemy
from sqlalchemy import func
from sqlalchemy.orm import Session

from rush.models import (
    BookAccount,
    LedgerEntry,
    LoanData,
    User,
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


def is_min_paid(session: Session, bill: LoanData) -> bool:
    _, min_due = get_account_balance_from_str(session, book_string=f"{bill.id}/bill/min_due/a")
    _, interest_received = get_account_balance_from_str(
        session, book_string=f"{bill.id}/bill/interest_received/a"
    )
    _, principal_received = get_account_balance_from_str(
        session, book_string=f"{bill.id}/bill/principal_received/a"
    )

    return interest_received + principal_received >= min_due


def is_bill_closed(session: Session, bill: LoanData) -> bool:
    # Check if principal is paid. If not, return false.
    _, principal_due = get_account_balance_from_str(
        session, book_string=f"{bill.id}/bill/principal_due/a"
    )
    if principal_due != 0:
        return False

    # Check if interest is paid. If not, return false.
    _, interest_due = get_account_balance_from_str(session, book_string=f"{bill.id}/bill/interest_due/a")
    if interest_due != 0:
        return False

    # Check if late fine is paid. If not, return false.
    _, late_fine_due = get_account_balance_from_str(
        session, book_string=f"{bill.id}/bill/late_fine_due/a"
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


def get_all_unpaid_bills(session: Session, user: User) -> List[LoanData]:
    unpaid_bills = []
    #     session.query(LoanData)
    #     .join(BookAccount, LoanData.id == BookAccount.identifier)
    #     .filter(LoanData.user_id == user.id)
    #     .all()
    # )
    all_bills = session.query(LoanData).filter(LoanData.user_id == user.id).all()
    for bill in all_bills:
        _, principal_due = get_account_balance_from_str(
            session, book_string=f"{bill.id}/bill/principal_due/a"
        )
        if principal_due > 0:
            unpaid_bills.append(principal_due)

    return unpaid_bills


# def get_interest_for_each_bill(session: Session, unpaid_bills: LoanData) -> Decimal:
#     interest_to_charge = []
#     for bill in unpaid_bills:
#         _, principal_due = get_account_balance_from_str(
#             session, book_string=f"{bill.id}/bill/principal_due/a"
#         )
#         if principal_due > 0:
#             _, principal_received = get_account_balance_from_str(
#                 session, book_string=f"{bill.id}/bill/principal_received/a"
#             )
#             total_principal_amount = principal_due + principal_received
#             interest_to_charge.append(
#                 total_principal_amount * Decimal(bill.rc_rate_of_interest_annual) / 1200
#             )

#     return sum(interest_to_charge)
