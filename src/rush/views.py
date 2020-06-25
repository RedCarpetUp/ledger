import json

from sqlalchemy import (
    String,
    cast,
)
from sqlalchemy.orm import Session

from rush.ledger_utils import get_account_balance_from_str
from rush.models import (
    BookAccount,
    CardTransaction,
    LedgerEntry,
    LedgerTriggerEvent,
    LoanData,
)


def bill_view(session: Session, user_id: int) -> str:

    opening_amount = 0
    opening_interest_due = 0
    opening_fine_due = 0
    index = 0
    bill_detials = []
    unpaid_bill_details = []
    all_bills = (
        session.query(LoanData)
        .filter(LoanData.user_id == user_id)
        .order_by(LoanData.agreement_date.desc())
        .all()
    )
    for bill in all_bills:
        _, principal_due = get_account_balance_from_str(session, book_string=f"{bill.id}/bill/billed/a")
        _, interest_due = get_account_balance_from_str(
            session=session, book_string=f"{bill.id}/bill/interest_due/a"
        )
        _, fine_due = get_account_balance_from_str(
            session=session, book_string=f"{bill.id}/bill/late_fine_due/a"
        )
        bill_detials.append(
            {
                "bill": bill,
                "principal_due": principal_due,
                "interest_due": interest_due,
                "fine_due": fine_due,
                "transactions": transaction_view(session, bill.id),
            }
        )
        if principal_due > 0:
            if index == 0:
                current_amount = principal_due
                current_interest_due = interest_due
                current_fine_due = fine_due
            else:
                opening_fine_due = opening_fine_due + fine_due
                opening_interest_due = opening_interest_due + interest_due
                opening_amount = opening_amount + principal_due
            unpaid_bill_details.append(
                {
                    "bill": bill,
                    "principal_due": principal_due,
                    "interest_due": interest_due,
                    "fine_due": fine_due,
                    "transactions": transaction_view(session, bill.id),
                }
            )
        index += 1

    opening_balance = opening_amount + opening_interest_due + opening_fine_due
    current_balance = current_amount + current_interest_due + current_fine_due
    total_interest = opening_interest_due + current_fine_due
    total_fine = opening_fine_due + current_fine_due

    return json.dumps(
        {
            "opening_balance": "{opening_balance}",
            "current_balance": "{current_balance}",
            "total_interest": "{total_interest}",
            "total_fine": "{total_fine}",
        }
    )


def transaction_view(session: Session, bill_id: int) -> LedgerTriggerEvent:

    all_book_accounts = (
        session.query(BookAccount.id).filter(BookAccount.identifier == bill_id).subquery()
    )

    event_ids = (
        session.query(LedgerEntry.event_id)
        .filter(
            LedgerEntry.debit_account.in_(all_book_accounts)
            | LedgerEntry.credit_account.in_(all_book_accounts)
        )
        .subquery()
    )

    # These are the only events which are associated to payments and swipes.
    all_transactions = (
        session.query(LedgerTriggerEvent.extra_details, CardTransaction.created_at, LedgerEntry.amount)
        .join(LedgerEntry, LedgerEntry.event_id == LedgerTriggerEvent.id)
        .outerjoin(
            CardTransaction,
            cast(LedgerTriggerEvent.extra_details["swipe_id"], String) == str(CardTransaction.id),
        )
        .filter(
            LedgerTriggerEvent.id.in_(event_ids),
            LedgerTriggerEvent.name.in_(["card_transaction", "bill_close"]),
        )
        .order_by(LedgerTriggerEvent.id.desc())
        .all()
    )

    return all_transactions
