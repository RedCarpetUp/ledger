import json
from decimal import Decimal

from sqlalchemy import (
    JSON,
    Integer,
    String,
    cast,
    text,
)
from sqlalchemy.orm import Session

from rush.ledger_utils import (
    get_account_balance_from_str,
    get_all_unpaid_bills,
    get_remaining_bill_balance,
    is_bill_closed,
)
from rush.models import (
    BookAccount,
    CardTransaction,
    LedgerEntry,
    LedgerTriggerEvent,
    LoanData,
)


def user_view(session, user_id: int) -> dict:
    index = 0
    total_due, min_amount, current_bill_principal_amount, current_bill_principal_interest = 0, 0, 0, 0
    for bill in get_all_unpaid_bills(session, user_id):
        bill_balance = get_remaining_bill_balance(session, bill)
        total_due = total_due + bill_balance["total_due"]
        min_amount = min_amount + bill_balance["interest_due"] + bill_balance["late_fine"]
        if index == 0:
            current_bill_principal_amount = bill_balance["principal_due"]
            current_bill_principal_interest = min_amount

    return {
        "max_to_pay": total_due,
        "min_to_pay": min_amount,
        "current_bill_balance": current_bill_principal_amount,
        "current_bill_interest": current_bill_principal_interest,
        "previous_amount_to_pay": total_due
        - current_bill_principal_amount
        - current_bill_principal_interest,
    }


def bill_view(session: Session, user_id: int) -> list:

    bill_details = []
    all_bills = (
        session.query(LoanData)
        .filter(LoanData.user_id == user_id)
        .order_by(LoanData.agreement_date.desc())
        .all()
    )
    for bill in all_bills:
        bill_balance = get_remaining_bill_balance(session, bill)
        bill_details.append(
            {
                "bill_id": bill.id,
                "principal_due": bill_balance["principal_due"],
                "interest_due": bill_balance["interest_due"],
                "fine_due": bill_balance["late_fine"],
                "paid_status": is_bill_closed(session, bill),
                "transactions": transaction_view(session, bill.id),
            }
        )


def transaction_view(session: Session, bill_id: int) -> list:

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
        session.query(
            LedgerTriggerEvent.name,
            LedgerTriggerEvent.amount,
            CardTransaction.description,
            CardTransaction.txn_time,
        )
        .outerjoin(
            CardTransaction,
            cast(LedgerTriggerEvent.extra_details["swipe_id"], String) == str(CardTransaction.id),
        )
        .filter(
            LedgerTriggerEvent.id.in_(event_ids),
            LedgerTriggerEvent.name.in_(["card_transaction", "bill_close"]),
        )
        .order_by(LedgerTriggerEvent.post_date.desc())
        .all()
    )
    transactions = []
    for transaction in all_transactions:
        transactions.append(
            {
                "transaction_type": transaction.name,
                "amount": transaction.amount,
                "transaction_date": transaction.txn_time,
                "description": transaction.description,
            }
        )
    return transactions
