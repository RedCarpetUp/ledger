import json
from decimal import Decimal

from sqlalchemy.orm import Session

from rush.ledger_utils import (
    get_account_balance_from_str,
    get_all_unpaid_bills,
)
from rush.models import (
    CardTransaction,
    LoanData,
)


def bill_view(session: Session, user_id: int) -> str:

    opening_amount = 0
    opening_interest_due = 0
    opening_fine_due = 0

    unpaid_bills = get_all_unpaid_bills(session, user_id)
    latest_bill = unpaid_bills.pop(0)
    _, current_amount = get_account_balance_from_str(
        session, book_string=f"{latest_bill.id}/bill/principal_due/a"
    )
    _, current_interest_due = get_account_balance_from_str(
        session=session, book_string=f"{latest_bill.id}/bill/interest_due/a"
    )
    _, current_fine_due = get_account_balance_from_str(
        session=session, book_string=f"{latest_bill.id}/bill/late_fine_due/a"
    )

    for bill in unpaid_bills:
        _, principal_due = get_account_balance_from_str(
            session, book_string=f"{bill.id}/bill/principal_due/a"
        )
        _, interest_due = get_account_balance_from_str(
            session=session, book_string=f"{bill.id}/bill/interest_due/a"
        )
        _, fine_due = get_account_balance_from_str(
            session=session, book_string=f"{latest_bill.id}/bill/late_fine_due/a"
        )
        opening_fine_due = opening_fine_due + fine_due
        opening_interest_due = opening_interest_due + interest_due
        opening_amount = opening_amount + principal_due

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


def transaction_view(session: Session, type_of_view: str, user_id: int, date: str) -> str:
    all_transactions_of_a_user = (
        session.query(LoanData)
        .join(CardTransaction, LoanData.id=CardTransaction.loan_id)
        .filter(LoanData.user_id=user_id, LoanData.agreement_date=date)
        .all()
    )
    # not sure if agreement_date is the criteria or not
    return "work"
