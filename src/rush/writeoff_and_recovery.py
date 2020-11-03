from decimal import Decimal

from sqlalchemy import and_

from rush.card import BaseLoan
from rush.ledger_utils import (
    create_ledger_entry_from_str,
    get_account_balance_from_str,
)
from rush.models import (
    BillFee,
    LedgerTriggerEvent,
    LoanData,
)
from rush.utils import get_current_ist_time


def write_off_all_loans_above_the_dpd(dpd: int = 30) -> None:
    pass


def write_off_loan(user_loan: BaseLoan) -> None:
    reverse_all_unpaid_fees(user_loan=user_loan)  # Remove all unpaid fees.
    total_outstanding = user_loan.get_total_outstanding()
    event = LedgerTriggerEvent(
        loan_id=user_loan.id,
        name="loan_written_off",
        amount=total_outstanding,
        post_date=get_current_ist_time(),
    )
    user_loan.session.add(event)
    user_loan.session.flush()

    write_off_event(user_loan=user_loan, event=event)
    # user_card.loan_status = 'WRITTEN_OFF'  # uncomment after user_loan PR is merged.


def write_off_event(user_loan: BaseLoan, event: LedgerTriggerEvent) -> None:
    # Add an expense for write-off. And reduce amount from the money we need to receive from lender.
    # When we raise the invoice to lender we add money to this receivable account.
    # If balance of the receivable account is in negative then we will add money from rc cash account.
    create_ledger_entry_from_str(
        session=user_loan.session,
        event_id=event.id,
        debit_book_str=f"{user_loan.id}/loan/writeoff_expenses/e",
        credit_book_str=f"{user_loan.lender_id}/lender/lender_receivable/a",
        amount=event.amount,
    )

    # if loan is reset, settle all remaining locked limit against write off expenses account.
    if user_loan.product_type == "term_loan_reset":
        _, locked_limit = get_account_balance_from_str(
            session=user_loan.session, book_string=f"{user_loan.id}/card/locked_limit/l"
        )
        assert locked_limit > Decimal("0")
        create_ledger_entry_from_str(
            session=user_loan.session,
            event_id=event.id,
            debit_book_str=f"{user_loan.id}/card/locked_limit/l",
            credit_book_str=f"{user_loan.id}/loan/writeoff_expenses/e",
            amount=locked_limit,
        )


def recovery_event(user_loan: BaseLoan, event: LedgerTriggerEvent) -> None:
    # Recovery event is reversal of write off event. Add money that we need to receive from lender.
    # Reduce the expenses because the money was recovered.
    create_ledger_entry_from_str(
        user_loan.session,
        event_id=event.id,
        debit_book_str=f"{user_loan.lender_id}/lender/lender_receivable/a",
        credit_book_str=f"{user_loan.loan_id}/loan/writeoff_expenses/e",
        amount=event.amount,
    )


def reverse_all_unpaid_fees(user_loan: BaseLoan) -> None:
    session = user_loan.session
    fees = (
        session.query(BillFee)
        .join(
            LoanData, and_(LoanData.loan_id == user_loan.loan_id, BillFee.identifier_id == LoanData.id)
        )
        .filter(BillFee.identifier_id == user_loan.loan_id, BillFee.fee_status == "UNPAID")
        .all()
    )

    for fee in fees:
        fee.fee_status = "REVERSED"
