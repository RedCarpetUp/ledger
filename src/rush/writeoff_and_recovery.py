from rush.card import BaseLoan
from rush.ledger_utils import create_ledger_entry_from_str
from rush.models import (
    LedgerTriggerEvent,
)
from rush.utils import get_current_ist_time


def write_off_all_loans_above_the_dpd(dpd: int = 30) -> None:
    pass


def write_off_loan(user_loan: BaseLoan) -> None:
    reverse_all_unpaid_fees(user_loan=user_loan)  # Remove all unpaid fees.
    total_outstanding = user_loan.get_total_outstanding()
    event = LedgerTriggerEvent(
        name="loan_written_off",
        amount=total_outstanding,
        post_date=get_current_ist_time(),
    )
    write_off_event(user_loan=user_loan, event=event)
    # user_card.loan_status = 'WRITTEN_OFF'  # uncomment after user_loan PR is merged.


def write_off_event(user_loan: BaseLoan, event: LedgerTriggerEvent) -> None:
    # Add an expense for write-off. And reduce amount from the money we need to receive from lender.
    # When we raise the invoice to lender we add money to this receivable account.
    # If balance of the receivable account is in negative then we will add money from rc cash account.
    create_ledger_entry_from_str(
        session=user_loan.session,
        event_id=event.id,
        debit_book_str=f"{user_loan.loan_id}/loan/write_off_expenses/e",
        credit_book_str=f"{user_loan.lender_id}/lender/lender_receivable/a",
        amount=event.amount,
    )


def reverse_all_unpaid_fees(user_loan: BaseLoan) -> None:
    session = user_loan.session
    # fee = session.query(Fee).filter(Fee.loan_id == user_card.loan_id, Fee.fee_status == "UNPAID").all()
    # fee.fee_status = "REVERSED"

    raise Exception("NotImplemented")
