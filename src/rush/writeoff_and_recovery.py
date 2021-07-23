from rush.card import BaseLoan
from rush.ledger_utils import create_ledger_entry_from_str
from rush.models import (
    Fee,
    LedgerLoanData,
    LedgerTriggerEvent,
    PaymentRequestsData,
)


def write_off_all_loans_above_the_dpd(dpd: int = 30) -> None:
    pass


def write_off_loan(user_loan: BaseLoan, payment_request_data: PaymentRequestsData) -> None:
    from rush.create_emi import update_journal_entry

    event = LedgerTriggerEvent(
        name="loan_written_off",
        loan_id=user_loan.loan_id,
        post_date=payment_request_data.intermediary_payment_date,
        extra_details={
            "payment_request_id": payment_request_data.payment_request_id,
        },
        amount=payment_request_data.payment_request_amount,
    )
    user_loan.session.add(event)
    user_loan.session.flush()
    reverse_all_unpaid_fees(user_loan=user_loan, event=event)  # Remove all unpaid fees
    write_off_event(user_loan=user_loan, event=event)
    user_loan.loan_status = "WRITTEN_OFF"  # uncomment after user_loan PR is merged.
    update_journal_entry(user_loan=user_loan, event=event)


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


def recovery_event(user_loan: BaseLoan, event: LedgerTriggerEvent) -> None:
    # Recovery event is reversal of write off event. Add money that we need to receive from lender.
    # Reduce the expenses because the money was recovered.
    create_ledger_entry_from_str(
        user_loan.session,
        event_id=event.id,
        debit_book_str=f"{user_loan.lender_id}/lender/lender_receivable/a",
        credit_book_str=f"{user_loan.loan_id}/loan/write_off_expenses/e",
        amount=event.amount,
    )


def reverse_all_unpaid_fees(user_loan: BaseLoan, event: LedgerTriggerEvent) -> None:
    session = user_loan.session
    fees = (
        session.query(Fee, LedgerLoanData)
        .filter(
            LedgerLoanData.loan_id == user_loan.loan_id,
            Fee.identifier_id == LedgerLoanData.id,
            Fee.identifier == "bill",
            Fee.fee_status == "UNPAID",
        )
        .all()
    )
    for fee, _ in fees:
        fee.fee_status = "REVERSED"
        create_ledger_entry_from_str(
            user_loan.session,
            event_id=event.id,
            debit_book_str=f"{fee.identifier_id}/bill/max/l",
            credit_book_str=f"{fee.identifier_id}/bill/max/a",
            amount=fee.remaining_fee_amount,
        )
