from decimal import Decimal
from typing import List, Tuple

from sqlalchemy.orm import Session

from rush.ledger_utils import (
    create_ledger_entry_from_str,
    get_account_balance_from_str,
    get_all_unpaid_bills,
)
from rush.models import (
    CardTransaction,
    LedgerTriggerEvent,
    LoanData,
    UserCard,
)


def lender_disbursal_event(session: Session, event: LedgerTriggerEvent) -> None:
    create_ledger_entry_from_str(
        session,
        event_id=event.id,
        debit_book_str=f"12345/redcarpet/rc_cash/a",
        credit_book_str=f"62311/lender/lender_capital/l",
        amount=event.amount,
    )


def m2p_transfer_event(session: Session, event: LedgerTriggerEvent) -> None:
    create_ledger_entry_from_str(
        session,
        event_id=event.id,
        debit_book_str=f"62311/lender/pool_balance/a",
        credit_book_str=f"12345/redcarpet/rc_cash/a",
        amount=event.amount,
    )


def card_transaction_event(session: Session, user_card: UserCard, event: LedgerTriggerEvent) -> None:
    amount = Decimal(event.amount)
    user_card_id = user_card.id
    swipe_id = event.extra_details["swipe_id"]
    bill = (
        session.query(LoanData)
        .filter(LoanData.id == CardTransaction.loan_id, CardTransaction.id == swipe_id)
        .scalar()
    )
    lender_id = bill.lender_id
    bill_id = bill.id
    # Reduce user's card balance
    create_ledger_entry_from_str(
        session,
        event_id=event.id,
        debit_book_str=f"{user_card_id}/card/available_limit/l",
        credit_book_str=f"{user_card_id}/card/available_limit/a",
        amount=amount,
    )

    # Move debt from one account to another. We will be charged interest on lender_payable.
    create_ledger_entry_from_str(
        session,
        event_id=event.id,
        debit_book_str=f"{lender_id}/lender/lender_capital/l",
        credit_book_str=f"{user_card.id}/card/lender_payable/l",
        amount=amount,
    )

    # Reduce money from lender's pool account
    create_ledger_entry_from_str(
        session,
        event_id=event.id,
        debit_book_str=f"{bill_id}/bill/unbilled/a",
        credit_book_str=f"{lender_id}/lender/pool_balance/a",
        amount=amount,
    )


def bill_generate_event(session: Session, bill: LoanData, event: LedgerTriggerEvent) -> None:
    bill_id = bill.id

    # Move all unbilled book amount to billed account
    _, unbilled_balance = get_account_balance_from_str(session, book_string=f"{bill_id}/bill/unbilled/a")

    create_ledger_entry_from_str(
        session,
        event_id=event.id,
        debit_book_str=f"{bill_id}/bill/principal_receivable/a",
        credit_book_str=f"{bill_id}/bill/unbilled/a",
        amount=unbilled_balance,
    )


def add_min_amount_event(session: Session, bill: LoanData, event: LedgerTriggerEvent) -> None:
    bill_id = bill.id

    create_ledger_entry_from_str(
        session,
        event_id=event.id,
        debit_book_str=f"{bill_id}/bill/min/a",
        credit_book_str=f"{bill_id}/bill/min/l",
        amount=event.amount,
    )


def payment_received_event(session: Session, user_card: UserCard, event: LedgerTriggerEvent) -> None:
    payment_received = Decimal(event.amount)
    unpaid_bills = get_all_unpaid_bills(session, user_card.user_id)

    payment_received = _adjust_for_min(session, unpaid_bills, payment_received, event.id)
    payment_received = _adjust_for_complete_bill(session, unpaid_bills, payment_received, event.id)

    if payment_received > 0:
        _adjust_for_prepayment(session)

    # Lender has received money, so we reduce our liability now.
    create_ledger_entry_from_str(
        session,
        event_id=event.id,
        debit_book_str=f"{user_card.id}/card/lender_payable/l",
        credit_book_str=f"{user_card.id}/card/pg_account/a",
        amount=payment_received,
    )


def _adjust_bill(
    session: Session, bill: LoanData, amount_to_adjust_in_this_bill: Decimal, event_id: int
) -> Decimal:
    def adjust(payment_to_adjust_from: Decimal, to_acc: str, from_acc: str) -> Decimal:
        if payment_to_adjust_from <= 0:
            return payment_to_adjust_from
        _, book_balance = get_account_balance_from_str(session, book_string=from_acc)
        if book_balance > 0:
            balance_to_adjust = min(payment_to_adjust_from, book_balance)
            create_ledger_entry_from_str(
                session,
                event_id=event_id,
                debit_book_str=to_acc,
                credit_book_str=from_acc,
                amount=balance_to_adjust,
            )
            payment_to_adjust_from -= balance_to_adjust
        return payment_to_adjust_from

    # Now adjust into other accounts.
    remaining_amount = adjust(
        amount_to_adjust_in_this_bill,
        to_acc=f"{bill.lender_id}/lender/pg_account/a",
        from_acc=f"{bill.id}/bill/late_fine_receivable/a",
    )
    remaining_amount = adjust(
        remaining_amount,
        to_acc=f"{bill.lender_id}/lender/pg_account/a",
        from_acc=f"{bill.id}/bill/interest_receivable/a",
    )
    remaining_amount = adjust(
        remaining_amount,
        to_acc=f"{bill.lender_id}/lender/pg_account/a",
        from_acc=f"{bill.id}/bill/principal_receivable/a",
    )
    return remaining_amount


def _adjust_for_min(
    session: Session, bills: List[LoanData], payment_received: Decimal, event_id: int
) -> Decimal:
    for bill in bills:
        min_due = bill.get_minimum_amount_to_pay(session)
        amount_to_adjust_in_this_bill = min(min_due, payment_received)
        payment_received -= amount_to_adjust_in_this_bill  # Remove amount from the original variable.

        # Reduce min amount
        create_ledger_entry_from_str(
            session,
            event_id=event_id,
            debit_book_str=f"{bill.id}/bill/min/l",
            credit_book_str=f"{bill.id}/bill/min/a",
            amount=amount_to_adjust_in_this_bill,
        )
        remaining_amount = _adjust_bill(session, bill, amount_to_adjust_in_this_bill, event_id)
        assert remaining_amount == 0  # Can't be more than 0
    return payment_received  # The remaining amount goes back to the main func.


def _adjust_for_complete_bill(
    session: Session, bills: List[LoanData], payment_received: Decimal, event_id: int
) -> Decimal:
    for bill in bills:
        payment_received = _adjust_bill(session, bill, payment_received, event_id)
    return payment_received  # The remaining amount goes back to the main func.


def _adjust_for_prepayment(session: Session) -> None:
    pass  # TODO


def accrue_interest_event(session: Session, bill: LoanData, event: LedgerTriggerEvent) -> None:
    create_ledger_entry_from_str(
        session,
        event_id=event.id,
        debit_book_str=f"{bill.id}/bill/interest_receivable/a",
        credit_book_str=f"{bill.id}/bill/interest_earned/r",
        amount=event.amount,
    )


def accrue_late_fine_event(session: Session, bill: LoanData, event: LedgerTriggerEvent) -> None:
    create_ledger_entry_from_str(
        session,
        event_id=event.id,
        debit_book_str=f"{bill.id}/bill/late_fine_receivable/a",
        credit_book_str=f"{bill.id}/bill/late_fine/r",
        amount=event.amount,
    )

    # Add into min amount of the bill too.
    add_min_amount_event(session, bill, event)
