from decimal import Decimal

from sqlalchemy.orm import Session

from rush.ledger_utils import (
    create_ledger_entry,
    get_account_balance,
    get_account_balance_from_str,
    get_book_account_by_string,
)
from rush.models import (
    CardTransaction,
    LedgerTriggerEvent,
    LoanData,
)


def card_transaction_event(session: Session, user_id: int, event: LedgerTriggerEvent) -> None:
    amount = event.amount
    swipe_id = event.extra_details["swipe_id"]
    bill_id = session.query(CardTransaction.loan_id).filter_by(id=swipe_id).scalar()
    # Get money from lender pool account to lender limit used.
    lender_pool_account = get_book_account_by_string(
        session=session, book_string="62311/lender/pool_account/l"
    )
    lender_limit_utilized = get_book_account_by_string(
        session=session, book_string="62311/lender/limit_utilized/a"
    )
    create_ledger_entry(
        session,
        event_id=event.id,
        from_book_id=lender_pool_account.id,
        to_book_id=lender_limit_utilized.id,
        amount=amount,
    )

    # Reduce user's card balance TODO This goes in negative right now. Need to add load money event.
    user_card_balance = get_book_account_by_string(
        session, book_string=f"{user_id}/user/user_card_balance/l"
    )
    unbilled_transactions = get_book_account_by_string(
        session, book_string=f"{bill_id}/bill/unbilled_transactions/a"
    )
    create_ledger_entry(
        session,
        event_id=event.id,
        from_book_id=user_card_balance.id,
        to_book_id=unbilled_transactions.id,
        amount=amount,
    )


def bill_generate_event(session: Session, bill: LoanData, event: LedgerTriggerEvent) -> None:
    # interest_monthly = 3
    # Move all unbilled book amount to principal due
    unbilled_book, unbilled_balance = get_account_balance_from_str(
        session, book_string=f"{bill.id}/bill/unbilled_transactions/a"
    )

    principal_due_book = get_book_account_by_string(
        session, book_string=f"{bill.id}/bill/principal_due/a"
    )
    create_ledger_entry(
        session,
        event_id=event.id,
        from_book_id=unbilled_book.id,
        to_book_id=principal_due_book.id,
        amount=unbilled_balance,
    )

    # Also store min amount. Assuming it to be 3% interest + 10% principal.
    min = unbilled_balance * Decimal("0.03") + unbilled_balance * Decimal("0.10")
    min_due_cp_book = get_book_account_by_string(session, book_string=f"{bill.id}/bill/min_due_cp/l")
    min_due_book = get_book_account_by_string(session, book_string=f"{bill.id}/bill/min_due/a")
    create_ledger_entry(
        session,
        event_id=event.id,
        from_book_id=min_due_cp_book.id,
        to_book_id=min_due_book.id,
        amount=min,
    )
    # principal_per_month = unbilled_balance / bill_tenure
    # interest_amount_per_month = unbilled_balance * interest_monthly / 100
    # total_interest = interest_amount_per_month * bill_tenure
    # total_bill_amount = unbilled_balance + total_interest


def payment_received_event(session: Session, bill: LoanData, event: LedgerTriggerEvent) -> None:
    payment_received = event.amount

    def adjust_dues(payment_to_adjust_from: Decimal, from_str: str, to_str: str) -> Decimal:
        if payment_to_adjust_from <= 0:
            return payment_to_adjust_from
        from_book, book_balance = get_account_balance_from_str(session, book_string=from_str)
        if book_balance > 0:
            balance_to_adjust = min(payment_to_adjust_from, book_balance)
            to_book = get_book_account_by_string(session, book_string=to_str)
            create_ledger_entry(
                session,
                event_id=event.id,
                from_book_id=from_book.id,
                to_book_id=to_book.id,
                amount=balance_to_adjust,
            )
            payment_to_adjust_from -= balance_to_adjust
        return payment_to_adjust_from

    remaining_amount = adjust_dues(
        payment_received,
        from_str=f"{bill.id}/bill/late_fee_due/a",
        to_str=f"{bill.id}/bill/late_fee_received/a",
    )
    remaining_amount = adjust_dues(
        remaining_amount,
        from_str=f"{bill.id}/bill/interest_due/a",
        to_str=f"{bill.id}/bill/interest_received/a",
    )
    remaining_amount = adjust_dues(
        remaining_amount,
        from_str=f"{bill.id}/bill/principal_due/a",
        to_str=f"{bill.id}/bill/principal_received/a",
    )
    # Add the rest to prepayment
    if remaining_amount > 0:
        pass
