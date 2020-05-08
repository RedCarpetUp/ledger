from decimal import Decimal

from sqlalchemy.orm import Session

from rush.ledger_utils import (
    create_ledger_entry,
    get_account_balance,
    get_account_balance_from_str,
    get_book_account_by_string,
)
from rush.models import LedgerTriggerEvent


def card_transaction_event(session: Session, user_id: int, event: LedgerTriggerEvent) -> None:
    amount = event.amount
    # swipe_id = event.extra_details["swipe_id"]
    # bill_id = session.query(CardTransaction.loan_id).filter_by(id=swipe_id).scalar()
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
        session, book_string=f"{user_id}/user/unbilled_transactions/a"
    )
    create_ledger_entry(
        session,
        event_id=event.id,
        from_book_id=user_card_balance.id,
        to_book_id=unbilled_transactions.id,
        amount=amount,
    )


def bill_close_event(session: Session, user_id: int, event: LedgerTriggerEvent) -> None:
    # interest_monthly = 3
    # Move all unbilled book amount to principal due
    unbilled_book = get_book_account_by_string(
        session, book_string=f"{user_id}/user/unbilled_transactions/a"
    )
    unbilled_balance = get_account_balance(session=session, book_account=unbilled_book)

    principal_due_book = get_book_account_by_string(
        session, book_string=f"{user_id}/user/principal_due/a"
    )
    create_ledger_entry(
        session,
        event_id=event.id,
        from_book_id=unbilled_book.id,
        to_book_id=principal_due_book.id,
        amount=unbilled_balance,
    )
    # principal_per_month = unbilled_balance / bill_tenure
    # interest_amount_per_month = unbilled_balance * interest_monthly / 100
    # total_interest = interest_amount_per_month * bill_tenure
    # total_bill_amount = unbilled_balance + total_interest


def payment_received_event(session: Session, user_id: int, event: LedgerTriggerEvent) -> None:
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
        from_str=f"{user_id}/user/late_fee_due/a",
        to_str=f"62311/lender/late_fee_received/a",
    )
    remaining_amount = adjust_dues(
        remaining_amount,
        from_str=f"{user_id}/user/interest_due/a",
        to_str=f"62311/lender/late_fee_received/a",
    )
    remaining_amount = adjust_dues(
        remaining_amount,
        from_str=f"{user_id}/user/principal_due/a",
        to_str=f"62311/lender/principal_received/a",
    )
    # Add the rest to prepayment
    if remaining_amount > 0:
        pass
