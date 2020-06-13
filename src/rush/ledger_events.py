from decimal import Decimal

from sqlalchemy.orm import Session

from rush.ledger_utils import (
    create_ledger_entry,
    get_account_balance,
    get_account_balance_from_str,
    get_book_account_by_string,
    get_remaining_bill_balance,
    is_min_paid,
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


def bill_generate_event(
    session: Session, previous_bill: LoanData, new_bill: LoanData, event: LedgerTriggerEvent
) -> None:
    # interest_monthly = 3
    # Move all unbilled book amount to principal due
    unbilled_book, unbilled_balance = get_account_balance_from_str(
        session, book_string=f"{new_bill.id}/bill/unbilled_transactions/a"
    )

    principal_due_book = get_book_account_by_string(
        session, book_string=f"{new_bill.id}/bill/principal_due/a"
    )
    create_ledger_entry(
        session,
        event_id=event.id,
        from_book_id=unbilled_book.id,
        to_book_id=principal_due_book.id,
        amount=unbilled_balance,
    )

    # Also store min amount. Assuming it to be 3% interest + 10% principal.
    min_due_cp_book = get_book_account_by_string(session, book_string=f"{new_bill.id}/bill/min_due_cp/l")
    min_due_book = get_book_account_by_string(session, book_string=f"{new_bill.id}/bill/min_due/a")

    # check if there is any previous balance remaining.
    if previous_bill:
        # TODO should late fee from previous bill come under this month's opening balance or in late fee?
        opening_balance = get_remaining_bill_balance(session, previous_bill)["total_due"]

        opening_balance_cp_book = get_book_account_by_string(
            session, book_string=f"{new_bill.id}/bill/opening_balance_cp/l"
        )
        opening_balance_book = get_book_account_by_string(
            session, book_string=f"{new_bill.id}/bill/opening_balance/a"
        )

        create_ledger_entry(
            session,
            event_id=event.id,
            from_book_id=opening_balance_cp_book.id,
            to_book_id=opening_balance_book.id,
            amount=opening_balance,
        )

        # Add opening balance to principal book as well.
        create_ledger_entry(
            session,
            event_id=event.id,
            from_book_id=opening_balance_cp_book.id,
            to_book_id=principal_due_book.id,
            amount=opening_balance,
        )

        # Check if previous bill's min was paid or not. If not, add remaining to this month's min.
        _, min_due = get_account_balance_from_str(
            session, book_string=f"{previous_bill.id}/bill/min_due/a"
        )
        _, interest_received = get_account_balance_from_str(
            session, book_string=f"{previous_bill.id}/bill/interest_received/a"
        )
        _, principal_received = get_account_balance_from_str(
            session, book_string=f"{previous_bill.id}/bill/principal_received/a"
        )
        remaining_min = min_due - (interest_received + principal_received)

        if remaining_min > 0:
            create_ledger_entry(
                session,
                event_id=event.id,
                from_book_id=min_due_cp_book.id,
                to_book_id=min_due_book.id,
                amount=remaining_min,
            )

    _, principal_due = get_account_balance_from_str(
        session=session, book_string=f"{new_bill.id}/bill/principal_due/a"
    )
    min_balance = principal_due * Decimal("0.03") + principal_due * Decimal("0.10")
    create_ledger_entry(
        session,
        event_id=event.id,
        from_book_id=min_due_cp_book.id,
        to_book_id=min_due_book.id,
        amount=min_balance,
    )


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
        from_str=f"{bill.id}/bill/late_fine_due/a",
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


def accrue_interest_event(session: Session, bills: LoanData, event: LedgerTriggerEvent) -> None:

    for bill in bills:
        _, principal_due = get_account_balance_from_str(
            session, book_string=f"{bill.id}/bill/principal_due/a"
        )
        if principal_due > 0:
            _, principal_received = get_account_balance_from_str(
                session, book_string=f"{bill.id}/bill/principal_received/a"
            )
            # Accrue interest on entire principal. # TODO check if flat interest or reducing here.
            total_principal_amount = principal_due + principal_received
            interest_to_charge = total_principal_amount * Decimal(bill.rc_rate_of_interest_annual) / 1200

            interest_due_book = get_book_account_by_string(
                session, book_string=f"{bill.id}/bill/interest_due/a"
            )
            create_ledger_entry(
                session,
                event_id=event.id,
                from_book_id=interest_due_cp_book.id,
                to_book_id=interest_due_book.id,
                amount=interest_to_charge,
            )


def accrue_late_fine_event(session: Session, bill: LoanData, event: LedgerTriggerEvent) -> None:
    late_fine_cp_book = get_book_account_by_string(session, book_string=f"{bill.id}/bill/late_fine_cp/l")
    late_fine_due_book = get_book_account_by_string(
        session, book_string=f"{bill.id}/bill/late_fine_due/a"
    )
    create_ledger_entry(
        session,
        event_id=event.id,
        from_book_id=late_fine_cp_book.id,
        to_book_id=late_fine_due_book.id,
        amount=Decimal(100),
    )
