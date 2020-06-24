import datetime
from decimal import Decimal

from sqlalchemy.orm import Session

from rush.ledger_utils import (
    create_ledger_entry,
    create_ledger_entry_from_str,
    get_account_balance,
    get_account_balance_from_str,
    get_book_account_by_string,
    get_remaining_bill_balance,
    is_bill_closed,
    is_min_paid,
)
from rush.models import (
    CardTransaction,
    LedgerTriggerEvent,
    LoanData,
)
from rush.utils import get_current_ist_time


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


def card_transaction_event(session: Session, user_id: int, event: LedgerTriggerEvent) -> None:
    amount = event.amount
    swipe_id = event.extra_details["swipe_id"]
    bill_id = session.query(CardTransaction.loan_id).filter_by(id=swipe_id).scalar()
    # Reduce user's card balance
    create_ledger_entry_from_str(
        session,
        event_id=event.id,
        debit_book_str=f"{user_id}/user/card_balance/l",
        credit_book_str=f"{user_id}/user/card_balance/a",
        amount=amount,
    )

    # Move debt from one account to another. We will be charged interest on lender_payable.
    create_ledger_entry_from_str(
        session,
        event_id=event.id,
        debit_book_str=f"62311/lender/lender_capital/l",
        credit_book_str="62311/lender/lender_payable/l",
        amount=amount,
    )

    # Reduce money from lender's pool account
    create_ledger_entry_from_str(
        session,
        event_id=event.id,
        debit_book_str=f"{bill_id}/bill/unbilled_transactions/a",
        credit_book_str="62311/lender/pool_balance/a",
        amount=amount,
    )


def bill_generate_event(session: Session, new_bill: LoanData, event: LedgerTriggerEvent) -> None:
    # interest_monthly = 3
    # Move all unbilled book amount to principal due
    _, unbilled_balance = get_account_balance_from_str(
        session, book_string=f"{new_bill.id}/bill/unbilled_transactions/a"
    )

    create_ledger_entry_from_str(
        session,
        event_id=event.id,
        debit_book_str=f"{new_bill.id}/bill/principal_due/a",
        credit_book_str=f"{new_bill.id}/bill/unbilled_transactions/a",
        amount=unbilled_balance,
    )
    _, principal_due = get_account_balance_from_str(
        session=session, book_string=f"{new_bill.id}/bill/principal_due/a"
    )

    # Also store min amount. Assuming it to be 3% interest + 10% principal.
    min_balance = principal_due * Decimal("0.03") + principal_due * Decimal("0.10")
    create_ledger_entry_from_str(
        session,
        event_id=event.id,
        debit_book_str=f"{new_bill.id}/bill/min_due/a",
        credit_book_str=f"{new_bill.id}/bill/min_due_cp/l",
        amount=min_balance,
    )


def payment_received_event(session: Session, bills: LoanData, event: LedgerTriggerEvent) -> None:
    payment_received = event.amount

    def adjust_dues(payment_to_adjust_from: Decimal, debit_str: str, credit_str: str) -> Decimal:
        if payment_to_adjust_from <= 0:
            return payment_to_adjust_from
        _, book_balance = get_account_balance_from_str(session, book_string=credit_str)
        if book_balance > 0:
            balance_to_adjust = min(payment_to_adjust_from, book_balance)
            create_ledger_entry_from_str(
                session,
                event_id=event.id,
                debit_book_str=debit_str,
                credit_book_str=credit_str,
                amount=balance_to_adjust,
            )
            payment_to_adjust_from -= balance_to_adjust
        return payment_to_adjust_from

    remaining_amount = Decimal(0)
    for bill in bills:
        remaining_amount = adjust_dues(
            payment_received,
            debit_str=f"{bill.id}/bill/late_fee_received/a",
            credit_str=f"{bill.id}/bill/late_fine_due/a",
        )
        remaining_amount = adjust_dues(
            remaining_amount,
            debit_str=f"{bill.id}/bill/interest_received/a",
            credit_str=f"{bill.id}/bill/interest_due/a",
        )
        remaining_amount = adjust_dues(
            remaining_amount,
            debit_str=f"{bill.id}/bill/principal_received/a",
            credit_str=f"{bill.id}/bill/principal_due/a",
        )
        if remaining_amount <= 0:
            break

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

            revenue_earned = get_book_account_by_string(
                session, book_string=f"{bill.id}/bill/revenue_earned/r"
            )
            assert type(revenue_earned.id) == int
            interest_due_book = get_book_account_by_string(
                session, book_string=f"{bill.id}/bill/interest_due/a"
            )
            create_ledger_entry(
                session,
                event_id=event.id,
                debit_book_id=interest_due_book.id,
                credit_book_id=revenue_earned.id,
                amount=interest_to_charge,
            )


def accrue_late_fine_event(session: Session, bill: LoanData, event: LedgerTriggerEvent) -> None:
    create_ledger_entry_from_str(
        session,
        event_id=event.id,
        debit_book_str=f"{bill.id}/bill/late_fine_due/a",
        credit_book_str=f"{bill.id}/bill/late_fine_cp/l",
        amount=event.amount,
    )


def refund_or_prepayment_event(
    session: Session, case: str, bill_id: int, event: LedgerTriggerEvent
) -> None:
    # Refund before bill generation
    if case == "before":
        create_ledger_entry_from_str(
            session,
            event_id=event.id,
            debit_book_str=f"62311/lender/merchant_refund/a",
            credit_book_str=f"{bill_id}/bill/unbilled_transactions/a",
            amount=event.amount,
        )
    elif case == "after":
        create_ledger_entry_from_str(
            session,
            event_id=event.id,
            debit_book_str=f"62311/lender/merchant_refund/a",
            credit_book_str=f"{bill_id}/bill/pre_payment/l",
            amount=event.amount,
        )
    elif case == "prepayment":
        create_ledger_entry_from_str(
            session,
            event_id=event.id,
            debit_book_str=f"62311/lender/lender_pg/a",
            credit_book_str=f"{bill_id}/bill/pre_payment/l",
            amount=event.amount,
        )


def lender_interest_incur_event(session: Session, event: LedgerTriggerEvent) -> None:
    all_lender_bills = (
        session.query(LoanData.id, LoanData.lender_rate_of_interest_annual, CardTransaction.txn_time)
        .join(CardTransaction, LoanData.id == CardTransaction.loan_id)
        .order_by(LoanData.id.desc())
        .all()
    )
    for bill in all_lender_bills:
        if is_bill_closed(session, bill) == False:
            _, lender_principal = get_account_balance_from_str(
                session, book_string=f"{bill.id}/bill/principal_due/a"
            )
            _, lender_unbilled = get_account_balance_from_str(
                session, book_string=f"{bill.id}/bill/unbilled_transactions/a"
            )
            time = get_current_ist_time() - bill.txn_time
            # subjected to change according to the trigger frequency
            lender_interest = (
                bill.lender_rate_of_interest_annual
                * int(time.days)
                * (lender_principal + lender_unbilled)
                / 36500
            )
            print(time.days)
            create_ledger_entry_from_str(
                session,
                event_id=event.id,
                debit_book_str=f"{bill.id}/redcarpet/redcarpet_expenses/l",
                credit_book_str="62311/lender/lender_payable/l",
                amount=lender_interest,
            )
