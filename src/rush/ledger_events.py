from decimal import Decimal
from typing import Optional

from sqlalchemy import Date
from sqlalchemy.orm import Session

from rush.card.base_card import (
    BaseBill,
    BaseLoan,
)
from rush.ledger_utils import (
    create_ledger_entry_from_str,
    get_account_balance_from_str,
)
from rush.models import (
    CardTransaction,
    Fee,
    LedgerLoanData,
    LedgerTriggerEvent,
    Loan,
)
from rush.recon.dmi_interest_on_portfolio import interest_on_dmi_portfolio
from rush.utils import get_gst_split_from_amount


def lender_disbursal_event(session: Session, event: LedgerTriggerEvent, lender_id: int) -> None:
    create_ledger_entry_from_str(
        session,
        event_id=event.id,
        debit_book_str=f"12345/redcarpet/rc_cash/a",
        credit_book_str=f"{lender_id}/lender/lender_capital/l",
        amount=event.amount,
    )


def m2p_transfer_event(session: Session, event: LedgerTriggerEvent, lender_id: int) -> None:
    create_ledger_entry_from_str(
        session,
        event_id=event.id,
        debit_book_str=f"{lender_id}/lender/pool_balance/a",
        credit_book_str=f"12345/redcarpet/rc_cash/a",
        amount=event.amount,
    )


def disburse_money_to_card(session: Session, user_loan: BaseLoan, event: LedgerTriggerEvent) -> None:
    create_ledger_entry_from_str(
        session,
        event_id=event.id,
        debit_book_str=f"{user_loan.loan_id}/card/card_balance/a",
        credit_book_str=f"{user_loan.lender_id}/lender/pool_balance/a",
        amount=event.amount,
    )


def card_transaction_event(
    session: Session, user_loan: BaseLoan, event: LedgerTriggerEvent, mcc: Optional[str] = None
) -> None:
    amount = Decimal(event.amount)
    swipe_id = event.extra_details["swipe_id"]
    bill = (
        session.query(LedgerLoanData)
        .filter(LedgerLoanData.id == CardTransaction.loan_id, CardTransaction.id == swipe_id)
        .scalar()
    )
    lender_id = user_loan.lender_id
    bill_id = bill.id

    user_books_prefix_str = f"{user_loan.loan_id}/card/{user_loan.get_limit_type(mcc=mcc)}"

    # Reduce user's card balance
    create_ledger_entry_from_str(
        session,
        event_id=event.id,
        debit_book_str=f"{user_books_prefix_str}/l",
        credit_book_str=f"{user_books_prefix_str}/a",
        amount=amount,
    )

    # Move debt from one account to another. We will be charged interest on lender_payable.
    create_ledger_entry_from_str(
        session,
        event_id=event.id,
        debit_book_str=f"{lender_id}/lender/lender_capital/l",
        credit_book_str=f"{user_loan.loan_id}/loan/lender_payable/l",
        amount=amount,
    )

    # Reduce money from lender's pool account
    create_ledger_entry_from_str(
        session,
        event_id=event.id,
        debit_book_str=f"{bill_id}/bill/unbilled/a",
        credit_book_str=f"{user_loan.loan_id}/card/card_balance/a",
        amount=amount,
    )


def bill_generate_event(
    session: Session, bill: BaseBill, user_loan: BaseLoan, event: LedgerTriggerEvent
) -> None:
    bill_id = bill.id

    # Move all unbilled book amount to billed account
    unbilled_balance = bill.get_unbilled_amount()

    create_ledger_entry_from_str(
        session,
        event_id=event.id,
        debit_book_str=f"{bill_id}/bill/principal_receivable/a",
        credit_book_str=f"{bill_id}/bill/unbilled/a",
        amount=unbilled_balance,
    )

    # checking prepayment_balance
    _, prepayment_balance = get_account_balance_from_str(
        session=session, book_string=f"{user_loan.loan_id}/loan/pre_payment/l"
    )
    if prepayment_balance > 0:
        balance = min(unbilled_balance, prepayment_balance)
        # reducing balance from pre payment and unbilled
        create_ledger_entry_from_str(
            session=session,
            event_id=event.id,
            debit_book_str=f"{user_loan.loan_id}/loan/pre_payment/l",
            credit_book_str=f"{bill_id}/bill/principal_receivable/a",
            amount=balance,
        )


def add_min_amount_event(
    session: Session, bill: BaseBill, event: LedgerTriggerEvent, amount: Decimal
) -> None:
    create_ledger_entry_from_str(
        session=session,
        event_id=event.id,
        debit_book_str=f"{bill.id}/bill/min/a",
        credit_book_str=f"{bill.id}/bill/min/l",
        amount=amount,
    )


def add_max_amount_event(
    session: Session, bill: BaseBill, event: LedgerTriggerEvent, amount: Decimal
) -> None:
    create_ledger_entry_from_str(
        session=session,
        event_id=event.id,
        debit_book_str=f"{bill.id}/bill/max/a",
        credit_book_str=f"{bill.id}/bill/max/l",
        amount=amount,
    )


def _adjust_bill(
    session: Session,
    bill: LedgerLoanData,
    amount_to_adjust_in_this_bill: Decimal,
    event_id: int,
    debit_acc_str: str,
) -> Decimal:
    def adjust_for_receivable(payment_to_adjust_from: Decimal, to_acc: str, from_acc: str) -> Decimal:
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
                amount=Decimal(balance_to_adjust),
            )
            payment_to_adjust_from -= balance_to_adjust
        return payment_to_adjust_from

    remaining_amount = amount_to_adjust_in_this_bill

    remaining_amount = adjust_for_receivable(
        remaining_amount,
        to_acc=debit_acc_str,
        from_acc=f"{bill.id}/bill/interest_receivable/a",
    )
    remaining_amount = adjust_for_receivable(
        remaining_amount,
        to_acc=debit_acc_str,
        from_acc=f"{bill.id}/bill/unbilled/a",
    )
    remaining_amount = adjust_for_receivable(
        remaining_amount,
        to_acc=debit_acc_str,
        from_acc=f"{bill.id}/bill/principal_receivable/a",
    )
    return remaining_amount


def _adjust_for_prepayment(
    session: Session, loan_id: int, event_id: int, amount: Decimal, debit_book_str: str
) -> None:
    create_ledger_entry_from_str(
        session,
        event_id=event_id,
        debit_book_str=debit_book_str,
        credit_book_str=f"{loan_id}/loan/pre_payment/l",
        amount=amount,
    )


def accrue_interest_event(
    session: Session, bill: BaseBill, event: LedgerTriggerEvent, amount: Decimal
) -> None:
    create_ledger_entry_from_str(
        session,
        event_id=event.id,
        debit_book_str=f"{bill.id}/bill/interest_receivable/a",
        credit_book_str=f"{bill.id}/bill/interest_accrued/r",
        amount=amount,
    )


def lender_interest_incur_event(
    session: Session, from_date: Date, to_date: Date, event: LedgerTriggerEvent
) -> None:
    interest_on_each_card = session.execute(
        interest_on_dmi_portfolio, params={"from_date": from_date, "to_date": to_date}
    )
    for loan_id, interest_to_incur in interest_on_each_card:
        create_ledger_entry_from_str(
            session,
            event_id=event.id,
            debit_book_str=f"{loan_id}/loan/lender_interest/e",
            credit_book_str=f"{loan_id}/loan/lender_payable/l",
            amount=interest_to_incur,
        )
        event.amount += interest_to_incur


def customer_refund_event(
    session: Session, loan_id: int, lender_id: int, event: LedgerTriggerEvent
) -> None:
    create_ledger_entry_from_str(
        session,
        event_id=event.id,
        debit_book_str=f"{loan_id}/loan/pre_payment/l",
        credit_book_str=f"{lender_id}/lender/pg_account/a",
        amount=Decimal(event.amount),
    )


def limit_assignment_event(
    session: Session, loan_id: int, event: LedgerTriggerEvent, amount: Decimal
) -> None:
    create_ledger_entry_from_str(
        session,
        event_id=event.id,
        debit_book_str=f"{loan_id}/card/available_limit/a",
        credit_book_str=f"{loan_id}/card/available_limit/l",
        amount=amount,
    )


def daily_dpd_event(session: Session, user_loan: BaseLoan) -> None:
    from rush.utils import get_current_ist_time

    event = LedgerTriggerEvent(
        name="daily_dpd", post_date=get_current_ist_time(), loan_id=user_loan.loan_id, amount=0
    )
    session.add(event)
    session.flush()


def limit_unlock_event(session: Session, loan: Loan, event: LedgerTriggerEvent, amount: Decimal) -> None:
    create_ledger_entry_from_str(
        session=session,
        event_id=event.id,
        debit_book_str=f"{loan.id}/card/locked_limit/l",
        credit_book_str=f"{loan.id}/card/locked_limit/a",
        amount=amount,
    )


def get_revenue_book_str_for_fee(fee: Fee) -> str:
    if fee.name == "late_fee":
        return f"{fee.identifier_id}/bill/late_fee/r"
    elif fee.name == "atm_fee":
        return f"{fee.identifier_id}/bill/atm_fee/r"
    elif fee.name == "card_activation_fees":
        return f"{fee.identifier_id}/loan/card_activation_fees/r"
    elif fee.name == "reset_joining_fees":
        return f"{fee.identifier_id}/loan/reset_joining_fees/r"
    elif fee.name == "card_reload_fees":
        return f"{fee.identifier_id}/loan/card_reload_fees/r"
    elif fee.name == "card_upgrade_fees":
        return f"{fee.identifier_id}/loan/card_upgrade_fees/r"
    else:
        return f"{fee.identifier_id}/{fee.identifier}/{fee.name}/r"


def adjust_for_revenue(
    session: Session, event_id: int, payment_to_adjust_from: Decimal, debit_str: str, fee: Fee
) -> Decimal:

    credit_book_str = get_revenue_book_str_for_fee(fee=fee)

    fee_to_adjust = min(payment_to_adjust_from, fee.remaining_fee_amount)
    gst_split = get_gst_split_from_amount(
        amount=fee_to_adjust,
        total_gst_rate=fee.igst_rate,
    )
    assert gst_split["gross_amount"] == fee_to_adjust
    # Settle for net fee
    create_ledger_entry_from_str(
        session,
        event_id=event_id,
        debit_book_str=debit_str,
        credit_book_str=credit_book_str,
        amount=gst_split["net_amount"],
    )
    fee.net_amount_paid += gst_split["net_amount"]

    if gst_split["cgst"]:  # Settle for cgst
        create_ledger_entry_from_str(
            session,
            event_id=event_id,
            debit_book_str=debit_str,
            credit_book_str=f"{fee.user_id}/user/cgst_payable/l",
            amount=gst_split["cgst"],
        )
        fee.cgst_paid += gst_split["cgst"]

    if gst_split["sgst"]:  # Settle for sgst
        create_ledger_entry_from_str(
            session,
            event_id=event_id,
            debit_book_str=debit_str,
            credit_book_str=f"{fee.user_id}/user/sgst_payable/l",
            amount=gst_split["sgst"],
        )
        fee.sgst_paid += gst_split["sgst"]

    if gst_split["igst"]:  # Settle for igst
        create_ledger_entry_from_str(
            session,
            event_id=event_id,
            debit_book_str=debit_str,
            credit_book_str=f"{fee.user_id}/user/igst_payable/l",
            amount=gst_split["igst"],
        )
        fee.igst_paid += gst_split["igst"]

    fee.gross_amount_paid += gst_split["gross_amount"]
    if fee.gross_amount == fee.gross_amount_paid:
        fee.fee_status = "PAID"

    return payment_to_adjust_from - fee_to_adjust


def reduce_revenue_for_fee_refund(
    session: Session,
    credit_book_str: str,
    fee: Fee,
) -> None:

    debit_book_str = get_revenue_book_str_for_fee(fee=fee)

    create_ledger_entry_from_str(
        session,
        event_id=fee.event_id,
        debit_book_str=debit_book_str,
        credit_book_str=credit_book_str,
        amount=Decimal(fee.net_amount_paid),
    )

    create_ledger_entry_from_str(
        session,
        event_id=fee.event_id,
        debit_book_str=f"{fee.user_id}/user/cgst_payable/l",
        credit_book_str=credit_book_str,
        amount=Decimal(fee.cgst_paid),
    )

    create_ledger_entry_from_str(
        session,
        event_id=fee.event_id,
        debit_book_str=f"{fee.user_id}/user/sgst_payable/l",
        credit_book_str=credit_book_str,
        amount=Decimal(fee.sgst_paid),
    )

    create_ledger_entry_from_str(
        session,
        event_id=fee.event_id,
        debit_book_str=f"{fee.user_id}/user/igst_payable/l",
        credit_book_str=credit_book_str,
        amount=Decimal(fee.igst_paid),
    )

    fee.fee_status = "REFUND"
