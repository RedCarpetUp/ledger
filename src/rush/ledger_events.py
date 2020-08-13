from decimal import Decimal
from typing import List

from sqlalchemy import Date
from sqlalchemy.orm import Session

from rush.card import BaseCard
from rush.card.base_card import BaseBill
from rush.ledger_utils import (
    create_ledger_entry_from_str,
    get_account_balance_from_str,
)
from rush.models import (
    CardTransaction,
    LedgerTriggerEvent,
    LoanData,
    UserCard,
)
from rush.recon.dmi_interest_on_portfolio import interest_on_dmi_portfolio


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


def card_transaction_event(session: Session, user_card: BaseCard, event: LedgerTriggerEvent) -> None:
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
        credit_book_str=f"{user_card_id}/card/lender_payable/l",
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


def bill_generate_event(
    session: Session, bill: BaseBill, user_card_id: int, event: LedgerTriggerEvent
) -> None:
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

    # checking prepayment_balance
    _, prepayment_balance = get_account_balance_from_str(
        session, book_string=f"{user_card_id}/card/pre_payment/l"
    )
    if prepayment_balance > 0:
        balance = min(unbilled_balance, prepayment_balance)
        # reducing balance from pre payment and unbilled
        create_ledger_entry_from_str(
            session,
            event_id=event.id,
            debit_book_str=f"{user_card_id}/card/pre_payment/l",
            credit_book_str=f"{bill_id}/bill/principal_receivable/a",
            amount=balance,
        )


def add_min_amount_event(
    session: Session, bill: BaseBill, event: LedgerTriggerEvent, amount: Decimal
) -> None:
    create_ledger_entry_from_str(
        session,
        event_id=event.id,
        debit_book_str=f"{bill.id}/bill/min/a",
        credit_book_str=f"{bill.id}/bill/min/l",
        amount=amount,
    )


def payment_received_event(
    session: Session, user_card: BaseCard, debit_book_str: str, event: LedgerTriggerEvent,
) -> None:
    payment_received = Decimal(event.amount)
    gateway_charges = event.extra_details.get("gateway_charges")
    if event.name == "merchant_refund":
        pass
    elif event.name == "payment_received":
        unpaid_bills = user_card.get_unpaid_bills()
        payment_received = _adjust_for_min(
            session, unpaid_bills, payment_received, event.id, debit_book_str=debit_book_str,
        )
        payment_received = _adjust_for_complete_bill(
            session, unpaid_bills, payment_received, event.id, debit_book_str=debit_book_str,
        )

    if payment_received > 0:  # if there's payment left to be adjusted.
        _adjust_for_prepayment(
            session, user_card.id, event.id, payment_received, debit_book_str=debit_book_str
        )

    if gateway_charges > 0:  # Adjust for gateway expenses.
        _adjust_for_gateway_expenses(session, event, debit_book_str)

    _, writeoff_balance = get_account_balance_from_str(
        session, book_string=f"{user_card.id}/card/writeoff_expenses/e"
    )
    if writeoff_balance > 0:
        amount = min(writeoff_balance, event.amount)
        _adjust_for_recovery(session, user_card.id, event.id, amount)

    else:
        _adjust_lender_payable(session, user_card.id, debit_book_str, gateway_charges, event)

    from rush.create_emi import slide_payments

    # Slide payment
    slide_payments(user_card=user_card, payment_event=event)


def _adjust_for_gateway_expenses(session: Session, event: LedgerTriggerEvent, credit_book_str: str):
    create_ledger_entry_from_str(
        session,
        event_id=event.id,
        debit_book_str="12345/redcarpet/gateway_expenses/e",
        credit_book_str=credit_book_str,
        amount=event.extra_details["gateway_charges"],
    )


def _adjust_for_recovery(session: Session, user_card_id: int, event_id: int, amount: Decimal) -> None:
    create_ledger_entry_from_str(
        session,
        event_id=event_id,
        debit_book_str=f"{user_card_id}/card/bad_debt_allowance/ca",
        credit_book_str=f"{user_card_id}/card/writeoff_expenses/e",
        amount=Decimal(amount),
    )


def _adjust_lender_payable(
    session: Session,
    user_card_id: int,
    credit_book_str: str,
    gateway_charges: Decimal,
    event: LedgerTriggerEvent,
) -> None:
    # Lender has received money, so we reduce our liability now.
    create_ledger_entry_from_str(
        session,
        event_id=event.id,
        debit_book_str=f"{user_card_id}/card/lender_payable/l",
        credit_book_str=credit_book_str,
        amount=Decimal(event.amount) - Decimal(gateway_charges),
    )


def _adjust_bill(
    session: Session,
    bill: LoanData,
    amount_to_adjust_in_this_bill: Decimal,
    event_id: int,
    debit_acc_str: str,
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
                amount=Decimal(balance_to_adjust),
            )
            payment_to_adjust_from -= balance_to_adjust
        return payment_to_adjust_from

    # Now adjust into other accounts.
    remaining_amount = adjust(
        amount_to_adjust_in_this_bill,
        to_acc=debit_acc_str,  # f"{bill.lender_id}/lender/pg_account/a"
        from_acc=f"{bill.id}/bill/atm_fee_receivable/a",
    )
    remaining_amount = adjust(
        remaining_amount,
        to_acc=debit_acc_str,  # f"{bill.lender_id}/lender/pg_account/a"
        from_acc=f"{bill.id}/bill/late_fine_receivable/a",
    )
    remaining_amount = adjust(
        remaining_amount, to_acc=debit_acc_str, from_acc=f"{bill.id}/bill/interest_receivable/a",
    )
    remaining_amount = adjust(
        remaining_amount, to_acc=debit_acc_str, from_acc=f"{bill.id}/bill/principal_receivable/a",
    )
    return remaining_amount


def _adjust_for_min(
    session: Session,
    bills: List[BaseBill],
    payment_received: Decimal,
    event_id: int,
    debit_book_str: str,
) -> Decimal:
    for bill in bills:
        min_due = bill.get_remaining_min()
        amount_to_adjust_in_this_bill = min(min_due, payment_received)
        # Remove amount from the original variable.
        payment_received -= amount_to_adjust_in_this_bill
        if amount_to_adjust_in_this_bill == 0:
            continue
        # Reduce min amount
        create_ledger_entry_from_str(
            session,
            event_id=event_id,
            debit_book_str=f"{bill.id}/bill/min/l",
            credit_book_str=f"{bill.id}/bill/min/a",
            amount=amount_to_adjust_in_this_bill,
        )
        remaining_amount = _adjust_bill(
            session, bill, amount_to_adjust_in_this_bill, event_id, debit_acc_str=debit_book_str,
        )
        assert remaining_amount == 0  # Can't be more than 0
    return payment_received  # The remaining amount goes back to the main func.


def _adjust_for_complete_bill(
    session: Session,
    bills: List[BaseBill],
    payment_received: Decimal,
    event_id: int,
    debit_book_str: str,
) -> Decimal:
    for bill in bills:
        payment_received = _adjust_bill(
            session, bill, payment_received, event_id, debit_acc_str=debit_book_str,
        )
    return payment_received  # The remaining amount goes back to the main func.


def _adjust_for_prepayment(
    session: Session, card_id: int, event_id: int, amount: Decimal, debit_book_str: str
) -> None:
    create_ledger_entry_from_str(
        session,
        event_id=event_id,
        debit_book_str=debit_book_str,
        credit_book_str=f"{card_id}/card/pre_payment/l",
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


def accrue_late_fine_event(session: Session, bill: LoanData, event: LedgerTriggerEvent) -> None:
    create_ledger_entry_from_str(
        session,
        event_id=event.id,
        debit_book_str=f"{bill.id}/bill/late_fine_receivable/a",
        credit_book_str=f"{bill.id}/bill/late_fine/r",
        amount=event.amount,
    )

    # Add into min amount of the bill too.
    add_min_amount_event(session, bill, event, event.amount)


def lender_interest_incur_event(
    session: Session, from_date: Date, to_date: Date, event: LedgerTriggerEvent
) -> None:
    interest_on_each_card = session.execute(
        interest_on_dmi_portfolio, params={"from_date": from_date, "to_date": to_date}
    )
    for card_id, interest_to_incur in interest_on_each_card:
        create_ledger_entry_from_str(
            session,
            event_id=event.id,
            debit_book_str=f"{card_id}/card/lender_interest/e",
            credit_book_str=f"{card_id}/card/lender_payable/l",
            amount=interest_to_incur,
        )
        event.amount += interest_to_incur


def writeoff_event(session: Session, user_card: UserCard, event: LedgerTriggerEvent) -> None:
    create_ledger_entry_from_str(
        session,
        event_id=event.id,
        debit_book_str=f"{user_card.id}/card/lender_payable/l",
        credit_book_str=f"{user_card.id}/card/bad_debt_allowance/ca",
        amount=event.amount,
    )
    create_ledger_entry_from_str(
        session,
        event_id=event.id,
        debit_book_str=f"{user_card.id}/card/writeoff_expenses/e",
        credit_book_str=f"{user_card.id}/redcarpet/redcarpet_account/a",
        amount=event.amount,
    )


def customer_refund_event(
    session: Session, card_id: int, lender_id: int, event: LedgerTriggerEvent
) -> None:
    create_ledger_entry_from_str(
        session,
        event_id=event.id,
        debit_book_str=f"{card_id}/card/pre_payment/l",
        credit_book_str=f"{lender_id}/lender/pg_account/a",
        amount=Decimal(event.amount),
    )


def limit_assignment_event(session: Session, card_id: int, event: LedgerTriggerEvent) -> None:
    create_ledger_entry_from_str(
        session,
        event_id=event.id,
        debit_book_str=f"{card_id}/card/available_limit/a",
        credit_book_str=f"{card_id}/card/available_limit/l",
        amount=Decimal(event.amount),
    )


def atm_fee_event(
    session: Session, user_card: BaseCard, bill: BaseBill, event: LedgerTriggerEvent
) -> None:
    create_ledger_entry_from_str(
        session,
        event_id=event.id,
        debit_book_str=f"{bill.id}/bill/atm_fee_receivable/a",
        credit_book_str=f"{bill.id}/bill/atm_fee_accrued/r",
        amount=Decimal(event.amount),
    )

    # Adjust atm fee in emis
    # from rush.create_emi import adjust_atm_fee_in_emis

    # adjust_atm_fee_in_emis(session, user_card, event.post_date)


def daily_dpd_event(session: Session, user_card: BaseCard) -> None:
    from rush.utils import get_current_ist_time

    event = LedgerTriggerEvent(
        name="daily_dpd", post_date=get_current_ist_time(), card_id=user_card.id, amount=0
    )
    session.add(event)
    session.flush()
