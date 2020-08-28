from decimal import Decimal
from typing import (
    List,
    Optional,
)

from sqlalchemy import (
    Date,
    and_,
)
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
    BillFee,
    CardTransaction,
    Fee,
    LedgerTriggerEvent,
    Loan,
    LoanData,
    LoanFee,
    ProductFee,
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


def disburse_money_to_card(session: Session, user_card: BaseLoan, event: LedgerTriggerEvent) -> None:
    create_ledger_entry_from_str(
        session,
        event_id=event.id,
        debit_book_str=f"{user_card.loan_id}/card/card_balance/a",
        credit_book_str=f"{user_card.lender_id}/lender/pool_balance/a",
        amount=event.amount,
    )


def card_transaction_event(
    session: Session, user_card: BaseLoan, event: LedgerTriggerEvent, mcc: Optional[str] = None
) -> None:
    amount = Decimal(event.amount)
    swipe_id = event.extra_details["swipe_id"]
    bill = (
        session.query(LoanData)
        .filter(LoanData.id == CardTransaction.loan_id, CardTransaction.id == swipe_id)
        .scalar()
    )
    lender_id = user_card.lender_id
    bill_id = bill.id

    user_books_prefix_str = f"{user_card.loan_id}/card/{user_card.get_limit_type(mcc=mcc)}"

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
        credit_book_str=f"{user_card.loan_id}/loan/lender_payable/l",
        amount=amount,
    )

    # Reduce money from lender's pool account
    create_ledger_entry_from_str(
        session,
        event_id=event.id,
        debit_book_str=f"{bill_id}/bill/unbilled/a",
        credit_book_str=f"{user_card.loan_id}/card/card_balance/a",
        amount=amount,
    )


def bill_generate_event(
    session: Session, bill: BaseBill, user_card: BaseLoan, event: LedgerTriggerEvent
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
        session, book_string=f"{user_card.loan_id}/loan/pre_payment/l"
    )
    if prepayment_balance > 0:
        balance = min(unbilled_balance, prepayment_balance)
        # reducing balance from pre payment and unbilled
        create_ledger_entry_from_str(
            session,
            event_id=event.id,
            debit_book_str=f"{user_card.loan_id}/loan/pre_payment/l",
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


def payment_received_event(
    session: Session, user_card: BaseLoan, debit_book_str: str, event: LedgerTriggerEvent
) -> None:
    payment_received = Decimal(event.amount)
    if event.name == "merchant_refund":
        pass
    elif event.name == "payment_received":
        unpaid_bills = user_card.get_unpaid_bills()
        actual_payment = payment_received
        payment_received = _adjust_for_min(
            session,
            unpaid_bills,
            payment_received,
            event.id,
            debit_book_str=debit_book_str,
        )
        payment_received = _adjust_for_complete_bill(
            session,
            unpaid_bills,
            payment_received,
            event.id,
            debit_book_str=debit_book_str,
        )

        if user_card.should_reinstate_limit_on_payment:
            user_card.reinstate_limit_on_payment(event=event, amount=actual_payment)

    if payment_received > 0:  # if there's payment left to be adjusted.
        _adjust_for_prepayment(
            session=session,
            loan_id=user_card.loan_id,
            event_id=event.id,
            amount=payment_received,
            debit_book_str=debit_book_str,
        )

    from rush.create_emi import slide_payments

    slide_payments(user_card=user_card, payment_event=event)


def _adjust_bill(
    session: Session,
    bill: LoanData,
    amount_to_adjust_in_this_bill: Decimal,
    event_id: int,
    debit_acc_str: str,
) -> Decimal:
    def adjust_for_revenue(payment_to_adjust_from: Decimal, debit_str: str, bill_fee: Fee) -> Decimal:
        if bill_fee.name == "late_fee":
            credit_book_str = f"{bill_fee.identifier_id}/bill/late_fine/r"
        elif bill_fee.name == "atm_fee":
            credit_book_str = f"{bill_fee.identifier_id}/bill/atm_fee/r"
        elif bill_fee.name == "card_activation_fees":
            credit_book_str = f"{bill_fee.identifier_id}/product/card_activation_fees/r"
        elif bill_fee.name == "card_reload_fees":
            credit_book_str = f"{bill_fee.identifier_id}/loan/card_reload_fees/r"
        fee_to_adjust = min(payment_to_adjust_from, bill_fee.gross_amount)
        gst_split = get_gst_split_from_amount(
            amount=fee_to_adjust,
            sgst_rate=bill_fee.sgst_rate,
            cgst_rate=bill_fee.cgst_rate,
            igst_rate=bill_fee.igst_rate,
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
        bill_fee.net_amount_paid = gst_split["net_amount"]

        # Settle for cgst
        create_ledger_entry_from_str(
            session,
            event_id=event_id,
            debit_book_str=debit_str,
            credit_book_str="12345/redcarpet/cgst_payable/l",
            amount=gst_split["cgst"],
        )
        bill_fee.cgst_paid = gst_split["cgst"]

        # Settle for sgst
        create_ledger_entry_from_str(
            session,
            event_id=event_id,
            debit_book_str=debit_str,
            credit_book_str="12345/redcarpet/sgst_payable/l",
            amount=gst_split["sgst"],
        )
        bill_fee.sgst_paid = gst_split["sgst"]

        # Settle for igst
        create_ledger_entry_from_str(
            session,
            event_id=event_id,
            debit_book_str=debit_str,
            credit_book_str="12345/redcarpet/igst_payable/l",
            amount=gst_split["igst"],
        )
        bill_fee.igst_paid = gst_split["igst"]

        bill_fee.gross_amount_paid = gst_split["gross_amount"]
        if bill_fee.gross_amount == bill_fee.gross_amount_paid:
            bill_fee.fee_status = "PAID"
        return payment_to_adjust_from - fee_to_adjust

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

    # settling pre loans fee first.
    pre_loan_fees = (
        session.query(ProductFee)
        .join(Loan, and_(Loan.id == bill.loan_id, ProductFee.identifier_id == Loan.sell_book_id))
        .filter(ProductFee.fee_status == "UNPAID")
        .all()
    )
    for fee in pre_loan_fees:
        remaining_amount = adjust_for_revenue(remaining_amount, debit_acc_str, fee)

    # TODO is the order right?
    # settle reload fees
    reload_fees = (
        session.query(LoanFee)
        .filter(LoanFee.identifier_id == bill.loan_id, LoanFee.fee_status == "UNPAID")
        .all()
    )
    for fee in reload_fees:
        remaining_amount = adjust_for_revenue(remaining_amount, debit_acc_str, fee)

    fees = (
        session.query(BillFee)
        .filter(BillFee.identifier_id == bill.id, BillFee.fee_status == "UNPAID")
        .all()
    )
    for fee in fees:
        remaining_amount = adjust_for_revenue(remaining_amount, debit_acc_str, fee)

    remaining_amount = adjust_for_receivable(
        remaining_amount,
        to_acc=debit_acc_str,
        from_acc=f"{bill.id}/bill/interest_receivable/a",
    )
    remaining_amount = adjust_for_receivable(
        remaining_amount,
        to_acc=debit_acc_str,
        from_acc=f"{bill.id}/bill/principal_receivable/a",
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
        if amount_to_adjust_in_this_bill == 0:
            continue
        # Remove amount from the original variable.
        payment_received -= amount_to_adjust_in_this_bill
        # Reduce min amount
        create_ledger_entry_from_str(
            session,
            event_id=event_id,
            debit_book_str=f"{bill.id}/bill/min/l",
            credit_book_str=f"{bill.id}/bill/min/a",
            amount=amount_to_adjust_in_this_bill,
        )
        remaining_amount = _adjust_bill(
            session,
            bill,
            amount_to_adjust_in_this_bill,
            event_id,
            debit_acc_str=debit_book_str,
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
            session,
            bill,
            payment_received,
            event_id,
            debit_acc_str=debit_book_str,
        )
    return payment_received  # The remaining amount goes back to the main func.


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


def daily_dpd_event(session: Session, user_card: BaseLoan) -> None:
    from rush.utils import get_current_ist_time

    event = LedgerTriggerEvent(
        name="daily_dpd", post_date=get_current_ist_time(), loan_id=user_card.loan_id, amount=0
    )
    session.add(event)
    session.flush()


def loan_disbursement_event(
    session: Session, loan: Loan, event: LedgerTriggerEvent, bill_id: int
) -> None:
    create_ledger_entry_from_str(
        session,
        event_id=event.id,
        debit_book_str=f"{bill_id}/bill/principal_receivable/a",
        credit_book_str=f"12345/redcarpet/rc_cash/a",  # TODO: confirm if this right.
        amount=event.amount,
    )

    create_ledger_entry_from_str(
        session,
        event_id=event.id,
        debit_book_str=f"{loan.lender_id}/lender/lender_capital/l",
        credit_book_str=f"{loan.loan_id}/loan/lender_payable/l",
        amount=event.amount,
    )
