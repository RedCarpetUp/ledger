from decimal import Decimal
from typing import Optional

from dateutil.relativedelta import relativedelta
from pendulum import DateTime
from sqlalchemy.orm import Session

from rush.anomaly_detection import run_anomaly
from rush.card import BaseLoan
from rush.card.base_card import BaseBill
from rush.create_emi import group_bills_to_create_loan_schedule
from rush.ledger_events import (
    _adjust_bill,
    _adjust_for_downpayment,
    _adjust_for_prepayment,
)
from rush.ledger_utils import (
    create_ledger_entry_from_str,
    get_account_balance_from_str,
)
from rush.models import (
    CardTransaction,
    LedgerTriggerEvent,
    LoanData,
)
from rush.utils import (
    div,
    mul,
)
from rush.writeoff_and_recovery import recovery_event


def payment_received(
    session: Session,
    user_loan: Optional[BaseLoan],
    payment_amount: Decimal,
    payment_date: DateTime,
    payment_request_id: str,
    payment_type: Optional[str] = None,
    user_product_id: Optional[int] = None,
    lender_id: Optional[int] = None,
) -> None:
    assert user_loan is not None or lender_id is not None

    lt = LedgerTriggerEvent(
        name="payment_received",
        loan_id=user_loan.loan_id if user_loan else None,
        amount=payment_amount,
        post_date=payment_date,
        extra_details={
            "payment_request_id": payment_request_id,
            "payment_type": payment_type,
            "user_product_id": user_product_id,
            "lender_id": user_loan.lender_id if user_loan else lender_id,
        },
    )
    session.add(lt)
    session.flush()

    payment_received_event(
        session=session,
        user_loan=user_loan,
        debit_book_str=f"{user_loan.lender_id if user_loan else lender_id}/lender/pg_account/a",
        event=lt,
    )

    # TODO: check if this code is needed for downpayment, since there is no user loan at that point of time.
    if payment_type == "downpayment":
        return

    run_anomaly(session=session, user_loan=user_loan, event_date=payment_date)
    gateway_charges = Decimal("0.5")
    settle_payment_in_bank(
        session=session,
        payment_request_id=payment_request_id,
        gateway_expenses=gateway_charges,
        gross_payment_amount=payment_amount,
        settlement_date=payment_date + relativedelta(days=2),
        user_loan=user_loan,
    )


def refund_payment(
    session: Session,
    user_loan: BaseLoan,
    payment_amount: Decimal,
    payment_date: DateTime,
    payment_request_id: str,
) -> None:
    lt = LedgerTriggerEvent(
        name="transaction_refund",
        loan_id=user_loan.loan_id,
        amount=payment_amount,
        post_date=payment_date,
        extra_details={"payment_request_id": payment_request_id},
    )
    session.add(lt)
    session.flush()

    # Checking if bill is generated or not. if not then reduce from unbilled else treat as payment.
    transaction_refund_event(session=session, user_loan=user_loan, event=lt)
    run_anomaly(session=session, user_loan=user_loan, event_date=payment_date)


def payment_received_event(
    session: Session,
    user_loan: BaseLoan,
    debit_book_str: str,
    event: LedgerTriggerEvent,
) -> None:
    payment_received = Decimal(event.amount)
    if event.name == "merchant_refund":
        pass
    elif event.name == "payment_received":
        if event.extra_details.get("payment_type") == "downpayment":
            _adjust_for_downpayment(session=session, event=event, amount=payment_received)
            return

        actual_payment = payment_received
        bills_data = find_amount_to_slide_in_bills(user_loan, payment_received)
        for bill_data in bills_data:
            adjust_for_min_max_accounts(bill_data["bill"], bill_data["amount_to_adjust"], event.id)
            remaining_amount = _adjust_bill(
                session,
                bill_data["bill"],
                bill_data["amount_to_adjust"],
                event.id,
                debit_acc_str=debit_book_str,
            )
            assert (
                remaining_amount == 0
            )  # The amount to adjust is computed for this bill. It should all settle.
            payment_received -= bill_data["amount_to_adjust"]
        if user_loan.should_reinstate_limit_on_payment:
            user_loan.reinstate_limit_on_payment(event=event, amount=actual_payment)

    if payment_received > 0:  # if there's payment left to be adjusted.
        _adjust_for_prepayment(
            session=session,
            loan_id=user_loan.loan_id,
            event_id=event.id,
            amount=payment_received,
            debit_book_str=debit_book_str,
        )

    is_in_write_off = (
        get_account_balance_from_str(session, f"{user_loan.loan_id}/loan/write_off_expenses/e")[1] > 0
    )
    if is_in_write_off:
        recovery_event(user_loan, event)
        # TODO set loan status to recovered.
    from rush.create_emi import slide_payments

    slide_payments(user_loan=user_loan, payment_event=event)


def find_amount_to_slide_in_bills(user_loan: BaseLoan, total_amount_to_slide: Decimal) -> list:
    unpaid_bills = user_loan.get_unpaid_bills()
    bills_dict = [
        {
            "bill": bill,
            "total_outstanding": bill.get_outstanding_amount(),
            "monthly_instalment": bill.get_scheduled_min_amount(),
            "amount_to_adjust": 0,
        }
        for bill in unpaid_bills
    ]
    total_amount_before_sliding = total_amount_to_slide
    total_loan_outstanding = sum(bill["total_outstanding"] for bill in bills_dict)
    total_min = sum(bill["monthly_instalment"] for bill in bills_dict)
    # Either nothing is left to slide or loan is completely settled and there is excess payment.
    while total_amount_to_slide > 0 and total_loan_outstanding > 0:
        for bill_data in bills_dict:
            # If bill isn't generated there's no minimum amount scheduled. So can slide entire amount.
            if not bill_data["bill"].table.is_generated:
                amount_to_slide_based_on_ratio = total_amount_to_slide
            else:
                amount_to_slide_based_on_ratio = mul(
                    div(bill_data["monthly_instalment"], total_min), total_amount_before_sliding
                )
            amount_to_adjust = min(
                amount_to_slide_based_on_ratio, total_amount_to_slide, bill_data["total_outstanding"]
            )
            bill_data["amount_to_adjust"] += amount_to_adjust
            bill_data["total_outstanding"] -= amount_to_adjust
            total_amount_to_slide -= amount_to_adjust
            total_loan_outstanding -= amount_to_adjust
    filtered_bills_dict = filter(lambda x: x["amount_to_adjust"] > 0, bills_dict)
    return filtered_bills_dict


def transaction_refund_event(session: Session, user_loan: BaseLoan, event: LedgerTriggerEvent) -> None:
    refund_amount = Decimal(event.amount)
    m2p_pool_account = f"{user_loan.lender_id}/lender/pool_balance/a"
    bills_data = find_amount_to_slide_in_bills(user_loan, refund_amount)

    for bill_data in bills_data:
        adjust_for_min_max_accounts(bill_data["bill"], bill_data["amount_to_adjust"], event.id)
        refund_amount = _adjust_bill(
            session,
            bill_data["bill"],
            bill_data["amount_to_adjust"],
            event.id,
            debit_acc_str=m2p_pool_account,
        )
    if refund_amount > 0:  # if there's payment left to be adjusted.
        _adjust_for_prepayment(
            session=session,
            loan_id=user_loan.loan_id,
            event_id=event.id,
            amount=refund_amount,
            debit_book_str=m2p_pool_account,
        )

    create_ledger_entry_from_str(
        session=session,
        event_id=event.id,
        debit_book_str=f"{user_loan.loan_id}/loan/lender_payable/l",
        credit_book_str=f"{user_loan.loan_id}/loan/refund_off_balance/l",  # Couldn't find anything relevant.
        amount=Decimal(event.amount),
    )

    group_bills_to_create_loan_schedule(user_loan=user_loan)


def settle_payment_in_bank(
    session: Session,
    payment_request_id: str,
    gateway_expenses: Decimal,
    gross_payment_amount: Decimal,
    settlement_date: DateTime,
    user_loan: BaseLoan,
) -> None:
    settled_amount = gross_payment_amount - gateway_expenses
    event = LedgerTriggerEvent(
        name="payment_settled",
        loan_id=user_loan.loan_id,
        amount=settled_amount,
        post_date=settlement_date,
    )
    session.add(event)
    session.flush()

    payment_settlement_event(
        session=session, gateway_expenses=gateway_expenses, user_loan=user_loan, event=event
    )


def payment_settlement_event(
    session: Session, gateway_expenses: Decimal, user_loan: BaseLoan, event: LedgerTriggerEvent
) -> None:
    if gateway_expenses > 0:  # Adjust for gateway expenses.
        create_ledger_entry_from_str(
            session=session,
            event_id=event.id,
            debit_book_str="12345/redcarpet/gateway_expenses/e",
            credit_book_str=f"{user_loan.lender_id}/lender/pg_account/a",
            amount=gateway_expenses,
        )
    _, writeoff_balance = get_account_balance_from_str(
        session=session, book_string=f"{user_loan.loan_id}/loan/writeoff_expenses/e"
    )
    if writeoff_balance > 0:
        amount = min(writeoff_balance, event.amount)
        create_ledger_entry_from_str(
            session=session,
            event_id=event.id,
            debit_book_str=f"{user_loan.loan_id}/loan/bad_debt_allowance/ca",
            credit_book_str=f"{user_loan.loan_id}/loan/writeoff_expenses/e",
            amount=amount,
        )

    # Lender has received money, so we reduce our liability now.
    create_ledger_entry_from_str(
        session=session,
        event_id=event.id,
        debit_book_str=f"{user_loan.loan_id}/loan/lender_payable/l",
        credit_book_str=f"{user_loan.lender_id}/lender/pg_account/a",
        amount=event.amount,
    )


def adjust_for_min_max_accounts(bill: BaseBill, payment_to_adjust_from: Decimal, event_id: int):
    min_due = bill.get_remaining_min()
    min_to_adjust_in_this_bill = min(min_due, payment_to_adjust_from)
    if min_to_adjust_in_this_bill != 0:
        # Reduce min amount
        create_ledger_entry_from_str(
            bill.session,
            event_id=event_id,
            debit_book_str=f"{bill.id}/bill/min/l",
            credit_book_str=f"{bill.id}/bill/min/a",
            amount=min_to_adjust_in_this_bill,
        )

    max_due = bill.get_remaining_max()
    max_to_adjust_in_this_bill = min(max_due, payment_to_adjust_from)
    if max_to_adjust_in_this_bill != 0:
        # Reduce min amount
        create_ledger_entry_from_str(
            bill.session,
            event_id=event_id,
            debit_book_str=f"{bill.id}/bill/max/l",
            credit_book_str=f"{bill.id}/bill/max/a",
            amount=max_to_adjust_in_this_bill,
        )
