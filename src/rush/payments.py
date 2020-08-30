from decimal import Decimal
from typing import Optional

from dateutil.relativedelta import relativedelta
from pendulum import DateTime
from sqlalchemy.orm import Session

from rush.anomaly_detection import run_anomaly
from rush.card import BaseLoan
from rush.ledger_events import (
    _adjust_for_complete_bill,
    _adjust_for_min,
    _adjust_for_prepayment,
    payment_received_event,
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
    original_swipe: CardTransaction,
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

    bill_of_original_transaction = (
        session.query(LoanData).filter_by(id=original_swipe.loan_id, is_generated=False).one_or_none()
    )
    # Checking if bill is generated or not. if not then reduce from unbilled else treat as payment.
    transaction_refund_event(
        session=session, user_loan=user_loan, event=lt, bill=bill_of_original_transaction
    )
    run_anomaly(session=session, user_loan=user_loan, event_date=payment_date)


def transaction_refund_event(
    session: Session, user_loan: BaseLoan, event: LedgerTriggerEvent, bill: LoanData
) -> None:
    refund_amount = Decimal(event.amount)
    m2p_pool_account = f"{user_loan.lender_id}/lender/pool_balance/a"
    if bill:  # refund happened before bill was generated so reduce it from unbilled.
        # TODO check if refund amount is <= unbilled.
        create_ledger_entry_from_str(
            session=session,
            event_id=event.id,
            debit_book_str=m2p_pool_account,
            credit_book_str=f"{bill.id}/bill/unbilled/a",
            amount=refund_amount,
        )
    else:  # Treat as payment.
        unpaid_bills = user_loan.get_unpaid_bills()
        refund_amount = _adjust_for_min(
            session=session,
            bills=unpaid_bills,
            payment_received=refund_amount,
            event_id=event.id,
            debit_book_str=m2p_pool_account,
        )
        refund_amount = _adjust_for_complete_bill(
            session=session,
            bills=unpaid_bills,
            payment_received=refund_amount,
            event_id=event.id,
            debit_book_str=m2p_pool_account,
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

    from rush.create_emi import group_bills_to_create_loan_schedule

    # Recreate loan level emis
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
