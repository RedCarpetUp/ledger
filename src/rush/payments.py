from decimal import Decimal

from pendulum import DateTime
from sqlalchemy.orm import Session

from rush.anomaly_detection import run_anomaly
from rush.card import BaseCard
from rush.ledger_events import (
    _adjust_for_complete_bill,
    _adjust_for_min,
    _adjust_for_prepayment,
    payment_received_event,
)
from rush.ledger_utils import create_ledger_entry_from_str
from rush.models import (
    CardTransaction,
    LedgerTriggerEvent,
    LoanData,
)


def payment_received(
    session: Session,
    user_card: BaseCard,
    payment_amount: Decimal,
    payment_date: DateTime,
    payment_request_id: str,
) -> None:
    lt = LedgerTriggerEvent(
        name="payment_received",
        card_id=user_card.id,
        amount=payment_amount,
        post_date=payment_date,
        extra_details={"payment_request_id": payment_request_id, "gateway_charges": 0.5},
    )
    session.add(lt)
    session.flush()

    payment_received_event(session, user_card, f"{user_card.lender_id}/lender/pg_account/a", lt)
    run_anomaly(session, user_card, payment_date)


def refund_payment(
    session: Session,
    user_card: BaseCard,
    payment_amount: Decimal,
    payment_date: DateTime,
    payment_request_id: str,
    original_swipe: CardTransaction,
) -> None:
    lt = LedgerTriggerEvent(
        name="transaction_refund",
        card_id=user_card.id,
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
    transaction_refund_event(session, user_card, lt, bill_of_original_transaction)
    run_anomaly(session, user_card, payment_date)


def transaction_refund_event(
    session: Session, user_card: BaseCard, event: LedgerTriggerEvent, bill: LoanData
) -> None:
    refund_amount = Decimal(event.amount)
    m2p_pool_account = f"{user_card.lender_id}/lender/pool_balance/a"
    if bill:  # refund happened before bill was generated so reduce it from unbilled.
        # TODO check if refund amount is <= unbilled.
        create_ledger_entry_from_str(
            session,
            event_id=event.id,
            debit_book_str=m2p_pool_account,
            credit_book_str=f"{bill.id}/bill/unbilled/a",
            amount=refund_amount,
        )
    else:  # Treat as payment.
        unpaid_bills = user_card.get_unpaid_bills()
        refund_amount = _adjust_for_min(
            session, unpaid_bills, refund_amount, event.id, debit_book_str=m2p_pool_account,
        )
        refund_amount = _adjust_for_complete_bill(
            session, unpaid_bills, refund_amount, event.id, debit_book_str=m2p_pool_account,
        )

        if refund_amount > 0:  # if there's payment left to be adjusted.
            _adjust_for_prepayment(
                session, user_card.id, event.id, refund_amount, debit_book_str=m2p_pool_account
            )
    #
    # _, writeoff_balance = get_account_balance_from_str(
    #     session, book_string=f"{user_card.id}/card/writeoff_expenses/e"
    # )
    # if writeoff_balance > 0:
    #     amount = min(writeoff_balance, event.amount)
    #     _adjust_for_recovery(session, user_card.id, event.id, amount)
    #
    # else:
    # Lender has received money, so we reduce our liability now.
    create_ledger_entry_from_str(
        session,
        event_id=event.id,
        debit_book_str=f"{user_card.id}/card/lender_payable/l",
        credit_book_str=f"{user_card.id}/card/refund_off_balance/l",  # Couldn't find anything relevant.
        amount=Decimal(event.amount),
    )

    # Slide payment in emi
    from rush.create_emi import slide_payments, update_event_with_dpd

    slide_payments(user_card=user_card, payment_event=event)
    # Update on card level
    update_event_with_dpd(event, user_card)
