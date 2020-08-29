from typing import List

from pendulum import DateTime
from sqlalchemy import func
from sqlalchemy.orm import Session

from rush.accrue_financial_charges import (
    can_remove_interest,
    is_late_fee_valid,
    reverse_incorrect_late_charges,
    reverse_interest_charges,
)
from rush.card.base_card import BaseLoan
from rush.models import LedgerTriggerEvent


def get_affected_events(session: Session, user_loan: BaseLoan) -> List[LedgerTriggerEvent]:
    rank_func = (
        func.rank()
        .over(order_by=LedgerTriggerEvent.post_date.desc(), partition_by=LedgerTriggerEvent.name)
        .label("rnk")
    )
    events = (
        session.query(LedgerTriggerEvent, rank_func)
        .filter(
            LedgerTriggerEvent.loan_id == user_loan.loan_id,
            # These are the only events which can be affected by a payment.
            LedgerTriggerEvent.name.in_(["accrue_interest", "charge_late_fine"]),
        )
        .from_self(LedgerTriggerEvent)
        .filter(rank_func == 1)
        .all()
    )
    return events


def get_payment_events(session: Session, user_loan: BaseLoan) -> List[LedgerTriggerEvent]:
    events = (
        session.query(LedgerTriggerEvent)
        .filter(
            LedgerTriggerEvent.loan_id == user_loan.loan_id,
            LedgerTriggerEvent.name.in_(["payment_received", "transaction_refund"]),
        )
        .all()
    )
    return events


def run_anomaly(session: Session, user_loan: BaseLoan, event_date: DateTime) -> None:
    """
    This checks for any anomalies after we have received the payment. If the interest needs to be
    removed because the complete payment has been made before due date. If the late fee event is not
    valid because there was a delay in payment. Etc.
    """
    events = get_affected_events(session=session, user_loan=user_loan)
    for event in events:
        if event.name == "accrue_interest":
            if can_remove_interest(
                session=session, user_loan=user_loan, interest_event=event, event_date=event_date
            ):
                reverse_interest_charges(
                    session, event_to_reverse=event, user_loan=user_loan, payment_date=event_date
                )
        elif event.name == "accrue_late_fine":
            is_charge_valid = is_late_fee_valid(session=session, user_loan=user_loan)
            if not is_charge_valid:
                reverse_incorrect_late_charges(session, user_loan=user_loan, event_to_reverse=event)
