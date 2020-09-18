from typing import List

from pendulum import DateTime
from sqlalchemy import func
from sqlalchemy.orm import Session

from rush.accrue_financial_charges import (
    can_remove_latest_accrued_interest,
    is_late_fee_valid,
    reverse_incorrect_late_charges,
    reverse_interest_charges,
)
from rush.card.base_card import BaseLoan
from rush.models import LedgerTriggerEvent

PAYMENT_AFFECTED_EVENTS = ("accrue_interest", "charge_late_fine")


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
            LedgerTriggerEvent.name.in_(list(PAYMENT_AFFECTED_EVENTS)),
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


def has_payment_anomaly(session: Session, user_loan: BaseLoan, payment_date: DateTime) -> bool:
    """
    Assuming that a potential payment anomaly can only occur if last event's post date is greater than payment's date.
    For example, interest got accrued on 16th. Payment came on 14th. This can be an anomaly.
    """
    last_event_date = (
        session.query(LedgerTriggerEvent.post_date)
        .filter(
            LedgerTriggerEvent.loan_id == user_loan.loan_id,
            LedgerTriggerEvent.name.in_(list(PAYMENT_AFFECTED_EVENTS)),  # Maybe this isn't necessary.
        )
        .order_by(LedgerTriggerEvent.id.desc())
        .limit(1)
        .scalar()
    )
    return last_event_date and last_event_date.date() > payment_date.date()


def run_anomaly(session: Session, user_loan: BaseLoan, event_date: DateTime) -> None:
    """
    This checks for any anomalies after we have received the payment. If the interest needs to be
    removed because the complete payment has been made before due date. If the late fee event is not
    valid because there was a delay in payment.
    """
    if not has_payment_anomaly(session, user_loan, event_date):
        return
    events = get_affected_events(session=session, user_loan=user_loan)
    for event in events:
        if event.name == "accrue_interest":
            if can_remove_latest_accrued_interest(
                session=session, user_loan=user_loan, interest_event=event
            ):
                reverse_interest_charges(
                    session, event_to_reverse=event, user_loan=user_loan, payment_date=event_date
                )
        elif event.name == "accrue_late_fine":
            # TODO this probably isn't tested.
            is_charge_valid = is_late_fee_valid(session=session, user_loan=user_loan)
            if not is_charge_valid:
                reverse_incorrect_late_charges(session, user_loan=user_loan, event_to_reverse=event)
