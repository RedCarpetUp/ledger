from decimal import Decimal
from typing import Optional

from pendulum import DateTime
from sqlalchemy.orm import Session

from rush.ledger_events import (
    accrue_interest_event,
    accrue_late_fine_event,
    payment_received_event,
)
from rush.ledger_utils import (
    create_ledger_entry,
    create_ledger_entry_from_str,
    get_account_balance_from_str,
    get_all_unpaid_bills,
    is_bill_closed,
    is_min_paid,
)
from rush.models import (
    LedgerEntry,
    LedgerTriggerEvent,
    LoanData,
    UserCard,
)
from rush.utils import (
    div,
    get_current_ist_time,
    mul,
)


def accrue_interest_prerequisites(
    session: Session, bill: LoanData, to_date: Optional[DateTime] = None
) -> bool:
    # If not closed, we can accrue interest.
    if not is_bill_closed(session, bill, to_date):
        return True
    return False  # prerequisites failed.


def accrue_interest_on_all_bills(session: Session, post_date: DateTime, user_card: UserCard) -> None:
    unpaid_bills = get_all_unpaid_bills(session, user_card.user_id)
    for bill in unpaid_bills:
        # TODO get tenure from loan table.
        interest_on_principal = mul(bill.principal, div(div(bill.rc_rate_of_interest_annual, 12), 100))
        min_event = LedgerTriggerEvent(
            name="accrue_interest", post_date=post_date, amount=interest_on_principal
        )
        session.add(min_event)
        session.flush()
        accrue_interest_event(session, bill, min_event)


def accrue_late_charges_prerequisites(
    session: Session, bill: LoanData, to_date: Optional[DateTime] = None
) -> bool:
    # if not paid, we can charge late fee.
    if not is_min_paid(session, bill, to_date):
        return True
    return False


def accrue_late_charges(session: Session, user_id: int) -> LoanData:
    bill = (
        session.query(LoanData)
        .filter(LoanData.user_id == user_id)
        .order_by(LoanData.agreement_date.desc())
        .first()
    )  # Get the latest bill of that user.
    can_charge_fee = bill.get_minimum_amount_to_pay(session) > 0
    #  accrue_late_charges_prerequisites(session, bill)
    if can_charge_fee:  # if min isn't paid charge late fine.
        # TODO get correct date here.
        lt = LedgerTriggerEvent(
            name="accrue_late_fine", post_date=get_current_ist_time(), amount=Decimal(100)
        )
        session.add(lt)
        session.flush()

        accrue_late_fine_event(session, bill, lt)
    return bill


def reverse_late_charges(session: Session, bill: LoanData, event_to_reverse: LedgerTriggerEvent) -> None:
    lt = LedgerTriggerEvent(name="reverse_accrue_late_fine", post_date=get_current_ist_time())
    session.add(lt)
    session.flush()

    reverse_late_charges_event(session, bill, lt, event_to_reverse)


def reverse_late_charges_event(
    session: Session, bill: LoanData, lt: LedgerTriggerEvent, event_to_reverse: LedgerTriggerEvent
) -> None:
    # Move from late_receivable to desired accounts.
    _, late_fee_received = get_account_balance_from_str(
        session, book_string=f"{bill.id}/bill/late_fee_received/a"
    )
    late_fee_to_reverse = min(late_fee_received, event_to_reverse.amount)
    # Remove any received late fine back to due.
    lt.amount = late_fee_to_reverse  # Store the amount in event.
    if late_fee_to_reverse > 0:
        create_ledger_entry_from_str(
            session,
            event_id=lt.id,
            debit_book_str=f"{bill.id}/bill/late_fine_receivable/a",
            credit_book_str=f"{bill.id}/bill/late_fee_received/a",
            amount=lt.amount,
        )

    # Get rid of the due late fine as well by reversing the old event's entries.
    entries_to_reverse = (
        session.query(LedgerEntry).filter(LedgerEntry.event_id == event_to_reverse.id).all()
    )
    for entry in entries_to_reverse:
        create_ledger_entry(
            session,
            event_id=lt.id,
            debit_book_id=entry.credit_account,
            credit_book_id=entry.debit_account,
            amount=entry.amount,
        )

    # making a list because payment received event works on list of bills
    unpaid_bill = []
    unpaid_bill.append(bill)

    # Trigger another payment event for the fee reversed.
    payment_received_event(session, unpaid_bill, lt)
