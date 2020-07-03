from datetime import timedelta
from decimal import Decimal
from typing import Optional

from pendulum import DateTime
from sqlalchemy.orm import Session

from rush.ledger_events import (
    _adjust_bill,
    accrue_interest_event,
    accrue_late_fine_event,
)
from rush.ledger_utils import (
    create_ledger_entry_from_str,
    get_account_balance_from_str,
    get_all_unpaid_bills,
    get_remaining_bill_balance,
    is_bill_closed,
)
from rush.models import (
    BookAccount,
    LedgerEntry,
    LedgerTriggerEvent,
    LoanData,
    UserCard,
)
from rush.utils import (
    div,
    get_current_ist_time,
    get_updated_fee_diff_amount_from_principal,
    mul,
)


def _get_total_outstanding(session, user_card):
    # Temp func.
    all_bills = (
        session.query(LoanData)
        .filter(LoanData.card_id == user_card.id, LoanData.is_generated.is_(True))
        .all()
    )
    total_outstanding = sum(get_remaining_bill_balance(session, bill)["total_due"] for bill in all_bills)
    return total_outstanding


def can_remove_interest(
    session: Session,
    user_card: UserCard,
    interest_event: LedgerTriggerEvent,
    event_date: Optional[DateTime] = None,
) -> bool:
    """
    We check if the payment has come before the due date and if the total outstanding amount is
    less than or equal to the interest we charged this month. If it is, then user has paid the complete
    payment and we can remove the interest.
    This function gets called at every payment. We also need to check if the interest is even there to
    be removed.
    """
    latest_bill = LoanData.get_latest_bill(session, user_card.user_id)
    # First check if there is even interest accrued in the latest bill.
    interest_accrued = get_account_balance_from_str(session, f"{latest_bill.id}/bill/interest_earned/r")
    if interest_accrued == 0:
        return False  # Nothing to remove.

    due_date = latest_bill.agreement_date + timedelta(days=user_card.interest_free_period_in_days)
    payment_came_after_due_date = event_date.date() > due_date
    if payment_came_after_due_date:
        return False

    this_month_interest = interest_event.amount  # The total interest amount which we last accrued.
    total_outstanding = _get_total_outstanding(session, user_card)  # TODO outstanding as of due_date.

    if total_outstanding <= this_month_interest:  # the amount has been paid sans interest.
        return True
    return False


def accrue_interest_on_all_bills(session: Session, post_date: DateTime, user_card: UserCard) -> None:
    unpaid_bills = get_all_unpaid_bills(session, user_card.user_id)
    accrue_event = LedgerTriggerEvent(
        name="accrue_interest", card_id=user_card.id, post_date=post_date, amount=0
    )
    session.add(accrue_event)
    session.flush()
    for bill in unpaid_bills:
        # TODO get tenure from loan table.
        interest_on_principal = mul(bill.principal, div(div(bill.rc_rate_of_interest_annual, 12), 100))
        # Adjust for rounding because total due amount has to be rounded
        interest_on_principal += get_updated_fee_diff_amount_from_principal(
            bill.principal, interest_on_principal
        )
        accrue_interest_event(session, bill, accrue_event, interest_on_principal)
        accrue_event.amount += interest_on_principal


def is_late_fee_valid(session: Session, user_card: UserCard) -> bool:
    """
    Late fee gets charged if user fails to pay the minimum due before the due date.
    We check if the min was paid before due date and there's late fee charged.
    """
    latest_bill = LoanData.get_latest_bill(session, user_card.user_id)
    # TODO get bill from event?

    # First check if there is even late fee accrued in the latest bill.
    late_fee_accrued = get_account_balance_from_str(session, f"{latest_bill.id}/bill/late_fine/r")
    if late_fee_accrued == 0:
        return False  # Nothing to remove.

    due_date = latest_bill.agreement_date + timedelta(days=user_card.interest_free_period_in_days)
    min_balance_as_of_due_date = latest_bill.get_minimum_amount_to_pay(session, due_date)
    if (
        min_balance_as_of_due_date > 0
    ):  # if there's balance pending in min then the late fee charge is valid.
        return True
    return False


def accrue_late_charges(session: Session, user_card: UserCard, post_date: DateTime) -> LoanData:
    latest_bill = LoanData.get_latest_bill(session, user_card.user_id)
    can_charge_fee = latest_bill.get_minimum_amount_to_pay(session) > 0
    #  accrue_late_charges_prerequisites(session, bill)
    if can_charge_fee:  # if min isn't paid charge late fine.
        # TODO get correct date here.
        # Adjust for rounding because total due amount has to be rounded
        event = LedgerTriggerEvent(
            name="accrue_late_fine", post_date=post_date, card_id=user_card.id, amount=Decimal(100)
        )
        session.add(event)
        session.flush()

        accrue_late_fine_event(session, latest_bill, event)
    return latest_bill


def reverse_interest_charges(
    session: Session, event_to_reverse: LedgerTriggerEvent, user_card: UserCard, payment_date: DateTime
) -> None:
    """
    This event is intended only when the complete amount has been paid and we need to remove the
    interest that we accrued before due_date. For example, interest gets accrued on 1st. Last date is
    15th. If user pays the complete principal before 15th, we remove the interest. Removing interest
    is more convenient than adding it on 16th.
    """
    event = LedgerTriggerEvent(
        name="reverse_interest_charges", card_id=user_card.id, post_date=payment_date
    )
    session.add(event)
    session.flush()

    # I first find what all bills the previous event touched.
    bills_and_ledger_entry = (
        session.query(LoanData, LedgerEntry)
        .distinct()
        .filter(
            LedgerEntry.debit_account == BookAccount.id,
            LedgerEntry.event_id == event_to_reverse.id,
            BookAccount.identifier_type == "bill",
            LoanData.id == BookAccount.identifier,
            BookAccount.book_name == "interest_receivable",
            LoanData.is_generated.is_(True),
        )
        .all()
    )

    inter_bill_movement_entries = []
    # I don't think this needs to be a list but I'm not sure. Ideally only one bill should be open.
    bills_to_slide = []
    for bill, ledger_entry in bills_and_ledger_entry:
        interest_that_was_added = ledger_entry.amount
        # We check how much got settled in the interest which we're planning to remove.
        _, interest_due = get_account_balance_from_str(session, f"{bill.id}/bill/interest_receivable/a")
        settled_amount = interest_that_was_added - interest_due

        if interest_due > 0:
            # We reverse the original entry by whatever is the remaining amount.
            create_ledger_entry_from_str(
                session,
                event_id=event.id,
                debit_book_str=f"{bill.id}/bill/interest_earned/r",
                credit_book_str=f"{bill.id}/bill/interest_receivable/a",
                amount=interest_due,
            )

        # We need to remove the amount that got adjusted in interest. interest_earned account needs
        # to be removed by the interest_that_was_added amount.
        d = {"acc_to_remove_from": f"{bill.id}/bill/interest_earned/r", "amount": settled_amount}
        inter_bill_movement_entries.append(d)  # Move amount from this bill to some other bill.

        if not is_bill_closed(
            session, bill
        ):  # The bill which is open and we slide the above entries in here.
            bills_to_slide.append(bill)

    for bill in bills_to_slide:
        for entry in inter_bill_movement_entries:
            if entry["amount"] == 0:
                continue
            remaining_amount = _adjust_bill(
                session, bill, entry["amount"], event.id, debit_acc_str=entry["acc_to_remove_from"]
            )
            # if not all of it got adjusted in this bill, move remaining amount to next bill.
            # if got adjusted then this will be 0.
            entry["amount"] = remaining_amount

    # Check if there's still amount that's left. If yes, then we received extra prepayment.
    is_prepayment = any(d["amount"] > 0 for d in inter_bill_movement_entries)
    if is_prepayment:
        pass  # TODO prepayment


def reverse_late_charges(
    session: Session, user_card: UserCard, event_to_reverse: LedgerTriggerEvent
) -> None:
    event = LedgerTriggerEvent(name="reverse_late_charges", post_date=get_current_ist_time())
    session.add(event)
    session.flush()

    bill = (
        session.query(LoanData)
        .distinct()
        .filter(
            LedgerEntry.debit_account == BookAccount.id,
            LedgerEntry.event_id == event_to_reverse.id,
            BookAccount.identifier_type == "bill",
            LoanData.id == BookAccount.identifier,
            BookAccount.book_name == "late_fine_receivable",
            LoanData.is_generated.is_(True),
        )
        .one()
    )
    late_fine_charged = event_to_reverse.amount
    # We check how much got settled in the interest which we're planning to remove.
    _, late_fine_due = get_account_balance_from_str(session, f"{bill.id}/bill/late_fine_receivable/a")
    settled_amount = late_fine_charged - late_fine_due

    if late_fine_due > 0:
        # We reverse the original entry by whatever is the remaining amount.
        create_ledger_entry_from_str(
            session,
            event_id=event.id,
            debit_book_str=f"{bill.id}/bill/late_fine/r",
            credit_book_str=f"{bill.id}/bill/late_fine_receivable/a",
            amount=late_fine_due,
        )
        # Remove from min too. But only what's due. Rest doesn't matter.
        create_ledger_entry_from_str(
            session,
            event_id=event.id,
            debit_book_str=f"{bill.id}/bill/min/l",
            credit_book_str=f"{bill.id}/bill/min/a",
            amount=late_fine_due,
        )
    if settled_amount > 0:
        remaining_amount = _adjust_bill(
            session, bill, settled_amount, event.id, debit_acc_str=f"{bill.id}/bill/late_fine/r"
        )
        if remaining_amount > 0:
            pass  # TODO prepayment
