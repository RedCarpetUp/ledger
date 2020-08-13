from datetime import timedelta
from decimal import Decimal
from typing import Optional

from pendulum import DateTime
from sqlalchemy.orm import Session

from rush.card import BaseCard
from rush.card.base_card import BaseBill
from rush.ledger_events import (
    _adjust_bill,
    _adjust_for_prepayment,
    accrue_interest_event,
    add_min_amount_event,
)
from rush.ledger_utils import (
    create_ledger_entry_from_str,
    get_account_balance_from_str,
    get_remaining_bill_balance,
    is_bill_closed,
)
from rush.models import (
    BookAccount,
    Fee,
    LedgerEntry,
    LedgerTriggerEvent,
    LoanData,
    UserCard,
)
from rush.utils import (
    add_gst_split_to_amount,
    div,
    get_current_ist_time,
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
    user_card: BaseCard,
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
    latest_bill = user_card.get_latest_generated_bill()
    # First check if there is even interest accrued in the latest bill.
    _, interest_accrued = get_account_balance_from_str(
        session, f"{latest_bill.id}/bill/interest_accrued/r"
    )
    if interest_accrued == 0:
        return False  # Nothing to remove.

    due_date = latest_bill.bill_start_date + timedelta(days=user_card.interest_free_period_in_days)
    payment_came_after_due_date = event_date.date() > due_date
    if payment_came_after_due_date:
        return False

    this_month_interest = interest_event.amount  # The total interest amount which we last accrued.
    total_outstanding = _get_total_outstanding(session, user_card)  # TODO outstanding as of due_date.

    if total_outstanding <= this_month_interest:  # the amount has been paid sans interest.
        return True
    return False


def accrue_interest_on_all_bills(session: Session, post_date: DateTime, user_card: BaseCard) -> None:
    unpaid_bills = user_card.get_unpaid_bills()
    accrue_event = LedgerTriggerEvent(
        name="accrue_interest", card_id=user_card.id, post_date=post_date, amount=0
    )
    session.add(accrue_event)
    session.flush()
    for bill in unpaid_bills:
        accrue_interest_event(session, bill, accrue_event, bill.table.interest_to_charge)
        accrue_event.amount += bill.table.interest_to_charge
    # adjust the given interest in schedule
    # adjust the given interest in schedule
    # from rush.create_emi import adjust_interest_in_emis

    # adjust_interest_in_emis(session, user_card, post_date)


def is_late_fee_valid(session: Session, user_card: BaseCard) -> bool:
    """
    Late fee gets charged if user fails to pay the minimum due before the due date.
    We check if the min was paid before due date and there's late fee charged.
    """
    latest_bill = user_card.get_latest_generated_bill()
    # TODO get bill from event?

    # First check if there is even late fee accrued in the latest bill.
    _, late_fee_accrued = get_account_balance_from_str(session, f"{latest_bill.id}/bill/late_fine/r")
    if late_fee_accrued == 0:
        return False  # Nothing to remove.

    due_date = latest_bill.bill_start_date + timedelta(days=user_card.interest_free_period_in_days)
    min_balance_as_of_due_date = latest_bill.get_remaining_min(due_date)
    if (
        min_balance_as_of_due_date > 0
    ):  # if there's balance pending in min then the late fee charge is valid.
        return True
    return False


def create_fee_entry(
    session: Session, bill: BaseBill, event: LedgerTriggerEvent, fee_name: str, net_fee_amount: Decimal
) -> Fee:
    f = Fee(
        bill_id=bill.id,
        event_id=event.id,
        card_id=bill.table.card_id,
        name=fee_name,
        net_amount=net_fee_amount,
        sgst_rate=Decimal(9),
        cgst_rate=Decimal(9),
        igst_rate=Decimal(0),
    )
    d = add_gst_split_to_amount(
        net_fee_amount, sgst_rate=f.sgst_rate, cgst_rate=f.cgst_rate, igst_rate=f.igst_rate
    )
    f.gross_amount = d["gross_amount"]
    session.add(f)
    return f


def accrue_late_charges(session: Session, user_card: BaseCard, post_date: DateTime) -> BaseBill:
    latest_bill = user_card.get_latest_generated_bill()
    can_charge_fee = latest_bill.get_remaining_min() > 0
    #  accrue_late_charges_prerequisites(session, bill)
    if can_charge_fee:  # if min isn't paid charge late fine.
        # TODO get correct date here.
        # Adjust for rounding because total due amount has to be rounded
        late_fee_to_charge_without_tax = Decimal(100)
        event = LedgerTriggerEvent(name="charge_late_fine", post_date=post_date, card_id=user_card.id)
        session.add(event)
        session.flush()
        fee = create_fee_entry(session, latest_bill, event, "late_fee", late_fee_to_charge_without_tax)
        event.amount = fee.gross_amount
        # Add into min amount of the bill too.
        add_min_amount_event(session, latest_bill, event, event.amount)
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
                debit_book_str=f"{bill.id}/bill/interest_accrued/r",
                credit_book_str=f"{bill.id}/bill/interest_receivable/a",
                amount=interest_due,
            )

        # We need to remove the amount that got adjusted in interest. interest_earned account needs
        # to be removed by the interest_that_was_added amount.
        d = {"acc_to_remove_from": f"{bill.id}/bill/interest_accrued/r", "amount": settled_amount}
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
    is_payment_left = any(d["amount"] > 0 for d in inter_bill_movement_entries)
    if is_payment_left:
        for entry in inter_bill_movement_entries:
            if entry["amount"] == 0:
                continue
            _adjust_for_prepayment(
                session, user_card.user_id, event.id, entry["amount"], entry["acc_to_remove_from"]
            )


def reverse_late_charges(
    session: Session, user_card: BaseCard, event_to_reverse: LedgerTriggerEvent
) -> None:
    event = LedgerTriggerEvent(
        name="reverse_late_charges",
        card_id=user_card.id,
        post_date=get_current_ist_time(),
        amount=event_to_reverse.amount,
    )
    session.add(event)
    session.flush()

    fee, bill = (
        session.query(Fee, LoanData)
        .filter(Fee.event_id == event_to_reverse.id, LoanData.id == Fee.bill_id)
        .one_or_none()
    )

    if fee.fee_status == "UNPAID":
        # Remove from min. But only what's remaining. Rest doesn't matter.
        create_ledger_entry_from_str(
            session,
            event_id=event.id,
            debit_book_str=f"{bill.id}/bill/min/l",
            credit_book_str=f"{bill.id}/bill/min/a",
            amount=fee.gross_amount - fee.gross_amount_paid,
        )
    if fee.gross_amount_paid > 0:
        # Need to remove money from all these accounts and slide them back to the same bill.
        acc_info = [
            {"acc_to_remove_from": f"{bill.id}/bill/late_fine/r", "amount": fee.net_amount_paid},
            {"acc_to_remove_from": "12345/redcarpet/cgst_payable/l", "amount": fee.cgst_paid},
            {"acc_to_remove_from": "12345/redcarpet/sgst_payable/l", "amount": fee.sgst_paid},
            {"acc_to_remove_from": "12345/redcarpet/igst_payable/l", "amount": fee.igst_paid},
        ]
        for acc in acc_info:
            if acc["amount"] == 0:
                continue
            remaining_amount = _adjust_bill(
                session, bill, acc["amount"], event.id, acc["acc_to_remove_from"]
            )
            acc["amount"] = remaining_amount
        # Check if there's still amount that's left. If yes, then we received extra prepayment.
        is_payment_left = any(d["amount"] > 0 for d in acc_info)
        if is_payment_left:
            for acc in acc_info:
                if acc["amount"] == 0:
                    continue
                # TODO maybe just call the entire payment received event here?
                _adjust_for_prepayment(
                    session, user_card.user_id, event.id, acc["amount"], acc["acc_to_remove_from"]
                )
    fee.fee_status = "REVERSED"
