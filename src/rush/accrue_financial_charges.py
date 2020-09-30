from datetime import timedelta
from decimal import Decimal
from typing import Optional

from dateutil.relativedelta import relativedelta
from pendulum import DateTime
from pendulum.constants import USECS_PER_SEC
from sqlalchemy.orm import Session

from rush.card.base_card import (
    BaseBill,
    BaseLoan,
)
from rush.ledger_events import (
    _adjust_bill,
    _adjust_for_prepayment,
    accrue_interest_event,
    add_max_amount_event,
    add_min_amount_event,
)
from rush.ledger_utils import (
    create_ledger_entry_from_str,
    get_account_balance_from_str,
    is_bill_closed,
)
from rush.models import (
    BillFee,
    BookAccount,
    CardEmis,
    Fee,
    LedgerEntry,
    LedgerTriggerEvent,
    LoanData,
    UserCard,
)
from rush.utils import (
    get_current_ist_time,
    get_gst_split_from_amount,
)


def can_remove_latest_accrued_interest(
    session: Session,
    user_loan: BaseLoan,
    interest_event: LedgerTriggerEvent,
) -> bool:
    """
    We check if the payment has come before the due date and if the total outstanding amount is
    less than or equal to the interest we charged this month. If it is, then user has paid the complete
    payment and we can remove the interest.
    This function gets called at every payment. We also need to check if the interest is even there to
    be removed.
    """
    latest_bill = user_loan.get_latest_generated_bill()
    # First check if there is even interest accrued in the latest bill.
    _, interest_accrued = get_account_balance_from_str(
        session, f"{latest_bill.table.id}/bill/interest_accrued/r"
    )
    if interest_accrued == 0:
        return False  # Nothing to remove.

    total_interest_accrued = interest_event.amount  # The total interest amount which we last accrued.
    remaining_amount = user_loan.get_remaining_max()
    # If the only amount that's left is less than or equal to the interest that was wrongly accrued.
    if remaining_amount <= total_interest_accrued:
        return True
    return False


def accrue_interest_on_all_bills(session: Session, post_date: DateTime, user_loan: BaseLoan) -> None:
    unpaid_bills = user_loan.get_unpaid_generated_bills()
    accrue_event = LedgerTriggerEvent(
        name="accrue_interest", loan_id=user_loan.loan_id, post_date=post_date, amount=0
    )
    session.add(accrue_event)
    session.flush()
    for bill in unpaid_bills:
        interest_to_charge = (
            session.query(CardEmis.interest)
            .filter(
                CardEmis.bill_id == bill.id,
                CardEmis.due_date < post_date,
                CardEmis.due_date > post_date - relativedelta(months=1),  # Should be within a month
                CardEmis.row_status == "active",
            )
            .order_by(CardEmis.due_date.desc())
            .limit(1)
            .scalar()
        )

        if interest_to_charge:
            accrue_event.amount += interest_to_charge
            accrue_interest_event(session, bill, accrue_event, interest_to_charge)
            add_max_amount_event(session, bill, accrue_event, interest_to_charge)

    from rush.create_emi import update_event_with_dpd

    # Dpd calculation
    update_event_with_dpd(user_loan=user_loan, event=accrue_event)


def is_late_fee_valid(session: Session, user_loan: BaseLoan) -> bool:
    """
    Late fee gets charged if user fails to pay the minimum due before the due date.
    We check if the min was paid before due date and there's late fee charged.
    """
    latest_bill = user_loan.get_latest_generated_bill()
    # TODO get bill from event?

    # First check if there is even late fee accrued in the latest bill.
    _, late_fee_accrued = get_account_balance_from_str(session, f"{latest_bill.id}/bill/late_fine/r")
    if late_fee_accrued == 0:
        return False  # Nothing to remove.

    remaining_min = latest_bill.get_remaining_min()
    remaining_min_after_late_fee_removal = remaining_min - late_fee_accrued
    # if min amount is still left even after removing late fee then the late fee charge is valid.
    if remaining_min_after_late_fee_removal > 0:
        return True
    return False


def create_bill_fee_entry(
    session: Session,
    user_id: int,
    bill: BaseBill,
    event: LedgerTriggerEvent,
    fee_name: str,
    gross_fee_amount: Decimal,
) -> Fee:
    f = BillFee(
        user_id=user_id,
        event_id=event.id,
        identifier_id=bill.id,
        name=fee_name,
        sgst_rate=Decimal(0),
        cgst_rate=Decimal(0),
        igst_rate=Decimal(18),
    )
    d = get_gst_split_from_amount(
        gross_fee_amount, sgst_rate=f.sgst_rate, cgst_rate=f.cgst_rate, igst_rate=f.igst_rate
    )
    f.net_amount = d["net_amount"]
    f.gross_amount = d["gross_amount"]
    session.add(f)
    # Add into min/max amount of the bill too.
    add_min_amount_event(session, bill, event, f.gross_amount)
    add_max_amount_event(session, bill, event, f.gross_amount)
    return f


def accrue_late_charges(
    session: Session,
    user_loan: BaseLoan,
    post_date: DateTime,
    late_fee_to_charge_incl_tax: Decimal = Decimal(100),
) -> BaseBill:
    latest_bill = user_loan.get_latest_generated_bill()
    can_charge_fee = user_loan.get_remaining_min() > 0
    if can_charge_fee:  # if min isn't paid charge late fine.
        # TODO get correct date here.
        # Adjust for rounding because total due amount has to be rounded
        event = LedgerTriggerEvent(
            name="charge_late_fine", post_date=post_date, loan_id=user_loan.loan_id
        )
        session.add(event)
        session.flush()
        fee = create_bill_fee_entry(
            session=session,
            user_id=user_loan.user_id,
            bill=latest_bill,
            event=event,
            fee_name="late_fee",
            gross_fee_amount=late_fee_to_charge_incl_tax,
        )
        event.amount = fee.gross_amount

        session.flush()

        from rush.create_emi import adjust_late_fee_in_emis, update_event_with_dpd

        adjust_late_fee_in_emis(session=session, user_loan=user_loan, bill=latest_bill)
        update_event_with_dpd(user_loan=user_loan, event=event)
    return latest_bill


def reverse_interest_charges(
    session: Session, event_to_reverse: LedgerTriggerEvent, user_loan: BaseLoan, payment_date: DateTime
) -> None:
    """
    This event is intended only when the complete amount has been paid and we need to remove the
    interest that we accrued before due_date. For example, interest gets accrued on 1st. Last date is
    15th. If user pays the complete principal before 15th, we remove the interest. Removing interest
    is more convenient than adding it on 16th.
    """
    event = LedgerTriggerEvent(
        name="reverse_interest_charges", loan_id=user_loan.loan_id, post_date=payment_date
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
        ):  # The bill which are open and we slide the above entries in here.
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
                session=session,
                loan_id=user_loan.loan_id,
                event_id=event.id,
                amount=entry["amount"],
                debit_book_str=entry["acc_to_remove_from"],
            )

    from rush.create_emi import update_event_with_dpd

    update_event_with_dpd(user_loan=user_loan, event=event)


def reverse_incorrect_late_charges(
    session: Session, user_loan: BaseLoan, event_to_reverse: LedgerTriggerEvent
) -> None:
    event = LedgerTriggerEvent(
        name="reverse_late_charges",
        loan_id=user_loan.loan_id,
        post_date=get_current_ist_time(),
        amount=event_to_reverse.amount,
    )
    session.add(event)
    session.flush()

    fee, bill = (
        session.query(BillFee, LoanData)
        .filter(BillFee.event_id == event_to_reverse.id, LoanData.id == BillFee.identifier_id)
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
                    session=session,
                    loan_id=user_loan.loan_id,
                    event_id=event.id,
                    amount=acc["amount"],
                    debit_book_str=acc["acc_to_remove_from"],
                )
    fee.fee_status = "REVERSED"

    # Adjust reversal of late fee in bill
    emi = (
        session.query(CardEmis)
        .filter(
            CardEmis.loan_id == user_loan.loan_id,
            CardEmis.bill_id == bill.id,
            CardEmis.emi_number == 1,
            CardEmis.row_status == "active",
        )
        .order_by(CardEmis.emi_number.asc())
        .first()
    )
    if fee and fee.gross_amount > 0:
        emi.total_closing_balance_post_due_date -= fee.gross_amount
        emi.total_due_amount -= fee.gross_amount
        emi.late_fee -= fee.gross_amount
        session.flush()

    from rush.create_emi import group_bills_to_create_loan_schedule, update_event_with_dpd

    # Recreate loan level emis
    group_bills_to_create_loan_schedule(user_loan=user_loan)
    # Update dpd
    update_event_with_dpd(user_loan=user_loan, event=event)
