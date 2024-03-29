from decimal import Decimal
from typing import Optional

from dateutil.relativedelta import relativedelta
from pendulum import DateTime
from sqlalchemy import func
from sqlalchemy.orm import Session

from rush.card.base_card import (
    BaseBill,
    BaseLoan,
)
from rush.create_emi import update_event_with_dpd
from rush.ledger_events import (
    _adjust_bill,
    _adjust_for_prepayment,
    accrue_interest_event,
    add_max_amount_event,
    add_min_amount_event,
    adjust_for_revenue,
)
from rush.ledger_utils import (
    create_ledger_entry_from_str,
    get_account_balance_from_str,
)
from rush.models import (
    BookAccount,
    Fee,
    LedgerLoanData,
    LedgerTriggerEvent,
    LoanSchedule,
    NewLedgerEntry,
)
from rush.utils import (
    add_gst_split_to_amount,
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
    interest_left_to_accrue = get_interest_left_to_accrue(session, user_loan)
    if interest_left_to_accrue <= 0:
        return

    unpaid_bills = user_loan.get_unpaid_generated_bills()
    # Find the emi number that's getting accrued at loan level.
    loan_schedule = user_loan.get_emi_to_accrue_interest(post_date=post_date)
    accrue_event = LedgerTriggerEvent(
        name="accrue_interest",
        loan_id=user_loan.loan_id,
        post_date=post_date,
        amount=0,
        extra_details={"emi_id": loan_schedule.id},
    )
    session.add(accrue_event)
    session.flush()

    # accrual actually happens for each bill using bill's schedule.
    for bill in unpaid_bills:
        if interest_left_to_accrue <= 0:
            break
        bill_schedule = (
            session.query(LoanSchedule)
            .filter(LoanSchedule.bill_id == bill.id, LoanSchedule.due_date == loan_schedule.due_date)
            .scalar()
        )
        if bill_schedule:
            interest_to_accrue = min(interest_left_to_accrue, bill_schedule.interest_to_accrue(session))
            accrue_event.amount += interest_to_accrue
            accrue_interest_event(session, bill, accrue_event, interest_to_accrue)
            add_max_amount_event(session, bill, accrue_event, interest_to_accrue)
            interest_left_to_accrue -= interest_to_accrue

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
    _, late_fee_accrued = get_account_balance_from_str(session, f"{latest_bill.id}/bill/late_fee/r")
    if late_fee_accrued == 0:
        return True  # Nothing to remove.

    remaining_min = latest_bill.get_remaining_min()
    remaining_min_after_late_fee_removal = remaining_min - late_fee_accrued
    # if min amount is still left even after removing late fee then the late fee charge is valid.
    if remaining_min_after_late_fee_removal > 0:
        return True
    return False


def create_bill_fee_entry(
    session: Session,
    user_loan: BaseLoan,
    bill: BaseBill,
    event: LedgerTriggerEvent,
    fee_name: str,
    gross_fee_amount: Decimal,
    include_gst_from_gross_amount: Optional[bool] = False,
) -> Fee:
    charge_gst = False
    if user_loan.lender_id == 62311:  # Charge gst only if DMI loan.
        charge_gst = True

    f = Fee(
        user_id=user_loan.user_id,
        event_id=event.id,
        identifier="bill",
        identifier_id=bill.id,
        name=fee_name,
        sgst_rate=Decimal(0),
        cgst_rate=Decimal(0),
        igst_rate=Decimal(18 if charge_gst else 0),
    )
    if include_gst_from_gross_amount:
        d = get_gst_split_from_amount(gross_fee_amount, total_gst_rate=Decimal(18))
    else:
        d = add_gst_split_to_amount(gross_fee_amount, total_gst_rate=Decimal(18))
    f.net_amount = d["net_amount"]
    f.gross_amount = d["gross_amount"]
    session.add(f)
    # Add into min/max amount of the bill too.
    add_min_amount_event(session, bill, event, f.gross_amount)
    add_max_amount_event(session, bill, event, f.gross_amount)
    update_event_with_dpd(user_loan=user_loan, event=event)
    return f


def create_loan_fee_entry(
    session: Session,
    user_loan: BaseLoan,
    event: LedgerTriggerEvent,
    fee_name: str,
    gross_fee_amount: Decimal,
    include_gst_from_gross_amount: Optional[bool] = False,
) -> Fee:
    charge_gst = False  # Temp way. Ideally should pick this from table with dates populated etc.
    if user_loan.lender_id == 62311:  # Charge gst only if DMI loan.
        charge_gst = True
    f = Fee(
        user_id=user_loan.user_id,
        event_id=event.id,
        identifier="loan",
        identifier_id=user_loan.id,
        name=fee_name,
        sgst_rate=Decimal(0),
        cgst_rate=Decimal(0),
        igst_rate=Decimal(18 if charge_gst else 0),
    )
    if include_gst_from_gross_amount:
        d = get_gst_split_from_amount(gross_fee_amount, total_gst_rate=f.igst_rate)
    else:
        d = add_gst_split_to_amount(gross_fee_amount, total_gst_rate=f.igst_rate)
    f.net_amount = d["net_amount"]
    f.gross_amount = d["gross_amount"]
    session.add(f)
    from rush.create_emi import update_event_with_dpd

    update_event_with_dpd(user_loan=user_loan, event=event)
    return f


def accrue_late_charges(
    session: Session,
    user_loan: BaseLoan,
    post_date: DateTime,
    late_fee_to_charge_incl_tax: Decimal = Decimal(100),
) -> BaseBill:
    latest_bill = user_loan.get_latest_generated_bill()
    # Production does not do any checks before levying late fees, so we don't need to here as well. ~ Ananth
    # can_charge_fee = user_loan.get_remaining_min() > 0
    can_charge_fee = True
    if can_charge_fee:  # if min isn't paid charge late fine.
        # TODO get correct date here.
        # Adjust for rounding because total due amount has to be rounded
        event = LedgerTriggerEvent(
            name="charge_late_fee", post_date=post_date, loan_id=user_loan.loan_id
        )
        session.add(event)
        session.flush()
        fee = create_bill_fee_entry(
            session=session,
            user_loan=user_loan,
            bill=latest_bill,
            event=event,
            fee_name="late_fee",
            gross_fee_amount=late_fee_to_charge_incl_tax,
            include_gst_from_gross_amount=True,
        )
        event.amount = fee.gross_amount

        session.flush()
    return latest_bill


def reverse_interest_charges(
    session: Session, event_to_reverse: LedgerTriggerEvent, user_loan: BaseLoan, payment_date: DateTime
) -> None:
    from rush.payments import (
        adjust_for_min_max_accounts,
        find_split_to_slide_in_loan,
    )

    """
    This event is intended only when the complete amount has been paid and we need to remove the
    interest that we accrued before due_date. For example, interest gets accrued on 1st. Last date is
    15th. If user pays the complete principal before 15th, we remove the interest. Removing interest
    is more convenient than adding it on 16th.
    """
    event = LedgerTriggerEvent(
        name="reverse_interest_charges",
        loan_id=user_loan.loan_id,
        post_date=payment_date,
        amount=event_to_reverse.amount,
    )
    session.add(event)
    session.flush()

    # I first find what all bills the previous event touched.
    bills_and_ledger_entry = (
        session.query(LedgerLoanData, NewLedgerEntry)
        .distinct()
        .filter(
            NewLedgerEntry.debit_account == BookAccount.id,
            NewLedgerEntry.event_id == event_to_reverse.id,
            BookAccount.identifier_type == "bill",
            LedgerLoanData.id == BookAccount.identifier,
            BookAccount.book_name == "interest_receivable",
            LedgerLoanData.is_generated.is_(True),
        )
        .all()
    )

    inter_bill_movement_entries = []
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
        # to be removed by the settled_amount.
        d = {"acc_to_remove_from": f"{bill.id}/bill/interest_accrued/r", "amount": settled_amount}
        inter_bill_movement_entries.append(d)  # Move amount from this bill to some other bill.

    total_amount_to_readjust = sum(d["amount"] for d in inter_bill_movement_entries)
    split_data = find_split_to_slide_in_loan(session, user_loan, total_amount_to_readjust)

    for d in split_data:
        for entry in inter_bill_movement_entries:
            amount_to_adjust_in_this_bill = min(d["amount_to_adjust"], entry["amount"])
            if amount_to_adjust_in_this_bill == 0:
                continue
            adjust_for_min_max_accounts(d["bill"], amount_to_adjust_in_this_bill, event.id)
            if d["type"] == "fee":
                adjust_for_revenue(
                    session=session,
                    event_id=event.id,
                    payment_to_adjust_from=amount_to_adjust_in_this_bill,
                    debit_str=entry["acc_to_remove_from"],
                    fee=d["fee"],
                )
            if d["type"] in ("interest", "principal"):
                remaining_amount = _adjust_bill(
                    session,
                    d["bill"],
                    amount_to_adjust_in_this_bill,
                    event.id,
                    debit_acc_str=entry["acc_to_remove_from"],
                )
                assert remaining_amount == 0
            # if not all of it got adjusted in this bill, move remaining amount to next bill.
            # if got adjusted then this will be 0.
            entry["amount"] -= amount_to_adjust_in_this_bill
            d["amount_to_adjust"] -= amount_to_adjust_in_this_bill

    # Check if there's still amount that's left. If yes, then we received extra prepayment.
    is_payment_left = any(e["amount"] > 0 for e in inter_bill_movement_entries)
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
        session.query(Fee, LedgerLoanData)
        .filter(
            Fee.event_id == event_to_reverse.id,
            LedgerLoanData.id == Fee.identifier_id,
            Fee.identifier == "bill",
        )
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
            {"acc_to_remove_from": f"{bill.id}/bill/late_fee/r", "amount": fee.net_amount_paid},
            {"acc_to_remove_from": f"{fee.user_id}/user/cgst_payable/l", "amount": fee.cgst_paid},
            {"acc_to_remove_from": f"{fee.user_id}/user/sgst_payable/l", "amount": fee.sgst_paid},
            {"acc_to_remove_from": f"{fee.user_id}/user/igst_payable/l", "amount": fee.igst_paid},
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

    from rush.create_emi import update_event_with_dpd

    # Update dpd
    update_event_with_dpd(user_loan=user_loan, event=event)


def get_interest_left_to_accrue(session: Session, user_loan: BaseLoan) -> Decimal:
    """
    Used to get remaining interest left to accrue in case user wants to close loan early.
    """
    total_interest_due = (
        session.query(func.sum(LoanSchedule.interest_due))
        .filter(LoanSchedule.loan_id == user_loan.loan_id, LoanSchedule.bill_id.is_(None))
        .scalar()
    )
    early_closing_fee = (
        session.query(func.sum(Fee.gross_amount_paid))
        .filter(
            Fee.identifier == "loan",
            Fee.identifier_id == user_loan.loan_id,
            Fee.name == "early_close_fee",
        )
        .scalar()
        or 0
    )
    all_bills = user_loan.get_all_bills()
    total_interest_accrued = sum(
        get_account_balance_from_str(session, book_string=f"{bill.id}/bill/interest_accrued/r")[1]
        for bill in all_bills
    )
    return total_interest_due - total_interest_accrued - early_closing_fee


def add_early_close_charges(
    session: Session, user_loan: BaseLoan, post_date: DateTime, amount: Decimal
) -> None:
    event = LedgerTriggerEvent(
        name="early_close_charges",
        loan_id=user_loan.loan_id,
        post_date=post_date,
        amount=amount,
    )
    session.add(event)
    session.flush()

    create_loan_fee_entry(
        session, user_loan, event, "early_close_fee", amount, include_gst_from_gross_amount=True
    )
