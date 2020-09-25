from typing import List

from dateutil.relativedelta import relativedelta
from sqlalchemy import (
    and_,
    func,
)
from sqlalchemy.orm import Session

from rush.anomaly_detection import get_payment_events
from rush.card.base_card import (
    BaseBill,
    BaseLoan,
)
from rush.models import (
    LedgerTriggerEvent,
    LoanSchedule,
)


def group_bills(session: Session, user_loan: BaseLoan):
    cumulative_values_query = (
        session.query(
            LoanSchedule.due_date,
            func.sum(LoanSchedule.principal_due).label("principal_due"),
            func.sum(LoanSchedule.interest_due).label("interest_due"),
            func.sum(LoanSchedule.total_closing_balance).label("total_closing_balance"),
            func.sum(LoanSchedule.total_closing_balance_post_due_date).label(
                "total_closing_balance_post_due_date"
            ),
        )
        .filter(
            LoanSchedule.loan_id == user_loan.loan_id,
            LoanSchedule.bill_id.isnot(None),
        )
        .group_by(LoanSchedule.due_date)
    ).subquery()
    q_results = (
        session.query(cumulative_values_query, LoanSchedule.id)
        .join(
            LoanSchedule,
            and_(
                LoanSchedule.loan_id == user_loan.loan_id,
                LoanSchedule.due_date == cumulative_values_query.c.due_date,
                LoanSchedule.bill_id.is_(None),
            ),
            isouter=True,
        )
        .order_by(cumulative_values_query.c.due_date)
        .all()
    )
    new_emi_objects = []
    update_emi_objects = []
    for emi_number, cumulative_values in enumerate(q_results, 1):
        cumulative_values_dict = cumulative_values._asdict()
        emi_id = cumulative_values.id
        if emi_id:  # If emi id is present then we update the record.
            update_emi_objects.append(cumulative_values_dict)
        else:
            loan_schedule = LoanSchedule(
                loan_id=user_loan.loan_id, emi_number=emi_number, **cumulative_values_dict
            )
            new_emi_objects.append(loan_schedule)
    session.bulk_update_mappings(LoanSchedule, update_emi_objects)
    session.bulk_save_objects(new_emi_objects)


def create_bill_schedule(session: Session, user_loan: BaseLoan, bill: BaseBill):
    emi_objects = []
    due_date = bill.table.bill_start_date
    for emi_number in range(1, bill.table.bill_tenure + 1):
        remaining_tenure = (bill.table.bill_tenure + 1) - emi_number  # Includes the current emi.
        due_date_deltas = bill.get_relative_delta_for_emi(
            emi_number=emi_number, amortization_date=user_loan.amortization_date
        )
        due_date += relativedelta(months=due_date_deltas["months"], day=due_date_deltas["days"])
        bill_schedule = LoanSchedule(
            loan_id=bill.table.loan_id,
            bill_id=bill.table.id,
            emi_number=emi_number,
            due_date=due_date,
            principal_due=bill.table.principal_instalment,
            interest_due=bill.get_interest_to_charge(user_loan.rc_rate_of_interest_monthly),
        )
        bill_schedule.total_closing_balance = bill_schedule.principal_due * remaining_tenure
        bill_schedule.total_closing_balance_post_due_date = (
            bill_schedule.total_closing_balance + bill_schedule.interest_due
        )
        emi_objects.append(bill_schedule)
    session.bulk_save_objects(emi_objects)
    group_bills(session, user_loan)


def reslide_all_payments(session: Session, user_loan: BaseLoan):
    reset_emis_for_reslide(session, user_loan)
    payment_events = get_payment_events(session, user_loan)
    emis = user_loan.get_loan_schedule()
    for payment_event in payment_events:
        slide_payment_to_emis(emis, payment_event)


def reset_emis_for_reslide(session: Session, user_loan: BaseLoan):
    session.query(LoanSchedule).filter(
        LoanSchedule.loan_id == user_loan.loan_id, LoanSchedule.bill_id.is_(None)
    ).update({"payment_received": 0, "payment_status": "UnPaid", "last_payment_date": None})


def slide_payment_to_emis(loan_schedule: List[LoanSchedule], payment_event: LedgerTriggerEvent):
    amount_to_slide = payment_event.amount
    for emi in loan_schedule:
        if emi.payment_status == "Paid":
            continue
        if amount_to_slide <= 0:  # This payment is slid so we move to the next one.
            break
        amount_slid = min(emi.total_due_amount, amount_to_slide)
        emi.payment_received = amount_slid
        if emi.total_due_amount == emi.payment_received:
            emi.payment_status = "Paid"
        emi.last_payment_date = payment_event.post_date
        amount_to_slide -= amount_slid
