from dateutil.relativedelta import relativedelta
from sqlalchemy.orm import Session

from rush.card.base_card import (
    BaseBill,
    BaseLoan,
)
from rush.models import LoanSchedule


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
