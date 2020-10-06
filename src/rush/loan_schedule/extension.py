from typing import (
    List,
    Optional,
)

from dateutil.relativedelta import relativedelta
from pendulum import date

from rush.card import BaseLoan
from rush.card.base_card import BaseBill
from rush.loan_schedule.loan_schedule import (
    group_bills,
    readjust_future_payment,
)
from rush.models import (
    LedgerTriggerEvent,
    LoanSchedule,
)
from rush.utils import div


def extend_bill_schedule(user_loan: BaseLoan, bill: BaseBill, from_date: date, new_tenure: int):
    event = LedgerTriggerEvent.new(
        user_loan.session,
        name="bill_extended",
        loan_id=bill.table.loan_id,
        post_date=from_date,
        extra_details={"bill_id": bill.table.id, "new_tenure": new_tenure},
    )
    new_instalment = div(bill.table.principal, new_tenure)
    # Update bill variables.
    bill.table.principal_instalment = new_instalment  # This is required for interest calc.
    bill.table.bill_tenure = new_tenure

    future_bill_emis = (
        bill.session.query(LoanSchedule)
        .filter(
            LoanSchedule.bill_id.isnot(None),
            LoanSchedule.bill_id == bill.table.id,
            LoanSchedule.due_date >= event.post_date.date(),
        )
        .order_by(LoanSchedule.emi_number)
        .all()
    )
    first_emi = future_bill_emis[0]
    last_emi = future_bill_emis[-1]

    # Create new emis.
    newly_created_emis = []
    last_emi_due_date = last_emi.due_date
    for emi_number in range(last_emi.emi_number + 1, new_tenure + 1):
        last_emi_due_date += relativedelta(months=1)
        bill_schedule = LoanSchedule(
            loan_id=bill.table.loan_id,
            bill_id=bill.table.id,
            emi_number=emi_number,
            due_date=last_emi_due_date,
        )
        newly_created_emis.append(bill_schedule)

    future_bill_emis += newly_created_emis

    # Correct amounts in all future emis.
    non_rounded_bill_instalment = first_emi.total_closing_balance / len(future_bill_emis)
    for bill_emi in future_bill_emis:
        remaining_tenure = new_tenure - bill_emi.emi_number + 1  # plus one to consider current emi
        bill_emi.principal_due = round(non_rounded_bill_instalment, 2)
        bill_emi.interest_due = bill.get_interest_to_charge(user_loan.rc_rate_of_interest_monthly)
        bill_emi.total_closing_balance = round(non_rounded_bill_instalment * remaining_tenure, 2)

    bill.session.bulk_save_objects(newly_created_emis)


def extend_schedule(
    user_loan: BaseLoan, new_tenure: int, from_date: date, bills: Optional[List[BaseBill]] = None
):
    # TODO this won't work if there's payment in future emis that is being readjusted. Can do it tho.
    if not bills:
        bills = user_loan.get_unpaid_generated_bills()
    for bill in bills:
        extend_bill_schedule(user_loan, bill, from_date, new_tenure)
    group_bills(user_loan)
    readjust_future_payment(user_loan, date_to_check_after=from_date)
