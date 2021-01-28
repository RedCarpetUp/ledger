from typing import (
    List,
    Optional,
)

from dateutil.relativedelta import relativedelta
from pendulum import date

from rush.card import BaseLoan
from rush.card.base_card import BaseBill
from rush.loan_schedule.calculations import get_interest_to_charge
from rush.loan_schedule.loan_schedule import (
    group_bills,
    readjust_future_payment,
)
from rush.models import (
    LedgerTriggerEvent,
    LoanSchedule,
)


def extend_bill_schedule(user_loan: BaseLoan, bill: BaseBill, from_date: date, new_tenure: int):
    event = LedgerTriggerEvent.new(
        user_loan.session,
        name="bill_extended",
        loan_id=bill.table.loan_id,
        post_date=from_date,
        extra_details={"bill_id": bill.table.id, "new_tenure": new_tenure},
    )
    # Update bill variables.
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
    opening_principal = first_emi.total_closing_balance
    instalment = bill.get_instalment_amount(
        principal=opening_principal, tenure=len(future_bill_emis)
    )  # Used only for reducing.
    instalment_without_rounding = bill.get_instalment_amount(
        principal=opening_principal, tenure=len(future_bill_emis), to_round=False
    )
    principal_due = first_emi.total_closing_balance / len(future_bill_emis)
    for bill_emi in future_bill_emis:
        if user_loan.interest_type == "reducing":
            interest_due = bill.get_interest_to_charge(
                principal=opening_principal, instalment=instalment_without_rounding
            )
            principal_due = instalment - interest_due
        else:
            interest_due = bill.get_interest_to_charge()
        bill_emi.principal_due = round(principal_due, 2)
        bill_emi.interest_due = round(interest_due, 2)
        bill_emi.total_closing_balance = round(opening_principal, 2)
        opening_principal -= principal_due

    bill.session.bulk_save_objects(newly_created_emis)


def extend_schedule(
    user_loan: BaseLoan, new_tenure: int, from_date: date, bills: Optional[List[BaseBill]] = None
):
    if not bills:
        bills = user_loan.get_unpaid_generated_bills()
    for bill in bills:
        extend_bill_schedule(user_loan, bill, from_date, new_tenure)
    group_bills(user_loan)
    readjust_future_payment(user_loan, date_to_check_after=from_date)
