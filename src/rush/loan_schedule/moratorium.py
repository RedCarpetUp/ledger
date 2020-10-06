from dateutil.relativedelta import relativedelta
from pendulum import date

from rush.card import BaseLoan
from rush.loan_schedule.loan_schedule import group_bills
from rush.models import (
    LedgerTriggerEvent,
    LoanSchedule,
)


def provide_moratorium(user_loan: BaseLoan, start_date: date, end_date: date):
    _ = LedgerTriggerEvent.new(
        user_loan.session,
        name="moratorium",
        loan_id=user_loan.loan_id,
        post_date=start_date,
        extra_details={"start_date": start_date.isoformat(), "end_date": end_date.isoformat()},
    )

    # Get future emis of all the bills whose emis are falling under moratorium period.
    bill_emis = (
        user_loan.session.query(LoanSchedule)
        .filter(
            LoanSchedule.loan_id == user_loan.loan_id,
            LoanSchedule.bill_id.isnot(None),
            LoanSchedule.due_date >= start_date,
        )
        .order_by(LoanSchedule.emi_number)
        .all()
    )
    bill_id_and_its_emis = {}
    for emi in bill_emis:
        bill_id_and_its_emis.setdefault(emi.bill_id, []).append(emi)

    newly_added_moratorium_emis = []
    for bill_id, emis in bill_id_and_its_emis.items():
        first_emi_before_moratorium = emis[0]
        new_emi_due_date = first_emi_before_moratorium.due_date
        new_emi_number = first_emi_before_moratorium.emi_number
        while True:  # Create new emis until moratorium period.
            if new_emi_due_date > end_date:
                break
            moratorium_emi = LoanSchedule(
                loan_id=first_emi_before_moratorium.loan_id,
                bill_id=bill_id,
                emi_number=new_emi_number,
                due_date=new_emi_due_date,
                principal_due=0,
                interest_due=0,
                total_closing_balance=first_emi_before_moratorium.total_closing_balance,
            )
            new_emi_due_date += relativedelta(months=1)
            new_emi_number += 1
            newly_added_moratorium_emis.append(moratorium_emi)

        total_emis_added = new_emi_number - first_emi_before_moratorium.emi_number
        # Pick interest from the number of emis that were newly added. i.e. if 3 emis were added
        # then we pick the total interest of first 3 emis before moratorium.
        moratorium_interest_to_be_added = sum(emi.interest_due for emi in emis[:total_emis_added])

        for updated_emi_number, emi in enumerate(emis, new_emi_number):
            if emi == first_emi_before_moratorium:  # Also the first emi after moratorium.
                emi.interest_due += moratorium_interest_to_be_added
            emi.emi_number = updated_emi_number
            emi.due_date = new_emi_due_date
            new_emi_due_date += relativedelta(months=1)
    user_loan.session.bulk_save_objects(newly_added_moratorium_emis)
    group_bills(user_loan)
