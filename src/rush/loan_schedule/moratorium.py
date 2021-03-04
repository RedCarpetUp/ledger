from dateutil.relativedelta import relativedelta
from pendulum import date
from sqlalchemy.orm import Session

from rush.card import BaseLoan
from rush.card.base_card import BaseBill
from rush.models import (
    LedgerTriggerEvent,
    LoanMoratorium,
    LoanSchedule,
    MoratoriumInterest,
)


def provide_moratorium(user_loan: BaseLoan, start_date: date, end_date: date):
    _ = LedgerTriggerEvent.new(
        user_loan.session,
        name="moratorium",
        loan_id=user_loan.loan_id,
        post_date=start_date,
        extra_details={"start_date": start_date.isoformat(), "end_date": end_date.isoformat()},
    )

    next_due_date_after_moratorium_ends = (
        user_loan.session.query(LoanSchedule.due_date)
        .filter(LoanSchedule.due_date > end_date)
        .order_by(LoanSchedule.due_date)
        .limit(1)
        .scalar()
    )

    loan_moratorium = LoanMoratorium.new(
        user_loan.session,
        loan_id=user_loan.loan_id,
        start_date=start_date,
        end_date=end_date,
        due_date_after_moratorium=next_due_date_after_moratorium_ends,
    )

    # Get future emis of all the bills
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
            user_loan.session.add(moratorium_emi)
            user_loan.session.flush()
            MoratoriumInterest.new(
                session=user_loan.session,
                moratorium_id=loan_moratorium.id,
                interest=first_emi_before_moratorium.interest_due,
                loan_schedule_id=moratorium_emi.id,
            )

            new_emi_due_date += relativedelta(months=1, day=15)
            new_emi_number += 1

        total_emis_added = new_emi_number - first_emi_before_moratorium.emi_number
        # Pick interest from the number of emis that were newly added. i.e. if 3 emis were added
        # then we pick the total interest of first 3 emis before moratorium.
        moratorium_interest_to_be_added = sum(emi.interest_due for emi in emis[:total_emis_added])

        for updated_emi_number, emi in enumerate(emis, new_emi_number):
            if emi == first_emi_before_moratorium:  # Also the first emi after moratorium.
                emi.interest_due += moratorium_interest_to_be_added
            emi.emi_number = updated_emi_number
            emi.due_date = new_emi_due_date
            new_emi_due_date += relativedelta(months=1, day=15)

    from rush.loan_schedule.loan_schedule import group_bills

    group_bills(user_loan)


def add_moratorium_emis(session: Session, user_loan: BaseLoan, bill: BaseBill):

    emi_number = 1
    opening_principal = bill.table.principal
    due_date = bill.table.bill_due_date

    if user_loan.interest_type == "reducing":
        interest_due = bill.get_interest_to_charge(principal=opening_principal)
    else:
        interest_due = bill.get_interest_to_charge()

    loan_moratorium = (
        session.query(LoanMoratorium)
        .filter(
            LoanMoratorium.loan_id == user_loan.loan_id,
        )
        .order_by(LoanMoratorium.start_date.desc())
        .first()
    )

    while due_date >= loan_moratorium.start_date and due_date <= loan_moratorium.end_date:
        moratorium_emi = LoanSchedule(
            loan_id=bill.table.loan_id,
            bill_id=bill.table.id,
            emi_number=emi_number,
            due_date=due_date,
            interest_due=0,
            principal_due=0,
            total_closing_balance=round(opening_principal, 2),
        )
        session.add(moratorium_emi)
        session.flush()
        MoratoriumInterest.new(
            session=session,
            moratorium_id=loan_moratorium.id,
            interest=round(interest_due, 2),
            loan_schedule_id=moratorium_emi.id,
        )
        emi_number += 1
        due_date_deltas = bill.get_relative_delta_for_emi(
            emi_number=emi_number, amortization_date=user_loan.amortization_date
        )
        due_date += relativedelta(**due_date_deltas)

    number_of_months_added = emi_number - 1
    moratorium_interest_to_be_added = interest_due * number_of_months_added
    due_date_deltas = bill.get_relative_delta_for_emi(
        emi_number=emi_number, amortization_date=user_loan.amortization_date
    )
    due_date -= relativedelta(**due_date_deltas)

    return {
        "number_of_months_added": number_of_months_added,
        "due_date": due_date,
        "moratorium_interest_to_be_added": moratorium_interest_to_be_added,
    }
