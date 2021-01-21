from dateutil.relativedelta import relativedelta
from pendulum import date
from sqlalchemy.orm import Session
from sqlalchemy import (
    and_,
    func,
)

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

    loan_moratorium = LoanMoratorium.new(
        user_loan.session,
        loan_id=user_loan.loan_id,
        start_date=start_date,
        end_date=end_date,
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

            MoratoriumInterest.new(
                session=user_loan.session,
                moratorium_id=loan_moratorium.id,
                emi_number=new_emi_number,
                interest=first_emi_before_moratorium.interest_due,
                bill_id=bill_id,
                due_date=new_emi_due_date,
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

    from rush.loan_schedule.loan_schedule import group_bills

    group_bills(user_loan)

    group_moratorium_bills(user_loan, loan_moratorium)


def add_moratorium_bills(session: Session, user_loan: BaseLoan, bill: BaseBill):

    emi_number = 1
    due_date = bill.table.bill_start_date
    opening_principal = bill.table.principal
    moratorium_emi_objects = []
    bill_due_date = bill.table.bill_due_date

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

    while LoanMoratorium.is_in_moratorium(
        session, loan_id=user_loan.loan_id, date_to_check_against=bill_due_date
    ):
        due_date_deltas = bill.get_relative_delta_for_emi(
            emi_number=emi_number, amortization_date=user_loan.amortization_date
        )
        due_date += relativedelta(**due_date_deltas)
        bill_schedule = LoanSchedule(
            loan_id=bill.table.loan_id,
            bill_id=bill.table.id,
            emi_number=emi_number,
            due_date=due_date,
            interest_due=0,
            principal_due=0,
            total_closing_balance=round(opening_principal, 2),
        )
        MoratoriumInterest.new(
            session=session,
            moratorium_id=loan_moratorium.id,
            emi_number=emi_number,
            interest=round(interest_due, 2),
            bill_id=bill.table.id,
            due_date=due_date,
        )
        emi_number += 1
        bill_due_date += relativedelta(months=1)
        moratorium_emi_objects.append(bill_schedule)
    group_moratorium_bills(user_loan, loan_moratorium)

    number_of_months_added = emi_number - 1
    interest_to_be_added = interest_due * number_of_months_added

    return {
        "moratorium_emi_objects": moratorium_emi_objects,
        "number_of_months_added": number_of_months_added,
        "due_date": due_date,
        "interest_to_be_added": interest_to_be_added,
    }


def group_moratorium_bills(user_loan: BaseLoan, loan_moratorium: LoanMoratorium):
    session = user_loan.session
    cumulative_values_query = (
        session.query(
            MoratoriumInterest.due_date,
            func.sum(MoratoriumInterest.interest).label("interest"),
        )
        .filter(
            MoratoriumInterest.moratorium_id == loan_moratorium.id,
            MoratoriumInterest.bill_id.isnot(None),
        )
        .group_by(MoratoriumInterest.due_date)
    ).subquery()
    q_results = (
        session.query(cumulative_values_query, MoratoriumInterest.id)
        .join(
            MoratoriumInterest,
            and_(
                MoratoriumInterest.moratorium_id == loan_moratorium.id,
                MoratoriumInterest.due_date == cumulative_values_query.c.due_date,
                MoratoriumInterest.bill_id.is_(None),
            ),
            isouter=True,
        )
        .order_by(cumulative_values_query.c.due_date)
        .all()
    )
    for emi_number, cumulative_values in enumerate(q_results, 1):
        cumulative_values_dict = cumulative_values._asdict()
        emi_id = cumulative_values.id
        if emi_id:  # If emi id is present then we update the record.
            session.query(MoratoriumInterest).filter_by(id=emi_id).update(cumulative_values_dict)
        else:
            MoratoriumInterest.new(
                session,
                moratorium_id=loan_moratorium.id,
                emi_number=emi_number,
                **cumulative_values_dict,
            )
