from collections import defaultdict
from datetime import date

from dateutil.relativedelta import relativedelta
from pendulum import datetime
from sqlalchemy import (
    and_,
    func,
)
from sqlalchemy.orm import Session

from rush.card.base_card import (
    BaseBill,
    BaseLoan,
)
from rush.loan_schedule.calculations import get_interest_to_charge
from rush.loan_schedule.moratorium import add_moratorium_bills
from rush.models import (
    LedgerTriggerEvent,
    LoanMoratorium,
    LoanSchedule,
    MoratoriumInterest,
    PaymentMapping,
)


def group_bills(user_loan: BaseLoan):
    session = user_loan.session
    cumulative_values_query = (
        session.query(
            LoanSchedule.due_date,
            func.sum(LoanSchedule.principal_due).label("principal_due"),
            func.sum(LoanSchedule.interest_due).label("interest_due"),
            func.sum(LoanSchedule.total_closing_balance).label("total_closing_balance"),
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
    for emi_number, cumulative_values in enumerate(q_results, 1):
        cumulative_values_dict = cumulative_values._asdict()
        emi_id = cumulative_values.id
        if emi_id:  # If emi id is present then we update the record.
            session.query(LoanSchedule).filter_by(id=emi_id).update(cumulative_values_dict)
        else:
            _ = LoanSchedule.new(
                session, loan_id=user_loan.loan_id, emi_number=emi_number, **cumulative_values_dict
            )


def create_bill_schedule(session: Session, user_loan: BaseLoan, bill: BaseBill):
    emi_objects = []
    due_date = bill.table.bill_start_date
    instalment = bill.get_instalment_amount()
    opening_principal = bill.table.principal
    downpayment = bill.get_down_payment()
    new_emi_number = 1
    interest_to_be_added = 0

    if LoanMoratorium.is_in_moratorium(
        session, loan_id=user_loan.loan_id, date_to_check_against=bill.table.bill_due_date
    ):
        data_after_moratorium = add_moratorium_bills(session, user_loan, bill)
        number_of_months_added = data_after_moratorium["number_of_months_added"]
        due_date = data_after_moratorium["due_date"]
        interest_to_be_added = data_after_moratorium["interest_to_be_added"]
        emi_objects.extend(data_after_moratorium["moratorium_emi_objects"])
        new_emi_number = number_of_months_added + 1
        bill.table.bill_tenure += number_of_months_added

    for emi_number in range(new_emi_number, bill.table.bill_tenure + 1):
        if user_loan.interest_type == "reducing":
            interest_due = bill.get_interest_to_charge(principal=opening_principal)
        else:
            interest_due = bill.get_interest_to_charge()
        principal_due = instalment - interest_due
        due_date_deltas = bill.get_relative_delta_for_emi(
            emi_number=emi_number, amortization_date=user_loan.amortization_date
        )
        due_date += relativedelta(**due_date_deltas)
        bill_schedule = LoanSchedule(
            loan_id=bill.table.loan_id,
            bill_id=bill.table.id,
            emi_number=emi_number,
            due_date=due_date,
            interest_due=round(interest_due + interest_to_be_added, 2),
            principal_due=round(principal_due, 2),
            total_closing_balance=round(opening_principal, 2),
        )
        interest_to_be_added = 0
        opening_principal -= principal_due
        if emi_number == 1 and downpayment:  # add downpayment in first emi
            bill_schedule.principal_due = downpayment - bill_schedule.interest_due
        emi_objects.append(bill_schedule)
    session.bulk_save_objects(emi_objects)
    group_bills(user_loan)
    readjust_future_payment(user_loan, bill.table.bill_close_date)


def slide_payment_to_emis(user_loan: BaseLoan, payment_event: LedgerTriggerEvent):
    """
    Settles a payment into loan's emi schedule.
    Also creates a payment split at emi level.
    """
    from rush.payments import get_payment_split_from_event

    payment_split = get_payment_split_from_event(user_loan.session, payment_event)

    # Payment can get adjusted in late fee, gst etc. For emi, we only need to settle the principal
    # and interest amount.
    amount_to_slide = payment_split.get("principal", 0) + payment_split.get("interest", 0)
    unpaid_emis = user_loan.get_loan_schedule(only_unpaid_emis=True)
    for emi in unpaid_emis:
        if amount_to_slide <= 0:
            break
        amount_slid = min(emi.remaining_amount, amount_to_slide)
        emi.payment_received += amount_slid
        if emi.can_mark_emi_paid():
            emi.payment_status = "Paid"
        emi.last_payment_date = payment_event.post_date
        emi.dpd = (emi.last_payment_date.date() - emi.due_date).days
        _ = PaymentMapping.new(
            user_loan.session,
            payment_request_id=payment_event.extra_details["payment_request_id"],
            emi_id=emi.id,
            amount_settled=amount_slid,
        )
        amount_to_slide -= amount_slid

    # After doing the sliding we check if the loan can be closed.
    if user_loan.get_remaining_max(payment_event.post_date) == 0:
        close_loan(user_loan, payment_event.post_date)


def close_loan(user_loan: BaseLoan, last_payment_date: datetime):
    """
    Payment has come which has closed the loan.
    In case of early payment, we need to nullify all future emis in schedule and set a new
    principal balance on the current emi.
    """
    future_emis = user_loan.get_loan_schedule(only_emis_after_date=last_payment_date.date())
    if not future_emis:  # Loan has closed naturally.
        return

    next_emi_due_date = future_emis[0].due_date
    for emi in future_emis:
        # set the received amount of first emi to closing balance as of that date.
        if emi.due_date == next_emi_due_date:
            emi.payment_received = emi.total_closing_balance
            emi.payment_status = "Paid"
            emi.last_payment_date = last_payment_date
        else:
            emi.payment_received = 0  # set principal to 0 of remaining future emis.
            emi.payment_status = "UnPaid"
            emi.last_payment_date = None

    # Do what we did above but for bill emis and for due amount.
    all_future_bill_emis = (
        user_loan.session.query(LoanSchedule)
        .filter(
            LoanSchedule.bill_id.isnot(None),
            LoanSchedule.loan_id == user_loan.loan_id,
            LoanSchedule.due_date >= last_payment_date.date(),
        )
        .all()
    )
    loan_moratorium = (
        user_loan.session.query(LoanMoratorium)
        .filter(
            LoanMoratorium.loan_id == user_loan.loan_id,
        )
        .order_by(LoanMoratorium.start_date.desc())
        .first()
    )
    moratorium_interest = (
        user_loan.session.query(func.sum(MoratoriumInterest.interest).label("total_moratorium_interest"))
        .join(
            LoanMoratorium,
            MoratoriumInterest.moratorium_id == LoanMoratorium.id,
        )
        .filter(
            LoanMoratorium.loan_id == user_loan.loan_id,
            MoratoriumInterest.bill_id.is_(None),
        )
        .group_by(MoratoriumInterest.moratorium_id)
        .first()
    )
    for bill_emi in all_future_bill_emis:
        if bill_emi.due_date == next_emi_due_date:
            bill_emi.principal_due = bill_emi.total_closing_balance
        else:
            bill_emi.principal_due = 0
        if loan_moratorium and bill_emi.due_date == next_emi_due_date:
            bill_emi.interest_due = moratorium_interest.total_moratorium_interest
            loan_moratorium = None
        else:
            bill_emi.interest_due = 0

    # Refresh the due amounts of loan schedule after altering bill's schedule.
    group_bills(user_loan)


def readjust_future_payment(user_loan: BaseLoan, date_to_check_after: date):
    """
    If user has paid more than the current emi then it gets settled in future emis.
    Once a new bill is generated the schedule gets changed. We need to readjust that
    future payment according to this new schedule now.

    date_to_check_after: Bill generation date. Any emi which has money settled after this
    date needs to be readjusted.
    """
    session = user_loan.session
    future_emis = user_loan.get_loan_schedule(only_emis_after_date=date_to_check_after)
    future_adjusted_emis = [emi for emi in future_emis if emi.payment_received > 0]

    if len(future_adjusted_emis) == 0:
        return

    emi_ids = []
    for emi in future_adjusted_emis:
        emi_ids.append(emi.id)
        emi.make_emi_unpaid()  # reset this emi. will be updating with new data below.

    # Get the payments that were adjusted in these emis.
    payment_mapping_data = (
        session.query(PaymentMapping)
        .filter(PaymentMapping.emi_id.in_(emi_ids), PaymentMapping.row_status == "active")
        .all()
    )

    new_mappings = defaultdict(dict)

    for payment_mapping in payment_mapping_data:
        payment_mapping.row_status = "inactive"
        amount_to_readjust = payment_mapping.amount_settled
        payment_request_id = payment_mapping.payment_request_id
        for emi in future_emis:
            if emi.remaining_amount == 0:  # skip is already paid from previous mapping
                continue
            if amount_to_readjust <= 0:
                break  # this mapping's amount is done. Move to next one.
            amount_slid = min(emi.remaining_amount, amount_to_readjust)
            emi.payment_received += amount_slid
            if emi.can_mark_emi_paid():
                emi.payment_status = "Paid"
            # TODO get payment request data for payment date.
            # emi.last_payment_date = payment_event.post_date
            if new_mappings[payment_request_id].get(emi.id):
                new_mappings[payment_request_id][emi.id] += amount_slid
            else:
                new_mappings[payment_request_id][emi.id] = amount_slid
            amount_to_readjust -= amount_slid
        assert amount_to_readjust == 0

    for payment_request_id, emi_ids in new_mappings.items():
        for emi_id, amount_slid in emi_ids.items():
            _ = PaymentMapping.new(
                session,
                payment_request_id=payment_request_id,
                emi_id=emi_id,
                amount_settled=amount_slid,
            )
