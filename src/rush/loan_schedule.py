from collections import defaultdict
from datetime import date

from dateutil.relativedelta import relativedelta
from sqlalchemy import (
    and_,
    func,
)
from sqlalchemy.orm import Session

from rush.card.base_card import (
    BaseBill,
    BaseLoan,
)
from rush.models import (
    LedgerTriggerEvent,
    LoanSchedule,
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
    non_rounded_bill_instalment = bill.table.principal / bill.table.bill_tenure
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
        bill_schedule.total_closing_balance = round(non_rounded_bill_instalment * remaining_tenure, 2)
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
    # TODO reduce the amount for fee payments.
    for emi in unpaid_emis:
        if amount_to_slide <= 0:
            break
        amount_slid = min(emi.remaining_amount, amount_to_slide)
        emi.payment_received += amount_slid
        if emi.remaining_amount == 0:
            emi.payment_status = "Paid"
        emi.last_payment_date = payment_event.post_date
        _ = PaymentMapping.new(
            user_loan.session,
            payment_request_id=payment_event.extra_details["payment_request_id"],
            emi_id=emi.id,
            amount_settled=amount_slid,
        )
        amount_to_slide -= amount_slid


def readjust_future_payment(user_loan: BaseLoan, date_to_check_after: date):
    """
    If user has paid more than the current emi then it gets settled in future emis.
    Once a new bill is generated the schedule gets changed. We need to readjust that
    future payment according to this new schedule now.

    date_to_check_after: Bill generation date. Any emi which has money settled after this
    date needs to be readjusted.
    """
    session = user_loan.session
    future_adjusted_emis = (
        session.query(LoanSchedule)
        .filter(
            LoanSchedule.loan_id == user_loan.loan_id,
            LoanSchedule.bill_id.is_(None),
            LoanSchedule.due_date >= date_to_check_after,
            LoanSchedule.payment_received > 0,  # Only if there's amount adjust in future emis.
        )
        .all()
    )

    # There should be at least 2 emis for the readjustment. If there's only one then
    # it doesn't matter because the amount will still be staying at that emi.
    if len(future_adjusted_emis) < 2:
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
        for emi in future_adjusted_emis:
            if emi.payment_status == "Paid":  # skip is already paid from previous mapping
                continue
            if amount_to_readjust <= 0:
                break  # this mapping's amount is done. Move to next one.
            amount_slid = min(emi.remaining_amount, amount_to_readjust)
            emi.payment_received += amount_slid
            if emi.remaining_amount == 0:
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
