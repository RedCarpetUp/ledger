from collections import defaultdict
from datetime import date
from decimal import Decimal
from typing import List

from dateutil.relativedelta import relativedelta
from pendulum import datetime
from sqlalchemy import (
    and_,
    func,
)
from sqlalchemy.orm import Session

from rush.card import get_user_loan
from rush.card.base_card import (
    BaseBill,
    BaseLoan,
    Loan,
)
from rush.loan_schedule.moratorium import add_moratorium_emis
from rush.models import (
    LedgerTriggerEvent,
    LoanMoratorium,
    LoanSchedule,
    MoratoriumInterest,
    PaymentMapping,
    PaymentRequestsData,
    PaymentSplit,
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
            _ = LoanSchedule.ledger_new(
                session, loan_id=user_loan.loan_id, emi_number=emi_number, **cumulative_values_dict
            )


def create_bill_schedule(session: Session, user_loan: BaseLoan, bill: BaseBill):
    emi_objects = []
    due_date = bill.table.bill_start_date
    instalment = bill.get_instalment_amount()
    opening_principal = bill.table.principal
    downpayment = bill.get_down_payment()
    new_emi_number = 1
    moratorium_interest_to_be_added = 0

    if LoanMoratorium.is_in_moratorium(
        session,
        loan_id=user_loan.loan_id,
        date_to_check_against=bill.table.bill_due_date,
    ):
        data_after_moratorium = add_moratorium_emis(session, user_loan, bill)
        number_of_months_added = data_after_moratorium["number_of_months_added"]
        due_date = data_after_moratorium["due_date"]
        moratorium_interest_to_be_added = data_after_moratorium["moratorium_interest_to_be_added"]
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
            interest_due=round(interest_due + moratorium_interest_to_be_added, 2),
            principal_due=round(principal_due, 2),
            total_closing_balance=round(opening_principal, 2),
        )
        moratorium_interest_to_be_added = 0
        opening_principal -= principal_due
        if emi_number == 1 and downpayment:  # add downpayment in first emi
            bill_schedule.principal_due = downpayment - bill_schedule.interest_due
        emi_objects.append(bill_schedule)
    session.bulk_save_objects(emi_objects)
    group_bills(user_loan)
    readjust_future_payment(user_loan, bill.table.bill_close_date)


def slide_payment_to_emis(
    user_loan: BaseLoan, payment_event: LedgerTriggerEvent, amount_to_slide: Decimal
):
    """
    Settles a payment into loan's emi schedule.
    Also creates a payment split at emi level.
    """
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

        mapping: PaymentMapping = (
            user_loan.session.query(PaymentMapping)
            .filter(
                PaymentMapping.emi_id == emi.id,
                PaymentMapping.payment_request_id == payment_event.extra_details["payment_request_id"],
                PaymentMapping.row_status == "active",
            )
            .scalar()
        )

        if mapping:
            mapping.amount_settled += amount_slid
        else:
            _ = PaymentMapping.ledger_new(
                user_loan.session,
                payment_request_id=payment_event.extra_details["payment_request_id"],
                emi_id=emi.id,
                amount_settled=amount_slid,
            )

        amount_to_slide -= amount_slid


def close_loan(user_loan: BaseLoan, last_payment_date: datetime):
    """
    Payment has come which has closed the loan.
    In case of early payment, we need to nullify all future emis in schedule and set a new
    principal balance on the current emi.
    """
    user_loan.loan_status = "COMPLETED"
    future_emis = user_loan.get_loan_schedule(only_emis_after_date=last_payment_date.date())
    if not future_emis:  # Loan has closed naturally.
        return

    emi_ids = [emi.id for emi in future_emis]

    # Aggregate old payment mappings to generate new ones
    new_mappings = (
        user_loan.session.query(
            PaymentMapping.payment_request_id, func.sum(PaymentMapping.amount_settled)
        )
        .filter(PaymentMapping.emi_id.in_(emi_ids), PaymentMapping.row_status == "active")
        .group_by(PaymentMapping.payment_request_id)
        .all()
    )

    # Mark old payment mappings inactive
    (
        user_loan.session.query(PaymentMapping)
        .filter(PaymentMapping.emi_id.in_(emi_ids), PaymentMapping.row_status == "active")
        .update({PaymentMapping.row_status: "inactive"}, synchronize_session=False)
    )

    closing_emi_id = emi_ids[0]

    # Create new payment mappings
    for payment_request_id, amount_settled in new_mappings:
        _ = PaymentMapping.ledger_new(
            user_loan.session,
            payment_request_id=payment_request_id,
            emi_id=closing_emi_id,
            amount_settled=amount_settled,
        )

    loan_moratorium = (
        user_loan.session.query(LoanMoratorium)
        .filter(
            LoanMoratorium.loan_id == user_loan.loan_id,
        )
        .order_by(LoanMoratorium.start_date.desc())
        .first()
    )
    got_closed_in_moratorium = (
        loan_moratorium
        and loan_moratorium.start_date <= last_payment_date.date() <= loan_moratorium.end_date
    )

    if got_closed_in_moratorium:
        moratorium_interest_future_emis = (
            user_loan.session.query(MoratoriumInterest)
            .filter(
                MoratoriumInterest.moratorium_id == loan_moratorium.id,
                LoanSchedule.id == MoratoriumInterest.loan_schedule_id,
                LoanSchedule.due_date >= last_payment_date.date(),
            )
            .all()
        )
        for moratorium_interest_emi in moratorium_interest_future_emis:
            moratorium_interest_emi.interest = 0

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

    for bill_emi in all_future_bill_emis:
        if bill_emi.due_date == next_emi_due_date:
            bill_emi.principal_due = bill_emi.total_closing_balance

            if loan_moratorium and (
                got_closed_in_moratorium
                or bill_emi.due_date == loan_moratorium.due_date_after_moratorium
            ):
                total_bill_moratorium_interest = MoratoriumInterest.get_bill_total_moratorium_interest(
                    session=user_loan.session,
                    loan_id=user_loan.loan_id,
                    bill_id=bill_emi.bill_id,
                )
                if not total_bill_moratorium_interest:
                    bill_emi.interest_due = 0  # means bill is new and wasn't part of moratorium.
                else:
                    bill_emi.interest_due = total_bill_moratorium_interest
            else:
                bill_emi.interest_due = 0
        else:
            bill_emi.principal_due = 0
            bill_emi.interest_due = 0
            bill_emi.total_closing_balance = 0

    # Refresh the due amounts of loan schedule after altering bill's schedule.
    group_bills(user_loan)
    # When loan is closed, dpd gets reset to -999
    user_loan.dpd = -999


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
            _ = PaymentMapping.ledger_new(
                session,
                payment_request_id=payment_request_id,
                emi_id=emi_id,
                amount_settled=amount_slid,
            )


def reset_loan_schedule(user_loan: Loan, session: Session) -> None:
    def reset_bill_emis(user_loan: Loan, session: Session) -> None:
        bills = user_loan.get_all_bills()
        for bill in bills:
            bill_emis = (
                session.query(LoanSchedule)
                .filter(LoanSchedule.loan_id == user_loan.loan_id, LoanSchedule.bill_id == bill.id)
                .order_by(LoanSchedule.emi_number)
                .all()
            )
            instalment = bill.get_instalment_amount()
            opening_principal = bill.table.principal
            interest_due = round(bill.get_interest_to_charge(), 2)
            principal_due = round(instalment - interest_due, 2)
            for bill_emi in bill_emis:
                bill_emi.interest_due = interest_due
                bill_emi.principal_due = principal_due
                bill_emi.total_closing_balance = round(opening_principal, 2)
                opening_principal -= principal_due

    def reset_payment_info(user_loan: Loan, session: Session) -> None:
        _ = (
            session.query(LoanSchedule)
            .filter(LoanSchedule.loan_id == user_loan.id)
            .update(
                {
                    LoanSchedule.last_payment_date.name: None,
                    LoanSchedule.payment_received.name: Decimal("0"),
                    LoanSchedule.dpd.name: -999,
                    LoanSchedule.payment_status.name: "UnPaid",
                }
            )
        )

    def make_emi_payment_mappings_inactive(user_loan: Loan, session: Session) -> None:
        emis = user_loan.get_loan_schedule()
        emi_ids = [emi.id for emi in emis]
        (
            session.query(PaymentMapping)
            .filter(PaymentMapping.emi_id.in_(emi_ids), PaymentMapping.row_status == "active")
            .update({PaymentMapping.row_status: "inactive"}, synchronize_session=False)
        )

    # returning if the loan is extended, because we don't have context of past tenure.
    is_loan_extended = (
        session.query(LedgerTriggerEvent.loan_id)
        .filter(
            LedgerTriggerEvent.loan_id == user_loan.loan_id, LedgerTriggerEvent.name == "bill_extended"
        )
        .first()
    )
    if is_loan_extended:
        return

    reset_bill_emis(user_loan=user_loan, session=session)
    reset_payment_info(user_loan=user_loan, session=session)
    group_bills(user_loan)
    make_emi_payment_mappings_inactive(user_loan, session=session)

    amount_to_slide_per_event = (
        session.query(LedgerTriggerEvent.id, func.sum(PaymentSplit.amount_settled))
        .filter(
            PaymentSplit.payment_request_id
            == LedgerTriggerEvent.extra_details["payment_request_id"].astext,
            PaymentSplit.component.in_(("principal", "interest", "unbilled", "early_close_fee")),
            LedgerTriggerEvent.name == "payment_received",
            LedgerTriggerEvent.loan_id == user_loan.loan_id,
        )
        .group_by(LedgerTriggerEvent.id)
        .all()
    )

    event_ids = [id for id, _ in amount_to_slide_per_event]
    payment_events = session.query(LedgerTriggerEvent).filter(LedgerTriggerEvent.id.in_(event_ids)).all()
    event_id_to_object_mapping = {event.id: event for event in payment_events}

    for event_id, amount_to_slide in amount_to_slide_per_event:
        payment_event = event_id_to_object_mapping[event_id]
        slide_payment_to_emis(user_loan, payment_event, amount_to_slide)
        if user_loan.can_close_loan(as_of_event_id=payment_event.id):
            close_loan(user_loan, payment_event.post_date)
            break
