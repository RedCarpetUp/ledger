from decimal import Decimal

from dateutil.relativedelta import relativedelta
from pendulum import DateTime
from sqlalchemy.orm import Session

from rush.accrue_financial_charges import create_bill_fee_entry
from rush.card.base_card import (
    BaseBill,
    BaseLoan,
)
from rush.create_emi import (
    adjust_atm_fee_in_emis,
    create_emis_for_bill,
    group_bills_to_create_loan_schedule,
    update_event_with_dpd,
)
from rush.ledger_events import (
    add_max_amount_event,
    bill_generate_event,
)
from rush.ledger_utils import get_account_balance_from_str
from rush.loan_schedule.loan_schedule import create_bill_schedule
from rush.min_payment import add_min_to_all_bills
from rush.models import (
    CardEmis,
    LedgerTriggerEvent,
)
from rush.utils import (
    div,
    get_current_ist_time,
    mul,
)


def get_or_create_bill_for_card_swipe(user_loan: BaseLoan, txn_time: DateTime) -> BaseBill:
    # Get the most recent bill
    last_bill = user_loan.get_latest_bill()
    txn_date = txn_time.date()
    lender_id = user_loan.lender_id
    if last_bill:
        does_swipe_belong_to_current_bill = txn_date < last_bill.bill_close_date
        if does_swipe_belong_to_current_bill:
            return {"result": "success", "bill": last_bill}
        new_bill_date = last_bill.bill_close_date
    else:
        new_bill_date = user_loan.amortization_date
    new_closing_date = new_bill_date + relativedelta(months=1)
    # Check if some months of bill generation were skipped and if they were then generate their bills
    months_diff = (txn_date.year - new_closing_date.year) * 12 + txn_date.month - new_closing_date.month
    if months_diff > 0:
        for i in range(months_diff + 1):
            new_bill = user_loan.create_bill(
                bill_start_date=new_bill_date + relativedelta(months=i, day=1),
                bill_close_date=new_bill_date + relativedelta(months=i + 1, day=1),
                bill_due_date=new_bill_date + relativedelta(months=i + 1, day=15),
                lender_id=lender_id,
                is_generated=False,
            )
            bill_generate(user_loan)
        last_bill = user_loan.get_latest_bill()
        new_bill_date = last_bill.bill_close_date
    new_bill = user_loan.create_bill(
        bill_start_date=new_bill_date,
        bill_close_date=new_bill_date + relativedelta(months=1, day=1),
        bill_due_date=new_bill_date + relativedelta(months=1, day=15),
        lender_id=lender_id,
        is_generated=False,
    )
    return {"result": "success", "bill": new_bill}


def bill_generate(
    user_loan: BaseLoan,
    creation_time: DateTime = get_current_ist_time(),
    skip_bill_schedule_creation: bool = False,
) -> BaseBill:
    session = user_loan.session
    bill = user_loan.get_latest_bill_to_generate()  # Get the first bill which is not generated.
    if not bill:
        bill = get_or_create_bill_for_card_swipe(
            user_loan=user_loan, txn_time=creation_time
        )  # TODO not sure about this
        if bill["result"] == "error":
            return bill
        bill = bill["bill"]
    lt = LedgerTriggerEvent(
        name="bill_generate",
        loan_id=user_loan.loan_id,
        post_date=bill.bill_close_date,
        extra_details={"bill_id": bill.id},
    )
    session.add(lt)
    session.flush()

    bill_generate_event(session=session, bill=bill, user_loan=user_loan, event=lt)

    bill.table.is_generated = True

    _, billed_amount = get_account_balance_from_str(
        session=session, book_string=f"{bill.id}/bill/principal_receivable/a"
    )
    lt.amount = billed_amount  # Set the amount for event
    principal_instalment = div(billed_amount, bill.table.bill_tenure)

    # Update the bill row here.
    bill.table.principal = billed_amount
    bill.table.principal_instalment = principal_instalment
    bill.table.interest_to_charge = bill.get_interest_to_charge(
        rate_of_interest=user_loan.rc_rate_of_interest_monthly
    )

    # Add to max amount to pay account.
    add_max_amount_event(session, bill, lt, billed_amount)

    # After the bill has generated. Call the min generation event on all unpaid bills.
    add_min_to_all_bills(session=session, post_date=bill.table.bill_close_date, user_loan=user_loan)

    if not skip_bill_schedule_creation:
        create_emis_for_bill(session=session, user_loan=user_loan, bill=bill)
        create_bill_schedule(session, user_loan, bill)

    atm_transactions_sum = bill.sum_of_atm_transactions()
    if atm_transactions_sum > 0:
        add_atm_fee(
            session=session,
            bill=bill,
            post_date=bill.table.bill_close_date,
            atm_transactions_amount=atm_transactions_sum,
            user_loan=user_loan,
            skip_schedule_grouping=skip_bill_schedule_creation,
        )

    return bill


def extend_tenure(
    session: Session, user_loan: BaseLoan, new_tenure: int, post_date: DateTime, bill: BaseBill = None
) -> None:
    def extension(bill: BaseBill):
        list_of_bills.append(bill.id)
        bill.table.bill_tenure = new_tenure
        principal_instalment = div(bill.table.principal, bill.table.bill_tenure)
        # Update the bill rows here
        bill.table.principal_instalment = principal_instalment
        bill.table.interest_to_charge = bill.get_interest_to_charge(
            rate_of_interest=user_loan.rc_rate_of_interest_monthly
        )

        # Get all emis of the bill
        all_emis = (
            session.query(CardEmis)
            .filter(
                CardEmis.loan_id == user_loan.id,
                CardEmis.row_status == "active",
                CardEmis.bill_id == bill.id,
            )
            .order_by(CardEmis.emi_number.asc())
            .all()
        )
        for emi in all_emis:
            if emi.due_date >= post_date.date():
                emi.row_status = "inactive"

        # Get emis pre post date. This is done to get per bill amount till date as well
        bill_accumalation_till_date = Decimal(0)
        pre_post_date_emis = [emi for emi in all_emis if emi.due_date < post_date.date()]
        for emi in pre_post_date_emis:
            if not emi.extra_details.get("moratorium"):
                bill_accumalation_till_date += emi.due_amount
        last_active_emi = pre_post_date_emis[-1]

        create_emis_for_bill(
            session=session,
            user_loan=user_loan,
            bill=bill,
            last_emi=last_active_emi,
            bill_accumalation_till_date=bill_accumalation_till_date,
        )

    list_of_bills = []
    if not bill:
        unpaid_bills = user_loan.get_unpaid_generated_bills()
        for unpaid_bill in unpaid_bills:
            extension(bill=unpaid_bill)
    else:
        extension(bill=bill)

    event = LedgerTriggerEvent(
        name="tenure_extended",
        loan_id=user_loan.id,
        post_date=post_date,
        extra_details={"bills": list_of_bills},
    )
    session.add(event)
    session.flush()

    # Recreate loan level emis
    group_bills_to_create_loan_schedule(user_loan=user_loan)


def add_atm_fee(
    session: Session,
    bill: BaseBill,
    post_date: DateTime,
    atm_transactions_amount: Decimal,
    user_loan: BaseLoan,
    skip_schedule_grouping: bool = False,
) -> None:
    atm_fee_perc = Decimal(2)
    atm_fee_without_gst = mul(atm_transactions_amount / 100, atm_fee_perc)

    event = LedgerTriggerEvent(name="atm_fee_added", loan_id=bill.table.loan_id, post_date=post_date)
    session.add(event)
    session.flush()

    fee = create_bill_fee_entry(
        session=session,
        user_id=user_loan.user_id,
        bill=bill,
        event=event,
        fee_name="atm_fee",
        gross_fee_amount=atm_fee_without_gst,
    )
    event.amount = fee.gross_amount

    if not skip_schedule_grouping:
        adjust_atm_fee_in_emis(session, user_loan, bill)

    update_event_with_dpd(user_loan=user_loan, event=event)


def close_bills(user_loan: BaseLoan, payment_date: DateTime):
    session = user_loan.session
    all_bills = user_loan.get_closed_bills()

    for bill in all_bills:
        all_paid = False
        bill_emis = (
            session.query(CardEmis)
            .filter(
                CardEmis.loan_id == user_loan.loan_id,
                CardEmis.row_status == "active",
                CardEmis.bill_id == bill.id,
            )
            .order_by(CardEmis.emi_number.asc())
            .all()
        )
        last_emi_number = bill_emis[-1].emi_number
        for emi in bill_emis:
            if all_paid:
                emi.payment_received = (
                    emi.atm_fee_received
                ) = (
                    emi.late_fee_received
                ) = (
                    emi.interest_received
                ) = (
                    emi.due_amount
                ) = (
                    emi.total_due_amount
                ) = (
                    emi.total_closing_balance
                ) = (
                    emi.total_closing_balance_post_due_date
                ) = emi.interest_current_month = emi.interest_next_month = emi.interest = Decimal(0)
                continue
            actual_closing_balance = emi.total_closing_balance_post_due_date
            if payment_date.date() <= emi.due_date:
                actual_closing_balance = emi.total_closing_balance
            if (
                emi.due_date >= payment_date.date() > (emi.due_date + relativedelta(months=-1))
            ) or emi.emi_number == last_emi_number:
                all_paid = True
                emi.total_closing_balance = (
                    emi.total_closing_balance_post_due_date
                ) = emi.interest = emi.interest_current_month = emi.interest_next_month = 0
                if payment_date.date() > emi.due_date:
                    only_principal = actual_closing_balance - (emi.interest + emi.atm_fee + emi.late_fee)
                else:
                    only_principal = actual_closing_balance - emi.atm_fee
                emi.total_due_amount = actual_closing_balance
                emi.due_amount = only_principal

    # Recreate loan level emis
    group_bills_to_create_loan_schedule(user_loan=user_loan)
