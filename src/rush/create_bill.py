from decimal import Decimal

from dateutil.relativedelta import relativedelta
from pendulum import DateTime
from sqlalchemy.orm import Session

from rush.accrue_financial_charges import create_bill_fee_entry
from rush.card.base_card import (
    BaseBill,
    BaseLoan,
)
from rush.card.transaction_loan import TransactionLoan
from rush.create_emi import (
    update_event_with_dpd,
    update_journal_entry,
)
from rush.ledger_events import (
    accrue_interest_event,
    add_max_amount_event,
    bill_generate_event,
)
from rush.ledger_utils import get_account_balance_from_str
from rush.loan_schedule.loan_schedule import create_bill_schedule
from rush.min_payment import add_min_to_all_bills
from rush.models import Base, LedgerTriggerEvent, LoanSchedule
from rush.utils import (
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

    transaction_loan: TransactionLoan = (
        session.query(BaseLoan)
        .filter(BaseLoan.product_type == "transaction_loan", BaseLoan.user_id == user_loan.user_id)
        .scalar()
    )

    # calculating accrued interest for transaction loan
    if transaction_loan:
        amount = transaction_loan.get_txn_to_add_in_bill()

        # adding card transaction for accrued interest
        if amount:
            pass

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

    # Update the bill row here.
    bill.table.principal = billed_amount

    # Add to max amount to pay account.
    add_max_amount_event(session, bill, lt, billed_amount)

    # After the bill has generated. Call the min generation event on all unpaid bills.
    add_min_to_all_bills(session=session, post_date=bill.table.bill_close_date, user_loan=user_loan)

    if not skip_bill_schedule_creation:
        create_bill_schedule(session, user_loan, bill)

        atm_transactions_sum = bill.sum_of_atm_transactions()
        if atm_transactions_sum > 0:
            add_atm_fee(
                session=session,
                bill=bill,
                post_date=bill.table.bill_close_date,
                atm_transactions_amount=atm_transactions_sum,
                user_loan=user_loan,
            )

    # Update Journal Entry
    update_journal_entry(user_loan=user_loan, event=lt)

    return bill


def add_atm_fee(
    session: Session,
    bill: BaseBill,
    post_date: DateTime,
    atm_transactions_amount: Decimal,
    user_loan: BaseLoan,
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

    update_event_with_dpd(user_loan=user_loan, event=event)
    update_journal_entry(user_loan=user_loan, event=event)
