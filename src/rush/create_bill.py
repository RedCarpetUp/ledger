from datetime import timedelta
from decimal import Decimal

from pendulum import (
    Date,
    DateTime,
)
from sqlalchemy.orm import Session

from rush.accrue_financial_charges import accrue_interest_on_all_bills
from rush.ledger_events import bill_generate_event
from rush.create_emi import create_emis_for_card, add_emi_on_new_bill
from rush.ledger_utils import get_account_balance_from_str
from rush.min_payment import add_min_to_all_bills
from rush.models import (
    LedgerTriggerEvent,
    LoanData,
    UserCard,
    CardEmis,
)
from rush.utils import div


def create_bill(
    session: Session,
    user_card: UserCard,
    new_bill_date: Date,
    lender_id: int,
    rc_rate_of_interest_annual: Decimal,
    lender_rate_of_interest_annual: Decimal,
    is_generated: bool,
) -> LoanData:
    new_bill = LoanData(
        user_id=user_card.user_id,
        card_id=user_card.id,
        lender_id=lender_id,
        agreement_date=new_bill_date,
        rc_rate_of_interest_annual=rc_rate_of_interest_annual,
        lender_rate_of_interest_annual=lender_rate_of_interest_annual,
        is_generated=is_generated,
    )
    session.add(new_bill)
    session.flush()
    return new_bill


def get_or_create_bill_for_card_swipe(
    session: Session, user_card: UserCard, txn_time: DateTime
) -> LoanData:
    # Get the most recent bill
    last_bill = (
        session.query(LoanData)
        .filter(LoanData.card_id == user_card.id)
        .order_by(LoanData.agreement_date.desc())
        .first()
    )
    if last_bill:
        last_bill_date = last_bill.agreement_date
        last_valid_statement_date = last_bill_date + timedelta(days=user_card.statement_period_in_days)
        does_swipe_belong_to_current_bill = txn_time.date() <= last_valid_statement_date
        if does_swipe_belong_to_current_bill:
            return last_bill
        new_bill_date = last_valid_statement_date + timedelta(days=1)
    else:
        new_bill_date = user_card.card_activation_date.date()
    new_bill = create_bill(
        session, user_card, new_bill_date, 62311, Decimal(36), Decimal(18), is_generated=False
    )
    return new_bill


def bill_generate(session: Session, user_card: UserCard) -> LoanData:
    bill = (
        session.query(LoanData)
        .filter(LoanData.user_id == user_card.user_id, LoanData.is_generated.is_(False))
        .order_by(LoanData.agreement_date)
        .first()
    )  # Get the first bill which is not generated.
    lt = LedgerTriggerEvent(name="bill_generate", card_id=user_card.id, post_date=bill.agreement_date)
    session.add(lt)
    session.flush()

    bill_generate_event(session, bill, user_card.id, lt)

    # TODO accrue interest too?
    bill.is_generated = True

    _, billed_amount = get_account_balance_from_str(
        session, book_string=f"{bill.id}/bill/principal_receivable/a"
    )
    bill.principal = billed_amount
    # TODO get tenure from table.
    principal_instalment = div(billed_amount, 12)
    bill.principal_instalment = principal_instalment

    # After the bill has generated. Call the min generation event on all unpaid bills.
    add_min_to_all_bills(session, bill.agreement_date, user_card)

    # If last emi does not exist then we can consider to be first set of emi creation
    last_emi = (
        session.query(CardEmis)
        .filter(CardEmis.card_id == user_card.id)
        .order_by(CardEmis.due_date.desc())
        .first()
    )
    if not last_emi:
        create_emis_for_card(session, user_card, bill)
    else:
        add_emi_on_new_bill(session, user_card, bill, last_emi.emi_number)

    # Accrue interest on all bills. Before the actual date, yes.
    accrue_interest_on_all_bills(session, bill.agreement_date, user_card)
    return bill
