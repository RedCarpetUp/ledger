from datetime import timedelta
from decimal import Decimal

from pendulum import (
    Date,
    DateTime,
)
from sqlalchemy.orm import Session

from rush.accrue_financial_charges import accrue_interest_on_all_bills
from rush.card import BaseCard
from rush.card.base_card import BaseBill
from rush.create_emi import (
    add_emi_on_new_bill,
    create_emis_for_card,
)
from rush.ledger_events import bill_generate_event
from rush.ledger_utils import get_account_balance_from_str
from rush.min_payment import add_min_to_all_bills
from rush.models import (
    CardEmis,
    LedgerTriggerEvent,
    LoanData,
    UserCard,
)
from rush.utils import div


def get_or_create_bill_for_card_swipe(user_card: BaseCard, txn_time: DateTime) -> BaseBill:
    # Get the most recent bill
    last_bill = user_card.get_latest_bill_to_generate()
    if last_bill:
        last_bill_date = last_bill.agreement_date
        last_valid_statement_date = last_bill_date + timedelta(days=user_card.statement_period_in_days)
        does_swipe_belong_to_current_bill = txn_time.date() <= last_valid_statement_date
        if does_swipe_belong_to_current_bill:
            return last_bill
        new_bill_date = last_valid_statement_date + timedelta(days=1)
    else:
        print(user_card.card_activation_date)
        new_bill_date = user_card.card_activation_date.date()
    new_bill = user_card.create_bill(
        new_bill_date=new_bill_date,
        lender_id=62311,
        rc_rate_of_interest_annual=Decimal(36),
        lender_rate_of_interest_annual=Decimal(18),
        is_generated=False,
    )
    return new_bill


def bill_generate(session: Session, user_card: BaseCard) -> BaseBill:
    bill = user_card.get_latest_bill_to_generate()  # Get the first bill which is not generated.
    lt = LedgerTriggerEvent(name="bill_generate", card_id=user_card.id, post_date=bill.agreement_date)
    session.add(lt)
    session.flush()

    bill_generate_event(session, bill, user_card.id, lt)

    bill.table.is_generated = True

    _, billed_amount = get_account_balance_from_str(
        session, book_string=f"{bill.id}/bill/principal_receivable/a"
    )
    principal_instalment = div(billed_amount, 12)  # TODO get tenure from table.

    # Update the bill row here.
    bill.table.principal = billed_amount
    bill.table.principal_instalment = principal_instalment
    bill.table.interest_to_charge = bill.get_interest_to_charge()

    # After the bill has generated. Call the min generation event on all unpaid bills.
    add_min_to_all_bills(session, bill.agreement_date, user_card)

    # TODO move this to a function.
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
