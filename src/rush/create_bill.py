from datetime import timedelta
from decimal import Decimal

from pendulum import (
    Date,
    DateTime,
)
from sqlalchemy.orm import Session

from rush.ledger_events import bill_generate_event
from rush.create_emi import create_emis_for_card, add_emi_on_new_bill
from rush.models import (
    LedgerTriggerEvent,
    LoanData,
    User,
    UserCard,
    CardEmis,
)


def create_bill(
    session: Session,
    user_card: UserCard,
    new_bill_date: Date,
    rc_rate_of_interest_annual: Decimal,
    lender_rate_of_interest_annual: Decimal,
) -> LoanData:
    new_bill = LoanData(
        user_id=user_card.user_id,
        card_id=user_card.id,
        agreement_date=new_bill_date,
        rc_rate_of_interest_annual=rc_rate_of_interest_annual,
        lender_rate_of_interest_annual=lender_rate_of_interest_annual,
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
        last_bill_date = last_bill.agreement_date.date()
        last_valid_statement_date = last_bill_date + timedelta(days=user_card.statement_period_in_days)
        does_swipe_belong_to_current_bill = txn_time.date() <= last_valid_statement_date
        if does_swipe_belong_to_current_bill:
            return last_bill
        new_bill_date = last_valid_statement_date + timedelta(days=1)
    else:
        new_bill_date = user_card.card_activation_date
    new_bill = create_bill(session, user_card, new_bill_date, 36, 18)
    return new_bill


def bill_generate(session: Session, generate_date: Date, user_id: int) -> LoanData:
    bill = (
        session.query(LoanData)
        .filter(LoanData.user_id == user_id)
        .order_by(LoanData.agreement_date.desc())
        .first()
    )  # Get the latest bill of that user.
    lt = LedgerTriggerEvent(name="bill_generate", post_date=generate_date)
    session.add(lt)
    session.flush()

    bill_generate_event(session, bill, lt)

    # Get the card conected with the users account.
    # TODO Consider for multiple cards
    user_card = session.query(UserCard).filter(UserCard.user_id == user_id).first()
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

    return bill
