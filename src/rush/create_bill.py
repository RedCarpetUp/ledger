from datetime import timedelta

from pendulum import (
    Date,
    DateTime,
)
from sqlalchemy.orm import Session

from rush.ledger_events import bill_generate_event
from rush.models import (
    LedgerTriggerEvent,
    LoanData,
    UserCard,
)


def create_bill(session: Session, user_card: UserCard, new_bill_date: Date) -> LoanData:
    new_bill = LoanData(user_id=user_card.user_id, card_id=user_card.id, agreement_date=new_bill_date)
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
    new_bill = create_bill(session, user_card, new_bill_date)
    return new_bill


def bill_generate(session: Session, generate_date: Date, user_id: int) -> LoanData:
    bill = (
        session.query(LoanData)
        .filter(LoanData.user_id == user_id)
        .order_by(LoanData.agreement_date.desc())
        .first()
    )  # Get the latest bill of that user.

    previous_bill = (  # Get 2nd last bill.
        session.query(LoanData)
        .filter(LoanData.user_id == user_id)
        .order_by(LoanData.agreement_date.desc())
        .offset(1)
        .first()
    )

    lt = LedgerTriggerEvent(name="bill_generate", post_date=generate_date)
    session.add(lt)
    session.flush()

    bill_generate_event(session, previous_bill, bill, lt)
    return bill
