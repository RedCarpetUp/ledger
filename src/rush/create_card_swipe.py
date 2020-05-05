from decimal import Decimal

from pendulum import DateTime
from sqlalchemy.orm.session import Session

from rush.create_bill import get_or_create_bill_for_card_swipe
from rush.models import (
    CardTransaction,
    UserCard,
)


def create_card_swipe(
    session: Session, user_card: UserCard, txn_time: DateTime, amount: Decimal, description: str
) -> CardTransaction:
    card_bill = get_or_create_bill_for_card_swipe(session, user_card, txn_time)
    swipe = CardTransaction(
        loan_id=card_bill.id, txn_time=txn_time, amount=amount, description=description
    )
    session.add(swipe)
    session.flush()
    return swipe
