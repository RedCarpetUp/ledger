from decimal import Decimal

from sqlalchemy.orm import Session

from rush.card.base_card import BaseCard
from rush.card.ruby_card import (
    RubyBill,
    RubyCard,
)
from rush.models import UserCard


def get_user_card(session: Session, user_id: int, card_type: str = "ruby") -> BaseCard:
    user_card = (
        session.query(UserCard)
        .filter(UserCard.user_id == user_id, UserCard.card_type == card_type)
        .one()
    )

    if user_card.card_type == "ruby":
        return RubyCard(session, RubyBill, user_card)


def create_user_card(session: Session, **kwargs) -> BaseCard:
    uc = UserCard(
        rc_rate_of_interest_monthly=Decimal(3), lender_rate_of_interest_annual=Decimal(18), **kwargs
    )
    session.add(uc)
    session.flush()

    if uc.card_type == "ruby":
        return RubyCard(session, RubyBill, uc)
    return uc
