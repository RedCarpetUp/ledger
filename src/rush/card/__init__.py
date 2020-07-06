from sqlalchemy.orm import Session

from rush.card.base_card import BaseCard
from rush.card.ruby_card import (
    RubyBill,
    RubyCard,
    FlipkartBill,
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
    elif user_card.card_type == "flipkart":
        return RubyCard(session, FlipkartBill, user_card)


def create_user_card(session: Session, **kwargs) -> BaseCard:
    uc = UserCard(**kwargs)
    session.add(uc)
    session.flush()

    if uc.card_type == "ruby":
        return RubyCard(session, RubyBill, uc)
    elif uc.card_type == "flipkart":
        return RubyCard(session, FlipkartBill, uc)
    return uc
