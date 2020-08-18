from decimal import Decimal

from pendulum import parse as parse_date  # type: ignore
from sqlalchemy import and_
from sqlalchemy.orm import Session

from rush.card.base_card import BaseCard
from rush.card.health_card import (
    HealthBill,
    HealthCard,
)
from rush.card.ruby_card import (
    RubyBill,
    RubyCard,
)
from rush.card.utils import get_product_id_from_card_type
from rush.models import (
    Loan,
    Product,
    UserCard,
)


def get_user_card(session: Session, user_id: int, card_type: str = "ruby") -> BaseCard:
    user_card, loan = (
        session.query(UserCard, Loan)
        .join(Product, and_(Product.product_name == card_type, Product.id == Loan.product_id))
        .filter(
            Loan.id == UserCard.loan_id,
            Loan.user_id == UserCard.user_id,
            UserCard.user_id == user_id,
            UserCard.card_type == card_type,
        )
        .one()
    )

    if user_card.card_type == "ruby":
        return RubyCard(session=session, bill_class=RubyBill, user_card=user_card, loan=loan)
    elif user_card.card_type == "health_card":
        return HealthCard(session=session, bill_class=HealthBill, user_card=user_card, loan=loan)


def create_user_card(session: Session, **kwargs) -> BaseCard:
    loan = Loan(
        user_id=kwargs["user_id"],
        product_id=get_product_id_from_card_type(session=session, card_type=kwargs["card_type"]),
        lender_id=kwargs.pop("lender_id"),
        rc_rate_of_interest_monthly=Decimal(3),
        lender_rate_of_interest_annual=Decimal(18),
    )
    session.add(loan)
    session.flush()

    kwargs["loan_id"] = loan.id

    uc = UserCard(**kwargs)
    session.add(uc)
    session.flush()

    if uc.card_type == "ruby":
        return RubyCard(session=session, bill_class=RubyBill, user_card=uc, loan=loan)
    elif uc.card_type == "health_card":
        return HealthCard(session=session, bill_class=HealthBill, user_card=uc, loan=loan)
    return uc
