from typing import Any

from sqlalchemy import and_
from sqlalchemy.orm import Session

# for now, these imports are required by get_product_class method to fetch all class within card module.
from rush.card.base_card import BaseLoan
from rush.card.health_card import HealthCard
from rush.card.ruby_card import RubyCard
from rush.card.term_loan import TermLoan
from rush.models import (
    Loan,
    Product,
)


def get_user_product(session: Session, user_id: int, card_type: str = "ruby") -> Loan:
    user_card = (
        session.query(Loan)
        .join(Product, and_(Product.product_name == card_type, Product.id == Loan.product_id))
        .filter(Loan.user_id == user_id, Loan.product_type == card_type)
        .one()
    )

    user_card.prepare(session=session)
    return user_card


def create_user_product(session: Session, **kwargs) -> Loan:
    loan = get_product_class(card_type=kwargs["card_type"]).create(session=session, **kwargs)
    return loan


def get_product_class(card_type: str) -> Any:
    """
        Only classes imported within this file will be listed here.
        Make sure to import every Product class.
    """

    for kls in BaseLoan.__subclasses__():
        if hasattr(kls, "__mapper_args__") and kls.__mapper_args__["polymorphic_identity"] == card_type:
            return kls
    else:
        raise Exception("NoValidProductImplementation")
