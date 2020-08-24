from decimal import Decimal

from sqlalchemy import and_
from sqlalchemy.orm import Session

from rush.card.base_card import (
    BaseBill,
    BaseLoan,
)
from rush.card.health_card import (
    HealthBill,
    HealthCard,
)
from rush.card.ruby_card import (
    RubyBill,
    RubyCard,
)
from rush.card.term_loan import TermLoan
from rush.card.utils import get_product_id_from_card_type
from rush.models import (
    Loan,
    Product,
    UserCard,
)
from rush.utils import get_current_ist_time

PRODUCT_TO_CLASS_MAPPING = {
    "ruby": (RubyCard, RubyBill),
    "health_card": (HealthCard, HealthBill),
    "base": (BaseLoan, BaseBill),
    "term_loan": (TermLoan, None),
}


def get_user_product(session: Session, user_id: int, card_type: str = "ruby") -> BaseLoan:
    user_card = (
        session.query(Loan)
        .join(Product, and_(Product.product_name == card_type, Product.id == Loan.product_id))
        .filter(Loan.user_id == user_id, Loan.product_type == card_type)
        .one()
    )

    user_card.prepare(session=session)
    return user_card


def create_user_card(session: Session, **kwargs) -> BaseLoan:
    klass, _ = PRODUCT_TO_CLASS_MAPPING.get(kwargs["card_type"]) or PRODUCT_TO_CLASS_MAPPING["base"]

    loan = klass(
        session=session,
        user_id=kwargs["user_id"],
        product_id=get_product_id_from_card_type(session=session, card_type=kwargs["card_type"]),
        lender_id=kwargs.pop("lender_id"),
        rc_rate_of_interest_monthly=Decimal(3),
        lender_rate_of_interest_annual=Decimal(18),
        amortization_date=kwargs.get(
            "card_activation_date", get_current_ist_time().date()
        ),  # TODO: change this later.
    )
    session.add(loan)
    session.flush()

    kwargs["loan_id"] = loan.id

    user_card = UserCard(**kwargs)
    session.add(user_card)
    session.flush()

    return loan


def create_term_loan(session: Session, **kwargs) -> TermLoan:
    klass, _ = PRODUCT_TO_CLASS_MAPPING.get(kwargs["card_type"]) or PRODUCT_TO_CLASS_MAPPING["term_loan"]

    loan = klass(
        session=session,
        user_id=kwargs["user_id"],
        product_id=get_product_id_from_card_type(session=session, card_type=kwargs["card_type"]),
        lender_id=kwargs["lender_id"],
        rc_rate_of_interest_monthly=Decimal(3),
        lender_rate_of_interest_annual=Decimal(18),
        amortization_date=kwargs.get(
            "loan_creation_date", get_current_ist_time().date()
        ),  # TODO: change this later.
    )
    session.add(loan)
    session.flush()

    kwargs["loan_id"] = loan.id

    loan.set_loan_data(**kwargs)
    loan.trigger_loan_creation_event(**kwargs)
    return loan
