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
    LoanData,
    Product,
    UserCard,
)
from rush.utils import get_current_ist_time

PRODUCT_TO_CLASS_MAPPING = {
    "ruby": (RubyCard, RubyBill),
    "health_card": (HealthCard, HealthBill),
    "base": (BaseLoan, BaseBill),
}


def get_user_card(session: Session, user_id: int, card_type: str = "ruby") -> BaseLoan:
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


def create_term_loan(session: Session, loan_class: TermLoan, **kwargs) -> LoanData:
    return loan_class.create(session=session, **kwargs)


def get_term_loan(session: Session, user_id: int, product_type: str) -> LoanData:
    term_loan = (
        session.query(LoanData)
        .join(Loan, and_(Loan.id == LoanData.loan_id, Loan.user_id == LoanData.user_id,))
        .join(Product, and_(Product.product_name == product_type, Loan.product_id == Product.id))
        .filter(LoanData.user_id == user_id,)
        .one()
    )

    return term_loan
