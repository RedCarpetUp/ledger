from decimal import Decimal
from typing import (
    Any,
    Generator,
    Optional,
)

from pendulum import Date
from sqlalchemy.orm import Session

# for now, these imports are required by get_product_class method to fetch all class within card module.
from rush.card.base_card import BaseLoan
from rush.card.health_card import HealthCard
from rush.card.rebel_card import RebelCard
from rush.card.reset_card import ResetCard
from rush.card.reset_card_v2 import ResetCardV2
from rush.card.rhythm_card import RhythmCard
from rush.card.ruby_card import RubyCard
from rush.card.term_loan import TermLoan
from rush.card.term_loan2 import TermLoan2
from rush.card.term_loan_pro import TermLoanPro
from rush.card.term_loan_pro2 import TermLoanPro2
from rush.card.transaction_loan import TransactionLoan
from rush.card.zeta_card import ZetaCard
from rush.ledger_events import limit_assignment_event
from rush.models import (
    LedgerTriggerEvent,
    Loan,
    UserCard,
)
from rush.utils import get_current_ist_time


def get_user_product(
    session: Session,
    user_id: int,
    card_type: str = "ruby",
    loan_id: Optional[int] = None,
    user_product_id: Optional[int] = None,
    _rows: str = "one",
) -> BaseLoan:
    user_product_query = session.query(Loan).filter(
        Loan.user_id == user_id, Loan.product_type == card_type
    )

    if loan_id is not None:
        user_product_query = user_product_query.filter(Loan.id == loan_id)

    if user_product_id is not None:
        user_product_query = user_product_query.filter(Loan.user_product_id == user_product_id)

    if _rows == "one":
        user_product = user_product_query.one()
    elif _rows == "one_or_none":
        user_product = user_product_query.one_or_none()

    if not user_product:
        return None

    user_product.prepare(session=session)
    return user_product


def get_user_loan(
    session: Session,
    loan_id: int,
) -> BaseLoan:
    user_loan = session.query(Loan).filter_by(id=loan_id).one()
    user_loan.prepare(session)
    return user_loan


def create_user_product(session: Session, **kwargs) -> Loan:
    loan_cls = get_product_class(card_type=kwargs["card_type"])
    loan = loan_cls.create(session=session, **kwargs)
    return loan


def get_product_class(card_type: str) -> Any:
    for subclass in BaseLoan.subclasses:
        if subclass.__mapper_args__["polymorphic_identity"] == card_type:
            return subclass


def activate_card(
    session: Session, user_loan: BaseLoan, user_card: UserCard, post_date: Optional[Date] = None
) -> None:
    activation_date = get_current_ist_time().date() if not post_date else post_date
    event = LedgerTriggerEvent(
        name="card_activation",
        loan_id=user_loan.loan_id,
        amount=Decimal("0"),
        post_date=activation_date,
        extra_details={},
    )

    session.add(event)
    session.flush()

    if not user_loan.amortization_date:
        user_loan.amortization_date = activation_date

    user_card.status = "ACTIVE"
    user_card.card_activation_date = activation_date


def disburse_card(
    session: Session, user_loan: BaseLoan, amount: Decimal, payment_request_id: str
) -> None:
    event = LedgerTriggerEvent(
        name="card_disbursal",
        loan_id=user_loan.loan_id,
        amount=amount,
        post_date=get_current_ist_time().date(),
        extra_details={"payment_request_id": payment_request_id},
    )
    session.add(event)
    session.flush()

    limit_assignment_event(session=session, loan_id=user_loan.loan_id, event=event, amount=amount)
