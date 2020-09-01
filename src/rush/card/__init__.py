from decimal import Decimal
from typing import (
    Any,
    Generator,
    Optional,
)

from sqlalchemy.orm import Session

# for now, these imports are required by get_product_class method to fetch all class within card module.
from rush.card.base_card import BaseLoan
from rush.card.health_card import HealthCard
from rush.card.ruby_card import RubyCard
from rush.card.term_loan import TermLoan
from rush.card.term_loan2 import TermLoan2
from rush.card.term_loan_pro import TermLoanPro
from rush.card.term_loan_pro2 import TermLoanPro2
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
) -> Loan:
    user_product_query = session.query(Loan).filter(
        Loan.user_id == user_id, Loan.product_type == card_type
    )

    if loan_id is not None:
        user_product_query = user_product_query.filter(Loan.id == loan_id)

    if user_product_id is not None:
        user_product_query = user_product_query.filter(Loan.id == user_product_id)

    user_product = user_product_query.one()

    user_product.prepare(session=session)
    return user_product


def create_user_product(session: Session, **kwargs) -> Loan:
    loan_cls = get_product_class(card_type=kwargs["card_type"])
    loan = loan_cls.create(session=session, **kwargs)
    return loan


def get_product_class(card_type: str) -> Any:
    """
    Only classes imported within this file will be listed here.
    Make sure to import every Product class.
    """

    def _subclasses(card_type: str) -> Generator[Any, Any, Any]:
        for kls in BaseLoan.__subclasses__():
            if (
                hasattr(kls, "__mapper_args__")
                and kls.__mapper_args__["polymorphic_identity"] == card_type
            ):
                yield kls
                break
            for sub_kls in kls.__subclasses__():
                if (
                    hasattr(sub_kls, "__mapper_args__")
                    and sub_kls.__mapper_args__["polymorphic_identity"] == card_type
                ):
                    yield sub_kls
                    break
        else:
            raise Exception("NoValidProductImplementation")

    return next(_subclasses(card_type=card_type))


def activate_card(session: Session, user_loan: BaseLoan, user_card: UserCard) -> None:
    event = LedgerTriggerEvent(
        name="card_activation",
        loan_id=user_loan.loan_id,
        amount=Decimal("0"),
        post_date=get_current_ist_time().date(),
        extra_details={},
    )

    session.add(event)
    session.flush()

    if not user_loan.amortization_date:
        user_loan.amortization_date = get_current_ist_time().date()

    # TODO: add this code later with test cases.
    # user_card.status = "ACTIVE"
    user_card.card_activation_date = user_loan.amortization_date


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

    limit_assignment_event(session=session, loan_id=user_loan.loan_id, event=event, amount=amount)
