import inspect
from decimal import Decimal
from typing import (
    Any,
    Optional,
)

from pendulum import Date
from sqlalchemy.orm import Session

from rush.card.base_card import BaseLoan
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
    """
    Returns the product class implementation based on card_type which
    would be the same as the polymorphic_identity defined within the class.

    The idea is to load all classes within the rush.card package and
    then compare card_type to their polymorphic_identity.

    This is done by:
    - Resolving the absolute path of the rush.card package (package_path)
    - Getting a list of all files in this package (for example, reset_card.py)
    - Looping over all these files and dynamically importing them as a Python module
    - Getting a list of all classes within each module using the inspect module
    - And finally, comparing card_type with the class' polymorphic_identity
    """

    import importlib
    from os import listdir
    from os.path import (
        dirname,
        isfile,
        join,
        realpath,
    )

    package_path = dirname(realpath(__file__))
    files = [file for file in listdir(package_path) if isfile(join(package_path, file))]

    for file in files:
        module = importlib.import_module(f"rush.card.{file[:-3]}")
        for class_name, klass in inspect.getmembers(module, inspect.isclass):
            if (
                hasattr(klass, "__mapper_args__")
                and klass.__mapper_args__["polymorphic_identity"] == card_type
            ):
                return klass

    raise Exception("NoValidProductImplementation")


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
