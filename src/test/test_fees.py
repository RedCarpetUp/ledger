from decimal import Decimal
from test.utils import (
    pay_payment_request,
    payment_request_data,
)

from pendulum import parse as parse_date  # type: ignore
from sqlalchemy.orm import Session

from rush.card import (
    create_user_product,
    get_user_product,
)
from rush.card.health_card import HealthCard
from rush.card.utils import (
    create_loan,
    create_loan_fee,
    create_user_product_mapping,
)
from rush.ledger_utils import get_account_balance_from_str
from rush.models import (
    Lenders,
    Product,
    User,
)
from rush.payments import payment_received


def create_lenders(session: Session) -> None:
    dmi = Lenders(id=62311, performed_by=123, lender_name="DMI")
    session.add(dmi)

    redux = Lenders(id=1756833, performed_by=123, lender_name="Redux")
    session.add(redux)
    session.flush()


def create_products(session: Session) -> None:
    hc_product = Product(product_name="health_card")
    session.add(hc_product)
    session.flush()

    rc_product = Product(product_name="term_loan_reset")
    session.add(rc_product)
    session.flush()


def card_db_updates(session: Session) -> None:
    create_products(session=session)
    pass


def create_user(session: Session) -> None:
    u = User(
        id=5,
        performed_by=123,
    )
    session.add(u)
    session.flush()


def create_test_user_loan(session: Session) -> HealthCard:
    uc = create_user_product(
        session=session,
        user_id=5,
        card_activation_date=parse_date("2020-07-01").date(),
        card_type="health_card",
        rc_rate_of_interest_monthly=Decimal(3),
        lender_id=62311,
        kit_number="10000",
        tenure=12,
    )

    return uc


def test_add_reload_fee(session: Session) -> None:
    create_lenders(session=session)
    card_db_updates(session=session)
    create_user(session=session)
    uc = create_test_user_loan(session=session)

    assert uc.product_type == "health_card"
    assert uc.get_limit_type(mcc="8011") == "health_limit"
    assert uc.get_limit_type(mcc="5555") == "available_limit"
    assert uc.should_reinstate_limit_on_payment == True

    user_loan = get_user_product(session=session, user_id=uc.user_id, card_type="health_card")
    assert isinstance(user_loan, HealthCard) == True

    reload_fee = create_loan_fee(
        session=session,
        user_loan=uc,
        gross_amount=Decimal("100"),
        post_date=parse_date("2020-08-01 00:00:00"),
        fee_name="card_reload_fees",
        include_gst_from_gross_amount=True,
    )

    assert reload_fee.identifier_id == uc.loan_id
    assert reload_fee.fee_status == "UNPAID"
    assert reload_fee.gross_amount == Decimal(100)


def test_reset_joining_fees(session: Session) -> None:
    create_lenders(session=session)
    create_products(session=session)
    create_user(session=session)

    user_product = create_user_product_mapping(
        session=session, user_id=5, product_type="term_loan_reset"
    )

    create_loan(session=session, user_product=user_product, lender_id=1756833)
    user_loan = get_user_product(
        session=session,
        user_id=user_product.user_id,
        card_type="term_loan_reset",
        user_product_id=user_product.id,
    )

    card_activation_fee = create_loan_fee(
        session=session,
        user_loan=user_loan,
        post_date=parse_date("2019-02-01 00:00:00"),
        gross_amount=Decimal(1000),
        include_gst_from_gross_amount=False,  # Doesn't matter because gst is not applied on Redux lender.
        fee_name="reset_joining_fees",
    )

    assert card_activation_fee.fee_status == "UNPAID"
    assert card_activation_fee.gross_amount == Decimal(1000)
