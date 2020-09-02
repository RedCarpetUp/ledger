from decimal import Decimal

from pendulum import parse as parse_date  # type: ignore
from sqlalchemy.orm import Session

from rush.card import (
    HealthCard,
    create_user_product,
    get_user_product,
)
from rush.card.utils import (
    add_pre_product_fee,
    add_reload_fee,
)
from rush.models import (
    CardKitNumbers,
    CardNames,
    Lenders,
    LoanFee,
    Product,
    ProductFee,
    User,
)


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

    cn = CardNames(name="ruby")
    session.add(cn)
    session.flush()

    ckn = CardKitNumbers(kit_number="10000", card_name_id=cn.id, last_5_digits="0000", status="active")
    session.add(ckn)
    session.flush()


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
        lender_id=62311,
        kit_number="10000",
    )

    return uc


def test_add_pre_product_fees(session: Session) -> None:
    create_lenders(session=session)
    create_products(session=session)
    create_user(session=session)

    card_activation_fee = add_pre_product_fee(
        session=session,
        user_id=5,
        product_type="health_card",
        fee_name="card_activation_fees",
        fee_amount=Decimal(1000),
    )

    sell_book_id = card_activation_fee.identifier_id

    assert type(card_activation_fee) == ProductFee
    assert sell_book_id is not None
    assert card_activation_fee.fee_status == "UNPAID"


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

    reload_fee = add_reload_fee(
        session=session,
        user_loan=uc,
        fee_amount=Decimal("100"),
        post_date=parse_date("2020-08-01").date(),
    )

    assert type(reload_fee) == LoanFee
    assert reload_fee.identifier_id == uc.loan_id
    assert reload_fee.fee_status == "UNPAID"


def test_reset_joiningg_fees(session: Session) -> None:
    create_lenders(session=session)
    create_products(session=session)
    create_user(session=session)

    card_activation_fee = add_pre_product_fee(
        session=session,
        user_id=5,
        product_type="term_loan_reset",
        fee_name="joining_fees",
        fee_amount=Decimal(1000),
    )

    sell_book_id = card_activation_fee.identifier_id

    assert type(card_activation_fee) == ProductFee
    assert sell_book_id is not None
    assert card_activation_fee.fee_status == "UNPAID"
