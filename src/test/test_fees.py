from decimal import Decimal

from pendulum import parse as parse_date  # type: ignore
from sqlalchemy.orm import Session

from rush.card.utils import add_pre_product_fee
from rush.models import (
    CardKitNumbers,
    CardNames,
    Lenders,
    Product,
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


# def card_db_updates(session: Session) -> None:
#     create_products(session=session)

#     cn = CardNames(name="ruby")
#     session.add(cn)
#     session.flush()

#     ckn = CardKitNumbers(kit_number="10000", card_name_id=cn.id, last_5_digits="0000", status="active")
#     session.add(ckn)
#     session.flush()


def create_user(session: Session) -> None:
    u = User(
        id=5,
        performed_by=123,
    )
    session.add(u)
    session.flush()


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

    ephemeral_account_id = card_activation_fee.ephemeral_account_id

    assert ephemeral_account_id is not None
    assert card_activation_fee.fee_status == "UNPAID"


def test_add_reload_fee() -> None:
    pass
