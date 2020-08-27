from decimal import Decimal
from typing import Optional

from pendulum import Date
from sqlalchemy.orm import Session

from rush.card.base_card import BaseLoan
from rush.models import (
    EphemeralAccount,
    Fee,
    LedgerTriggerEvent,
    Product,
)
from rush.utils import (
    add_gst_split_to_amount,
    get_current_ist_time,
)


def get_product_id_from_card_type(session: Session, card_type: str) -> int:
    return session.query(Product.id).filter(Product.product_name == card_type,).scalar()


def add_pre_product_fee(
    session: Session,
    user_id: int,
    product_type: str,
    fee_name: str,
    fee_amount: Decimal,
    post_date: Optional[Date] = None,
    ephemeral_account_id: Optional[int] = None,
) -> Fee:
    """
        In case of no ephemeral_account_id, it will always create a new ephemeral account.
        For multiple pre-product fees, this needs to be maintained by ledger user. Also, same ephemeral
        account should be passed to loan during loan creation.
    """

    if post_date is None:
        post_date = get_current_ist_time().date()

    event = LedgerTriggerEvent(
        name="pre_product_fee_added",
        post_date=get_current_ist_time().date(),
        extra_details={"fee_name": fee_name},
    )
    session.add(event)
    session.flush()

    if ephemeral_account_id is None:
        ephemeral_account = EphemeralAccount(user_id=user_id, product_type=product_type)
        session.add(ephemeral_account)
        session.flush()

        ephemeral_account_id = ephemeral_account.id

    f = Fee(
        event_id=event.id,
        ephemeral_account_id=ephemeral_account_id,
        name=fee_name,
        net_amount=fee_amount,
        sgst_rate=Decimal(0),  # TODO: check what should be the value.
        cgst_rate=Decimal(0),  # TODO: check what should be the value.
        igst_rate=Decimal(18),  # TODO: check what should be the value.
    )

    d = add_gst_split_to_amount(
        fee_amount, sgst_rate=f.sgst_rate, cgst_rate=f.cgst_rate, igst_rate=f.igst_rate
    )
    f.gross_amount = d["gross_amount"]
    session.add(f)

    return f


def add_reload_fee(
    session: Session, user_loan: BaseLoan, fee_amount: Decimal, post_date: Optional[Date] = None,
) -> Fee:
    if post_date is None:
        post_date = get_current_ist_time().date()

    event = LedgerTriggerEvent(name="reload_fee_added", post_date=get_current_ist_time().date(),)
    session.add(event)
    session.flush()

    f = Fee(
        loan_id=user_loan.id,
        event_id=event.id,
        name="card_reload_fees",
        net_amount=fee_amount,
        sgst_rate=Decimal(0),  # TODO: check what should be the value.
        cgst_rate=Decimal(0),  # TODO: check what should be the value.
        igst_rate=Decimal(18),  # TODO: check what should be the value.
    )

    d = add_gst_split_to_amount(
        fee_amount, sgst_rate=f.sgst_rate, cgst_rate=f.cgst_rate, igst_rate=f.igst_rate
    )
    f.gross_amount = d["gross_amount"]
    session.add(f)

    return f
