from decimal import Decimal
from typing import (
    Any,
    Dict,
    Optional,
)

from pendulum import Date
from sqlalchemy.orm import Session

from rush.models import (
    Fee,
    LedgerTriggerEvent,
    Loan,
    LoanFee,
    Product,
    ProductFee,
    UserCard,
    UserInstrument,
    UserProduct,
    UserUPI,
)
from rush.utils import (
    add_gst_split_to_amount,
    get_current_ist_time,
)


def get_product_id_from_card_type(session: Session, card_type: str) -> int:
    return (
        session.query(Product.id)
        .filter(
            Product.product_name == card_type,
        )
        .scalar()
    )


def create_user_product_mapping(session: Session, user_id: int, product_type: str) -> UserProduct:
    user_product = UserProduct(user_id=user_id, product_type=product_type)
    session.add(user_product)
    session.flush()

    return user_product


def get_user_product_mapping(
    session: Session,
    user_id: int,
    product_type: str,
    user_product_id: Optional[int] = None,
    _rows="one_or_none",
) -> Optional[UserProduct]:
    assert _rows in ("one", "one_or_none")

    query = session.query(UserProduct).filter(
        UserProduct.user_id == user_id, UserProduct.product_type == product_type
    )

    if user_product_id:
        query = query.filter(UserProduct.id == user_product_id)

    if _rows == "one":
        user_product = query.one()
    elif _rows == "one_or_none":
        user_product = query.one_or_none()

    return user_product


def add_pre_product_fee(
    session: Session,
    user_id: int,
    product_type: str,
    fee_name: str,
    fee_amount: Decimal,
    post_date: Optional[Date] = None,
    user_product_id: Optional[int] = None,
) -> Fee:
    """
    In case of no sell_book_id, it will always create a new ephemeral account.
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

    if user_product_id is None:
        user_product_id = create_user_product_mapping(
            session=session, user_id=user_id, product_type=product_type
        ).id

    f = ProductFee(
        event_id=event.id,
        identifier_id=user_product_id,
        name=fee_name,
        fee_status="UNPAID",
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
    session: Session,
    user_loan: Loan,
    fee_amount: Decimal,
    post_date: Optional[Date] = None,
) -> Fee:
    assert user_loan.amortization_date is not None

    if post_date is None:
        post_date = get_current_ist_time().date()

    event = LedgerTriggerEvent(
        name="reload_fee_added",
        post_date=get_current_ist_time().date(),
    )
    session.add(event)
    session.flush()

    f = LoanFee(
        identifier_id=user_loan.id,
        event_id=event.id,
        name="card_reload_fees",
        fee_status="UNPAID",
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


def add_card_to_loan(session: Session, loan: Loan, card_info: Dict[str, Any]) -> UserCard:
    event = LedgerTriggerEvent(
        name="add_card",
        loan_id=loan.loan_id,
        amount=Decimal("0"),
        post_date=get_current_ist_time().date(),
        extra_details={},
    )

    session.add(event)
    session.flush()

    card_info["user_id"] = loan.user_id
    card_info["loan_id"] = loan.loan_id

    user_card = UserCard(**card_info)
    session.add(user_card)
    session.flush()

    return user_card


def add_upi_to_loan(session: Session, loan: Loan, upi_info: Dict[str, Any]) -> UserUPI:
    event = LedgerTriggerEvent(
        name="add_upi",
        loan_id=loan.loan_id,
        amount=Decimal("0"),
        post_date=get_current_ist_time().date(),
        extra_details={},
    )

    session.add(event)
    session.flush()

    upi_info["user_id"] = loan.user_id
    upi_info["loan_id"] = loan.loan_id

    user_upi = UserUPI(**upi_info)
    session.add(user_upi)
    session.flush()

    return user_upi


def add_instrument_to_loan(
    session: Session, instrument_type: str, loan: Loan, instrument_info: Dict[str, Any]
) -> UserInstrument:
    assert instrument_type in ("upi", "card")

    if instrument_type == "upi":
        return add_upi_to_loan(session=session, loan=loan, upi_info=instrument_info)

    elif instrument_type == "card":
        return add_card_to_loan(session=session, loan=loan, card_info=instrument_info)


def get_downpayment_amount(
    product_type: str,
    product_price: Decimal,
    tenure: int,
    downpayment_perc: Decimal,
    interest_rate: Optional[Decimal] = None,
) -> Decimal:
    from rush.card import get_product_class

    request_data = {
        "product_price": product_price,
        "tenure": tenure,
        "downpayment_perc": downpayment_perc,
    }
    if interest_rate:
        request_data["interest_rate"] = interest_rate

    return get_product_class(card_type=product_type).bill_class.calculate_downpayment_amount_payable(
        **request_data
    )
