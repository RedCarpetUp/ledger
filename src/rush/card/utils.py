from decimal import Decimal
from typing import (
    Any,
    Dict,
    Optional,
)

from pendulum import (
    Date,
    DateTime,
)
from sqlalchemy import func
from sqlalchemy.orm import Session

from rush.accrue_financial_charges import create_loan_fee_entry
from rush.card import BaseLoan
from rush.models import (
    CardTransaction,
    Fee,
    LedgerTriggerEvent,
    Loan,
    LoanData,
    Product,
    UserCard,
    UserInstrument,
    UserProduct,
    UserUPI,
)
from rush.utils import get_current_ist_time


def get_product_id_from_card_type(session: Session, card_type: str) -> int:
    return (
        session.query(Product.id)
        .filter(
            Product.product_name == card_type,
        )
        .scalar()
    )


def create_user_product_mapping(
    session: Session, user_id: int, product_type: str, lender_id: int = 62311
) -> UserProduct:
    user_product = UserProduct.new(session, user_id=user_id, product_type=product_type)
    session.flush()

    # Ideally we should've just created a loan object. User product is useless and should be removed.
    Loan.new(
        session,
        user_id=user_id,
        user_product_id=user_product.id,
        product_type=product_type,
        lender_id=lender_id,
    )
    session.flush()
    return user_product


def get_user_product_mapping(
    session: Session,
    user_id: int,
    product_type: Optional[str],
    user_product_id: Optional[int] = None,
    loan_id: Optional[int] = None,
) -> Optional[UserProduct]:
    assert (product_type or user_product_id) is not None

    query = (
        session.query(UserProduct)
        .outerjoin(Loan, Loan.user_product_id == UserProduct.id)
        .filter(UserProduct.user_id == user_id)
    )

    if loan_id:
        query = query.filter(Loan.id == loan_id)

    if user_product_id:
        query = query.filter(UserProduct.id == user_product_id)

    if product_type:
        query = query.filter(UserProduct.product_type == product_type)

    user_product = query.order_by(UserProduct.id.desc()).first()

    return user_product


def get_product_type_from_user_product_id(session: Session, user_product_id: int) -> Optional[str]:
    return session.query(UserProduct.product_type).filter(UserProduct.id == user_product_id).scalar()


def create_activation_fee(
    session: Session,
    user_loan: BaseLoan,
    post_date: DateTime,
    gross_amount: Decimal,
    include_gst_from_gross_amount: bool,
    fee_name: str,
) -> Fee:
    event = LedgerTriggerEvent(
        name="activation_fee",
        post_date=post_date,
    )
    session.add(event)
    session.flush()

    fee = create_loan_fee_entry(
        session=session,
        user_loan=user_loan,
        event=event,
        fee_name=fee_name,
        gross_fee_amount=gross_amount,
        include_gst_from_gross_amount=include_gst_from_gross_amount,
    )
    event.amount = fee.gross_amount
    return fee


def create_reload_fee(
    session: Session,
    user_loan: BaseLoan,
    post_date: DateTime,
    gross_fee_amount: Decimal,
) -> Fee:
    event = LedgerTriggerEvent(
        name="reload_fee",
        post_date=post_date,
    )
    session.add(event)
    session.flush()

    fee = create_loan_fee_entry(
        session=session,
        user_loan=user_loan,
        event=event,
        fee_name="card_reload_fees",
        gross_fee_amount=gross_fee_amount,
        include_gst_from_gross_amount=True,
    )
    event.amount = fee.gross_amount
    return fee


def create_upgrade_fee(
    session: Session,
    user_loan: BaseLoan,
    post_date: DateTime,
    gross_fee_amount: Decimal,
) -> Fee:
    event = LedgerTriggerEvent(
        name="upgrade_fee",
        post_date=post_date,
    )
    session.add(event)
    session.flush()

    fee = create_loan_fee_entry(
        session=session,
        user_loan=user_loan,
        event=event,
        fee_name="card_upgrade_fees",
        gross_fee_amount=gross_fee_amount,
        include_gst_from_gross_amount=True,
    )
    event.amount = fee.gross_amount
    return fee


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


def get_daily_spend(
    session: Session, loan: Loan, date_to_check_against: Optional[Date] = None
) -> Decimal:
    if not date_to_check_against:
        date_to_check_against = get_current_ist_time().date()

    # from sqlalchemy import and_
    daily_spent = (
        session.query(func.sum(CardTransaction.amount))
        .join(LoanData, LoanData.id == CardTransaction.loan_id)
        .filter(
            LoanData.loan_id == loan.id,
            LoanData.user_id == loan.user_id,
            func.date_trunc("day", CardTransaction.txn_time) == date_to_check_against,
            CardTransaction.status == "CONFIRMED",
        )
        .group_by(LoanData.loan_id)
        .scalar()
    )
    return daily_spent or 0


def get_weekly_spend(
    session: Session, loan: Loan, date_to_check_against: Optional[Date] = None
) -> Decimal:
    if not date_to_check_against:
        date_to_check_against = get_current_ist_time().date()

    to_date = date_to_check_against.subtract(days=7)

    weekly_spent = (
        session.query(func.sum(CardTransaction.amount))
        .join(LoanData, LoanData.id == CardTransaction.loan_id)
        .filter(
            LoanData.loan_id == loan.id,
            LoanData.user_id == loan.user_id,
            func.date_trunc("day", CardTransaction.txn_time).between(to_date, date_to_check_against),
            CardTransaction.status == "CONFIRMED",
        )
        .group_by(LoanData.loan_id)
        .scalar()
    )
    return weekly_spent or 0


def get_daily_total_transactions(
    session: Session, loan: Loan, date_to_check_against: Optional[Date]
) -> Decimal:
    if not date_to_check_against:
        date_to_check_against = get_current_ist_time().date()

    daily_txns = (
        session.query(func.count(CardTransaction.id))
        .join(LoanData, LoanData.id == CardTransaction.loan_id)
        .filter(
            LoanData.loan_id == loan.id,
            LoanData.user_id == loan.user_id,
            func.date_trunc("day", CardTransaction.txn_time) == date_to_check_against,
            CardTransaction.status == "CONFIRMED",
        )
        .group_by(LoanData.loan_id)
        .scalar()
    )
    return daily_txns or 0
