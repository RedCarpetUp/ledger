from decimal import Decimal

from sqlalchemy.orm.session import Session

from rush.card import create_user_product
from rush.card.base_card import BaseLoan
from rush.card.transaction_loan import TransactionLoan
from rush.card.utils import create_user_product_mapping
from rush.models import (
    CardTransaction,
    LoanData,
)
from rush.payments import payment_received
from rush.utils import get_current_ist_time


def transaction_to_loan(session: Session, txn_id: int, user_id: int) -> TransactionLoan:
    txn = session.query(CardTransaction).filter(CardTransaction.id == txn_id).scalar()

    if not txn:
        return None

    user_loan = (
        session.query(BaseLoan)
        .join(LoanData, LoanData.loan_id == BaseLoan.id)
        .join(CardTransaction, CardTransaction.loan_id == LoanData.id)
        .filter(CardTransaction.id == txn_id)
        .scalar()
    )

    # making txn ineligible for billing
    txn.loan_id = None

    user_product = create_user_product_mapping(
        session=session, user_id=user_id, product_type="transaction_loan"
    )

    # making 0 amount downpayment
    payment_received(
        session=session,
        user_loan=None,
        payment_amount=0,
        payment_date=get_current_ist_time().date(),
        payment_request_id="dummy_downpayment",
        payment_type="downpayment",
        user_product_id=user_product.id,
        lender_id=user_loan.lender_id,
    )

    # loan for txn amount
    txn_loan = create_user_product(
        session=session,
        user_id=user_id,
        card_type="transaction_loan",
        lender_id=user_loan.lender_id,
        interest_free_period_in_days=15,
        tenure=12,
        amount=txn.amount,
        product_order_date=get_current_ist_time().date(),
        user_product_id=user_product.id,
        downpayment_percent=Decimal("0"),
    )

    session.flush()

    return txn_loan
