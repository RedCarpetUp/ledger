from decimal import Decimal
from typing import Optional

import pendulum
from sqlalchemy.orm.session import Session
from sqlalchemy.sql.sqltypes import DateTime

from rush.card import create_user_product
from rush.card.base_card import BaseLoan
from rush.card.utils import create_user_product_mapping
from rush.models import (
    CardTransaction,
    LoanData,
)


def transaction_to_loan(
    session: Session,
    transaction_id: int,
    user_id: int,
    post_date: DateTime,
    tenure: int,
    interest_rate: Optional[int] = None,
) -> dict:
    transaction: CardTransaction = (
        session.query(CardTransaction).filter(CardTransaction.id == transaction_id).scalar()
    )

    if not transaction:
        return {"result": "error", "message": "Invalid Transaction ID"}

    # checking if the bill has already been generated for this transaction
    bill: LoanData = session.query(LoanData).filter(LoanData.id == transaction.loan_id).scalar()

    if bill.is_generated:
        return {"result": "error", "message": "Bill for this transaction has already been generated."}

    user_loan: BaseLoan = (
        session.query(BaseLoan)
        .join(LoanData, LoanData.loan_id == BaseLoan.id)
        .filter(LoanData.id == bill.id)
        .scalar()
    )

    user_product = create_user_product_mapping(
        session=session, user_id=user_id, product_type="transaction_loan", lender_id=1756833
    )

    # loan for transaction amount
    transaction_loan = create_user_product(
        session=session,
        user_id=user_id,
        card_type="transaction_loan",
        lender_id=user_loan.lender_id,
        interest_free_period_in_days=15,
        tenure=tenure,
        amount=transaction.amount,
        product_order_date=pendulum.datetime(
            bill.bill_start_date.year, bill.bill_start_date.month, bill.bill_start_date.day
        ),
        user_product_id=user_product.id,
        downpayment_percent=Decimal("0"),
        credit_book=f"{transaction.loan_id}/bill/unbilled/a",
        parent_loan_id=user_loan.id,
        can_close_early=True,
    )

    transaction_loan_bill: LoanData = (
        session.query(LoanData).filter(LoanData.loan_id == transaction_loan.id).scalar()
    )
    transaction.loan_id = transaction_loan_bill.id

    session.flush()

    return {"result": "success", "data": transaction_loan}
