from sqlalchemy import and_
from sqlalchemy.orm import Session

from rush.models import (
    Loan,
    LoanData,
    Product,
)
from rush.term_loan.base_loan import BaseLoan


def create_term_loan(session: Session, loan_class: BaseLoan, **kwargs) -> LoanData:
    return loan_class.create(session=session, **kwargs)


def get_term_loan(session: Session, user_id: int, product_type: str) -> LoanData:
    term_loan = (
        session.query(LoanData)
        .join(
            Loan,
            and_(
                Loan.id == LoanData.loan_id,
                Loan.user_id == LoanData.user_id,
                Loan.row_status == "active"
            )
        )
        .join(
            Product,
            and_(
                Product.product_name == product_type,
                Loan.product_id == Product.id
            )
        )
        .filter(
            LoanData.user_id == user_id,
        )
        .one()
    )

    return term_loan
