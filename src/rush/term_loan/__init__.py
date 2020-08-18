from sqlalchemy.orm import Session

from rush.term_loan.base_loan import BaseLoan

from rush.models import (
    LoanData,
    Product,
)
from rush.term_loan.base_loan import BaseLoan


def create_term_loan(session: Session, loan_class: BaseLoan, **kwargs) -> LoanData:
    return loan_class.create(session=session, **kwargs)


def get_term_loan(session: Session, user_id: int, product_type: str) -> LoanData:
    term_loan = (
        session.query(LoanData)
        .join(Product.product_name == product_type)
        .filter(
            LoanData.user_id == user_id,
            LoanData.product_id == Product.id,
            LoanData.row_status == "active",
        )
        .one()
    )

    return term_loan
