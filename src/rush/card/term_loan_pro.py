from decimal import Decimal
from typing import Type

from pendulum import Date
from sqlalchemy.orm import Session

from rush.card.base_card import B
from rush.card.term_loan import (
    TermLoan,
    TermLoanBill,
)


class TermLoanProBill(TermLoanBill):
    def get_down_payment(self, include_first_emi=False) -> Decimal:
        return super().get_down_payment(include_first_emi)


class TermLoanPro(TermLoan):
    bill_class: Type[B] = TermLoanProBill
    session: Session = None

    __mapper_args__ = {"polymorphic_identity": "term_loan_pro"}

    @staticmethod
    def calculate_first_emi_date(product_order_date: Date) -> Date:
        return product_order_date.add(months=1)
