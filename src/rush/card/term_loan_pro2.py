from typing import Type

from pendulum import Date
from sqlalchemy.orm import Session

from rush.card.base_card import B
from rush.card.term_loan import TermLoan
from rush.card.term_loan_pro import TermLoanProBill


class TermLoanPro2(TermLoan):
    bill_class: Type[B] = TermLoanProBill
    session: Session = None

    __mapper_args__ = {"polymorphic_identity": "term_loan_pro_2"}

    @staticmethod
    def calculate_first_emi_date(product_order_date: Date) -> Date:
        if 1 <= product_order_date.day <= 5:
            return product_order_date.add(months=1).replace(day=1)
        elif 6 <= product_order_date.day <= 25:
            return product_order_date.add(months=1).replace(day=15)
        else:
            return product_order_date.add(months=2).replace(day=1)
