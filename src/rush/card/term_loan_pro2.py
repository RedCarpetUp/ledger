from decimal import Decimal
from typing import (
    Dict,
    Type,
)

from pendulum import Date
from sqlalchemy.orm import Session

from rush.card.base_card import B
from rush.card.term_loan import (
    TermLoan,
    TermLoanBill,
)


class TermLoanPro2Bill(TermLoanBill):
    round_emi_to_nearest: Decimal = Decimal("10")
    add_emi_one_to_downpayment: bool = False

    def get_relative_delta_for_emi(self, emi_number: int, amortization_date: Date) -> Dict[str, int]:
        """
        Sample for Tenure Loan Pro2:
        +-----------+------------------+----------------------+---------------------+--------------+---------------------+
        | loan_id   | loan_type        | product_order_date   | agreement_date      | emi_number   | due_date            |
        |-----------+------------------+----------------------+---------------------+--------------+---------------------|
        | 1051798   | Tenure Loan Pro2 | 2018-12-31 00:00:00  | 2018-12-29 00:00:00 | 1            | 2019-02-01 00:00:00 |
        | 1051798   | Tenure Loan Pro2 | 2018-12-31 00:00:00  | 2018-12-29 00:00:00 | 2            | 2019-03-01 00:00:00 |
        | 1051798   | Tenure Loan Pro2 | 2018-12-31 00:00:00  | 2018-12-29 00:00:00 | 3            | 2019-04-01 00:00:00 |
        +-----------+------------------+----------------------+---------------------+--------------+---------------------+
        """
        if emi_number == 1:
            return {"months": 0, "days": 0}
        return {"months": 1, "days": 0}


class TermLoanPro2(TermLoan):
    bill_class: Type[B] = TermLoanPro2Bill
    session: Session = None
    downpayment_perc: Decimal = Decimal("20")

    __mapper_args__ = {"polymorphic_identity": "term_loan_pro_2"}

    @staticmethod
    def calculate_amortization_date(product_order_date: Date) -> Date:
        if 1 <= product_order_date.day <= 5:
            return product_order_date.add(months=1).replace(day=1)
        elif 6 <= product_order_date.day <= 25:
            return product_order_date.add(months=1).replace(day=15)
        else:
            return product_order_date.add(months=2).replace(day=1)
