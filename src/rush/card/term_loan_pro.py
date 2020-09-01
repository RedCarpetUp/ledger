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


class TermLoanProBill(TermLoanBill):
    round_emi_to_nearest: Decimal = Decimal("10")
    add_emi_one_to_downpayment: bool = False

    def get_relative_delta_for_emi(self, emi_number: int, amortization_date: Date) -> Dict[str, int]:
        """
        Sample Tenure Loan Pro:
        +-----------+-----------------+----------------------+---------------------+--------------+---------------------+
        | loan_id   | loan_type       | product_order_date   | agreement_date      | emi_number   | due_date            |
        |-----------+-----------------+----------------------+---------------------+--------------+---------------------|
        | 646104    | Tenure Loan Pro | 2018-10-22 00:00:00  | 2018-10-20 00:00:00 | 1            | 2018-11-22 00:00:00 |
        | 646104    | Tenure Loan Pro | 2018-10-22 00:00:00  | 2018-10-20 00:00:00 | 2            | 2018-12-22 00:00:00 |
        | 646104    | Tenure Loan Pro | 2018-10-22 00:00:00  | 2018-10-20 00:00:00 | 3            | 2019-01-22 00:00:00 |
        +-----------+-----------------+----------------------+---------------------+--------------+---------------------+

        """
        if emi_number == 1:
            return {"months": 0, "days": 0}
        return {"months": 1, "days": 0}


class TermLoanPro(TermLoan):
    bill_class: Type[B] = TermLoanProBill
    session: Session = None

    __mapper_args__ = {"polymorphic_identity": "term_loan_pro"}

    @staticmethod
    def calculate_amortization_date(product_order_date: Date) -> Date:
        return product_order_date.add(months=1)
