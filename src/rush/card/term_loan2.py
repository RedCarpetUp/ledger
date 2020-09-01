from decimal import Decimal
from typing import (
    Dict,
    Tuple,
    Type,
)

from pendulum import Date
from sqlalchemy.orm import Session

from rush.card.base_card import B
from rush.card.term_loan import (
    TermLoan,
    TermLoanBill,
)


class TermLoan2Bill(TermLoanBill):
    round_emi_to_nearest: Decimal = Decimal("10")
    add_emi_one_to_downpayment: bool = True

    @staticmethod
    def calculate_bill_start_and_close_date(amortization_date: Date, tenure: int) -> Tuple[Date]:
        bill_start_date = amortization_date
        # not sure about bill close date.

        if bill_start_date.day < 15:
            normalized_amortization_date = bill_start_date.replace(day=1)
        elif bill_start_date.day >= 15 and bill_start_date.day < 25:
            normalized_amortization_date = bill_start_date.replace(day=15)
        else:
            normalized_amortization_date = bill_start_date.add(months=1).replace(day=1)

        bill_close_date = normalized_amortization_date.add(months=tenure - 1)

        return bill_start_date, bill_close_date

    def get_relative_delta_for_emi(self, emi_number: int, amortization_date: Date) -> Dict[str, int]:
        """
        Sample for TenureLoan2:
        +-----------+--------------+----------------------+---------------------+--------------+---------------------+
        | loan_id   | loan_type    | product_order_date   | agreement_date      | emi_number   | due_date            |
        |-----------+--------------+----------------------+---------------------+--------------+---------------------|
        | 1015092   | Tenure Loan2 | 2018-12-22 00:00:00  | 2018-12-22 00:00:00 | 1            | 2018-12-22 00:00:00 |
        | 1015092   | Tenure Loan2 | 2018-12-22 00:00:00  | 2018-12-22 00:00:00 | 2            | 2019-01-15 00:00:00 |
        | 1015092   | Tenure Loan2 | 2018-12-22 00:00:00  | 2018-12-22 00:00:00 | 3            | 2019-02-15 00:00:00 |
        +-----------+--------------+----------------------+---------------------+--------------+---------------------+

        """

        if emi_number == 1:
            return {"months": 0, "days": 0}
        elif emi_number == 2:
            if amortization_date.day < 15:
                months = 1
                days = 1 - amortization_date.day
            elif amortization_date.day >= 15 and amortization_date.day < 25:
                months = 1
                days = 15 - amortization_date.day
            else:
                months = 2
                days = 1 - amortization_date.day

            return {"months": months, "days": days}
        else:
            return {"months": 1, "days": 0}


class TermLoan2(TermLoan):
    bill_class: Type[B] = TermLoan2Bill
    session: Session = None
    downpayment_perc: Decimal = Decimal("20")

    __mapper_args__ = {"polymorphic_identity": "term_loan_2"}

    @staticmethod
    def calculate_amortization_date(product_order_date: Date) -> Date:
        return product_order_date
