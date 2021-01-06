from typing import Type

from dateutil.relativedelta import relativedelta
from sqlalchemy.sql.sqltypes import DateTime

from rush.card.term_loan import (
    B,
    TermLoan,
    TermLoanBill,
)
from rush.models import LoanSchedule


class TransactionLoanBill(TermLoanBill):
    pass


class TransactionLoan(TermLoan):
    bill_class: Type[B] = TermLoanBill

    __mapper_args__ = {"polymorphic_identity": "transaction_loan"}
