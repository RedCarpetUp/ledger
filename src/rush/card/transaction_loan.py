from typing import Type

from rush.card.term_loan import (
    TermLoan,
    TermLoanBill,
    B,
)


class TransactionLoanBill(TermLoanBill):
    pass


class TransactionLoan(TermLoan):
    bill_class: Type[B] = TermLoanBill

    __mapper_args__ = {"polymorphic_identity": "transaction_loan"}
