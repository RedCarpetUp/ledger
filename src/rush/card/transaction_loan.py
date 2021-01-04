from typing import Type
from dateutil.relativedelta import relativedelta

from sqlalchemy.sql.sqltypes import DateTime

from rush.card.term_loan import (
    TermLoan,
    TermLoanBill,
    B,
)
from rush.models import LoanSchedule


class TransactionLoanBill(TermLoanBill):
    pass


class TransactionLoan(TermLoan):
    bill_class: Type[B] = TermLoanBill

    @classmethod
    def get_txn_to_add_in_bill(self, date: DateTime):
        return (
            self.session.query(LoanSchedule.total_due_amount)
            .filter(
                LoanSchedule.bill_id == None,
                LoanSchedule.due_date < date.date(),
                LoanSchedule.due_date > date.date() - relativedelta(months=1),
            )
            .order_by(LoanSchedule.due_date.desc())
            .limit(1)
            .scalar()
        )

    __mapper_args__ = {"polymorphic_identity": "transaction_loan"}
