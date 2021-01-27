from typing import Type

from dateutil.relativedelta import relativedelta
from sqlalchemy.sql.sqltypes import DateTime

from rush.card.term_loan import (
    B,
    TermLoan,
    TermLoanBill,
)
from rush.ledger_utils import create_ledger_entry_from_str
from rush.models import (
    LedgerTriggerEvent,
    LoanSchedule,
)


class TransactionLoanBill(TermLoanBill):
    pass


class TransactionLoan(TermLoan):
    bill_class: Type[B] = TermLoanBill

    def disbursal(self, **kwargs):
        event = LedgerTriggerEvent(
            performed_by=kwargs["user_id"],
            name="transaction_to_loan",
            loan_id=kwargs["parent_loan_id"],
            post_date=kwargs["product_order_date"],
            amount=kwargs["amount"],
            extra_details={"child_loan_id": kwargs["loan_id"]},
        )

        self.session.add(event)
        self.session.flush()

        bill_id = kwargs["loan_data"].id

        create_ledger_entry_from_str(
            session=self.session,
            event_id=event.id,
            debit_book_str=f"{bill_id}/bill/principal_receivable/a",
            credit_book_str=kwargs["credit_book"],
            amount=kwargs["amount"],
        )

        return event

    __mapper_args__ = {"polymorphic_identity": "transaction_loan"}
