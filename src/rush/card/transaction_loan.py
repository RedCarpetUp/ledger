from calendar import monthrange
from decimal import Decimal
from typing import (
    Dict,
    Type,
)

from pendulum.date import Date
from sqlalchemy.orm.session import Session
from sqlalchemy.sql.sqltypes import DateTime

from rush.card.base_card import BaseLoan
from rush.card.term_loan import (
    B,
    TermLoan,
    TermLoanBill,
)
from rush.ledger_utils import create_ledger_entry_from_str
from rush.models import (
    CardTransaction,
    LedgerTriggerEvent,
    LoanData,
)


class TransactionLoanBill(TermLoanBill):
    def get_relative_delta_for_emi(self, emi_number: int, amortization_date: Date) -> Dict[str, int]:
        if emi_number == 1:
            delta = (
                monthrange(amortization_date.year, amortization_date.month)[
                    1
                ]  # Number of days in this month
                - amortization_date.day
                + 15
            )
            return {"months": 0, "days": delta}

        return {"months": 1, "days": 0}


class TransactionLoan(TermLoan):
    bill_class: Type[B] = TransactionLoanBill

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


def transaction_to_loan(
    session: Session,
    transaction_id: int,
    user_id: int,
    post_date: DateTime,
    tenure: int,
    interest_rate: Decimal,
) -> dict:
    transaction: CardTransaction = (
        session.query(CardTransaction).filter(CardTransaction.id == transaction_id).scalar()
    )

    if not transaction:
        return {"result": "error", "message": "Invalid Transaction ID"}

    # Checking if the bill has already been generated for this transaction
    bill: LoanData = session.query(LoanData).filter(LoanData.id == transaction.loan_id).scalar()

    if bill.is_generated:
        return {"result": "error", "message": "Bill for this transaction has already been generated."}

    user_loan: BaseLoan = (
        session.query(BaseLoan)
        .join(LoanData, LoanData.loan_id == BaseLoan.id)
        .filter(LoanData.id == bill.id)
        .scalar()
    )

    from rush.card import create_user_product
    from rush.card.utils import (
        create_loan,
        create_user_product_mapping,
    )

    user_product = create_user_product_mapping(
        session=session, user_id=user_id, product_type="transaction_loan"
    )
    create_loan(session=session, user_product=user_product, lender_id=user_loan.lender_id)

    # Loan for transaction amount
    transaction_loan = create_user_product(
        session=session,
        user_id=user_id,
        card_type="transaction_loan",
        interest_free_period_in_days=15,
        tenure=tenure,
        amount=transaction.amount,
        product_order_date=post_date,
        user_product_id=user_product.id,
        downpayment_percent=Decimal("0"),
        credit_book=f"{transaction.loan_id}/bill/unbilled/a",
        parent_loan_id=user_loan.id,
        rc_rate_of_interest_monthly=interest_rate,
        can_close_early=False,
    )

    transaction_loan_bill: LoanData = (
        session.query(LoanData).filter(LoanData.loan_id == transaction_loan.id).scalar()
    )
    transaction.loan_id = transaction_loan_bill.id

    session.flush()

    return {
        "result": "success",
        "message": "Transaction has been successfully converted to emi",
        "data": transaction_loan,
    }
