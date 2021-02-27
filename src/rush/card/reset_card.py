from decimal import Decimal
from typing import Type

from dateutil.relativedelta import relativedelta
from pendulum import Date
from sqlalchemy.orm.session import Session

from rush.card.base_card import B
from rush.card.term_loan import (
    TermLoan,
    TermLoanBill,
)
from rush.ledger_events import (
    add_max_amount_event,
    loan_disbursement_event,
)
from rush.ledger_utils import create_ledger_entry_from_str
from rush.min_payment import add_min_to_all_bills
from rush.models import (
    Fee,
    LedgerTriggerEvent,
    Loan,
    LoanData,
)


class ResetBill(TermLoanBill):
    round_emi_to = "one"


class ResetCard(TermLoan):
    bill_class: Type[B] = ResetBill

    __mapper_args__ = {"polymorphic_identity": "term_loan_reset"}

    @staticmethod
    def calculate_first_emi_date(product_order_date: Date) -> Date:
        return product_order_date.add(months=1)

    @classmethod
    def create(cls, session: Session, **kwargs) -> Loan:
        user_product_id = kwargs["user_product_id"]
        loan = session.query(cls).filter(cls.user_product_id == user_product_id).one()
        loan.prepare(session=session)

        loan.rc_rate_of_interest_monthly = kwargs.get("interest_rate")
        loan.lender_rate_of_interest_annual = kwargs.get("lender_rate_of_interest_annual", Decimal(18))
        loan.amortization_date = kwargs.get("product_order_date")
        loan.min_tenure = kwargs.get("min_tenure")
        loan.min_multiplier = kwargs.get("min_multiplier")
        loan.tenure_in_months = kwargs.get("tenure")
        loan.interest_type = "flat"
        loan.downpayment_percent = Decimal(0)
        loan.can_close_early = False

        bill_start_date, bill_close_date = cls.bill_class.calculate_bill_start_and_close_date(
            first_bill_date=cls.calculate_first_emi_date(product_order_date=loan.amortization_date),
            tenure=kwargs["tenure"],
        )

        loan_data = LoanData(
            user_id=loan.user_id,
            loan_id=loan.loan_id,
            bill_start_date=bill_start_date,
            bill_close_date=bill_close_date,
            bill_due_date=bill_start_date + relativedelta(days=kwargs["interest_free_period_in_days"]),
            is_generated=True,
            bill_tenure=loan.tenure_in_months,
            principal=kwargs["amount"],
        )

        session.add(loan_data)
        session.flush()
        kwargs["loan_id"] = loan.loan_id
        kwargs["loan_data"] = loan_data
        event = loan.disburse(**kwargs)
        create_ledger_entry_from_str(
            session=session,
            event_id=event.id,
            debit_book_str=f"{loan.id}/card/locked_limit/a",
            credit_book_str=f"{loan.id}/card/locked_limit/l",
            amount=kwargs["amount"],
        )
        # unlock some limit if required.

        # create emis for term loan.
        from rush.loan_schedule.loan_schedule import create_bill_schedule

        bill = loan.convert_to_bill_class(loan_data)

        create_bill_schedule(
            session=session,
            user_loan=loan,
            bill=bill,
        )

        # assert joining fees.
        joining_fees = (
            session.query(Fee.identifier_id)
            .filter(
                Fee.identifier_id == loan.loan_id,
                Fee.identifier == "loan",
                Fee.name == "reset_joining_fees",
                Fee.fee_status == "PAID",
            )
            .scalar()
        )
        assert joining_fees is not None

        add_max_amount_event(session=session, bill=bill, event=event, amount=kwargs["amount"])

        add_min_to_all_bills(session=session, post_date=kwargs["product_order_date"], user_loan=loan)
        return loan
