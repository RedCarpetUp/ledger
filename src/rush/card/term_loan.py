from decimal import Decimal
from typing import (
    Dict,
    Tuple,
    Type,
)

from dateutil.relativedelta import relativedelta
from pendulum import (
    Date,
    date,
)
from sqlalchemy import func
from sqlalchemy.orm import Session

from rush.card.base_card import (
    B,
    BaseBill,
    BaseLoan,
)
from rush.ledger_events import loan_disbursement_event
from rush.models import (
    LedgerTriggerEvent,
    Loan,
    LoanData,
    LoanSchedule,
    PaymentRequestsData,
)


class TermLoanBill(BaseBill):
    round_emi_to = "ten"

    @staticmethod
    def calculate_bill_start_and_close_date(first_bill_date: Date, tenure: int) -> Tuple[Date]:
        bill_start_date = first_bill_date
        # not sure about bill close date.
        bill_close_date = bill_start_date.add(months=tenure - 1)

        return bill_start_date, bill_close_date

    def get_relative_delta_for_emi(self, emi_number: int, amortization_date: Date) -> Dict[str, int]:
        """
        Sample Tenure Loan:
            +-----------+-------------+----------------------+---------------------+--------------+---------------------+--------------+
            | loan_id   | loan_type   | product_order_date   | agreement_date      | emi_number   | due_date            | due_amount   |
            |-----------+-------------+----------------------+---------------------+--------------+---------------------+--------------|
            | 8826      | Tenure Loan | 2015-10-09 00:00:00  | 2015-10-09 00:00:00 | 1            | 2015-10-09 00:00:00 | 1420.0       |
            | 8826      | Tenure Loan | 2015-10-09 00:00:00  | 2015-10-09 00:00:00 | 2            | 2015-11-09 00:00:00 | 610.0        |
            | 8826      | Tenure Loan | 2015-10-09 00:00:00  | 2015-10-09 00:00:00 | 3            | 2015-12-09 00:00:00 | 610.0        |
            +-----------+-------------+----------------------+---------------------+--------------+---------------------+--------------+

        """
        if emi_number == 1:
            return {"months": 0, "days": 0}
        return {"months": 1, "days": 0}

    def sum_of_atm_transactions(self):
        return Decimal("0")

    def get_down_payment(self, include_first_emi: bool = True) -> Decimal:
        return super().get_down_payment(include_first_emi)

    def get_interest_to_accrue(self, for_date: date):
        # Get the next emi's interest.
        interest_to_accrue = (
            self.session.query(LoanSchedule.interest_due)
            .filter(
                LoanSchedule.bill_id == self.table.id,
                LoanSchedule.due_date > for_date,
            )
            .order_by(LoanSchedule.due_date)
            .limit(1)
            .scalar()
        )
        return interest_to_accrue


def get_down_payment_for_loan(loan: BaseLoan) -> Decimal:
    session = loan.session
    total_downpayment = (
        session.query(func.sum(LedgerTriggerEvent.amount))
        .filter(
            LedgerTriggerEvent.name == "payment_received",
            LedgerTriggerEvent.loan_id == loan.id,
            LedgerTriggerEvent.extra_details["payment_request_id"].astext
            == PaymentRequestsData.payment_request_id,
            PaymentRequestsData.type == "downpayment",
            PaymentRequestsData.row_status == "active",
        )
        .scalar()
    )
    return total_downpayment


class TermLoan(BaseLoan):
    bill_class: Type[B] = TermLoanBill
    session: Session = None

    __mapper_args__ = {"polymorphic_identity": "term_loan"}

    @staticmethod
    def calculate_first_emi_date(product_order_date: Date) -> Date:
        return product_order_date

    @classmethod
    def create(cls, session: Session, **kwargs) -> Loan:
        user_product_id = kwargs["user_product_id"]
        loan = session.query(cls).filter(cls.user_product_id == user_product_id).one()
        loan.prepare(session=session)

        loan.rc_rate_of_interest_monthly = kwargs.get("rc_rate_of_interest_monthly", Decimal(3))
        loan.lender_rate_of_interest_annual = kwargs.get("lender_rate_of_interest_annual", Decimal(18))
        loan.amortization_date = kwargs.get("product_order_date")
        loan.min_tenure = kwargs.get("min_tenure")
        loan.min_multiplier = kwargs.get("min_multiplier")
        loan.interest_type = kwargs.get("interest_type", "flat")
        loan.tenure_in_months = kwargs.get("tenure")
        # Don't want to overwrite default value in case of None.
        if kwargs.get("interest_free_period_in_days"):
            loan.interest_free_period_in_days = kwargs.get("interest_free_period_in_days")
        loan.downpayment_percent = kwargs["downpayment_percent"]

        kwargs["loan_id"] = loan.id

        bill_start_date, bill_close_date = cls.bill_class.calculate_bill_start_and_close_date(
            first_bill_date=cls.calculate_first_emi_date(product_order_date=loan.amortization_date),
            tenure=kwargs["tenure"],
        )

        loan_data = LoanData(
            user_id=loan.user_id,
            loan_id=loan.id,
            bill_start_date=bill_start_date,
            bill_close_date=bill_close_date,
            bill_due_date=bill_start_date + relativedelta(days=loan.interest_free_period_in_days),
            is_generated=True,
            bill_tenure=loan.tenure_in_months,
            principal=kwargs["amount"],
        )
        session.add(loan_data)
        session.flush()

        bill = loan.convert_to_bill_class(loan_data)

        down_payment_paid = get_down_payment_for_loan(loan)
        down_payment_due = bill.get_down_payment()
        assert down_payment_paid == down_payment_due

        event = LedgerTriggerEvent(
            name="disbursal",
            loan_id=loan.id,
            post_date=kwargs["product_order_date"],
            amount=kwargs["amount"],
        )

        session.add(event)
        session.flush()

        loan_disbursement_event(
            session=session,
            loan=loan,
            event=event,
            bill_id=loan_data.id,
            downpayment_amount=down_payment_paid,
        )

        # create emis for term loan.
        from rush.loan_schedule.loan_schedule import create_bill_schedule

        create_bill_schedule(
            session=session,
            user_loan=loan,
            bill=bill,
        )

        return loan
