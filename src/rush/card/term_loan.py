from decimal import Decimal
from typing import (
    Dict,
    Tuple,
    Type,
)

from dateutil.relativedelta import relativedelta
from pendulum import (
    Date,
    DateTime,
    date,
)
from sqlalchemy import (
    and_,
    func,
)
from sqlalchemy.orm import Session

from rush.card.base_card import (
    B,
    BaseBill,
    BaseLoan,
)
from rush.ledger_events import add_max_amount_event
from rush.models import (
    Fee,
    Loan,
    LoanData,
    LoanSchedule,
    PaymentMapping,
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

    def is_bill_closed(self, to_date: DateTime = None) -> bool:
        # Check if loan closed or not. Because for term loan there is only one bill.
        return self.user_loan.loan_status == "COMPLETED"


def is_down_payment_paid(loan: BaseLoan) -> bool:
    session = loan.session
    payment_amount = (
        session.query(func.sum(PaymentMapping.amount_settled))
        .join(
            LoanSchedule,
            and_(
                LoanSchedule.loan_id == loan.id,
                LoanSchedule.emi_number == 1,
                LoanSchedule.bill_id.is_(None),
            ),
        )
        .filter(PaymentMapping.emi_id == LoanSchedule.id, PaymentMapping.row_status == "active")
        .scalar()
    ) or 0

    loan_data = (
        session.query(LoanData)
        .filter(LoanData.loan_id == loan.id, LoanData.user_id == loan.user_id)
        .one()
    )

    bill = loan.convert_to_bill_class(loan_data)
    down_payment_due = bill.get_down_payment()
    return payment_amount >= down_payment_due


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
        loan.can_close_early = kwargs.get("can_close_early") or False
        loan.tenure_in_months = kwargs.get("tenure")
        loan.loan_status = "Not Started"
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
        kwargs["loan_data"] = loan_data

        bill = loan.convert_to_bill_class(loan_data)

        event = loan.disburse(**kwargs)

        # create emis for term loan.
        from rush.loan_schedule.loan_schedule import create_bill_schedule

        create_bill_schedule(
            session=session,
            user_loan=loan,
            bill=bill,
        )

        add_max_amount_event(session=session, bill=bill, event=event, amount=kwargs["amount"])

        return loan

    def get_emi_to_accrue_interest(self, post_date: Date):
        loan_schedule = (
            self.session.query(LoanSchedule)
            .filter(
                LoanSchedule.loan_id == self.loan_id,
                LoanSchedule.bill_id.is_(None),
                LoanSchedule.due_date == post_date,
            )
            .scalar()
        )
        return loan_schedule

    def can_close_loan(self, as_of_event_id: int) -> bool:
        if (
            not self.get_all_bills()
        ):  # This func gets called even before schedule is created. So to avoid that.
            return False
        from rush.accrue_financial_charges import get_interest_left_to_accrue

        # For term loans, we have to receive entire schedule amount so look there instead of max account.
        max_remaining = self.get_remaining_max(event_id=as_of_event_id, include_child_loans=False)
        interest_left_to_accrue = get_interest_left_to_accrue(self.session, self)
        total_remaining_amount = max_remaining + interest_left_to_accrue
        return total_remaining_amount <= 0
