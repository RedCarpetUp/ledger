from decimal import Decimal
from typing import (
    Dict,
    Type,
)

from dateutil.relativedelta import relativedelta
from pendulum import Date
from sqlalchemy.orm import Session

from rush.card.base_card import (
    B,
    BaseBill,
    BaseLoan,
)
from rush.card.utils import create_user_product_mapping
from rush.ledger_events import loan_disbursement_event
from rush.models import (
    LedgerTriggerEvent,
    Loan,
    LoanData,
)
from rush.utils import div


class TermLoanProBill(BaseBill):
    round_emi_to_nearest: Decimal = Decimal("10")

    def sum_of_atm_transactions(self) -> Decimal:
        return Decimal(0)

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


class TermLoanPro(BaseLoan):
    bill_class: Type[B] = TermLoanProBill
    session: Session = None
    downpayment_perc: Decimal = Decimal("20")

    __mapper_args__ = {"polymorphic_identity": "term_loan_pro"}

    @staticmethod
    def calculate_amortization_date(product_order_date: Date) -> Date:
        return product_order_date.add(months=1)

    @classmethod
    def create(cls, session: Session, **kwargs) -> Loan:
        user_product_id = kwargs.get("user_product_id")
        if not user_product_id:
            user_product_id = create_user_product_mapping(
                session=session, user_id=kwargs["user_id"], product_type=kwargs["card_type"]
            ).id

        loan = cls(
            session=session,
            user_id=kwargs["user_id"],
            user_product_id=user_product_id,
            lender_id=kwargs["lender_id"],
            rc_rate_of_interest_monthly=Decimal(3),
            lender_rate_of_interest_annual=Decimal(18),
            amortization_date=kwargs.get(
                "loan_creation_date", cls.calculate_amortization_date(kwargs["product_order_date"])
            ),  # TODO: change this later.
        )
        session.add(loan)
        session.flush()

        kwargs["loan_id"] = loan.id

        bill_start_date = loan.amortization_date
        # not sure about bill close date.
        bill_close_date = bill_start_date.add(months=kwargs["tenure"] - 1).add(
            days=kwargs["interest_free_period_in_days"]
        )

        loan_data = LoanData(
            user_id=kwargs["user_id"],
            loan_id=kwargs["loan_id"],
            bill_start_date=bill_start_date,
            bill_close_date=bill_close_date,
            bill_due_date=bill_start_date + relativedelta(days=kwargs["interest_free_period_in_days"]),
            is_generated=True,
            bill_tenure=kwargs["tenure"],
            principal=kwargs["amount"],
            principal_instalment=div(kwargs["amount"], kwargs["tenure"]),
        )
        session.add(loan_data)
        session.flush()

        event = LedgerTriggerEvent(
            performed_by=kwargs["user_id"],
            name="termloan_disbursal_event",
            loan_id=kwargs["loan_id"],
            post_date=kwargs["product_order_date"],
            amount=kwargs["amount"],
        )

        session.add(event)
        session.flush()

        loan_disbursement_event(session=session, loan=loan, event=event, bill_id=loan_data.id)

        # create emis for term loan.
        from rush.create_emi import create_emis_for_bill

        bill = cls.bill_class(session=session, loan_data=loan_data)
        loan_data.interest_to_charge = bill.get_interest_to_charge(
            rate_of_interest=loan.rc_rate_of_interest_monthly
        )

        create_emis_for_bill(
            session=session,
            user_loan=loan,
            bill=bill,
            # interest=bill.get_interest_to_charge(rate_of_interest=loan.rc_rate_of_interest_monthly),
        )

        return loan
